// Package recovery coordinates idempotent operations across the catalogue and
// snapshot repository without depending on filesystem adapters.
package recovery

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type KeyLocks struct {
	mu   sync.Mutex
	refs map[string]*keyLockRef
}

type keyLockRef struct {
	mu   sync.Mutex
	refs int
}

func NewKeyLocks() *KeyLocks {
	return &KeyLocks{refs: make(map[string]*keyLockRef)}
}

// Lock acquires each distinct stable key in lexical order. The returned
// function releases in reverse order and removes unused lock entries.
func (k *KeyLocks) Lock(names []string) func() {
	if k == nil {
		return func() {}
	}
	ordered := append([]string(nil), names...)
	sort.Strings(ordered)
	ordered = compactStrings(ordered)

	refs := make([]*keyLockRef, len(ordered))
	k.mu.Lock()
	for i, name := range ordered {
		ref := k.refs[name]
		if ref == nil {
			ref = &keyLockRef{}
			k.refs[name] = ref
		}
		ref.refs++
		refs[i] = ref
	}
	k.mu.Unlock()
	for _, ref := range refs {
		ref.mu.Lock()
	}
	return func() {
		for i := len(refs) - 1; i >= 0; i-- {
			refs[i].mu.Unlock()
		}
		k.mu.Lock()
		for i, name := range ordered {
			refs[i].refs--
			if refs[i].refs == 0 {
				delete(k.refs, name)
			}
		}
		k.mu.Unlock()
	}
}

func compactStrings(values []string) []string {
	if len(values) == 0 {
		return values
	}
	out := values[:1]
	for _, value := range values[1:] {
		if value != out[len(out)-1] {
			out = append(out, value)
		}
	}
	return out
}

type Coordinator struct {
	catalogue  ports.Catalogue
	repository ports.SnapshotRepository
	locks      *KeyLocks
	// mutationMu serializes every catalogue or checkpoint mutation with each
	// per-incarnation GC step. It is the only fence: GC runs while the daemon
	// serves, so a step holds it for one incarnation at a time.
	mutationMu sync.Mutex
	random     io.Reader
}

func NewCoordinator(catalogue ports.Catalogue, repository ports.SnapshotRepository, random io.Reader) *Coordinator {
	return &Coordinator{catalogue: catalogue, repository: repository, locks: NewKeyLocks(), random: random}
}

// Create commits fresh catalogue metadata before the caller creates or exposes
// any runtime session.
func (c *Coordinator) Create(ctx context.Context, record domain.CatalogueRecord) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.locks == nil || c.random == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete create dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{record.Name})
	defer unlock()
	if _, exists, err := c.catalogue.Record(record.Name); err != nil {
		return domain.CatalogueRecord{}, err
	} else if exists {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q already exists", record.Name)
	}
	if record.IncarnationID == (domain.IncarnationID{}) {
		id, err := domain.NewIncarnationID(c.random)
		if err != nil {
			return domain.CatalogueRecord{}, fmt.Errorf("recovery: generate incarnation: %w", err)
		}
		record.IncarnationID = id
	}
	record.Committed = nil
	record.DegradedReason = ""
	if err := record.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid fresh record: %w", err)
	}
	if err := c.catalogue.Create(record); err != nil {
		return domain.CatalogueRecord{}, err
	}
	return record, nil
}

// Rename atomically moves one catalogue key while retaining the immutable
// incarnation and all checkpoint references.
func (c *Coordinator) Rename(ctx context.Context, oldName, newName string) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.locks == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete rename dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{oldName, newName})
	defer unlock()
	record, ok, err := c.catalogue.Record(oldName)
	if err != nil {
		return domain.CatalogueRecord{}, err
	}
	if !ok {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q not found", oldName)
	}
	if existing, exists, err := c.catalogue.Record(newName); err != nil {
		return domain.CatalogueRecord{}, err
	} else if exists && (newName != oldName || existing.IncarnationID != record.IncarnationID) {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q already exists", newName)
	}
	record.Name = newName
	if err := record.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid rename: %w", err)
	}
	if oldName != newName {
		if err := c.catalogue.Rename(oldName, record); err != nil {
			return domain.CatalogueRecord{}, err
		}
	}
	return record, nil
}

// Delete removes the current catalogue record before deleting its incarnation.
// Callers that captured lifecycle identity must use DeleteExact.
func (c *Coordinator) Delete(ctx context.Context, name string) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return errors.New("recovery: incomplete delete dependencies")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	if err := c.catalogue.Delete(record.Name); err != nil {
		return err
	}
	return c.repository.DeleteIncarnation(ctx, record.IncarnationID)
}

// DeleteExact removes a catalogue record only while it still identifies the
// lifecycle captured by the caller. A stale teardown cannot delete a same-name
// replacement created after that capture.
func (c *Coordinator) DeleteExact(ctx context.Context, name string, incarnation domain.IncarnationID, createdAt int64) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return errors.New("recovery: incomplete exact delete dependencies")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok || record.IncarnationID != incarnation || record.CreatedAt != createdAt {
		return nil
	}
	if err := c.catalogue.Delete(name); err != nil {
		return err
	}
	return c.repository.DeleteIncarnation(ctx, incarnation)
}

// errGarbageCollectionCatalogue stops a pass: without a readable catalogue no
// incarnation can be classified.
var errGarbageCollectionCatalogue = errors.New("recovery: read catalogue for garbage collection")

// CollectGarbage applies retention to every repository incarnation. Each
// incarnation is collected in its own fenced step that re-reads the catalogue
// under mutationMu, so its keep decision is never stale relative to an
// operation that creates an incarnation or commits a generation, and a
// mutation waits for at most one incarnation instead of a whole pass. Failures
// are joined so one bad incarnation does not stop the rest.
func (c *Coordinator) CollectGarbage(ctx context.Context) (int, error) {
	if c == nil || c.catalogue == nil || c.repository == nil {
		return 0, errors.New("recovery: incomplete garbage collection dependencies")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	ids, err := c.repository.SnapshotIncarnations(ctx)
	if err != nil {
		return 0, err
	}
	var collected error
	for _, id := range ids {
		if err := ctx.Err(); err != nil {
			return len(ids), errors.Join(collected, err)
		}
		if err := c.collectIncarnation(ctx, id); errors.Is(err, errGarbageCollectionCatalogue) {
			return len(ids), errors.Join(collected, err)
		} else if err != nil {
			collected = errors.Join(collected, err)
		}
	}
	return len(ids), collected
}

func (c *Coordinator) collectIncarnation(ctx context.Context, id domain.IncarnationID) error {
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	// A catalogue read failure never produces a keep decision, so destructive
	// collection cannot run without known-good catalogue state.
	records, err := c.catalogue.Records()
	if err != nil {
		return fmt.Errorf("%w: %w", errGarbageCollectionCatalogue, err)
	}
	var keep *domain.CheckpointRef
	for _, record := range records {
		if record.IncarnationID != id {
			continue
		}
		keep = &domain.CheckpointRef{}
		if record.Committed != nil {
			*keep = *record.Committed
		}
		break
	}
	return c.repository.CollectIncarnationGarbage(ctx, id, keep)
}
