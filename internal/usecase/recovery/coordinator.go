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
	// mutationMu serializes the startup GC snapshot and collection with every
	// catalogue or checkpoint mutation. Startup normally provides this fence by
	// running before socket publication; retaining it here also makes the
	// coordinator safe when used directly by a composition root.
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

// Delete removes the catalogue record before deleting its incarnation. A crash
// between those steps leaves an orphan that startup garbage collection removes.
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

// CollectGarbage takes a catalogue snapshot and applies retention while all
// catalogue mutations and checkpoint publications are fenced. Its keep map is
// therefore never stale relative to an operation that creates an incarnation
// or commits a new generation.
func (c *Coordinator) CollectGarbage(ctx context.Context) (int, error) {
	if c == nil || c.catalogue == nil || c.repository == nil {
		return 0, errors.New("recovery: incomplete garbage collection dependencies")
	}
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()

	records, err := c.catalogue.Records()
	if err != nil {
		return 0, err
	}
	keep := make(map[domain.IncarnationID]domain.CheckpointRef, len(records))
	for _, record := range records {
		if record.Committed != nil {
			keep[record.IncarnationID] = *record.Committed
			continue
		}
		keep[record.IncarnationID] = domain.CheckpointRef{}
	}
	if err := c.repository.CollectGarbage(ctx, keep); err != nil {
		return len(keep), err
	}
	return len(keep), nil
}
