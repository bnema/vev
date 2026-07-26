// Package recovery coordinates crash-resumable operations across the catalogue
// and snapshot repository without depending on filesystem adapters.
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

var ErrPendingRecoveryUnsupported = errors.New("pending recovery intent requires transaction recovery")

var recoveryListingBudget = ports.MaintenanceBudget{Entries: 64, Bytes: 64 << 10}

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
	journal    ports.RecoveryJournal
	locks      *KeyLocks
	random     io.Reader
}

func NewCoordinator(catalogue ports.Catalogue, repository ports.SnapshotRepository, journal ports.RecoveryJournal, random io.Reader) *Coordinator {
	return &Coordinator{catalogue: catalogue, repository: repository, journal: journal, locks: NewKeyLocks(), random: random}
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
	unlock := c.locks.Lock([]string{record.Name})
	defer unlock()
	if _, exists := c.catalogue.Record(record.Name); exists {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q already exists", record.Name)
	}
	id, err := domain.NewIncarnationID(c.random)
	if err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: generate incarnation: %w", err)
	}
	record.IncarnationID = id
	record.RecoveryState = domain.RecoveryFresh
	record.Committed = nil
	record.Fallbacks = [2]*domain.CheckpointRef{}
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
	unlock := c.locks.Lock([]string{oldName, newName})
	defer unlock()
	record, ok := c.catalogue.Record(oldName)
	if !ok {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: session %q not found", oldName)
	}
	if existing, exists := c.catalogue.Record(newName); exists && (newName != oldName || existing.IncarnationID != record.IncarnationID) {
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

// Delete executes the durable deletion protocol. Every completed boundary is
// independently sufficient for Recover to roll the operation forward.
func (c *Coordinator) Delete(ctx context.Context, name string) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return errors.New("recovery: incomplete delete dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok := c.catalogue.Record(name)
	if !ok {
		return nil
	}
	return c.deleteLocked(ctx, record)
}

func (c *Coordinator) deleteLocked(ctx context.Context, record domain.CatalogueRecord) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.RecoveryState != domain.RecoveryDeleting {
		record.RecoveryState = domain.RecoveryDeleting
		record.DegradedReason = ""
		if err := record.Validate(); err != nil {
			return fmt.Errorf("recovery: invalid deleting record: %w", err)
		}
		if err := c.catalogue.Replace(record.Name, record); err != nil {
			return err
		}
	}
	tombstone := domain.DeletionTombstone{Name: record.Name, IncarnationID: record.IncarnationID}
	if err := c.repository.WriteDeletionTombstone(ctx, tombstone); err != nil {
		return err
	}
	if err := c.repository.QuarantineDeletionSources(ctx, tombstone, true); err != nil {
		return err
	}
	if err := c.catalogue.Delete(record.Name); err != nil {
		return err
	}
	return c.repository.DeleteDeletionTombstone(ctx, record.IncarnationID)
}

// Recover rolls deleting records and strict deletion tombstones forward. The
// two sources are enumerated independently because catalogue removal precedes
// tombstone removal in the commit protocol.
func (c *Coordinator) Recover(ctx context.Context) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.journal == nil || c.locks == nil {
		return errors.New("recovery: incomplete coordinator dependencies")
	}
	intents, err := c.journal.ListDiscards(ctx)
	if err != nil {
		return err
	}
	for _, candidate := range c.catalogue.Records() {
		if candidate.RecoveryState != domain.RecoveryDeleting {
			continue
		}
		unlock := c.locks.Lock([]string{candidate.Name})
		current, ok := c.catalogue.Record(candidate.Name)
		if ok && current.IncarnationID == candidate.IncarnationID && current.RecoveryState == domain.RecoveryDeleting {
			err = c.deleteLocked(ctx, current)
		}
		unlock()
		if err != nil {
			return err
		}
	}

	cursor := ports.DeletionTombstoneCursor{}
	for {
		page, err := c.repository.ListDeletionTombstones(ctx, cursor, recoveryListingBudget)
		if err != nil {
			return err
		}
		for _, tombstone := range page.Tombstones {
			if err := c.recoverDeletionTombstone(ctx, tombstone); err != nil {
				return err
			}
		}
		if page.Done {
			if len(intents) != 0 {
				return ErrPendingRecoveryUnsupported
			}
			return nil
		}
		if page.Next.After == cursor.After {
			return errors.New("recovery: tombstone listing did not advance")
		}
		cursor = page.Next
	}
}

func (c *Coordinator) recoverDeletionTombstone(ctx context.Context, tombstone domain.DeletionTombstone) error {
	if err := tombstone.Validate(); err != nil {
		return err
	}
	unlock := c.locks.Lock([]string{tombstone.Name})
	defer unlock()
	record, exists := c.catalogue.Record(tombstone.Name)
	includeLegacyName := true
	if exists {
		switch {
		case record.IncarnationID != tombstone.IncarnationID:
			includeLegacyName = false
		case record.RecoveryState != domain.RecoveryDeleting:
			return fmt.Errorf("recovery: deletion tombstone conflicts with non-deleting session %q", tombstone.Name)
		}
	}
	if err := c.repository.QuarantineDeletionSources(ctx, tombstone, includeLegacyName); err != nil {
		return err
	}
	if exists && record.IncarnationID == tombstone.IncarnationID {
		if err := c.catalogue.Delete(tombstone.Name); err != nil {
			return err
		}
	}
	return c.repository.DeleteDeletionTombstone(ctx, tombstone.IncarnationID)
}
