package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
)

// ResetProtocolIncompatible replaces one exact record with a fresh incarnation
// that retains only its name. A protocol boundary never restores metadata or
// checkpoints whose semantics may have changed.
func (c *Coordinator) ResetProtocolIncompatible(ctx context.Context, name string, expectedIncarnation domain.IncarnationID) (domain.CatalogueRecord, bool, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil || c.random == nil {
		return domain.CatalogueRecord{}, false, errors.New("recovery: incomplete protocol-reset dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{name})
	defer unlock()

	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	if !ok || record.IncarnationID != expectedIncarnation {
		return domain.CatalogueRecord{}, false, nil
	}
	next, err := domain.NewIncarnationID(c.random)
	if err != nil {
		return domain.CatalogueRecord{}, false, fmt.Errorf("recovery: generate protocol-reset incarnation: %w", err)
	}
	fresh := domain.CatalogueRecord{Name: record.Name, IncarnationID: next}
	if err := fresh.Validate(); err != nil {
		return domain.CatalogueRecord{}, false, fmt.Errorf("recovery: invalid protocol-reset replacement: %w", err)
	}
	if err := c.catalogue.Replace(record.Name, fresh); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	if err := c.repository.DeleteIncarnation(ctx, record.IncarnationID); err != nil {
		return fresh, true, err
	}
	return fresh, true, nil
}

// ResetIncompatible replaces an exact healthy checkpoint authority with a
// fresh incarnation. Stale observations are harmless no-ops.
func (c *Coordinator) ResetIncompatible(ctx context.Context, name string, expectedIncarnation domain.IncarnationID, expectedCheckpoint domain.CheckpointRef) (domain.CatalogueRecord, bool, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil || c.random == nil {
		return domain.CatalogueRecord{}, false, errors.New("recovery: incomplete incompatible-reset dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{name})
	defer unlock()

	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	if !ok || record.IncarnationID != expectedIncarnation || record.DegradedReason != "" ||
		record.Committed == nil || *record.Committed != expectedCheckpoint {
		return domain.CatalogueRecord{}, false, nil
	}
	return c.replaceIncarnationLocked(ctx, record, true, "reset")
}

// replaceIncarnationLocked commits a fresh catalogue authority before removing
// the old repository incarnation. The caller must hold mutationMu and the
// record name's KeyLock.
func (c *Coordinator) replaceIncarnationLocked(ctx context.Context, record domain.CatalogueRecord, clearTabNames bool, operation string) (domain.CatalogueRecord, bool, error) {
	old := record.IncarnationID
	next, err := domain.NewIncarnationID(c.random)
	if err != nil {
		return domain.CatalogueRecord{}, false, fmt.Errorf("recovery: generate replacement incarnation: %w", err)
	}
	fresh := record
	fresh.IncarnationID = next
	fresh.Committed = nil
	fresh.DegradedReason = ""
	if clearTabNames {
		fresh.TabNames = nil
	}
	if err := fresh.Validate(); err != nil {
		return domain.CatalogueRecord{}, false, fmt.Errorf("recovery: invalid %s replacement: %w", operation, err)
	}
	if err := c.catalogue.Replace(record.Name, fresh); err != nil {
		return domain.CatalogueRecord{}, false, err
	}
	if err := c.repository.DeleteIncarnation(ctx, old); err != nil {
		return fresh, true, err
	}
	return fresh, true, nil
}
