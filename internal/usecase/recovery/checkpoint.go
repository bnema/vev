package recovery

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

var (
	ErrCheckpointRecordNotFound = errors.New("recovery: checkpoint catalogue record not found")
	ErrCheckpointConflict       = errors.New("recovery: checkpoint publication conflicts with catalogue")
)

// PublishCheckpoint publishes repository data before atomically making its
// reference authoritative in the catalogue. A catalogue failure deliberately
// leaves the durable publication as a same-incarnation forward orphan.
func (c *Coordinator) PublishCheckpoint(ctx context.Context, name string, publication ports.SnapshotPublication) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete checkpoint coordinator dependencies")
	}
	if err := ctx.Err(); err != nil {
		return domain.CatalogueRecord{}, err
	}
	c.mutationMu.Lock()
	defer c.mutationMu.Unlock()
	unlock := c.locks.Lock([]string{name})
	defer unlock()

	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return domain.CatalogueRecord{}, err
	}
	if !ok {
		return domain.CatalogueRecord{}, ErrCheckpointRecordNotFound
	}
	if publication.Name != name || publication.Name != record.Name || publication.IncarnationID != record.IncarnationID ||
		publication.Generation == 0 || len(publication.Manifest) == 0 ||
		!equalCheckpointRefs(publication.ParentCheckpoint, record.Committed) {
		return domain.CatalogueRecord{}, ErrCheckpointConflict
	}
	if err := c.repository.Publish(ctx, publication); err != nil {
		return domain.CatalogueRecord{}, err
	}

	ref := domain.CheckpointRef{Generation: publication.Generation, ManifestDigest: sha256.Sum256(publication.Manifest)}
	next := record
	next.Committed = copyCheckpointRef(&ref)
	next.DegradedReason = ""
	if err := next.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid checkpoint transition: %w", err)
	}
	if err := c.catalogue.Replace(name, next); err != nil {
		return domain.CatalogueRecord{}, err
	}
	return next, nil
}

func equalCheckpointRefs(a, b *domain.CheckpointRef) bool { return a.Equal(b) }

func copyCheckpointRef(ref *domain.CheckpointRef) *domain.CheckpointRef {
	if ref == nil {
		return nil
	}
	cloned := *ref
	return &cloned
}
