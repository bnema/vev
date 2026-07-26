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
	ErrFallbackNotFound         = errors.New("recovery: checkpoint is not a catalogue fallback")
)

func shiftedCheckpoint(record domain.CatalogueRecord, next domain.CheckpointRef) domain.CatalogueRecord {
	prior := copyCheckpointRef(record.Committed)
	priorFallback := copyCheckpointRef(record.Fallbacks[0])
	nextRecord := record
	nextRecord.RecoveryState = domain.RecoveryHealthy
	nextRecord.Committed = copyCheckpointRef(&next)
	nextRecord.Fallbacks = [2]*domain.CheckpointRef{prior, priorFallback}
	nextRecord.DegradedReason = ""
	return nextRecord
}

// PublishCheckpoint publishes repository data before atomically making its
// reference authoritative in the catalogue. A catalogue failure deliberately
// leaves the durable publication as a same-incarnation forward orphan.
func (c *Coordinator) PublishCheckpoint(ctx context.Context, name string, publication ports.SnapshotPublication) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete checkpoint coordinator dependencies")
	}
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
	next := shiftedCheckpoint(record, ref)
	if err := next.Validate(); err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: invalid checkpoint transition: %w", err)
	}
	if err := c.catalogue.Replace(name, next); err != nil {
		return domain.CatalogueRecord{}, err
	}
	return next, nil
}

// PromoteFallback validates a directly indexed fallback and any alternatives,
// commits the bounded validated set, and repairs HEAD only after the catalogue
// commit is durable.
func (c *Coordinator) PromoteFallback(ctx context.Context, name string, ref domain.CheckpointRef) (ports.FallbackPromotionOutcome, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return ports.FallbackPromotionOutcome{}, errors.New("recovery: incomplete checkpoint coordinator dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	return c.promoteFallbackLocked(ctx, name, ref)
}

func (c *Coordinator) promoteFallbackLocked(ctx context.Context, name string, ref domain.CheckpointRef) (ports.FallbackPromotionOutcome, error) {
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return ports.FallbackPromotionOutcome{}, err
	}
	if !ok {
		return ports.FallbackPromotionOutcome{}, ErrCheckpointRecordNotFound
	}
	found := false
	for _, fallback := range record.Fallbacks {
		if fallback != nil && *fallback == ref {
			found = true
			break
		}
	}
	if !found {
		return ports.FallbackPromotionOutcome{}, ErrFallbackNotFound
	}
	if err := c.validateCheckpoint(ctx, record, ref); err != nil {
		return ports.FallbackPromotionOutcome{}, err
	}

	alternatives := make([]domain.CheckpointRef, 0, 2)
	candidates := make([]*domain.CheckpointRef, 0, 3)
	candidates = append(candidates, record.Committed)
	candidates = append(candidates, record.Fallbacks[:]...)
	for _, candidate := range candidates {
		if candidate == nil || *candidate == ref || candidate.Generation >= ref.Generation || len(alternatives) == 2 {
			continue
		}
		if err := c.validateCheckpoint(ctx, record, *candidate); err == nil {
			alternatives = append(alternatives, *candidate)
		}
	}

	next := record
	next.RecoveryState = domain.RecoveryHealthy
	next.Committed = copyCheckpointRef(&ref)
	next.Fallbacks = [2]*domain.CheckpointRef{}
	next.DegradedReason = ""
	for i := range alternatives {
		next.Fallbacks[i] = copyCheckpointRef(&alternatives[i])
	}
	if err := next.Validate(); err != nil {
		return ports.FallbackPromotionOutcome{}, fmt.Errorf("recovery: invalid fallback promotion: %w", err)
	}
	if err := c.catalogue.Replace(name, next); err != nil {
		return ports.FallbackPromotionOutcome{}, err
	}
	outcome := ports.FallbackPromotionOutcome{Record: next, CatalogueCommitted: true}
	outcome.HEADRepairError = c.repository.RepairHEAD(ctx, record.IncarnationID, ref)
	return outcome, nil
}

func (c *Coordinator) validateCheckpoint(ctx context.Context, record domain.CatalogueRecord, ref domain.CheckpointRef) error {
	generation, err := c.repository.LoadCheckpoint(ctx, record.IncarnationID, record.Name, ref)
	if err != nil {
		return err
	}
	return validateLoadedGeneration(record, ref, generation)
}

func equalCheckpointRefs(a, b *domain.CheckpointRef) bool { return a.Equal(b) }

func copyCheckpointRef(ref *domain.CheckpointRef) *domain.CheckpointRef {
	if ref == nil {
		return nil
	}
	cloned := *ref
	return &cloned
}
