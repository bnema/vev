package recovery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"slices"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// RecoveryAction identifies an explicit operator choice for degraded state.
type RecoveryAction uint8

const (
	RecoveryRetry RecoveryAction = iota + 1
	RecoveryRestoreFallback
	RecoveryExport
	RecoveryDiscard
)

var (
	ErrRecoveryRecordNotFound = errors.New("recovery: session not found")
	ErrSessionNotDegraded     = errors.New("recovery: session is not degraded")
	ErrDiscardConflict        = errors.New("recovery: discard intent conflicts with catalogue")
	ErrDiscardIntentInvalid   = errors.New("recovery: discard intent is unusable")
)

// Retry directly validates the committed checkpoint. A failed validation is
// read-only; only a successful validation is allowed to clear degraded state.
func (c *Coordinator) Retry(ctx context.Context, name string) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return errors.New("recovery: incomplete retry dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRecoveryRecordNotFound
	}
	if record.RecoveryState != domain.RecoveryDegraded || record.Committed == nil {
		return ErrSessionNotDegraded
	}
	if err := c.validateCheckpoint(ctx, record, *record.Committed); err != nil {
		return err
	}
	next := record
	next.RecoveryState = domain.RecoveryHealthy
	next.DegradedReason = ""
	if err := next.Validate(); err != nil {
		return fmt.Errorf("recovery: invalid retry transition: %w", err)
	}
	return c.catalogue.Replace(name, next)
}

// RestoreFallback promotes only a fallback already displayed from the
// catalogue. PromoteFallback performs direct validation before replacement.
func (c *Coordinator) RestoreFallback(ctx context.Context, name string, ref domain.CheckpointRef) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil {
		return errors.New("recovery: incomplete fallback dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRecoveryRecordNotFound
	}
	if record.RecoveryState != domain.RecoveryDegraded {
		return ErrSessionNotDegraded
	}
	outcome, err := c.promoteFallbackLocked(ctx, name, ref)
	if err != nil {
		return err
	}
	return outcome.HEADRepairError
}

// Export writes a deterministic, self-contained copy of the committed
// generation without changing catalogue or repository state.
func (c *Coordinator) Export(ctx context.Context, name string, w io.Writer) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil || w == nil {
		return errors.New("recovery: incomplete export dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok {
		return ErrRecoveryRecordNotFound
	}
	if record.RecoveryState != domain.RecoveryDegraded || record.Committed == nil {
		return ErrSessionNotDegraded
	}
	generation, err := c.repository.LoadCheckpoint(ctx, record.IncarnationID, record.Name, *record.Committed)
	if err != nil {
		return err
	}
	if err := validateLoadedGeneration(record, *record.Committed, generation); err != nil {
		return err
	}
	encoded, err := encodeExport(generation)
	if err != nil {
		return err
	}
	_, err = w.Write(encoded)
	return err
}

func validateLoadedGeneration(record domain.CatalogueRecord, ref domain.CheckpointRef, generation ports.SnapshotGeneration) error {
	if generation.IncarnationID != record.IncarnationID || generation.Name != record.Name ||
		generation.Generation != ref.Generation || sha256.Sum256(generation.Manifest) != ref.ManifestDigest {
		return ErrCheckpointConflict
	}
	return nil
}

func encodeExport(generation ports.SnapshotGeneration) ([]byte, error) {
	var out bytes.Buffer
	out.WriteString("VEVX")
	_ = binary.Write(&out, binary.BigEndian, uint16(1))
	out.Write(generation.IncarnationID[:])
	_ = binary.Write(&out, binary.BigEndian, generation.Generation)
	if err := writeExportBytes(&out, []byte(generation.Name)); err != nil {
		return nil, err
	}
	if err := writeExportBytes(&out, generation.Manifest); err != nil {
		return nil, err
	}
	digests := make([]ports.SnapshotDigest, 0, len(generation.Objects))
	for digest := range generation.Objects {
		digests = append(digests, digest)
	}
	slices.SortFunc(digests, func(a, b ports.SnapshotDigest) int { return bytes.Compare(a[:], b[:]) })
	if uint64(len(digests)) > uint64(^uint32(0)) {
		return nil, errors.New("recovery: too many export objects")
	}
	_ = binary.Write(&out, binary.BigEndian, uint32(len(digests)))
	for _, digest := range digests {
		out.Write(digest[:])
		if err := writeExportBytes(&out, generation.Objects[digest]); err != nil {
			return nil, err
		}
	}
	return out.Bytes(), nil
}

func writeExportBytes(w io.Writer, data []byte) error {
	if uint64(len(data)) > uint64(^uint32(0)) {
		return errors.New("recovery: export field too large")
	}
	if err := binary.Write(w, binary.BigEndian, uint32(len(data))); err != nil {
		return err
	}
	_, err := w.Write(data)
	return err
}

// Discard commits to a new incarnation by saving the complete intent before
// any quarantine or catalogue mutation. There is no rollback after that point.
func (c *Coordinator) Discard(ctx context.Context, name, reason string) (domain.CatalogueRecord, error) {
	if c == nil || c.catalogue == nil || c.repository == nil || c.journal == nil || c.locks == nil || c.random == nil {
		return domain.CatalogueRecord{}, errors.New("recovery: incomplete discard dependencies")
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()
	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return domain.CatalogueRecord{}, err
	}
	if !ok {
		return domain.CatalogueRecord{}, ErrRecoveryRecordNotFound
	}
	if record.RecoveryState != domain.RecoveryDegraded {
		return domain.CatalogueRecord{}, ErrSessionNotDegraded
	}
	if reason == "" {
		return domain.CatalogueRecord{}, errors.New("recovery: discard reason is required")
	}
	newID, err := domain.NewIncarnationID(c.random)
	if err != nil {
		return domain.CatalogueRecord{}, fmt.Errorf("recovery: generate replacement incarnation: %w", err)
	}
	intent := domain.DiscardIntent{
		OldRecord: record, OldIncarnation: record.IncarnationID, NewIncarnation: newID,
		SessionName: record.Name, Reason: reason,
	}
	if err := c.journal.SaveDiscard(ctx, intent); err != nil {
		return domain.CatalogueRecord{}, err
	}
	if err := c.recoverDiscardLocked(ctx, intent); err != nil {
		return domain.CatalogueRecord{}, err
	}
	return freshReplacement(intent), nil
}

func (c *Coordinator) recoverDiscard(ctx context.Context, intent domain.DiscardIntent) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.journal == nil || c.locks == nil {
		return errors.New("recovery: incomplete discard recovery dependencies")
	}
	unlock := c.locks.Lock([]string{intent.SessionName})
	defer unlock()
	return c.recoverDiscardLocked(ctx, intent)
}

func (c *Coordinator) recoverDiscardLocked(ctx context.Context, intent domain.DiscardIntent) error {
	if err := validateDiscardIntent(intent); err != nil {
		return err
	}
	current, ok, err := c.catalogue.Record(intent.SessionName)
	if err != nil {
		return err
	}
	if !ok {
		// Catalogue deletion is terminal for this session identity. There is no
		// live record to replace or quarantine, so retaining the intent would
		// manufacture a permanent startup conflict.
		return c.journal.DeleteDiscard(ctx, intent.OldIncarnation)
	}
	next := freshReplacement(intent)
	if err := next.Validate(); err != nil {
		return fmt.Errorf("%w: invalid discard replacement: %w", ErrDiscardIntentInvalid, err)
	}
	switch current.IncarnationID {
	case intent.OldIncarnation:
		if !current.Equal(intent.OldRecord) {
			return fmt.Errorf("%w: old record changed", ErrDiscardConflict)
		}
	case intent.NewIncarnation:
		// Step 4 is already durable. The replacement is live from this point on,
		// so it may legitimately have advanced to a committed checkpoint before a
		// later step failed. Only its identity may be required here: demanding
		// byte equality with the pristine replacement would fail closed forever
		// on a session that is in fact healthy, and destroy committed state if it
		// were overwritten. Steps 2-3 and 5 below are idempotent.
		if current.Name != intent.SessionName {
			return fmt.Errorf("%w: replacement session identity changed", ErrDiscardConflict)
		}
	default:
		return fmt.Errorf("%w: unexpected incarnation", ErrDiscardConflict)
	}

	if err := c.repository.SaveQuarantineDescriptor(ctx, quarantineDescriptor(intent)); err != nil {
		return err
	}
	if err := c.repository.QuarantineIncarnation(ctx, intent.OldIncarnation); err != nil {
		return err
	}
	if current.IncarnationID == intent.OldIncarnation {
		if err := c.catalogue.Replace(intent.SessionName, next); err != nil {
			return err
		}
	}
	return c.journal.DeleteDiscard(ctx, intent.OldIncarnation)
}

func validateDiscardIntent(intent domain.DiscardIntent) error {
	if err := intent.OldRecord.Validate(); err != nil {
		return fmt.Errorf("%w: invalid old record: %w", ErrDiscardIntentInvalid, err)
	}
	if intent.SessionName != intent.OldRecord.Name || intent.OldIncarnation != intent.OldRecord.IncarnationID ||
		intent.NewIncarnation == (domain.IncarnationID{}) || intent.NewIncarnation == intent.OldIncarnation || intent.Reason == "" {
		return fmt.Errorf("%w: inconsistent intent fields", ErrDiscardIntentInvalid)
	}
	return nil
}

func quarantineDescriptor(intent domain.DiscardIntent) domain.QuarantineDescriptor {
	return domain.QuarantineDescriptor{
		OldRecord: intent.OldRecord, OldIncarnation: intent.OldIncarnation,
		ReplacementIncarnation: intent.NewIncarnation, SessionName: intent.SessionName, Reason: intent.Reason,
	}
}

func freshReplacement(intent domain.DiscardIntent) domain.CatalogueRecord {
	next := intent.OldRecord
	next.IncarnationID = intent.NewIncarnation
	next.RecoveryState = domain.RecoveryFresh
	next.Committed = nil
	next.Fallbacks = [2]*domain.CheckpointRef{}
	next.DegradedReason = ""
	return next
}
