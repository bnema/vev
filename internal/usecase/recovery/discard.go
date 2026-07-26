package recovery

import (
	"context"
	"errors"
	"fmt"

	"github.com/bnema/vev/internal/domain"
)

var (
	ErrRecoveryRecordNotFound = errors.New("recovery: session not found")
	ErrSessionNotDegraded     = errors.New("recovery: session is not degraded")
	ErrDiscardConflict        = errors.New("recovery: discard intent conflicts with catalogue")
	ErrDiscardIntentInvalid   = errors.New("recovery: discard intent is unusable")
)

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
	next.DegradedReason = ""
	return next
}
