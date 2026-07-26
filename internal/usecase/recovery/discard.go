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
)

// Discard throws away a broken session's persisted state and replaces it with a
// fresh incarnation. It is retry-idempotent, not crash-resumable: a crash
// leaves either the old record (rerun discard) or a new record plus an orphan
// directory that startup garbage collection removes.
func (c *Coordinator) Discard(ctx context.Context, name string) error {
	if c == nil || c.catalogue == nil || c.repository == nil || c.locks == nil || c.random == nil {
		return errors.New("recovery: incomplete discard dependencies")
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	unlock := c.locks.Lock([]string{name})
	defer unlock()

	record, ok, err := c.catalogue.Record(name)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("discard %q: %w", name, ErrRecoveryRecordNotFound)
	}
	if record.RecoveryState == domain.RecoveryFresh {
		return nil
	}
	if record.RecoveryState != domain.RecoveryDegraded {
		return ErrSessionNotDegraded
	}

	old := record.IncarnationID
	next, err := domain.NewIncarnationID(c.random)
	if err != nil {
		return fmt.Errorf("recovery: generate replacement incarnation: %w", err)
	}
	fresh := record
	fresh.IncarnationID = next
	fresh.RecoveryState = domain.RecoveryFresh
	fresh.Committed = nil
	fresh.DegradedReason = ""
	if err := fresh.Validate(); err != nil {
		return fmt.Errorf("recovery: invalid discard replacement: %w", err)
	}
	if err := c.catalogue.Replace(name, fresh); err != nil {
		return err
	}
	return c.repository.DeleteIncarnation(ctx, old)
}
