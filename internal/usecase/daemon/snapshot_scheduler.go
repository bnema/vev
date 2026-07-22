package daemon

import (
	"context"
	"time"
)

func (d *Daemon) snapshotRepositorySaver(ctx context.Context) {
	if d == nil {
		return
	}
	timer := d.clock.NewTimer(24 * time.Hour)
	defer timer.Stop()
	for {
		delay := d.scheduleEligibleRepositorySnapshots()
		timer.Reset(delay)
		select {
		case <-ctx.Done():
			return
		case <-d.snapshotWake:
		case <-timer.C():
		}
	}
}

func (d *Daemon) scheduleEligibleRepositorySnapshots() time.Duration {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	now := d.clock.Now()
	for _, sess := range sessions {
		if !sess.snapEligible.Load() || !sess.snapDirty.Load() {
			continue
		}
		sess.snapshotMu.Lock()
		due := !sess.snapshotPending && (sess.snapshotNextEligibleAt.IsZero() || !now.Before(sess.snapshotNextEligibleAt))
		sess.snapshotMu.Unlock()
		if due {
			d.scheduleSnapshot(sess)
		}
	}

	// Scheduling can synchronously fail or mark a capture pending, so inspect
	// the final state to select the actual earliest timer deadline.
	var next time.Time
	for _, sess := range sessions {
		if !sess.snapEligible.Load() || !sess.snapDirty.Load() {
			continue
		}
		sess.snapshotMu.Lock()
		eligibleAt := sess.snapshotNextEligibleAt
		pending := sess.snapshotPending
		sess.snapshotMu.Unlock()
		if pending || eligibleAt.IsZero() {
			continue
		}
		if next.IsZero() || eligibleAt.Before(next) {
			next = eligibleAt
		}
	}
	if next.IsZero() {
		return 24 * time.Hour
	}
	if delay := next.Sub(now); delay > 0 {
		return delay
	}
	return 0
}
