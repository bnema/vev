package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
)

func (d *Daemon) scheduleSnapshot(sess *session) bool {
	return d.scheduleSnapshotWithFinalFallback(sess, false)
}

// scheduleFinalSnapshot captures the terminal state even when an older capture
// is pending. A removed session cannot be retried by the repository scheduler, so the
// worker drains its retained terminal state before it stops.
func (d *Daemon) scheduleFinalSnapshot(sess *session) bool {
	return d.scheduleSnapshotWithFinalFallback(sess, true)
}

func (d *Daemon) scheduleSnapshotWithFinalFallback(sess *session, final bool) bool {
	if d == nil || sess == nil || !d.snapsEnabled || d.snapshotRepository == nil || !sess.snapEligible.Load() {
		return true
	}
	kind := snapshotAttemptRoutine
	if final {
		kind = snapshotAttemptForced
	}
	sess.snapshotMu.Lock()
	if sess.snapshotQuarantined || !sess.snapDirty.Load() {
		available := !sess.snapshotQuarantined
		sess.snapshotMu.Unlock()
		return available
	}
	if final && sess.snapshotGeneration > sess.snapshotForcedGeneration {
		// Do not let an older queued or in-flight routine publication satisfy a
		// terminal checkpoint. The intent is retained until a publication at
		// this generation (or newer) succeeds.
		sess.snapshotForcedGeneration = sess.snapshotGeneration
	}
	if sess.snapshotPending ||
		(!final && !sess.snapshotNextEligibleAt.IsZero() && d.clock.Now().Before(sess.snapshotNextEligibleAt)) {
		sess.snapshotMu.Unlock()
		return true
	}
	if sess.snapshotPublicationContext == nil {
		sess.snapshotPublicationContext, sess.snapshotPublicationCancel = context.WithCancel(context.Background())
	}
	// A capture's repository generation advances only from the last successful
	// repository publication. Mutations are independently versioned, so several
	// changes coalesced before this checkpoint still publish the immediate next
	// repository generation and a failed attempt retries that same generation.
	generation := sess.snapshotPublishedGeneration + 1
	mutationRevision := sess.snapshotGeneration
	// A restored or test-created dirty session can predate mutation revision
	// tracking. Normalize that state into the first revision before capture.
	if mutationRevision == 0 {
		mutationRevision = 1
		sess.snapshotGeneration = mutationRevision
	}
	publicationContext := sess.snapshotPublicationContext
	var parentCheckpoint *domain.CheckpointRef
	if sess.snapshotPublishedCheckpoint != nil {
		parent := *sess.snapshotPublishedCheckpoint
		parentCheckpoint = &parent
	}
	sess.snapshotPendingCaptures++
	sess.snapshotPending = true
	sess.signalSnapshotChangedLocked()
	sess.snapshotMu.Unlock()

	capture, ok := d.captureSnapshotState(sess, generation)
	if capture != nil {
		capture.mutationRevision = mutationRevision
		capture.attemptKind = kind
		capture.publicationContext = publicationContext
		capture.parentCheckpoint = parentCheckpoint
	}
	if !ok {
		d.finishSnapshotCapture(capture, false)
		return false
	}
	sess.snapshotMu.Lock()
	quarantined := sess.snapshotQuarantined
	if !quarantined {
		sess.snapshotQueuedCapture = capture
	}
	sess.snapshotMu.Unlock()
	if quarantined {
		d.finishSnapshotCapture(capture, false)
		return false
	}
	if d.enqueueSnapshotCapture(capture) || (final && d.enqueueFinalSnapshotCapture(capture)) {
		return true
	}
	// Coalesce under saturation or shutdown: the latest state stays dirty and
	// will be captured on a later tick once an active worker has room.
	d.finishSnapshotCapture(capture, false)
	return false
}

// enqueueSnapshotCapture serializes worker shutdown with the non-blocking
// queue send. The queue is deliberately never closed: producers can race
// shutdown without risking a send-on-closed panic.
func (d *Daemon) enqueueSnapshotCapture(capture *snapshotCapture) bool {
	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	if d.snapshotWorkerClosing || d.snapshotWorkerCancel == nil || d.snapshotWorkerCtx == nil || d.snapshotWorkerCtx.Err() != nil {
		return false
	}
	select {
	case d.snapshotJobs <- capture:
		return true
	default:
		return false
	}
}

// enqueueFinalSnapshotCapture hands terminal captures to the existing worker
// without making session teardown wait behind its normal bounded queue. A
// blocked worker retains at most one terminal capture per session, replacing
// stale state with the newest immutable capture.
func (d *Daemon) enqueueFinalSnapshotCapture(capture *snapshotCapture) bool {
	if capture == nil || capture.session == nil {
		return false
	}
	d.snapshotWorkerMu.Lock()
	if d.snapshotWorkerClosing || d.snapshotWorkerCancel == nil || d.snapshotWorkerCtx == nil || d.snapshotWorkerCtx.Err() != nil {
		d.snapshotWorkerMu.Unlock()
		return false
	}
	if d.snapshotFinalJobs == nil {
		d.snapshotFinalJobs = make(map[*session]*snapshotCapture)
	}
	replaced, exists := d.snapshotFinalJobs[capture.session]
	if !exists && len(d.snapshotFinalJobs) >= snapshotFinalQueueCapacity {
		d.snapshotWorkerMu.Unlock()
		d.log.Warn("terminal snapshot retention saturated; capture rejected", "session", capture.name, "capacity", snapshotFinalQueueCapacity)
		return false
	}
	// Finish the stale capture before publishing its replacement. Otherwise a
	// fast worker could persist the replacement and then have this stale failure
	// mark the session dirty again.
	d.finishSnapshotCapture(replaced, false)
	if !exists {
		d.snapshotFinalOrder = append(d.snapshotFinalOrder, capture.session)
	}
	d.snapshotFinalJobs[capture.session] = capture
	select {
	case d.snapshotWorkerFinalWake <- struct{}{}:
	default:
	}
	d.snapshotWorkerMu.Unlock()
	return true
}
