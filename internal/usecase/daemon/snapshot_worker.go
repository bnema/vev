package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
)

func (d *Daemon) reportSnapshotFailure(capture *snapshotCapture, phase string, cause error) {
	if d == nil || capture == nil || capture.session == nil || cause == nil {
		return
	}
	signature := snapshotFailureSignature(phase, cause)
	d.snapshotNoticeMu.Lock()
	changed := d.snapshotActiveFailureSignature != signature
	d.snapshotActiveFailureSignature = signature
	d.snapshotNoticeMu.Unlock()

	n := domain.Notification{
		Code:     domain.NoticeSnapshotWrite,
		Severity: domain.NoticeError,
		Message:  "couldn't save session state; recent state may be lost on restart",
		// The history is intentionally diagnostic only at the error-class level.
		// Raw causes can include user paths and terminal content.
		Details: signature,
		Time:    d.clock.Now(),
	}
	n, _ = d.notices.recordSnapshotFailure(n)
	d.log.Warn("writing session snapshot failed", "phase", phase, "class", signature, "session", capture.name)
	if changed {
		d.deliverGlobal(n)
	}
}

func (d *Daemon) clearSnapshotFailure() {
	if d == nil {
		return
	}
	d.snapshotNoticeMu.Lock()
	d.snapshotActiveFailureSignature = ""
	d.snapshotNoticeMu.Unlock()
}

func (d *Daemon) startSnapshotEncodeWorker() {
	if d == nil {
		return
	}
	d.snapshotWorkerMu.Lock()
	if d.snapshotWorkerCancel != nil {
		d.snapshotWorkerMu.Unlock()
		return
	}
	// Shutdown owns the worker lifetime so it can flush captures after Serve
	// cancels its parent context. The worker is always stopped explicitly.
	workerCtx, cancel := context.WithCancel(context.Background())
	d.snapshotWorkerID++
	workerID := d.snapshotWorkerID
	d.snapshotWorkerCtx = workerCtx
	d.snapshotWorkerCancel = cancel
	d.snapshotWorkerDone = make(chan struct{})
	d.snapshotWorkerFlush = make(chan struct{})
	d.snapshotWorkerFinalWake = make(chan struct{}, 1)
	d.snapshotFinalJobs = make(map[*session]*snapshotCapture)
	d.snapshotFinalOrder = nil
	d.snapshotWorkerClosing = false
	d.snapshotWorkerInFlight = nil
	done := d.snapshotWorkerDone
	flush := d.snapshotWorkerFlush
	finalWake := d.snapshotWorkerFinalWake
	d.snapshotWorkerMu.Unlock()
	go d.runSnapshotEncodeWorker(workerCtx, workerID, done, flush, finalWake)
}

// runSnapshotEncodeWorker owns the worker state machine. Each event either
// continues processing, drains the terminal queues for shutdown, or stops;
// publication details are kept out of the select loop.
func (d *Daemon) runSnapshotEncodeWorker(workerCtx context.Context, workerID uint64, done chan<- struct{}, flush, finalWake <-chan struct{}) {
	defer close(done)
	for {
		select {
		case <-workerCtx.Done():
			return
		case <-flush:
			d.flushSnapshotCaptures(workerCtx, workerID)
			return
		case <-finalWake:
			if !d.drainFinalSnapshotCaptures(workerCtx, workerID) {
				return
			}
		case capture := <-d.snapshotJobs:
			if !d.publishSnapshotCapture(workerCtx, workerID, capture) {
				return
			}
		}
	}
}

func (d *Daemon) flushSnapshotCaptures(workerCtx context.Context, workerID uint64) {
	for {
		select {
		case capture := <-d.snapshotJobs:
			if !d.publishSnapshotCapture(workerCtx, workerID, capture) {
				return
			}
		default:
			_ = d.drainFinalSnapshotCaptures(workerCtx, workerID)
			return
		}
	}
}

func (d *Daemon) drainFinalSnapshotCaptures(workerCtx context.Context, workerID uint64) bool {
	for capture := d.takeFinalSnapshotCapture(); capture != nil; capture = d.takeFinalSnapshotCapture() {
		if !d.publishSnapshotCapture(workerCtx, workerID, capture) {
			return false
		}
	}
	return true
}

func (d *Daemon) publishSnapshotCapture(workerCtx context.Context, workerID uint64, capture *snapshotCapture) bool {
	if workerCtx.Err() != nil || !startSnapshotPublication(capture) || !d.setSnapshotWorkerInFlight(workerID, capture) {
		d.finishSnapshotCapture(capture, false)
		return workerCtx.Err() == nil
	}
	publication, err := d.incrementalPublication(capture)
	publicationContext := capture.publicationContext
	if publicationContext == nil {
		publicationContext = workerCtx
	}
	if err == nil && workerCtx.Err() == nil {
		if err = publicationContext.Err(); err == nil {
			err = d.snapshotRepository.Publish(publicationContext, publication)
		}
	}
	d.clearSnapshotWorkerInFlight(workerID, capture)
	if err != nil && workerCtx.Err() == nil && publicationContext.Err() == nil {
		// Global, not session-scoped: by now the session may already be torn
		// down, so a session notice would be dead-on-arrival. No lock is held
		// here (clearSnapshotWorkerInFlight released its own).
		d.reportSnapshotFailure(capture, "publish", err)
	}
	d.finishSnapshotCapture(capture, err == nil && workerCtx.Err() == nil)
	return workerCtx.Err() == nil
}

func (d *Daemon) takeFinalSnapshotCapture() *snapshotCapture {
	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	for len(d.snapshotFinalOrder) > 0 {
		sess := d.snapshotFinalOrder[0]
		d.snapshotFinalOrder[0] = nil
		d.snapshotFinalOrder = d.snapshotFinalOrder[1:]
		capture := d.snapshotFinalJobs[sess]
		delete(d.snapshotFinalJobs, sess)
		if capture != nil {
			return capture
		}
	}
	return nil
}

func (d *Daemon) setSnapshotWorkerInFlight(workerID uint64, capture *snapshotCapture) bool {
	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	if d.snapshotWorkerID != workerID || d.snapshotWorkerCancel == nil || d.snapshotWorkerCtx == nil || d.snapshotWorkerCtx.Err() != nil {
		return false
	}
	d.snapshotWorkerInFlight = capture
	return true
}

func (d *Daemon) clearSnapshotWorkerInFlight(workerID uint64, capture *snapshotCapture) {
	d.snapshotWorkerMu.Lock()
	defer d.snapshotWorkerMu.Unlock()
	if d.snapshotWorkerID == workerID && d.snapshotWorkerInFlight == capture {
		d.snapshotWorkerInFlight = nil
	}
}

func (d *Daemon) stopSnapshotEncodeWorker() {
	if d == nil {
		return
	}
	deadline := newSnapshotShutdownDeadline(d.clock)
	defer deadline.stop()
	d.stopSnapshotEncodeWorkerWithDeadline(deadline)
}

// stopSnapshotEncodeWorkerWithDeadline performs the one deadline-aware join
// for a Serve shutdown. Queues are never closed, and detached writers only
// retain immutable captures plus contexts/channels that remain live, so an
// uncooperative repository call cannot race teardown or block process exit.
func (d *Daemon) stopSnapshotEncodeWorkerWithDeadline(deadline *snapshotShutdownDeadline) {
	if d == nil {
		return
	}
	d.snapshotWorkerMu.Lock()
	cancel := d.snapshotWorkerCancel
	if cancel == nil || d.snapshotWorkerClosing {
		d.snapshotWorkerMu.Unlock()
		return
	}
	// Stop accepting producers, then let the single worker drain its bounded
	// queue. A non-context-aware store may still block indefinitely, so the
	// shared shutdown deadline bounds the one and only join.
	d.snapshotWorkerClosing = true
	flush := d.snapshotWorkerFlush
	done := d.snapshotWorkerDone
	close(flush)
	d.snapshotWorkerMu.Unlock()

	if deadline == nil {
		deadline = newSnapshotShutdownDeadline(d.clock)
		defer deadline.stop()
	}
	select {
	case <-done:
		d.finishStoppedSnapshotWorker(false)
	case <-deadline.Done():
		cancel()
		d.finishStoppedSnapshotWorker(true)
	}
}

func (d *Daemon) finishStoppedSnapshotWorker(abandoned bool) {
	d.snapshotWorkerMu.Lock()
	inFlight := d.snapshotWorkerInFlight
	d.snapshotWorkerCtx = nil
	d.snapshotWorkerCancel = nil
	d.snapshotWorkerDone = nil
	d.snapshotWorkerFlush = nil
	d.snapshotWorkerFinalWake = nil
	d.snapshotWorkerClosing = false
	d.snapshotWorkerInFlight = nil
	queued := make([]*snapshotCapture, 0, len(d.snapshotJobs)+len(d.snapshotFinalJobs))
	for _, capture := range d.snapshotFinalJobs {
		queued = append(queued, capture)
	}
	d.snapshotFinalJobs = nil
	d.snapshotFinalOrder = nil
	for {
		select {
		case capture := <-d.snapshotJobs:
			queued = append(queued, capture)
		default:
			d.snapshotWorkerMu.Unlock()
			if abandoned {
				d.finishSnapshotCapture(inFlight, false)
			}
			for _, capture := range queued {
				d.finishSnapshotCapture(capture, false)
			}
			return
		}
	}
}
