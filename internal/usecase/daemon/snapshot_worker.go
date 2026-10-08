package daemon

import (
	"context"
	"errors"
	"sort"

	"github.com/bnema/vev/internal/domain"
)

func (d *Daemon) reportSnapshotFailure(capture *snapshotCapture, phase string, cause error) {
	if d == nil || capture == nil || capture.session == nil || cause == nil {
		return
	}
	signature := snapshotFailureSignature(phase, cause)
	d.snapshots.noticeMu.Lock()
	changed := d.snapshots.activeFailureSignature != signature
	d.snapshots.activeFailureSignature = signature
	d.snapshots.noticeMu.Unlock()

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
	d.snapshots.noticeMu.Lock()
	d.snapshots.activeFailureSignature = ""
	d.snapshots.noticeMu.Unlock()
}

func (d *Daemon) startSnapshotEncodeWorker() {
	if d == nil {
		return
	}
	d.snapshots.mu.Lock()
	if d.snapshots.cancel != nil {
		d.snapshots.mu.Unlock()
		return
	}
	// Shutdown owns the worker lifetime so it can flush captures after Serve
	// cancels its parent context. The worker is always stopped explicitly.
	workerCtx, cancel := context.WithCancel(context.Background())
	d.snapshots.id++
	workerID := d.snapshots.id
	d.snapshots.ctx = workerCtx
	d.snapshots.cancel = cancel
	d.snapshots.done = make(chan struct{})
	d.snapshots.flush = make(chan struct{})
	d.snapshots.finalWake = make(chan struct{}, 1)
	d.snapshots.finalJobs = make(map[*session]*snapshotCapture)
	d.snapshots.finalOrder = nil
	d.snapshots.closing = false
	d.snapshots.inFlight = nil
	done := d.snapshots.done
	flush := d.snapshots.flush
	finalWake := d.snapshots.finalWake
	d.snapshots.mu.Unlock()
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
		case capture := <-d.snapshots.jobs:
			if !d.publishSnapshotCapture(workerCtx, workerID, capture) {
				return
			}
		}
	}
}

func (d *Daemon) flushSnapshotCaptures(workerCtx context.Context, workerID uint64) {
	for {
		select {
		case capture := <-d.snapshots.jobs:
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
			if d.recovery != nil {
				var record domain.CatalogueRecord
				record, err = d.recovery.PublishCheckpoint(publicationContext, capture.name, publication)
				if err == nil {
					if record.Committed == nil {
						err = errors.New("snapshot: checkpoint publication returned no committed checkpoint")
					} else {
						capture.checkpoint = *record.Committed
					}
				}
			} else {
				err = d.snapshotRepository.Publish(publicationContext, publication)
			}
		}
	}
	d.clearSnapshotWorkerInFlight(workerID, capture)
	if err == nil && workerCtx.Err() == nil {
		markSnapshotCaptureObjectsPublished(capture)
	}
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
	d.snapshots.mu.Lock()
	defer d.snapshots.mu.Unlock()
	for len(d.snapshots.finalOrder) > 0 {
		sess := d.snapshots.finalOrder[0]
		d.snapshots.finalOrder[0] = nil
		d.snapshots.finalOrder = d.snapshots.finalOrder[1:]
		capture := d.snapshots.finalJobs[sess]
		delete(d.snapshots.finalJobs, sess)
		if capture != nil {
			return capture
		}
	}
	return nil
}

func (d *Daemon) setSnapshotWorkerInFlight(workerID uint64, capture *snapshotCapture) bool {
	d.snapshots.mu.Lock()
	defer d.snapshots.mu.Unlock()
	if d.snapshots.id != workerID || d.snapshots.cancel == nil || d.snapshots.ctx == nil || d.snapshots.ctx.Err() != nil {
		return false
	}
	d.snapshots.inFlight = capture
	return true
}

func (d *Daemon) clearSnapshotWorkerInFlight(workerID uint64, capture *snapshotCapture) {
	d.snapshots.mu.Lock()
	defer d.snapshots.mu.Unlock()
	if d.snapshots.id == workerID && d.snapshots.inFlight == capture {
		d.snapshots.inFlight = nil
	}
}

func (d *Daemon) stopSnapshotEncodeWorker() {
	if d == nil {
		return
	}
	_ = d.StopDurableWriters(context.Background())
	d.WaitDurableWriters()
}

// StopDurableWriters stops maintenance scheduling, cancels cooperative
// maintenance calls, stops snapshot admission, and requests the snapshot
// worker's final drain. The context bounds checkpoint success only:
// cancellation never detaches a writer from its owner.
func (d *Daemon) StopDurableWriters(ctx context.Context) []string {
	if d == nil {
		return nil
	}
	cancel, done := d.requestDurableWriterStop()
	if done == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		// Snapshot identity before cancellation can let a cooperative worker clear
		// its in-flight capture. The caller persists timeout notices before the
		// unconditional ownership join.
		names := d.durableWriterFailureNames()
		cancel()
		return names
	}
}

// WaitDurableWriters is the unconditional ownership barrier. It has no timeout
// or status branch: every writer goroutine must exit before Serve may return.
// Restoration is joined with the others because it mutates durable state too;
// it observes serveCtx cancellation, so waiting for it cannot outlive an
// uncooperative repository call any longer than the snapshot worker does.
func (d *Daemon) WaitDurableWriters() {
	if d == nil {
		return
	}
	d.snapshots.mu.Lock()
	snapshotDone := d.snapshots.done
	maintenanceDone := d.snapshots.maintenanceWorkerDone
	restoreDone := d.snapshots.restoreWorkerDone
	d.snapshots.mu.Unlock()
	if snapshotDone != nil {
		<-snapshotDone
		d.finishStoppedSnapshotWorker(false)
	}
	if maintenanceDone != nil {
		<-maintenanceDone
	}
	// Restoration never blocks on the workers above (snapshot admission is
	// non-blocking), so joining it last cannot deadlock.
	if restoreDone != nil {
		<-restoreDone
	}
}

func (d *Daemon) requestDurableWriterStop() (context.CancelFunc, <-chan struct{}) {
	d.snapshots.mu.Lock()
	defer d.snapshots.mu.Unlock()
	d.cancelDurableMaintenanceLocked()
	cancel := d.snapshots.cancel
	if cancel == nil {
		return nil, nil
	}
	if !d.snapshots.closing {
		d.snapshots.closing = true
		close(d.snapshots.flush)
	}
	return cancel, d.snapshots.done
}

// durableWriterFailureNames includes admitted buffered captures as well as the
// active and final queues. snapshotAdmitted tracks normal captures from queue
// admission through completion, so worker dequeue cannot make one disappear.
func (d *Daemon) durableWriterFailureNames() []string {
	d.snapshots.mu.Lock()
	defer d.snapshots.mu.Unlock()
	seen := make(map[string]struct{})
	add := func(capture *snapshotCapture) {
		if capture != nil && capture.name != "" {
			seen[capture.name] = struct{}{}
		}
	}
	add(d.snapshots.inFlight)
	for _, capture := range d.snapshots.finalJobs {
		add(capture)
	}
	for capture := range d.snapshots.admitted {
		add(capture)
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (d *Daemon) finishStoppedSnapshotWorker(abandoned bool) {
	d.snapshots.mu.Lock()
	inFlight := d.snapshots.inFlight
	d.snapshots.ctx = nil
	d.snapshots.cancel = nil
	d.snapshots.done = nil
	d.snapshots.flush = nil
	d.snapshots.finalWake = nil
	d.snapshots.closing = false
	d.snapshots.inFlight = nil
	queued := make([]*snapshotCapture, 0, len(d.snapshots.jobs)+len(d.snapshots.finalJobs))
	for _, capture := range d.snapshots.finalJobs {
		queued = append(queued, capture)
	}
	d.snapshots.finalJobs = nil
	d.snapshots.finalOrder = nil
	for {
		select {
		case capture := <-d.snapshots.jobs:
			queued = append(queued, capture)
		default:
			d.snapshots.mu.Unlock()
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
