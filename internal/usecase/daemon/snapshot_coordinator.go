package daemon

import (
	"context"
	"time"

	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func snapshotCoordinatorContext(sess *session) (context.Context, context.CancelFunc) {
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	if sess.snapshotPublicationContext == nil {
		sess.snapshotPublicationContext, sess.snapshotPublicationCancel = context.WithCancel(context.Background())
	}
	return sess.snapshotPublicationContext, sess.snapshotPublicationCancel
}

// snapshotCoordinatorQuarantine identifies one stop request. A later teardown
// can supersede it, in which case only that later owner may resume publication.
type snapshotCoordinatorQuarantine struct {
	done  <-chan struct{}
	epoch uint64
}

// quarantineSnapshotCoordinator prevents a deleted session from starting any
// further publication. The returned join channel is closed after an already
// started repository call returns; callers must wait without daemon or session
// locks before deleting repository state.
func quarantineSnapshotCoordinator(sess *session) <-chan struct{} {
	return quarantineSnapshotCoordinatorWithEpoch(sess).done
}

// quarantineSnapshotCoordinatorRetainingQueuedCapture is used after a forced
// shutdown checkpoint times out. Its queued capture is logical retry state, not
// a repository operation: startSnapshotPublication rejects it while
// quarantined, and the worker eventually discards it after its blocked call
// returns.
func quarantineSnapshotCoordinatorRetainingQueuedCapture(sess *session) <-chan struct{} {
	return quarantineSnapshotCoordinatorWithOptions(sess, true).done
}

func quarantineSnapshotCoordinatorWithEpoch(sess *session) snapshotCoordinatorQuarantine {
	return quarantineSnapshotCoordinatorWithOptions(sess, false)
}

func quarantineSnapshotCoordinatorWithOptions(sess *session, retainQueuedCapture bool) snapshotCoordinatorQuarantine {
	if sess == nil {
		done := make(chan struct{})
		close(done)
		return snapshotCoordinatorQuarantine{done: done}
	}
	sess.snapshotMu.Lock()
	sess.snapshotQuarantineEpoch++
	epoch := sess.snapshotQuarantineEpoch
	sess.snapshotQuarantined = true
	sess.snapEligible.Store(false)
	if sess.snapshotPublicationCancel != nil {
		sess.snapshotPublicationCancel()
	}
	if sess.snapshotQueuedCapture != nil && !retainQueuedCapture {
		// A rename or purge must discard a capture under the old identity. The
		// global queue may still contain it, but startSnapshotPublication
		// compares identity and will discard it even if this session is renamed.
		sess.snapshotQueuedCapture.coordinatorDiscarded = true
		sess.snapshotQueuedCapture = nil
		if sess.snapshotPendingCaptures > 0 {
			sess.snapshotPendingCaptures--
		}
		sess.snapshotPending = sess.snapshotPendingCaptures > 0 || sess.snapshotInFlightCapture != nil
	}
	done := sess.snapshotPublicationDone
	if done == nil {
		done = make(chan struct{})
		close(done)
	}
	sess.signalSnapshotChangedLocked()
	sess.snapshotMu.Unlock()
	return snapshotCoordinatorQuarantine{done: done, epoch: epoch}
}

// resumeSnapshotCoordinatorForNewIdentity starts a fresh repository lineage
// only after rename committed its new in-memory name. The epoch prevents a
// concurrent teardown from being undone by a late rename completion.
func resumeSnapshotCoordinatorForNewIdentity(sess *session, quarantine snapshotCoordinatorQuarantine) bool {
	if sess == nil {
		return false
	}
	sess.snapshotMu.Lock()
	if !sess.snapshotQuarantined || sess.snapshotQuarantineEpoch != quarantine.epoch {
		sess.snapshotMu.Unlock()
		return false
	}
	if sess.snapshotPublicationCancel != nil {
		sess.snapshotPublicationCancel()
	}
	sess.snapshotPublicationContext, sess.snapshotPublicationCancel = context.WithCancel(context.Background())
	sess.snapshotQuarantined = false
	// A repository name owns its own generation stream. Reset all scheduler
	// state while eligibility remains false, then atomically make generation 1
	// dirty and wake the scheduler.
	sess.snapshotGeneration = 1
	sess.snapshotCapturedGeneration = 0
	sess.snapshotPublishedGeneration = 0
	sess.snapshotForcedGeneration = 0
	sess.snapshotNextEligibleAt = time.Time{}
	sess.snapshotAttempted = false
	sess.snapshotAttemptKind = snapshotAttemptRoutine
	sess.snapshotFailureSig = ""
	sess.snapshotChunkCache = newSnapshotChunkCache(snapshotChunkCacheLimit)
	sess.snapDirty.Store(true)
	sess.snapEligible.Store(true)
	sess.signalSnapshotChangedLocked()
	wake := sess.snapshotWake
	sess.snapshotMu.Unlock()
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	return true
}

func markSnapshotDirty(sess *session) {
	if sess == nil || !sess.snapEligible.Load() {
		return
	}
	sess.snapshotMu.Lock()
	sess.snapshotGeneration++
	sess.snapDirty.Store(true)
	sess.signalSnapshotChangedLocked()
	wake := sess.snapshotWake
	sess.snapshotMu.Unlock()
	if wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

// snapshotChangeLocked returns a channel closed by the next snapshot state
// transition. snapshotMu must be held.
func (sess *session) snapshotChangeLocked() chan struct{} {
	if sess.snapshotChanged == nil {
		sess.snapshotChanged = make(chan struct{})
	}
	return sess.snapshotChanged
}

// signalSnapshotChangedLocked wakes waiters after a snapshot state transition.
// snapshotMu must be held.
func (sess *session) signalSnapshotChangedLocked() {
	close(sess.snapshotChangeLocked())
	sess.snapshotChanged = make(chan struct{})
}

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
	generation := sess.snapshotGeneration
	publicationContext := sess.snapshotPublicationContext
	sess.snapshotCapturedGeneration = generation
	sess.snapshotAttempted = true
	sess.snapshotAttemptKind = kind
	sess.snapshotPendingCaptures++
	sess.snapshotPending = true
	sess.signalSnapshotChangedLocked()
	sess.snapshotMu.Unlock()

	capture, ok := d.captureSnapshotState(sess, generation)
	if capture != nil {
		capture.attemptKind = kind
		capture.publicationContext = publicationContext
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

// waitForSnapshotGeneration waits only for the bounded terminal checkpoint
// deadline. It observes successful publication, rather than dirty state, so a
// concurrent later mutation cannot turn a completed forced checkpoint into a
// false timeout.
func (d *Daemon) waitForSnapshotGeneration(sess *session, generation uint64) bool {
	if d == nil || sess == nil || generation == 0 {
		return true
	}
	deadline := newSnapshotShutdownDeadline(d.clock)
	defer deadline.stop()
	return d.waitForSnapshotGenerationWithDeadline(sess, generation, deadline)
}

// waitForSnapshotGenerationWithDeadline waits for a forced checkpoint without
// allocating another shutdown interval. Serve shares one deadline across every
// session and the subsequent worker drain.
func (d *Daemon) waitForSnapshotGenerationWithDeadline(sess *session, generation uint64, deadline *snapshotShutdownDeadline) bool {
	if d == nil || sess == nil || generation == 0 {
		return true
	}
	if deadline == nil {
		return d.waitForSnapshotGeneration(sess, generation)
	}
	for {
		sess.snapshotMu.Lock()
		published := sess.snapshotPublishedGeneration >= generation
		changed := sess.snapshotChangeLocked()
		sess.snapshotMu.Unlock()
		if published {
			return true
		}
		select {
		case <-changed:
		case <-deadline.Done():
			return false
		}
	}
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

// captureSession rotates history tails and clones visible frames while holding
// each pane lock. The returned capture contains only immutable state; encoding
// and persistence are deliberately deferred to snapshotEncodeWorker.
func (d *Daemon) captureSnapshotState(sess *session, generation uint64) (*snapshotCapture, bool) {
	sess.mu.Lock()
	capture := &snapshotCapture{
		session:    sess,
		generation: generation,
		name:       sess.name,
		createdAt:  uint64(sess.createdAt),
		active:     uint16(max(sess.active, 0)),
	}
	ephemeral := sess.ephemeral
	fallbackCwd := sess.cwd
	tabs := append([]*tab(nil), sess.tabs...)
	sess.mu.Unlock()
	if ephemeral || capture.name == "" {
		return capture, false
	}

	capture.tabs = make([]snapshotCaptureTab, 0, len(tabs))
	for _, tb := range tabs {
		tb.mu.Lock()
		tabCapture := snapshotCaptureTab{
			stableID:   tb.stableID,
			cols:       uint16(max(tb.size.Cols, 0)),
			rows:       uint16(max(tb.size.Rows, 0)),
			nextPaneID: uint64(max(tb.nextPaneID, 0)),
			tree:       tb.tree.Clone(),
		}
		if tb.tree != nil {
			tabCapture.focus = tb.tree.Focus
		}
		panes := make([]*pane, 0, len(tb.panes))
		for _, p := range tb.panes {
			panes = append(panes, p)
		}
		tb.mu.Unlock()

		tabCapture.panes = make([]snapshotCapturePane, 0, len(panes))
		for _, p := range panes {
			p.mu.Lock()
			pty := p.pty
			pid := 0
			if pty != nil {
				pid = pty.Pid()
			}
			paneCapture := snapshotCapturePane{
				id:       p.id,
				stableID: p.stableID,
				visible:  p.screen.PrimaryVisibleSnapshot(),
			}
			paneCapture.sealed = p.history.SnapshotView()
			paneCapture.tail = paneCapture.sealed.Tail()
			p.mu.Unlock()
			paneCapture.cwd = fallbackCwd
			if d.procCwd != nil && pid > 0 {
				if cwd, err := d.procCwd(pid); err == nil && cwd != "" {
					paneCapture.cwd = cwd
				}
			}
			paneCapture.process = d.capturePaneProcess(pty, pid)
			tabCapture.panes = append(tabCapture.panes, paneCapture)
		}
		capture.tabs = append(capture.tabs, tabCapture)
	}
	return capture, true
}

// captureSession remains the synchronous producer-facing trigger for callers
// such as teardown and benchmarks; the actual encoding and Write stay async.
func (d *Daemon) captureSession(sess *session) bool {
	markSnapshotDirty(sess)
	return d.scheduleSnapshot(sess)
}

// startSnapshotPublication moves a retained capture from the producer queue to
// the repository worker. Quarantine and this transition share snapshotMu, so a
// repository call cannot begin after destructive teardown has quarantined the
// session.
func startSnapshotPublication(capture *snapshotCapture) bool {
	if capture == nil || capture.session == nil {
		return false
	}
	sess := capture.session
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	if sess.snapshotQuarantined || sess.snapshotQueuedCapture != capture {
		return false
	}
	sess.snapshotQueuedCapture = nil
	sess.snapshotInFlightCapture = capture
	sess.snapshotPublicationDone = make(chan struct{})
	sess.snapshotPending = true
	sess.signalSnapshotChangedLocked()
	return true
}

func (d *Daemon) finishSnapshotCapture(capture *snapshotCapture, succeeded bool) {
	if capture == nil {
		return
	}
	capture.finishOnce.Do(func() {
		shouldScheduleForcedSuccessor := false
		capture.session.snapshotMu.Lock()
		if capture.session.snapshotPendingCaptures > 0 {
			capture.session.snapshotPendingCaptures--
		}
		if capture.session.snapshotQueuedCapture == capture {
			capture.session.snapshotQueuedCapture = nil
		}
		if capture.session.snapshotInFlightCapture == capture {
			capture.session.snapshotInFlightCapture = nil
			if capture.session.snapshotPublicationDone != nil {
				close(capture.session.snapshotPublicationDone)
				capture.session.snapshotPublicationDone = nil
			}
		}
		capture.session.snapshotPending = capture.session.snapshotPendingCaptures > 0 ||
			capture.session.snapshotQueuedCapture != nil || capture.session.snapshotInFlightCapture != nil
		if !capture.coordinatorDiscarded && capture.attemptKind == snapshotAttemptRoutine && d.snapshotRepository != nil {
			// The interval is measured from completion, including a failed
			// attempt. A forced teardown checkpoint deliberately leaves it alone.
			capture.session.snapshotNextEligibleAt = d.clock.Now().Add(snapshotInterval)
		}
		if succeeded {
			capture.session.snapshotPublishedGeneration = max(capture.session.snapshotPublishedGeneration, capture.generation)
		}
		if succeeded && capture.session.snapshotGeneration == capture.generation {
			capture.session.snapDirty.Store(false)
			capture.session.snapshotFailureSig = ""
		} else if !succeeded {
			capture.session.snapDirty.Store(true)
		}
		if succeeded && capture.session.snapshotForcedGeneration != 0 &&
			capture.generation >= capture.session.snapshotForcedGeneration {
			capture.session.snapshotForcedGeneration = 0
		}
		// A failed forced publication remains dirty for ordinary retry rather
		// than recursively creating forced jobs. A later forced request records
		// a newer intent and can create its one bounded successor.
		shouldScheduleForcedSuccessor = capture.session.snapshotForcedGeneration != 0 &&
			capture.generation < capture.session.snapshotForcedGeneration &&
			capture.session.snapshotQueuedCapture == nil && capture.session.snapshotInFlightCapture == nil &&
			(succeeded || capture.attemptKind != snapshotAttemptForced)
		capture.session.signalSnapshotChangedLocked()
		wake := capture.session.snapshotWake
		capture.session.snapshotMu.Unlock()
		if wake != nil {
			select {
			case wake <- struct{}{}:
			default:
			}
		}
		if shouldScheduleForcedSuccessor {
			// This runs after publication completion and outside snapshotMu. It
			// bypasses routine eligibility while preserving the single queued and
			// single in-flight capture bound.
			_ = d.scheduleFinalSnapshot(capture.session)
		}
		if succeeded {
			d.clearSnapshotFailure()
		}
	})
}

// reportSnapshotFailure records every failed persistence attempt but only
// displays a toast when its stable phase/error class changes. This keeps a
// blocked disk from repeatedly repainting every attached client while retaining
// the count in notification history.

func (d *Daemon) capturePaneProcess(pty interface{ ForegroundPgid() (int, error) }, shellPid int) *snapcodec.Process {
	if d == nil || pty == nil || shellPid <= 0 || d.procGroupArgv == nil {
		return nil
	}
	pgid, err := pty.ForegroundPgid()
	if err != nil || pgid <= 0 || pgid == shellPid {
		return nil
	}
	argv, err := d.procGroupArgv(pgid, shellPid)
	if err != nil || len(argv) == 0 || argv[0] == "" {
		return nil
	}
	strategy := detectProcessStrategy(argv)
	return &snapcodec.Process{
		Argv:     append([]string(nil), argv...),
		Strategy: strategy,
		Opts: snapcodec.ProcessOpts{
			AgentSessionID: extractAgentSessionID(strategy, argv),
		},
	}
}

// snapshotRepositorySaver waits until the earliest completion-derived routine
// eligibility. State changes wake it immediately, so a newly dirty session is
// captured without waiting for an unrelated session's interval.
