package daemon

import (
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// S1 render coordinator contract. One coordinator serves one attached
// session: producers mutate authoritative state and publish an invalidation;
// only the coordinator decides when the transitional composition target runs.
// Producers never compose or send. This file currently carries the inert
// contract — state, options, and signatures exist so the behavioral
// specification compiles — while wake scheduling, ACK gating, synchronized
// output handling, and producer migration land in the GREEN slice.

// Coordinator deadline bounds. Urgent transitions (input echo, overlay state
// changes) must become visible within urgentRenderDeadline; bulk output
// coalesces inside the adaptive [minOutputRenderDeadline,
// maxOutputRenderDeadline] window.
const (
	urgentRenderDeadline    = 2 * time.Millisecond
	minOutputRenderDeadline = 8 * time.Millisecond
	maxOutputRenderDeadline = 16 * time.Millisecond
)

// invalidationClass ranks a producer transition's latency demand.
type invalidationClass uint8

const (
	// invalidateOutput is bulk PTY/screen churn served by the adaptive window.
	invalidateOutput invalidationClass = iota
	// invalidateUrgent is an interactive transition served within 2ms.
	invalidateUrgent
)

// renderInvalidation is one producer-published state transition.
type renderInvalidation struct {
	class invalidationClass
	// reset requests a sticky FullRedraw: once requested it survives
	// coalescing and ACK deferral until a wake actually delivers it.
	reset bool
	// producer identifies the publishing call-site for provenance.
	producer string
}

// renderWake is one coalesced composition request delivered to the
// transitional composition target (replaced by the S2 pipeline).
type renderWake struct {
	reset     bool
	urgent    bool
	coalesced int
	// attachment is snapshotted under coordinator ownership. The wake
	// consumer must compose only for this exact client, never by rereading
	// session.client after an attach publication.
	attachment *attachedClient
	// watchdog marks a flush forced by the synchronized-output watchdog.
	watchdog bool
}

// resizeRequestMetadata is the coordinator-owned latest requested resize
// state: S3's transaction entry point reads it through resizeSnapshot. It is
// metadata ownership only — S1/S2 dispatch stays with the retained PR #71
// attachment timer path.
type resizeRequestMetadata struct {
	size   domain.Size
	source *attachedClient
	epoch  uint64
}

// renderCoordinatorOptions wires one coordinator instance.
type renderCoordinatorOptions struct {
	clock ports.Clock
	// wake is the transitional composition target.
	wake func(renderWake)
	// ackReady reports whether the attachment may compose another output
	// state (the outputStateStream window has capacity).
	ackReady func() bool
	// syncRenderable reports whether a pane's synchronized batch currently
	// contributes to the attached composition. It must not acquire c.mu.
	syncRenderable func(*pane) bool
	// syncActive is retained for existing test harness construction; per-pane
	// lifecycle and syncRenderable are the coordinator gate.
	syncActive func() bool
	// onInvalidate observes every published invalidation (test-visible hook).
	onInvalidate func(renderInvalidation)
	// afterSyncGateEvaluated is a package-private deterministic test seam. It
	// runs unlocked after visibility predicates and before registry validation.
	afterSyncGateEvaluated func()
}

// renderCoordinator fans in producer invalidations for one attached session.
// renderCoordinatorBurstMetrics is deliberately observational. It permits
// benchmark/pprof callers to measure producer bursts without influencing
// scheduling or transport policy.
type renderCoordinatorBurstMetrics struct {
	invalidations atomic.Uint64
	wakes         atomic.Uint64
	coalesced     atomic.Uint64
}

// renderCoordinatorBurstMetricsSnapshot is an immutable, internal view for
// benchmark reporting. Scheduling only writes the atomic counters above.
type renderCoordinatorBurstMetricsSnapshot struct {
	invalidations uint64
	wakes         uint64
	coalesced     uint64
}

type renderCoordinator struct {
	mu   sync.Mutex
	opts renderCoordinatorOptions

	// Coalesced pending state: latest wins, reset stays sticky, and at most
	// one deadline wake may be armed at a time (cap-1).
	pending       bool
	pendingReset  bool
	pendingUrgent bool
	// pendingPreview is accounted separately from the target primary wake:
	// viewers may consume a coalesced target snapshot while that target is
	// still ACK-blocked.
	pendingPreview bool
	// ackDeferred records that an expired deadline or watchdog was blocked by
	// output-window capacity. ACK notifications may flush only this state.
	ackDeferred bool
	// deadlineDue closes the handoff between a timer worker receiving its tick
	// and fire observing unavailable ACK capacity.
	deadlineDue bool
	coalesced   int
	// outputPressure carries recent bulk-burst pressure between completed
	// batches. It selects a bounded 8–16ms deadline without letting a busy
	// batch perpetually reset its own timer.
	outputPressure int

	// previewWake receives the same coalesced wakes for the legacy single
	// subscriber. previewWakes tracks picker subscriptions by viewer, so one
	// inactive session cannot replace another viewer's live preview.
	previewWake  func(renderWake)
	previewWakes map[*attachedClient]func(renderWake)

	// attachment is the currently bound client identity; callbacks from any
	// other identity are stale and must not mutate coordinator state.
	attachment *attachedClient
	// detached rejects invalidations after detach/park until a new attachment.
	detached bool
	torndown bool

	resize  resizeRequestMetadata
	metrics renderCoordinatorBurstMetrics
	// generation invalidates callbacks from superseded deadline/watchdog timers.
	// Each worker also owns an explicit cancellation channel: fake timers may
	// expose a nil or never-firing C channel, so Stop alone cannot release a
	// worker waiting in select.
	generation       uint64
	armed            bool
	normalTimer      ports.Timer
	normalCancel     chan struct{}
	normalWorkerDone chan struct{}
	// syncBatches is keyed by the stable pane owner. Each pane owns an
	// independent watchdog; composition remains gated until every batch ends.
	syncBatches map[*pane]*syncBatch
	// syncRegistryVersion changes for every batch identity mutation. fire
	// snapshots it before evaluating visibility and retries if a producer
	// publishes or completes a batch during that unlocked evaluation.
	syncRegistryVersion uint64
}

// newRenderCoordinator constructs a coordinator bound to opts. It starts no
// goroutine; deadline scheduling is armed lazily by invalidate.
func newRenderCoordinator(opts renderCoordinatorOptions) *renderCoordinator {
	return &renderCoordinator{opts: opts}
}

// detachNormalTimerLocked releases coordinator ownership. The caller must stop
// the returned timer after dropping c.mu: Timer methods are external callbacks.
func (c *renderCoordinator) detachNormalTimerLocked() ports.Timer {
	timer := c.normalTimer
	c.normalTimer = nil
	if c.normalCancel != nil {
		close(c.normalCancel)
		c.normalCancel = nil
	}
	return timer
}

func stopTimer(timer ports.Timer) {
	if timer != nil {
		timer.Stop()
	}
}

type syncBatch struct {
	generation uint64
	// renderable is evaluated outside c.mu: tab/session visibility locks may be
	// acquired by this predicate.
	renderable func() bool
	force      func()
	timer      ports.Timer
	cancel     chan struct{}
	done       chan struct{}
}

func detachSyncBatchLocked(batch *syncBatch) ports.Timer {
	timer := batch.timer
	batch.timer = nil
	if batch.cancel != nil {
		close(batch.cancel)
		batch.cancel = nil
	}
	return timer
}

func (c *renderCoordinator) detachSyncTimersLocked() []ports.Timer {
	timers := make([]ports.Timer, 0, len(c.syncBatches))
	for pane, batch := range c.syncBatches {
		timers = append(timers, detachSyncBatchLocked(batch))
		delete(c.syncBatches, pane)
		c.syncRegistryVersion++
	}
	return timers
}

func stopTimers(timers []ports.Timer) {
	for _, timer := range timers {
		stopTimer(timer)
	}
}

// invalidate publishes one producer state transition. Consecutive
// invalidations coalesce under a single armed deadline; the latest state and
// the stickiest reset win.
func (c *renderCoordinator) invalidate(inv renderInvalidation) {
	c.mu.Lock()
	if c.torndown || c.detached {
		c.mu.Unlock()
		return
	}
	onInvalidate := c.opts.onInvalidate
	c.metrics.invalidations.Add(1)
	wasPending, wasUrgent, wasPreviewPending := c.pending, c.pendingUrgent, c.pendingPreview
	if !wasPending {
		c.ackDeferred = false
		c.deadlineDue = false
	}
	c.pending = true
	c.pendingReset = c.pendingReset || inv.reset
	c.pendingUrgent = c.pendingUrgent || inv.class == invalidateUrgent
	c.pendingPreview = c.pendingPreview || c.previewWake != nil || len(c.previewWakes) != 0
	c.coalesced++
	arm := !wasPending || (!wasUrgent && c.pendingUrgent) || (!wasPreviewPending && c.pendingPreview)
	var old ports.Timer
	if arm {
		c.generation++
		old = c.detachNormalTimerLocked()
	}
	gen := c.generation
	delay := minOutputRenderDeadline + time.Duration(c.outputPressure)*time.Millisecond
	if c.pendingUrgent {
		delay = urgentRenderDeadline
	}
	clock := c.opts.clock
	c.armed = c.armed || arm
	c.mu.Unlock()
	stopTimer(old)
	if onInvalidate != nil {
		onInvalidate(inv)
	}
	if !arm || clock == nil {
		return
	}

	// NewTimer and C may re-enter the coordinator. Publish only if this
	// generation is still the reserved deadline after those external calls.
	timer := clock.NewTimer(delay)
	timerC := timer.C()
	cancel := make(chan struct{})
	done := make(chan struct{})
	c.mu.Lock()
	valid := !c.torndown && c.pending && c.generation == gen && c.normalTimer == nil
	if valid && timerC != nil {
		c.normalTimer, c.normalCancel, c.normalWorkerDone = timer, cancel, done
	}
	c.mu.Unlock()
	if !valid {
		stopTimer(timer)
		return
	}
	if timerC == nil {
		stopTimer(timer)
		c.fire(gen, false, true)
		return
	}
	go func() {
		defer close(done)
		select {
		case <-timerC:
			c.markDeadlineDue(gen)
			c.fire(gen, false, true)
		case <-cancel:
		}
	}()
	runtime.Gosched()
}

// notifyAck reports that the client acknowledged an output state, releasing
// at most one deadline/watchdog-deferred wake. It never bypasses an unexpired
// normal or urgent deadline.
func (c *renderCoordinator) notifyAck() {
	c.mu.Lock()
	if (!c.ackDeferred && !c.deadlineDue) || !c.pending {
		c.mu.Unlock()
		return
	}
	gen := c.generation
	c.mu.Unlock()
	c.fire(gen, false, false)
}

// noteSyncBegin records a synchronized-output batch for its stable pane and
// arms that pane's watchdog. Overlapping pane batches are independent.
// markDeadlineDue records a received timer tick before the worker probes ACK
// readiness, so a concurrent ACK cannot miss an already-expired deadline.
func (c *renderCoordinator) markDeadlineDue(gen uint64) {
	c.mu.Lock()
	if c.fireValidLocked(gen, false) {
		c.deadlineDue = true
	}
	c.mu.Unlock()
}

func (c *renderCoordinator) noteSyncBegin(p *pane, gen uint64, force ...func()) {
	var renderable func() bool
	if c.opts.syncRenderable != nil {
		renderable = func() bool { return c.opts.syncRenderable(p) }
	}
	c.noteSyncBeginWithRenderability(p, gen, renderable, force...)
}

// noteSyncBeginWithRenderability records lifecycle unconditionally while the
// supplied predicate decides dynamically whether this batch gates composition.
func (c *renderCoordinator) noteSyncBeginWithRenderability(p *pane, gen uint64, renderable func() bool, force ...func()) {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return
	}
	if c.syncBatches == nil {
		c.syncBatches = make(map[*pane]*syncBatch)
	}
	var old ports.Timer
	if previous := c.syncBatches[p]; previous != nil {
		old = detachSyncBatchLocked(previous)
	}
	batch := &syncBatch{generation: gen, renderable: renderable}
	if len(force) != 0 {
		batch.force = force[0]
	}
	c.syncBatches[p] = batch
	c.syncRegistryVersion++
	clock := c.opts.clock
	c.mu.Unlock()
	stopTimer(old)
	if clock == nil {
		return
	}

	// Both timer operations are external. A reentrant callback may replace or
	// remove batch; identity validation below rejects this stale timer.
	timer := clock.NewTimer(maxSyncUpdateDuration)
	timerC := timer.C()
	cancel := make(chan struct{})
	done := make(chan struct{})
	c.mu.Lock()
	valid := !c.torndown && c.syncBatches[p] == batch && batch.generation == gen
	if valid {
		batch.timer, batch.cancel, batch.done = timer, cancel, done
	}
	c.mu.Unlock()
	if !valid {
		stopTimer(timer)
		return
	}
	go func() {
		defer close(done)
		select {
		case <-timerC:
		case <-cancel:
			return
		}
		// Keep the batch registered while force runs. A concurrent render must
		// continue to observe the synchronized-output gate until the VT has
		// authoritatively closed the batch.
		c.mu.Lock()
		current := c.syncBatches[p]
		valid := !c.torndown && current == batch && current.generation == gen
		c.mu.Unlock()
		if !valid {
			return
		}
		if batch.force != nil {
			batch.force()
		}

		// force may synchronously end this batch, replace it with a newer
		// generation, or tear down the coordinator. Remove only the exact
		// snapshot that expired, then publish one urgent completion wake.
		c.mu.Lock()
		current = c.syncBatches[p]
		valid = !c.torndown && current == batch && current.generation == gen
		var stopped ports.Timer
		if valid {
			delete(c.syncBatches, p)
			c.syncRegistryVersion++
			stopped = detachSyncBatchLocked(batch)
			if len(c.syncBatches) == 0 && c.pending {
				c.pendingUrgent = true
			}
		}
		c.mu.Unlock()
		if valid {
			stopTimer(stopped)
			// Reevaluate the aggregate gate only after forcing this pane.
			c.fireCurrent(true)
		}
	}()
}

// noteSyncEnd records completion for exactly one pane batch. It only flushes
// after the aggregate gate opens.
func (c *renderCoordinator) noteSyncEnd(p *pane, gen uint64) {
	// Visibility may acquire session/tab locks, so evaluate it outside c.mu.
	c.mu.Lock()
	batch := c.syncBatches[p]
	if batch == nil || batch.generation != gen {
		c.mu.Unlock()
		return
	}
	renderable := batch.renderable
	c.mu.Unlock()
	wasRenderable := renderable == nil || renderable()
	if c.removeSyncEnd(p, gen, wasRenderable) {
		c.fireCurrent(false)
	}
}

// removeSyncEnd removes a batch without invoking a predicate or callback. PTY
// processing calls it while pane.mu is still held, so a deadline can never
// compose the post-Write partial screen between mutation and publication.
// The caller must invoke fireCurrent only after releasing pane.mu.
func (c *renderCoordinator) removeSyncEnd(p *pane, gen uint64, renderable bool) bool {
	c.mu.Lock()
	batch := c.syncBatches[p]
	if batch == nil || batch.generation != gen {
		c.mu.Unlock()
		return false
	}
	delete(c.syncBatches, p)
	c.syncRegistryVersion++
	timer := detachSyncBatchLocked(batch)
	if renderable && len(c.syncBatches) == 0 && c.pending {
		c.pendingUrgent = true
	}
	c.mu.Unlock()
	stopTimer(timer)
	return renderable
}

// noteSyncPaneRemoved releases a pane watchdog when pane lifecycle ends.
func (c *renderCoordinator) noteSyncPaneRemoved(p *pane) {
	c.mu.Lock()
	var timer ports.Timer
	if batch := c.syncBatches[p]; batch != nil {
		delete(c.syncBatches, p)
		c.syncRegistryVersion++
		timer = detachSyncBatchLocked(batch)
	}
	c.mu.Unlock()
	stopTimer(timer)
}

// subscribePreview installs fn as the preview observer for coalesced wakes.
func (c *renderCoordinator) subscribePreview(fn func(renderWake)) {
	c.mu.Lock()
	c.previewWake = fn
	c.mu.Unlock()
}

// teardownPreview removes the legacy preview subscription.
func (c *renderCoordinator) teardownPreview() { c.mu.Lock(); c.previewWake = nil; c.mu.Unlock() }

// subscribePreviewFor installs the dynamic picker observer owned by viewer.
// The owner key makes target changes, detach, and session teardown precise.
func (c *renderCoordinator) subscribePreviewFor(viewer *attachedClient, fn func(renderWake)) {
	if viewer == nil || fn == nil {
		return
	}
	c.mu.Lock()
	if !c.torndown {
		if c.previewWakes == nil {
			c.previewWakes = make(map[*attachedClient]func(renderWake))
		}
		c.previewWakes[viewer] = fn
	}
	c.mu.Unlock()
}

// teardownPreviewFor removes only viewer's dynamic picker observer.
func (c *renderCoordinator) teardownPreviewFor(viewer *attachedClient) {
	c.mu.Lock()
	delete(c.previewWakes, viewer)
	c.mu.Unlock()
}

// attach binds ac as the coordinator's current attachment identity.
func (c *renderCoordinator) attach(ac *attachedClient) {
	c.mu.Lock()
	if !c.torndown {
		c.attachment = ac
		c.detached = false
	}
	c.mu.Unlock()
}

// noteDetach invalidates pending wakes and stale callbacks for a detaching
// attachment.
func (c *renderCoordinator) noteDetach(ac *attachedClient) {
	c.mu.Lock()
	var timer ports.Timer
	var timers []ports.Timer
	if c.attachment == ac {
		c.attachment = nil
		c.detached = true
		c.pending = false
		c.ackDeferred = false
		c.pendingPreview = false
		c.generation++
		c.armed = false
		timer = c.detachNormalTimerLocked()
		timers = c.detachSyncTimersLocked()
	}
	c.mu.Unlock()
	stopTimer(timer)
	stopTimers(timers)
}

// noteReplace hands the coordinator from old to replacement; callbacks
// captured by old become stale.
func (c *renderCoordinator) noteReplace(old, replacement *attachedClient) {
	c.mu.Lock()
	// A coordinator may be installed while replacing a legacy attachment
	// which predated coordinator ownership. In that case nil has no pending
	// identity to invalidate, and the replacement becomes the first bound
	// attachment atomically with this lifecycle transition.
	var timer ports.Timer
	var timers []ports.Timer
	if c.attachment == old || c.attachment == nil {
		c.attachment = replacement
		c.detached = false
		c.pending = false
		c.ackDeferred = false
		c.pendingPreview = false
		c.generation++
		c.armed = false
		timer = c.detachNormalTimerLocked()
		timers = c.detachSyncTimersLocked()
	}
	c.mu.Unlock()
	stopTimer(timer)
	stopTimers(timers)
}

// notePark invalidates pending wakes when the attachment parks for resume.
func (c *renderCoordinator) notePark(ac *attachedClient) { c.noteDetach(ac) }

// noteSessionTeardown terminally invalidates the coordinator.
func (c *renderCoordinator) noteSessionTeardown() {
	c.mu.Lock()
	c.torndown = true
	c.attachment = nil
	c.previewWake = nil
	c.previewWakes = nil
	c.pending = false
	c.ackDeferred = false
	c.pendingPreview = false
	c.generation++
	c.armed = false
	timer := c.detachNormalTimerLocked()
	timers := c.detachSyncTimersLocked()
	c.mu.Unlock()
	stopTimer(timer)
	stopTimers(timers)
}

// recordResizeRequest records the latest requested geometry and source before
// the request delegates to the retained PR #71 attachment path. It returns
// the strictly monotonically increased epoch, or 0 when the source is stale.
func (c *renderCoordinator) recordResizeRequest(size domain.Size, source *attachedClient) uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.torndown || c.attachment != source {
		return 0
	}
	c.resize.epoch++
	c.resize.size = size
	c.resize.source = source
	return c.resize.epoch
}

// resizeSnapshot returns a locked copy of the latest resize metadata.
func (c *renderCoordinator) resizeSnapshot() resizeRequestMetadata {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.resize
}

// burstMetricsSnapshot returns a consistent read-only metric view for
// benchmark reporting without exposing coordinator policy outside this package.
func (c *renderCoordinator) burstMetricsSnapshot() renderCoordinatorBurstMetricsSnapshot {
	return renderCoordinatorBurstMetricsSnapshot{
		invalidations: c.metrics.invalidations.Load(),
		wakes:         c.metrics.wakes.Load(),
		coalesced:     c.metrics.coalesced.Load(),
	}
}

// renderCoordinator returns the coordinator installed for this session.
func (s *session) renderCoordinator() *renderCoordinator { return s.coordinator.Load() }

// installRenderCoordinator publishes rc as the session's coordinator.
func (s *session) installRenderCoordinator(rc *renderCoordinator) { s.coordinator.Store(rc) }

func (c *renderCoordinator) fireCurrent(watchdog bool) {
	c.mu.Lock()
	gen := c.generation
	c.mu.Unlock()
	c.fire(gen, watchdog, watchdog)
}

// invalidateRender is the sole producer fan-in. In tests and transitional
// headless paths without an attached coordinator it retains the old private
// compositor; attached sessions always schedule through their coordinator.
func (d *Daemon) invalidateRender(sess *session, ac *attachedClient, reset bool, producer string) {
	if sess != nil {
		if rc := sess.renderCoordinator(); rc != nil {
			rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: reset, producer: producer})
			return
		}
	}
	if ac != nil {
		d.paint(sess, ac, reset)
	}
}

// invalidateRenderNow publishes through the coordinator but immediately
// flushes the wake when the client can accept state. Attach uses this path so
// the required first full frame never depends on a debounce timer.
func (d *Daemon) invalidateRenderNow(sess *session, ac *attachedClient, reset bool, producer string) {
	if sess != nil {
		if rc := sess.renderCoordinator(); rc != nil {
			rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: reset, producer: producer})
			rc.fireCurrent(false)
			return
		}
	}
	if ac != nil {
		d.paint(sess, ac, reset)
	}
}

func (c *renderCoordinator) fire(gen uint64, watchdog, deadline bool) {
	// Readiness probes enter the attachment send path, which may in turn read
	// coordinator metadata. Never hold c.mu across that external callback.
	c.mu.Lock()
	if !c.fireValidLocked(gen, watchdog) {
		c.mu.Unlock()
		return
	}
	ackReady := c.opts.ackReady
	c.mu.Unlock()
	ready := ackReady == nil || ackReady()

	// The readiness callback can detach, replace, or publish a sync batch.
	// syncGateOpenLocked retries predicate evaluation on every registry change
	// and returns holding c.mu, so pending consumption has no intervening gap.
	c.mu.Lock()
	if !c.fireValidLocked(gen, watchdog) {
		c.mu.Unlock()
		return
	}
	if !c.syncGateOpenLocked() {
		c.mu.Unlock()
		return
	}
	if !c.fireValidLocked(gen, watchdog) {
		c.mu.Unlock()
		return
	}
	w := renderWake{reset: c.pendingReset, urgent: c.pendingUrgent, coalesced: c.coalesced, watchdog: watchdog, attachment: c.attachment}
	if !ready {
		if deadline {
			c.ackDeferred = true
		}
		preview, previews := c.takePendingPreviewsLocked()
		c.mu.Unlock()
		c.notifyPreviews(w, preview, previews)
		return
	}
	preview, previews := c.takePendingPreviewsLocked()
	c.metrics.wakes.Add(1)
	c.metrics.coalesced.Add(uint64(w.coalesced))
	if !w.urgent {
		if w.coalesced > 1 {
			c.outputPressure += w.coalesced - 1
			if c.outputPressure > int((maxOutputRenderDeadline-minOutputRenderDeadline)/time.Millisecond) {
				c.outputPressure = int((maxOutputRenderDeadline - minOutputRenderDeadline) / time.Millisecond)
			}
		} else if c.outputPressure > 0 {
			c.outputPressure--
		}
	}
	timer := c.detachNormalTimerLocked()
	c.pending, c.pendingReset, c.pendingUrgent, c.ackDeferred, c.deadlineDue, c.coalesced, c.armed = false, false, false, false, false, 0, false
	wake := c.opts.wake
	c.mu.Unlock()
	stopTimer(timer)
	if wake != nil {
		wake(w)
	}
	c.notifyPreviews(w, preview, previews)
}

func (c *renderCoordinator) fireValidLocked(gen uint64, watchdog bool) bool {
	return !c.torndown && c.pending && (watchdog || gen == c.generation)
}

// syncGateOpen evaluates pane visibility without c.mu, then reacquires and
// validates the complete batch identity set before accepting that result.
// syncGateOpen evaluates visibility outside c.mu and returns with c.mu held
// only when the registry version still matches the snapshot. The caller must
// consume pending state before unlocking, eliminating the gate-to-consume gap.
func (c *renderCoordinator) syncGateOpenLocked() bool {
	for {
		version := c.syncRegistryVersion
		batches := make([]*syncBatch, 0, len(c.syncBatches))
		for _, batch := range c.syncBatches {
			batches = append(batches, batch)
		}
		c.mu.Unlock()

		gated := false
		for _, batch := range batches {
			if batch.renderable == nil || batch.renderable() {
				gated = true
				break
			}
		}
		if hook := c.opts.afterSyncGateEvaluated; hook != nil {
			hook()
		}

		c.mu.Lock()
		if version == c.syncRegistryVersion {
			return !gated
		}
	}
}

// takePendingPreviewsLocked snapshots and consumes the preview delivery for
// this target generation. Unlike the target primary, this is never ACK-gated.
func (c *renderCoordinator) takePendingPreviewsLocked() (func(renderWake), []func(renderWake)) {
	if !c.pendingPreview {
		return nil, nil
	}
	c.pendingPreview = false
	previews := make([]func(renderWake), 0, len(c.previewWakes))
	for _, fn := range c.previewWakes {
		previews = append(previews, fn)
	}
	return c.previewWake, previews
}

func (c *renderCoordinator) notifyPreviews(w renderWake, preview func(renderWake), previews []func(renderWake)) {
	if preview != nil {
		preview(w)
	}
	for _, fn := range previews {
		fn(w)
	}
}
