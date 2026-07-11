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
	// syncActive reports whether a synchronized-output batch is open.
	syncActive func() bool
	// onInvalidate observes every published invalidation (test-visible hook).
	onInvalidate func(renderInvalidation)
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
	torndown   bool

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
}

// newRenderCoordinator constructs a coordinator bound to opts. It starts no
// goroutine; deadline scheduling is armed lazily by invalidate.
func newRenderCoordinator(opts renderCoordinatorOptions) *renderCoordinator {
	return &renderCoordinator{opts: opts}
}

// cancelNormalTimerLocked and cancelSyncTimerLocked release timer workers even
// when a test clock supplies an inert channel. Caller holds c.mu.
func (c *renderCoordinator) cancelNormalTimerLocked() {
	if c.normalTimer != nil {
		c.normalTimer.Stop()
		c.normalTimer = nil
	}
	if c.normalCancel != nil {
		close(c.normalCancel)
		c.normalCancel = nil
	}
}

type syncBatch struct {
	generation uint64
	force      func()
	timer      ports.Timer
	cancel     chan struct{}
	done       chan struct{}
}

func cancelSyncBatchLocked(batch *syncBatch) {
	if batch.timer != nil {
		batch.timer.Stop()
		batch.timer = nil
	}
	if batch.cancel != nil {
		close(batch.cancel)
		batch.cancel = nil
	}
}

func (c *renderCoordinator) cancelSyncTimersLocked() {
	for pane, batch := range c.syncBatches {
		cancelSyncBatchLocked(batch)
		delete(c.syncBatches, pane)
	}
}

// invalidate publishes one producer state transition. Consecutive
// invalidations coalesce under a single armed deadline; the latest state and
// the stickiest reset win.
func (c *renderCoordinator) invalidate(inv renderInvalidation) {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return
	}
	if c.opts.onInvalidate != nil {
		c.opts.onInvalidate(inv)
	}
	c.metrics.invalidations.Add(1)
	wasPending, wasUrgent, wasPreviewPending := c.pending, c.pendingUrgent, c.pendingPreview
	if !wasPending {
		c.ackDeferred = false
	}
	c.pending = true
	c.pendingReset = c.pendingReset || inv.reset
	c.pendingUrgent = c.pendingUrgent || inv.class == invalidateUrgent
	c.pendingPreview = c.pendingPreview || c.previewWake != nil || len(c.previewWakes) != 0
	c.coalesced++
	// Cap one normal deadline for each primary or preview batch. Once a
	// preview has been delivered while the target is ACK-blocked, a later
	// target transition needs its own deadline even though the primary remains
	// pending.
	arm := !wasPending || (!wasUrgent && c.pendingUrgent) || (!wasPreviewPending && c.pendingPreview)
	if arm {
		c.generation++
		c.cancelNormalTimerLocked()
	}
	gen := c.generation
	delay := minOutputRenderDeadline + time.Duration(c.outputPressure)*time.Millisecond
	if c.pendingUrgent {
		// Urgent state always supersedes bulk pressure and is never extended.
		delay = urgentRenderDeadline
	}
	clock := c.opts.clock
	c.armed = c.armed || arm
	if !arm || clock == nil {
		c.mu.Unlock()
		return
	}
	timer := clock.NewTimer(delay)
	timerC := timer.C()
	// A nil timer channel is inert, so it cannot need cancellation. Complete
	// synchronously before allocating a worker or its coordination channels.
	if timerC == nil {
		c.mu.Unlock()
		timer.Stop()
		c.fire(gen, false, true)
		return
	}
	cancel := make(chan struct{})
	done := make(chan struct{})
	c.normalTimer, c.normalCancel, c.normalWorkerDone = timer, cancel, done
	c.mu.Unlock()
	go func() {
		defer close(done)
		select {
		case <-timerC:
			c.fire(gen, false, true)
		case <-cancel:
		}
	}()
	// Give fake-clock callbacks a chance to block on C before the test
	// advances it; real clocks are unaffected.
	runtime.Gosched()
}

// notifyAck reports that the client acknowledged an output state, releasing
// at most one deadline/watchdog-deferred wake. It never bypasses an unexpired
// normal or urgent deadline.
func (c *renderCoordinator) notifyAck() {
	c.mu.Lock()
	if !c.ackDeferred || !c.pending {
		c.mu.Unlock()
		return
	}
	gen := c.generation
	c.mu.Unlock()
	c.fire(gen, false, false)
}

// noteSyncBegin records a synchronized-output batch for its stable pane and
// arms that pane's watchdog. Overlapping pane batches are independent.
func (c *renderCoordinator) noteSyncBegin(p *pane, gen uint64, force ...func()) {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return
	}
	if c.syncBatches == nil {
		c.syncBatches = make(map[*pane]*syncBatch)
	}
	if old := c.syncBatches[p]; old != nil {
		cancelSyncBatchLocked(old)
	}
	batch := &syncBatch{generation: gen}
	if len(force) != 0 {
		batch.force = force[0]
	}
	c.syncBatches[p] = batch
	clock := c.opts.clock
	if clock == nil {
		c.mu.Unlock()
		return
	}
	batch.timer = clock.NewTimer(maxSyncUpdateDuration)
	batch.cancel = make(chan struct{})
	batch.done = make(chan struct{})
	timer, cancel, done := batch.timer, batch.cancel, batch.done
	c.mu.Unlock()
	go func() {
		defer close(done)
		select {
		case <-timer.C():
		case <-cancel:
			return
		}
		c.mu.Lock()
		current := c.syncBatches[p]
		valid := !c.torndown && current == batch && current.generation == gen
		if valid {
			delete(c.syncBatches, p)
			cancelSyncBatchLocked(batch)
			if len(c.syncBatches) == 0 && c.pending {
				c.pendingUrgent = true
			}
		}
		c.mu.Unlock()
		if valid {
			if batch.force != nil {
				batch.force()
			}
			// Reevaluate the aggregate gate only after forcing this pane.
			c.fireCurrent(true)
		}
	}()
}

// noteSyncEnd records completion for exactly one pane batch. It only flushes
// after the aggregate gate opens.
func (c *renderCoordinator) noteSyncEnd(p *pane, gen uint64) {
	c.mu.Lock()
	batch := c.syncBatches[p]
	if batch == nil || batch.generation != gen {
		c.mu.Unlock()
		return
	}
	delete(c.syncBatches, p)
	cancelSyncBatchLocked(batch)
	if len(c.syncBatches) == 0 && c.pending {
		c.pendingUrgent = true
	}
	c.mu.Unlock()
	c.fireCurrent(false)
}

// noteSyncPaneRemoved releases a pane watchdog when pane lifecycle ends.
func (c *renderCoordinator) noteSyncPaneRemoved(p *pane) {
	c.mu.Lock()
	if batch := c.syncBatches[p]; batch != nil {
		delete(c.syncBatches, p)
		cancelSyncBatchLocked(batch)
	}
	c.mu.Unlock()
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
	}
	c.mu.Unlock()
}

// noteDetach invalidates pending wakes and stale callbacks for a detaching
// attachment.
func (c *renderCoordinator) noteDetach(ac *attachedClient) {
	c.mu.Lock()
	if c.attachment == ac {
		c.attachment = nil
		c.pending = false
		c.ackDeferred = false
		c.pendingPreview = false
		c.generation++
		c.armed = false
		c.cancelNormalTimerLocked()
		c.cancelSyncTimersLocked()
	}
	c.mu.Unlock()
}

// noteReplace hands the coordinator from old to replacement; callbacks
// captured by old become stale.
func (c *renderCoordinator) noteReplace(old, replacement *attachedClient) {
	c.mu.Lock()
	if c.attachment == old {
		c.attachment = replacement
		c.pending = false
		c.ackDeferred = false
		c.pendingPreview = false
		c.generation++
		c.armed = false
		c.cancelNormalTimerLocked()
		c.cancelSyncTimersLocked()
	}
	c.mu.Unlock()
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
	c.cancelNormalTimerLocked()
	c.cancelSyncTimersLocked()
	c.mu.Unlock()
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
	c.mu.Lock()
	if c.torndown || !c.pending || (!watchdog && gen != c.generation) {
		c.mu.Unlock()
		return
	}
	if len(c.syncBatches) != 0 {
		c.mu.Unlock()
		return
	}
	w := renderWake{reset: c.pendingReset, urgent: c.pendingUrgent, coalesced: c.coalesced, watchdog: watchdog}
	preview, previews := c.takePendingPreviewsLocked()
	if c.opts.ackReady != nil && !c.opts.ackReady() {
		if deadline {
			c.ackDeferred = true
		}
		c.mu.Unlock()
		c.notifyPreviews(w, preview, previews)
		return
	}
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
	c.cancelNormalTimerLocked()
	c.pending, c.pendingReset, c.pendingUrgent, c.ackDeferred, c.coalesced, c.armed = false, false, false, false, 0, false
	wake := c.opts.wake
	c.mu.Unlock()
	if wake != nil {
		wake(w)
	}
	c.notifyPreviews(w, preview, previews)
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
