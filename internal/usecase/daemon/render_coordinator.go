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
	coalesced     int
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
	syncGeneration   uint64
	syncWatchdog     func()
	syncTimer        ports.Timer
	syncCancel       chan struct{}
	syncWorkerDone   chan struct{}
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

func (c *renderCoordinator) cancelSyncTimerLocked() {
	if c.syncTimer != nil {
		c.syncTimer.Stop()
		c.syncTimer = nil
	}
	if c.syncCancel != nil {
		close(c.syncCancel)
		c.syncCancel = nil
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
	wasPending, wasUrgent := c.pending, c.pendingUrgent
	c.pending = true
	c.pendingReset = c.pendingReset || inv.reset
	c.pendingUrgent = c.pendingUrgent || inv.class == invalidateUrgent
	c.coalesced++
	// Cap one normal deadline; an urgent transition may supersede it.
	arm := !wasPending || (!wasUrgent && c.pendingUrgent)
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
	cancel := make(chan struct{})
	done := make(chan struct{})
	c.normalTimer, c.normalCancel, c.normalWorkerDone = timer, cancel, done
	c.mu.Unlock()
	go func() {
		defer close(done)
		select {
		case <-timer.C():
			c.fire(gen, false)
		case <-cancel:
		}
	}()
	// A nil timer channel is an inert test clock, not a schedulable deadline.
	// Complete through the coordinator so manual daemon fixtures retain their
	// deterministic synchronous contract without a direct paint fallback.
	if timer.C() == nil {
		c.fire(gen, false)
		return
	}
	// Give fake-clock callbacks a chance to block on C before the test
	// advances it; real clocks are unaffected.
	runtime.Gosched()
}

// notifyAck reports that the client acknowledged an output state, releasing
// at most one deferred wake.
func (c *renderCoordinator) notifyAck() {
	c.mu.Lock()
	gen := c.generation
	c.mu.Unlock()
	c.fire(gen, false)
}

// noteSyncBegin records that a synchronized-output batch opened for gen and
// arms the completion watchdog. force is supplied by the pane owner so the
// coordinator can end a wedged VT batch before it composes the pending state.
func (c *renderCoordinator) noteSyncBegin(gen uint64, force ...func()) {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return
	}
	c.cancelSyncTimerLocked()
	c.syncGeneration = gen
	c.syncWatchdog = nil
	if len(force) != 0 {
		c.syncWatchdog = force[0]
	}
	clock := c.opts.clock
	if clock == nil {
		c.mu.Unlock()
		return
	}
	timer := clock.NewTimer(maxSyncUpdateDuration)
	cancel := make(chan struct{})
	done := make(chan struct{})
	c.syncTimer, c.syncCancel, c.syncWorkerDone = timer, cancel, done
	c.mu.Unlock()
	go func() {
		defer close(done)
		select {
		case <-timer.C():
		case <-cancel:
			return
		}
		c.mu.Lock()
		valid := !c.torndown && c.syncGeneration == gen
		force := c.syncWatchdog
		if valid {
			c.syncGeneration = 0
			c.syncWatchdog = nil
			c.cancelSyncTimerLocked()
		}
		c.mu.Unlock()
		if valid {
			if force != nil {
				force()
			}
			c.fireCurrent(true)
		}
	}()
}

// noteSyncEnd records that the batch for gen completed, flushing pending
// state in one wake.
func (c *renderCoordinator) noteSyncEnd(gen uint64) {
	c.mu.Lock()
	if c.syncGeneration != gen {
		c.mu.Unlock()
		return
	}
	c.syncGeneration = 0
	c.syncWatchdog = nil
	c.cancelSyncTimerLocked()
	c.mu.Unlock()
	c.fireCurrent(false)
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
		c.generation++
		c.armed = false
		c.cancelNormalTimerLocked()
		c.cancelSyncTimerLocked()
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
		c.generation++
		c.armed = false
		c.cancelNormalTimerLocked()
		c.cancelSyncTimerLocked()
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
	c.generation++
	c.armed = false
	c.cancelNormalTimerLocked()
	c.cancelSyncTimerLocked()
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
	c.fire(gen, watchdog)
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

func (c *renderCoordinator) fire(gen uint64, watchdog bool) {
	c.mu.Lock()
	if c.torndown || !c.pending || (!watchdog && gen != c.generation) {
		c.mu.Unlock()
		return
	}
	if !watchdog && c.syncGeneration != 0 {
		c.mu.Unlock()
		return
	}
	if c.opts.ackReady != nil && !c.opts.ackReady() {
		c.mu.Unlock()
		return
	}
	w := renderWake{reset: c.pendingReset, urgent: c.pendingUrgent, coalesced: c.coalesced, watchdog: watchdog}
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
	c.pending, c.pendingReset, c.pendingUrgent, c.coalesced, c.armed = false, false, false, 0, false
	wake, preview := c.opts.wake, c.previewWake
	previews := make([]func(renderWake), 0, len(c.previewWakes))
	for _, fn := range c.previewWakes {
		previews = append(previews, fn)
	}
	c.mu.Unlock()
	if wake != nil {
		wake(w)
	}
	if preview != nil {
		preview(w)
	}
	for _, fn := range previews {
		fn(w)
	}
}
