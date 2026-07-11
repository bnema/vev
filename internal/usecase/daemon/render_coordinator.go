package daemon

import (
	"runtime"
	"sync"
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
type renderCoordinator struct {
	mu   sync.Mutex
	opts renderCoordinatorOptions

	// Coalesced pending state: latest wins, reset stays sticky, and at most
	// one deadline wake may be armed at a time (cap-1).
	pending       bool
	pendingReset  bool
	pendingUrgent bool
	coalesced     int

	// previewWake receives the same coalesced wakes while a preview
	// subscription is installed.
	previewWake func(renderWake)

	// attachment is the currently bound client identity; callbacks from any
	// other identity are stale and must not mutate coordinator state.
	attachment *attachedClient
	torndown   bool

	resize resizeRequestMetadata
	// generation invalidates callbacks from superseded deadline/watchdog timers.
	generation     uint64
	armed          bool
	syncGeneration uint64
}

// newRenderCoordinator constructs a coordinator bound to opts. It starts no
// goroutine; deadline scheduling is armed lazily by invalidate.
func newRenderCoordinator(opts renderCoordinatorOptions) *renderCoordinator {
	return &renderCoordinator{opts: opts}
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
	wasPending, wasUrgent := c.pending, c.pendingUrgent
	c.pending = true
	c.pendingReset = c.pendingReset || inv.reset
	c.pendingUrgent = c.pendingUrgent || inv.class == invalidateUrgent
	c.coalesced++
	// Cap one normal deadline; an urgent transition may supersede it.
	arm := !wasPending || (!wasUrgent && c.pendingUrgent)
	if arm {
		c.generation++
	}
	gen := c.generation
	delay := minOutputRenderDeadline
	if c.pendingUrgent {
		delay = urgentRenderDeadline
	}
	clock := c.opts.clock
	c.armed = c.armed || arm
	c.mu.Unlock()
	if !arm || clock == nil {
		return
	}
	timer := clock.NewTimer(delay)
	go func() { <-timer.C(); c.fire(gen, false) }()
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
// arms the completion watchdog.
func (c *renderCoordinator) noteSyncBegin(gen uint64) {
	c.mu.Lock()
	if c.torndown {
		c.mu.Unlock()
		return
	}
	c.syncGeneration = gen
	clock := c.opts.clock
	c.mu.Unlock()
	if clock == nil {
		return
	}
	timer := clock.NewTimer(maxSyncUpdateDuration)
	go func() {
		<-timer.C()
		c.mu.Lock()
		valid := !c.torndown && c.syncGeneration == gen
		c.mu.Unlock()
		if valid {
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
	c.mu.Unlock()
	c.fireCurrent(false)
}

// subscribePreview installs fn as the preview observer for coalesced wakes.
func (c *renderCoordinator) subscribePreview(fn func(renderWake)) {
	c.mu.Lock()
	c.previewWake = fn
	c.mu.Unlock()
}

// teardownPreview removes the preview subscription.
func (c *renderCoordinator) teardownPreview() { c.mu.Lock(); c.previewWake = nil; c.mu.Unlock() }

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
	c.pending = false
	c.generation++
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
	d.paint(sess, ac, reset)
}

func (c *renderCoordinator) fire(gen uint64, watchdog bool) {
	c.mu.Lock()
	if c.torndown || !c.pending || (!watchdog && gen != c.generation) {
		c.mu.Unlock()
		return
	}
	if !watchdog && c.opts.syncActive != nil && c.opts.syncActive() {
		c.mu.Unlock()
		return
	}
	if c.opts.ackReady != nil && !c.opts.ackReady() {
		c.mu.Unlock()
		return
	}
	w := renderWake{reset: c.pendingReset, urgent: c.pendingUrgent, coalesced: c.coalesced, watchdog: watchdog}
	c.pending, c.pendingReset, c.pendingUrgent, c.coalesced, c.armed = false, false, false, 0, false
	wake, preview := c.opts.wake, c.previewWake
	c.mu.Unlock()
	if wake != nil {
		wake(w)
	}
	if preview != nil {
		preview(w)
	}
}
