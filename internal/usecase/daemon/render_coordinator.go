package daemon

import (
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
}

// newRenderCoordinator constructs a coordinator bound to opts. It starts no
// goroutine; deadline scheduling is armed lazily by invalidate.
func newRenderCoordinator(opts renderCoordinatorOptions) *renderCoordinator {
	return &renderCoordinator{opts: opts}
}

// invalidate publishes one producer state transition. Consecutive
// invalidations coalesce under a single armed deadline; the latest state and
// the stickiest reset win.
func (c *renderCoordinator) invalidate(renderInvalidation) {}

// notifyAck reports that the client acknowledged an output state, releasing
// at most one deferred wake.
func (c *renderCoordinator) notifyAck() {}

// noteSyncBegin records that a synchronized-output batch opened for gen and
// arms the completion watchdog.
func (c *renderCoordinator) noteSyncBegin(uint64) {}

// noteSyncEnd records that the batch for gen completed, flushing pending
// state in one wake.
func (c *renderCoordinator) noteSyncEnd(uint64) {}

// subscribePreview installs fn as the preview observer for coalesced wakes.
func (c *renderCoordinator) subscribePreview(func(renderWake)) {}

// teardownPreview removes the preview subscription.
func (c *renderCoordinator) teardownPreview() {}

// attach binds ac as the coordinator's current attachment identity.
func (c *renderCoordinator) attach(*attachedClient) {}

// noteDetach invalidates pending wakes and stale callbacks for a detaching
// attachment.
func (c *renderCoordinator) noteDetach(*attachedClient) {}

// noteReplace hands the coordinator from old to replacement; callbacks
// captured by old become stale.
func (c *renderCoordinator) noteReplace(old, replacement *attachedClient) {}

// notePark invalidates pending wakes when the attachment parks for resume.
func (c *renderCoordinator) notePark(*attachedClient) {}

// noteSessionTeardown terminally invalidates the coordinator.
func (c *renderCoordinator) noteSessionTeardown() {}

// recordResizeRequest records the latest requested geometry and source before
// the request delegates to the retained PR #71 attachment path. It returns
// the strictly monotonically increased epoch, or 0 when the source is stale.
func (c *renderCoordinator) recordResizeRequest(domain.Size, *attachedClient) uint64 { return 0 }

// resizeSnapshot returns a locked copy of the latest resize metadata.
func (c *renderCoordinator) resizeSnapshot() resizeRequestMetadata { return resizeRequestMetadata{} }

// renderCoordinator returns the coordinator installed for this session.
func (s *session) renderCoordinator() *renderCoordinator { return s.coordinator.Load() }

// installRenderCoordinator publishes rc as the session's coordinator.
func (s *session) installRenderCoordinator(rc *renderCoordinator) { s.coordinator.Store(rc) }
