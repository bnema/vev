package daemon

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Coordinator contracts and support types are kept with their ownership
// semantics, separate from the scheduling transitions in render_coordinator.

// One coordinator serves a session: producers mutate authoritative state and
// publish an invalidation; only the coordinator decides when composition runs.
// Producers never compose or send.

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

// renderWake is one coalesced composition request.
type renderWake struct {
	reset     bool
	urgent    bool
	coalesced int
	// lease is snapshotted under coordinator ownership. The wake consumer must
	// compose only for this exact active attachment incarnation, never by
	// rereading session.client after an attach publication.
	lease *attachmentLease
	// watchdog marks a flush forced by the synchronized-output watchdog.
	watchdog bool
}

// attachmentLease is the sole lifecycle identity for a primary render
// attachment. It is only mutated while renderCoordinator.mu is held; callbacks
// carry its pointer and must revalidate it before every side effect.
type attachmentLease struct {
	attachment *attachedClient
	ready      bool
	active     bool
	// A lease is an immutable callback capability. Its lifecycle bits are
	// changed only under renderCoordinator.mu; callbacks revalidate it at each
	// short effect boundary and never retain coordinator ownership while routing
	// arbitrary handlers.
}

// resizeRequestMetadata is the coordinator-owned latest requested resize
// state. Resize dispatch reads it through resizeSnapshot and validates the
// coordinator epoch before applying it.
type resizeRequestMetadata struct {
	size   domain.Size
	source *attachedClient
	// lease binds every delayed resize/retry callback to the exact attachment
	// incarnation that published this request.
	lease     *attachmentLease
	epoch     uint64 // latest requested epoch
	committed uint64 // latest published epoch
}

// renderCoordinatorOptions wires one coordinator instance.
type renderCoordinatorOptions struct {
	clock    ports.Clock
	observer ports.RuntimeObserver
	// wake composes a coalesced render request.
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

// ackBlockedSpan hands an ACK-blocked endpoint pair between coordinator
// transitions without holding c.mu while the observer runs. A consuming ACK
// may arrive while Start is blocked in observer I/O; finish records that
// closure and publishStart emits it only after Start has returned.
type ackBlockedSpan struct {
	observer    ports.RuntimeObserver
	correlation ports.RuntimeCorrelation

	mu             sync.Mutex
	startPublished bool
	finished       bool
	endPending     bool
	endValid       bool
}

func newACKBlockedSpan(observer ports.RuntimeObserver) *ackBlockedSpan {
	return &ackBlockedSpan{observer: observer, correlation: ports.NewRuntimeCorrelation()}
}

func (s *ackBlockedSpan) publishStart() {
	s.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("daemon", s.correlation, ports.RuntimeACKBlockedStart, 0, true))

	s.mu.Lock()
	s.startPublished = true
	endPending, endValid := s.endPending, s.endValid
	s.endPending = false
	s.mu.Unlock()
	if endPending {
		s.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("daemon", s.correlation, ports.RuntimeACKBlockedEnd, 0, endValid))
	}
}

// finish closes a span at most once. It never invokes the observer before the
// blocked start has completed publication.
func (s *ackBlockedSpan) finish(valid bool) {
	s.mu.Lock()
	if s.finished {
		s.mu.Unlock()
		return
	}
	s.finished = true
	if !s.startPublished {
		s.endPending, s.endValid = true, valid
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()
	s.observer.ObserveRuntime(ports.NewRuntimeMarkWithCorrelation("daemon", s.correlation, ports.RuntimeACKBlockedEnd, 0, valid))
}

// renderCoordinatorBurstMetricsSnapshot is an immutable, internal view for
// benchmark reporting. Scheduling only writes the atomic counters above.
type renderCoordinatorBurstMetricsSnapshot struct {
	invalidations uint64
	wakes         uint64
	coalesced     uint64
}
