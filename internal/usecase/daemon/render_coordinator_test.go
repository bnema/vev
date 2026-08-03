package daemon

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
)

// --- coordinator harness ------------------------------------------------------

// coordinatorMockClock uses generated mocks while retaining deterministic timer
// channels and recorded deadlines for coordinator scheduling assertions.
type coordinatorMockClock struct {
	clock  *portsmocks.MockClock
	timers chan *coordinatorMockTimer
}

type coordinatorMockTimer struct {
	mock     *portsmocks.MockTimer
	ch       chan time.Time
	duration time.Duration
}

func newCoordinatorMockClock(t *testing.T, capacity int) *coordinatorMockClock {
	t.Helper()
	clk := &coordinatorMockClock{
		clock:  portsmocks.NewMockClock(t),
		timers: make(chan *coordinatorMockTimer, capacity),
	}
	clk.clock.EXPECT().Now().Return(time.Time{}).Maybe()
	clk.clock.EXPECT().NewTimer(mock.MatchedBy(func(d time.Duration) bool {
		return d == urgentRenderDeadline ||
			(d >= minOutputRenderDeadline && d <= maxOutputRenderDeadline) ||
			d == maxSyncUpdateDuration ||
			d == time.Second ||
			d == 15*time.Minute
	})).RunAndReturn(func(d time.Duration) ports.Timer {
		timer := &coordinatorMockTimer{
			mock:     portsmocks.NewMockTimer(t),
			duration: d,
		}
		timer.ch = make(chan time.Time, 1)
		// A normal deadline reads C both in its worker and in the inert-clock
		// guard; a watchdog reads it in its worker. The generated expectation
		// makes every channel access observable without a hand-written Timer.
		timer.mock.EXPECT().C().Maybe().Return((<-chan time.Time)(timer.ch))
		timer.mock.EXPECT().Stop().Maybe().Return(true)
		clk.timers <- timer
		return timer.mock
	}).Maybe()
	return clk
}

// TestRenderCoordinatorTimerCallbacksReenterCoordinator is deliberately a
// deadlock regression: every external timer method synchronously reads
// coordinator metadata. Before two-phase timer ownership this blocked while
// c.mu was held during normal arming, urgent replacement, sync arming, and
// detach cancellation.
// Detach cancels and stops a selected worker but never synchronously joins it;
// the stale worker must be rejected when its external readiness probe returns.
func TestRenderCoordinatorDetachDoesNotJoinSelectedDeadlineWorker(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	timerC := make(chan time.Time, 1)
	stopCalled := make(chan struct{})
	clock.EXPECT().NewTimer(minOutputRenderDeadline).Return(timer).Once()
	timer.EXPECT().C().Return((<-chan time.Time)(timerC)).Once()
	timer.EXPECT().Stop().Run(func() { close(stopCalled) }).Return(true).Once()

	entered := make(chan struct{})
	release := make(chan struct{})
	rc := newRenderCoordinator(renderCoordinatorOptions{
		clock: clock,
		ackReady: func() bool {
			close(entered)
			<-release
			return true
		},
	})
	ac := &attachedClient{}
	rc.attach(ac)
	rc.invalidate(renderInvalidation{class: invalidateOutput})
	timerC <- time.Time{}
	<-entered

	detached := make(chan struct{})
	go func() {
		rc.noteDetach(ac)
		close(detached)
	}()
	<-stopCalled
	awaitTestCompletion(t, detached, "detach synchronously waited for a selected timer worker")
	close(release)
}

func TestRenderCoordinatorTimerCallbacksReenterCoordinator(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	var rc *renderCoordinator
	clock.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(time.Duration) ports.Timer {
		timer := portsmocks.NewMockTimer(t)
		ch := make(chan time.Time)
		timer.EXPECT().C().Run(func() {
			_ = rc.resizeSnapshot()
			_ = rc.burstMetricsSnapshot()
		}).Return((<-chan time.Time)(ch)).Once()
		timer.EXPECT().Stop().Run(func() {
			_ = rc.resizeSnapshot()
			_ = rc.burstMetricsSnapshot()
		}).Return(true).Once()
		return timer
	}).Times(3)
	rc = newRenderCoordinator(renderCoordinatorOptions{clock: clock})
	ac := &attachedClient{}
	rc.attach(ac)

	rc.invalidate(renderInvalidation{class: invalidateOutput}) // normal arm
	rc.invalidate(renderInvalidation{class: invalidateUrgent}) // urgent promotion
	rc.noteSyncBegin(nil, 1)                                   // sync arm
	rc.noteDetach(ac)                                          // cancel normal deadline only
	rc.noteSyncPaneRemoved(nil)                                // pane destruction cancels sync watchdog
}

// coordinatorHarness wires one coordinator to generated clock/timer mocks and
// recording hooks. Every assertion is channel- or counter-based; nothing sleeps.
type coordinatorHarness struct {
	clk        *coordinatorMockClock
	wakes      chan renderWake
	previews   chan renderWake
	ackReady   atomic.Bool
	syncActive atomic.Bool
	rc         *renderCoordinator
}

func newCoordinatorHarness(t *testing.T) *coordinatorHarness {
	t.Helper()
	h := &coordinatorHarness{
		clk:      newCoordinatorMockClock(t, 16),
		wakes:    make(chan renderWake, 16),
		previews: make(chan renderWake, 16),
	}
	h.ackReady.Store(true)
	h.rc = newRenderCoordinator(renderCoordinatorOptions{
		clock:      h.clk.clock,
		wake:       func(w renderWake) { h.wakes <- w },
		ackReady:   func() bool { return h.ackReady.Load() },
		syncActive: func() bool { return h.syncActive.Load() },
	})
	return h
}

// armedTimers drains every deadline armed so far. Arming happens
// synchronously inside the coordinator call, so a non-blocking drain is
// deterministic.
func (h *coordinatorHarness) armedTimers(t *testing.T) []*coordinatorMockTimer {
	t.Helper()
	var timers []*coordinatorMockTimer
	for {
		select {
		case tm := <-h.clk.timers:
			timers = append(timers, tm)
		default:
			return timers
		}
	}
}

// Coordinator calls and fake-clock advancement are synchronous test steps.
// These helpers deliberately never wait on wall time: a missing event is a
// deterministic contract failure, not a slow behavior to poll for.
func awaitWake(t *testing.T, ch chan renderWake) renderWake {
	t.Helper()
	return awaitTestValue(t, ch, "coordinator did not publish a wake after fake-clock advancement")
}

func requireNoWake(t *testing.T, ch chan renderWake) {
	t.Helper()
	select {
	case w := <-ch:
		t.Fatalf("unexpected coordinator wake: %+v", w)
	default:
	}
}

func awaitInvalidation(t *testing.T, ch chan renderInvalidation) renderInvalidation {
	t.Helper()
	return awaitTestValue(t, ch, "producer did not publish a coordinator invalidation")
}

func requireNoInvalidation(t *testing.T, ch chan renderInvalidation) {
	t.Helper()
	select {
	case inv := <-ch:
		t.Fatalf("unexpected coordinator invalidation: %+v", inv)
	default:
	}
}

func captureResizeCallbackDone(t *testing.T, rc *renderCoordinator) <-chan struct{} {
	t.Helper()
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.resizeLane.token == nil {
		t.Fatal("coordinator did not publish a resize callback completion")
	}
	return rc.resizeLane.token.done
}

func awaitCoordinatorScheduledTimer(t *testing.T, clk *coordinatorMockClock) *coordinatorMockTimer {
	t.Helper()
	select {
	case tm := <-clk.timers:
		return tm
	default:
		t.Fatal("coordinator did not synchronously arm a fake-clock timer")
		return nil
	}
}

func awaitLatestCoordinatorTimer(t *testing.T, clk *coordinatorMockClock) *coordinatorMockTimer {
	t.Helper()
	timer := awaitCoordinatorScheduledTimer(t, clk)
	for {
		select {
		case timer = <-clk.timers:
		default:
			return timer
		}
	}
}

func requireNoCoordinatorOutputFrame(t *testing.T, sends chan ports.Frame) {
	t.Helper()
	select {
	case frame := <-sends:
		t.Fatalf("unexpected output frame: %+v", frame)
	default:
	}
}

// --- wake coalescing and deadlines --------------------------------------------

func TestRenderCoordinatorCoalescesWakesUnderOneDeadline(t *testing.T) {
	cases := []struct {
		name          string
		invalidations []renderInvalidation
		wantTimers    int
		wantMin       time.Duration
		wantMax       time.Duration
		wantWake      renderWake
	}{
		{
			name:          "urgent transition wakes within two milliseconds",
			invalidations: []renderInvalidation{{class: invalidateUrgent, reset: true, producer: "input.go"}},
			wantTimers:    1,
			wantMax:       urgentRenderDeadline,
			wantWake:      renderWake{reset: true, urgent: true, coalesced: 1},
		},
		{
			name: "output burst coalesces into one adaptive deadline",
			invalidations: []renderInvalidation{
				{class: invalidateOutput, producer: "render.go"},
				{class: invalidateOutput, producer: "render.go"},
				{class: invalidateOutput, producer: "render.go"},
			},
			wantTimers: 1,
			wantMin:    minOutputRenderDeadline,
			wantMax:    maxOutputRenderDeadline,
			wantWake:   renderWake{coalesced: 3},
		},
		{
			name: "full redraw stays sticky across later incremental transitions",
			invalidations: []renderInvalidation{
				{class: invalidateOutput},
				{class: invalidateOutput, reset: true},
				{class: invalidateOutput},
			},
			wantTimers: 1,
			wantMin:    minOutputRenderDeadline,
			wantMax:    maxOutputRenderDeadline,
			wantWake:   renderWake{reset: true, coalesced: 3},
		},
		{
			name: "urgent transition tightens a pending output deadline",
			invalidations: []renderInvalidation{
				{class: invalidateOutput},
				{class: invalidateUrgent},
			},
			wantTimers: 2,
			wantMax:    urgentRenderDeadline,
			wantWake:   renderWake{urgent: true, coalesced: 2},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			for _, inv := range tc.invalidations {
				h.rc.invalidate(inv)
			}
			timers := h.armedTimers(t)
			require.Lenf(t, timers, tc.wantTimers,
				"cap-1 coalescing must arm exactly %d deadline timer(s) for %d invalidations",
				tc.wantTimers, len(tc.invalidations))
			last := timers[len(timers)-1]
			require.GreaterOrEqual(t, last.duration, tc.wantMin)
			require.LessOrEqual(t, last.duration, tc.wantMax)

			last.ch <- time.Time{}
			require.Equal(t, tc.wantWake, awaitWake(t, h.wakes),
				"one wake must deliver the latest coalesced state")
			requireNoWake(t, h.wakes)
		})
	}
}

func TestRenderCoordinatorAdaptsOutputDeadlineAfterBurstAndDecays(t *testing.T) {
	h := newCoordinatorHarness(t)

	// A burst establishes pressure without extending its already-armed first
	// deadline; the following output deadline grows to the 16ms ceiling.
	for range 9 {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
	}
	burst := h.armedTimers(t)
	require.Len(t, burst, 1)
	require.Equal(t, minOutputRenderDeadline, burst[0].duration)
	burst[0].ch <- time.Time{}
	awaitWake(t, h.wakes)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
	atCap := h.armedTimers(t)
	require.Len(t, atCap, 1)
	require.Equal(t, maxOutputRenderDeadline, atCap[0].duration,
		"burst pressure must be capped at the 16ms output deadline")
	atCap[0].ch <- time.Time{}
	awaitWake(t, h.wakes)

	// Quiet singleton batches decay pressure toward the 8ms floor.
	var previous = maxOutputRenderDeadline
	for range 8 {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
		timers := h.armedTimers(t)
		require.Len(t, timers, 1)
		require.LessOrEqual(t, timers[0].duration, previous)
		require.GreaterOrEqual(t, timers[0].duration, minOutputRenderDeadline)
		previous = timers[0].duration
		timers[0].ch <- time.Time{}
		awaitWake(t, h.wakes)
	}
	require.Equal(t, minOutputRenderDeadline, previous)
}

func TestRenderCoordinatorUrgentDeadlineCannotBeExtended(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.rc.invalidate(renderInvalidation{class: invalidateOutput})
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	for range 32 {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	}
	timers := h.armedTimers(t)
	require.Len(t, timers, 2, "only the urgent promotion may replace the output timer")
	require.Equal(t, urgentRenderDeadline, timers[len(timers)-1].duration)
}

// --- ACK gating ----------------------------------------------------------------

func TestRenderCoordinatorAckReadinessReentersWithoutBlockingResize(t *testing.T) {
	owner := &attachedClient{}
	wakes := make(chan renderWake, 1)
	ackEntered := make(chan struct{})
	resizeDone := make(chan uint64, 1)
	fireDone := make(chan struct{})

	var rc *renderCoordinator
	rc = newRenderCoordinator(renderCoordinatorOptions{
		ackReady: func() bool {
			close(ackEntered)
			// The readiness probe models the output send path reading coordinator
			// metadata after sendMu is held. It must never run under c.mu.
			_ = rc.resizeSnapshot()
			return true
		},
		wake: func(w renderWake) { wakes <- w },
	})
	rc.attach(owner)
	rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})

	go func() {
		rc.fire(1, false, true)
		close(fireDone)
	}()
	awaitTestCompletion(t, ackEntered, "ack readiness was not evaluated")

	go func() { resizeDone <- rc.recordResizeRequest(domain.Size{Cols: 120, Rows: 40}, owner) }()
	epoch := awaitTestValue(t, resizeDone, "ack readiness re-entry blocked resize metadata")
	require.Equal(t, uint64(1), epoch)
	wake := awaitWake(t, wakes)
	require.Same(t, owner, wake.lease.attachment)
	wake.lease = nil
	require.Equal(t, renderWake{coalesced: 1}, wake)
	awaitTestCompletion(t, fireDone, "fire did not return after wake")
}

func TestRenderCoordinatorAckDoesNotBypassAnUnexpiredDeadline(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
	timer := awaitCoordinatorScheduledTimer(t, h.clk)
	require.GreaterOrEqual(t, timer.duration, minOutputRenderDeadline)

	// An unrelated ACK must not turn a normal output deadline into an
	// immediate wake or cancel its timer.
	h.rc.notifyAck()
	requireNoWake(t, h.wakes)
	timer.mock.AssertNotCalled(t, "Stop")

	timer.ch <- time.Time{}
	awaitWake(t, h.wakes)
	requireNoWake(t, h.wakes)
}

func TestRenderCoordinatorAckFlushesOnlyExpiredAckDeferredWork(t *testing.T) {
	t.Run("expired deadline flushes exactly once after readiness", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.ackReady.Store(false)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true, producer: "render.go"})
		timer := awaitCoordinatorScheduledTimer(t, h.clk)
		timer.ch <- time.Time{}
		requireNoWake(t, h.wakes)

		// Repeated ACKs while capacity remains unavailable are no-ops.
		h.rc.notifyAck()
		requireNoWake(t, h.wakes)
		h.ackReady.Store(true)
		h.rc.notifyAck()
		w := awaitWake(t, h.wakes)
		require.True(t, w.reset)
		requireNoWake(t, h.wakes)
		h.rc.notifyAck()
		requireNoWake(t, h.wakes)
	})

	t.Run("lifecycle clears deferred work and urgent explicit fires stay immediate", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.ackReady.Store(false)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		timer := awaitCoordinatorScheduledTimer(t, h.clk)
		timer.ch <- time.Time{}
		requireNoWake(t, h.wakes)
		h.rc.beginSessionTeardown().finish()
		h.ackReady.Store(true)
		h.rc.notifyAck()
		requireNoWake(t, h.wakes)

		immediate := newCoordinatorHarness(t)
		immediate.rc.invalidate(renderInvalidation{class: invalidateUrgent})
		immediate.rc.fireCurrent(false)
		w := awaitWake(t, immediate.wakes)
		require.True(t, w.urgent)
	})
}

func TestRenderCoordinatorAckGateBlocksCompositionUntilAck(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.ackReady.Store(false)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput})
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true})
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	for _, timer := range h.armedTimers(t) {
		timer.ch <- time.Time{}
	}
	requireNoWake(t, h.wakes)

	h.ackReady.Store(true)
	h.rc.notifyAck()
	w := awaitWake(t, h.wakes)
	require.True(t, w.reset, "the deferred full redraw must stay sticky through the ack gate")
	require.Equal(t, 3, w.coalesced, "the post-ack wake must carry every deferred transition")
	requireNoWake(t, h.wakes)

	h.rc.notifyAck()
	requireNoWake(t, h.wakes)
}

// --- synchronized output ---------------------------------------------------------

func TestRenderCoordinatorConcurrentSynchronizedPaneBatches(t *testing.T) {
	t.Run("end orders, watchdogs, and stale callbacks are isolated per pane", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		firstForced, secondForced := make(chan struct{}, 1), make(chan struct{}, 1)
		firstPane, secondPane := &pane{}, &pane{}

		// Distinct stable pane owners must remain independent.
		h.rc.noteSyncBegin(firstPane, 11, func() { firstForced <- struct{}{} })
		firstWatchdog := h.armedTimers(t)[0]
		h.rc.noteSyncBegin(secondPane, 22, func() { secondForced <- struct{}{} })
		secondWatchdog := h.armedTimers(t)[0]
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})

		// Completing either pane leaves the aggregate gate closed.
		h.rc.noteSyncEnd(firstPane, 11)
		requireNoWake(t, h.wakes)
		// A stale callback for the completed pane must not force it or wake.
		firstWatchdog.ch <- time.Time{}
		select {
		case <-firstForced:
			t.Fatal("stale watchdog forced a completed pane")
		default:
		}
		requireNoWake(t, h.wakes)

		// The remaining pane's watchdog forces only its own batch, then the
		// aggregate completion produces exactly one urgent wake.
		secondWatchdog.ch <- time.Time{}
		<-secondForced
		w := awaitWake(t, h.wakes)
		require.True(t, w.watchdog)
		require.True(t, w.urgent, "aggregate completion must wake urgently")
		requireNoWake(t, h.wakes)
	})

	t.Run("later pane can end first without releasing an earlier pane", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		firstForced := make(chan struct{}, 1)
		firstPane, secondPane := &pane{}, &pane{}
		h.rc.noteSyncBegin(firstPane, 11, func() { firstForced <- struct{}{} })
		firstWatchdog := h.armedTimers(t)[0]
		h.rc.noteSyncBegin(secondPane, 22)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})

		h.rc.noteSyncEnd(secondPane, 22)
		requireNoWake(t, h.wakes)
		firstWatchdog.ch <- time.Time{}
		<-firstForced
		w := awaitWake(t, h.wakes)
		require.True(t, w.watchdog)
		require.True(t, w.urgent, "final pane completion must wake urgently")
		requireNoWake(t, h.wakes)
	})

	t.Run("pane removal cancels only that pane watchdog", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		firstPane, secondPane := &pane{}, &pane{}
		firstForced, secondForced := make(chan struct{}, 1), make(chan struct{}, 1)
		h.rc.noteSyncBegin(firstPane, 11, func() { firstForced <- struct{}{} })
		firstWatchdog := h.armedTimers(t)[0]
		h.rc.noteSyncBegin(secondPane, 22, func() { secondForced <- struct{}{} })
		secondWatchdog := h.armedTimers(t)[0]
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})

		h.rc.noteSyncPaneRemoved(firstPane)
		firstWatchdog.ch <- time.Time{}
		select {
		case <-firstForced:
			t.Fatal("removed pane watchdog forced output")
		default:
		}
		requireNoWake(t, h.wakes)
		secondWatchdog.ch <- time.Time{}
		<-secondForced
		require.True(t, awaitWake(t, h.wakes).watchdog)
	})
}

func TestRenderCoordinatorSyncGateFollowsDynamicRenderability(t *testing.T) {
	t.Run("inactive batch that becomes active gates activation output until end", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		var visible atomic.Bool
		p := &pane{}
		h.rc.noteSyncBeginWithRenderability(p, 1, visible.Load)
		visible.Store(true)
		h.rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "activation"})
		for _, timer := range h.armedTimers(t) {
			timer.ch <- time.Time{}
		}
		requireNoWake(t, h.wakes)

		h.rc.noteSyncEnd(p, 1)
		w := awaitWake(t, h.wakes)
		require.True(t, w.urgent)
		requireNoWake(t, h.wakes)
	})

	t.Run("hidden batch does not stall newly active output or wake on late end", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		var visible atomic.Bool
		visible.Store(true)
		p := &pane{}
		h.rc.noteSyncBeginWithRenderability(p, 1, visible.Load)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "batch"})
		visible.Store(false)
		h.rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "new-active-tab"})
		timers := h.armedTimers(t)
		require.NotEmpty(t, timers)
		timers[len(timers)-1].ch <- time.Time{}
		w := awaitWake(t, h.wakes)
		require.False(t, w.watchdog)
		h.rc.noteSyncEnd(p, 1)
		requireNoWake(t, h.wakes)
	})
}

func TestRenderCoordinatorSyncGateRetriesWhenBatchPublishesDuringPredicate(t *testing.T) {
	h := newCoordinatorHarness(t)
	first, second := &pane{}, &pane{}
	gateEvaluated := make(chan struct{})
	releaseGate := make(chan struct{})
	var hookOnce sync.Once
	h.rc.opts.afterSyncGateEvaluated = func() {
		hookOnce.Do(func() {
			close(gateEvaluated)
			<-releaseGate
		})
	}
	h.rc.noteSyncBeginWithRenderability(first, 1, func() bool { return false })
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "test"})

	fireDone := make(chan struct{})
	go func() {
		h.rc.fireCurrent(false)
		close(fireDone)
	}()
	<-gateEvaluated
	// This publication is deliberately between an open predicate result and
	// pending consumption. A stale gate result must be rejected and retried.
	h.rc.noteSyncBeginWithRenderability(second, 2, func() bool { return true })
	close(releaseGate)
	awaitTestCompletion(t, fireDone, "fire did not finish after retrying the registry snapshot")

	requireNoWake(t, h.wakes)
	h.rc.noteSyncEnd(second, 2)
	require.Equal(t, renderWake{urgent: true, coalesced: 1}, awaitWake(t, h.wakes))
}

func TestRenderCoordinatorSynchronizedOutput(t *testing.T) {
	t.Run("completion flushes pending state in one wake", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.syncActive.Store(true)
		h.rc.noteSyncBegin(nil, 1)
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs, "sync begin must arm the completion watchdog")
		require.Equal(t, maxSyncUpdateDuration, watchdogs[len(watchdogs)-1].duration)

		h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true})
		timers := h.armedTimers(t)
		require.Len(t, timers, 1)
		h.rc.mu.Lock()
		normalWorkerDone := h.rc.normalLane.token.done
		h.rc.mu.Unlock()
		require.NotNil(t, normalWorkerDone)
		timers[0].ch <- time.Time{}
		requireWorkerExit(t, normalWorkerDone)
		requireNoWake(t, h.wakes)

		h.syncActive.Store(false)
		h.rc.noteSyncEnd(nil, 1)
		w := awaitWake(t, h.wakes)
		require.True(t, w.reset)
		requireNoWake(t, h.wakes)
	})

	t.Run("watchdog ends a wedged batch before it flushes", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.syncActive.Store(true)
		forced := make(chan struct{}, 1)
		h.rc.noteSyncBegin(nil, 7, func() { forced <- struct{}{} })
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs, "sync begin must arm the completion watchdog")
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})

		watchdogs[0].ch <- time.Time{}
		<-forced
		w := awaitWake(t, h.wakes)
		require.True(t, w.watchdog, "a wedged synchronized batch must be force-flushed by the watchdog")
		requireNoWake(t, h.wakes)
	})

	t.Run("watchdog retains gate while force closes the VT batch", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		entered := make(chan struct{})
		release := make(chan struct{})
		h.rc.noteSyncBegin(nil, 1, func() {
			close(entered)
			<-release
		})
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		watchdogs[0].ch <- time.Time{}
		<-entered

		// A concurrent deadline/fire sees the retained batch gate and cannot
		// publish the partial VT state while force is blocked.
		h.rc.fireCurrent(false)
		requireNoWake(t, h.wakes)
		close(release)
		w := awaitWake(t, h.wakes)
		require.True(t, w.watchdog)
		require.True(t, w.urgent)
		requireNoWake(t, h.wakes)
	})

	t.Run("watchdog flushes after the snapshotted generation is replaced", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		h.rc.mu.Lock()
		staleGeneration := h.rc.normalLane.generation
		h.rc.mu.Unlock()

		// Simulate a new urgent publication after fireCurrent(true) snapshots its
		// generation but before fire validates it.
		h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
		h.rc.fire(staleGeneration, true, true)

		w := awaitWake(t, h.wakes)
		require.True(t, w.watchdog)
		require.True(t, w.urgent)
		require.Equal(t, 2, w.coalesced)
		requireNoWake(t, h.wakes)
	})

	t.Run("stale watchdog generation cannot wake a completed batch", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		h.syncActive.Store(true)
		h.rc.noteSyncBegin(nil, 1)
		watchdogs := h.armedTimers(t)
		require.NotEmpty(t, watchdogs, "sync begin must arm the completion watchdog")
		h.syncActive.Store(false)
		h.rc.noteSyncEnd(nil, 1)

		watchdogs[0].ch <- time.Time{}
		requireNoWake(t, h.wakes)
	})
}

// --- preview subscription --------------------------------------------------------

func TestRenderCoordinatorPreviewSubscription(t *testing.T) {
	h := newCoordinatorHarness(t)
	h.rc.subscribePreview(func(w renderWake) { h.previews <- w })

	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	timers := h.armedTimers(t)
	require.NotEmpty(t, timers, "invalidate must arm a deadline timer")
	timers[len(timers)-1].ch <- time.Time{}
	owner := awaitWake(t, h.wakes)
	preview := awaitWake(t, h.previews)
	require.Equal(t, owner, preview, "a subscribed preview observes the same coalesced wake")
	requireNoWake(t, h.previews)

	h.rc.teardownPreview()
	h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
	timers = h.armedTimers(t)
	require.NotEmpty(t, timers, "invalidate must arm a deadline timer")
	timers[len(timers)-1].ch <- time.Time{}
	awaitWake(t, h.wakes)
	requireNoWake(t, h.previews)
}

// --- lifecycle and stale callbacks -----------------------------------------------

func TestRenderCoordinatorPreviewWakesDoNotWaitForTargetAck(t *testing.T) {
	cases := []struct {
		name   string
		attach bool
	}{
		{name: "attached cross-session target blocked on ack", attach: true},
		{name: "headless cross-session target blocked on ack"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			h.ackReady.Store(false)
			if tc.attach {
				h.rc.attach(&attachedClient{})
			}
			viewer := &attachedClient{}
			h.rc.subscribePreviewFor(viewer, 1, func(w renderWake) { h.previews <- w })

			h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "target output"})
			timers := h.armedTimers(t)
			require.Len(t, timers, 1)
			timers[0].ch <- time.Time{}

			preview := awaitWake(t, h.previews)
			if tc.attach {
				require.NotNil(t, preview.lease.attachment)
			}
			preview.lease = nil
			require.Equal(t, renderWake{coalesced: 1}, preview,
				"the viewer preview must receive target output without target ACK capacity")
			requireNoWake(t, h.wakes)
			requireNoWake(t, h.previews)

			h.ackReady.Store(true)
			h.rc.notifyAck()
			wake := awaitWake(t, h.wakes)
			if tc.attach {
				require.NotNil(t, wake.lease.attachment)
			}
			wake.lease = nil
			require.Equal(t, renderWake{coalesced: 1}, wake,
				"the target primary frame remains pending for its own ACK")
			requireNoWake(t, h.previews)
		})
	}
}

func TestRenderCoordinatorDetachedTargetPublishesPreviewOnlyInvalidations(t *testing.T) {
	h := newCoordinatorHarness(t)
	target, viewer := &attachedClient{}, &attachedClient{}
	h.ackReady.Store(false)
	h.rc.attach(target)
	h.rc.subscribePreviewFor(viewer, 1, func(w renderWake) { h.previews <- w })
	h.rc.noteDetach(target)

	// A detached target only accepts PTY/session-owned invalidations. The old
	// attachment remains stale and must not revive its render path.
	h.rc.invalidateForAttachment(target, renderInvalidation{class: invalidateOutput, producer: "stale attachment"})
	require.Empty(t, h.armedTimers(t))

	for _, producer := range []string{"first PTY output", "second PTY output"} {
		h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: producer})
		timers := h.armedTimers(t)
		require.Len(t, timers, 1)
		timers[0].ch <- time.Time{}
		preview := awaitWake(t, h.previews)
		require.Nil(t, preview.lease, "headless preview wakes must not retain a revoked primary lease")
		require.Equal(t, renderWake{coalesced: 1}, preview)
		requireNoWake(t, h.wakes)

		h.rc.mu.Lock()
		require.False(t, h.rc.pending)
		require.False(t, h.rc.pendingPreview)
		require.False(t, h.rc.ackDeferred)
		require.False(t, h.rc.deadlineDue)
		require.Zero(t, h.rc.coalesced)
		require.True(t, h.rc.primaryDetachedLocked())
		require.NotNil(t, h.rc.lease)
		require.False(t, h.rc.lease.active)
		h.rc.mu.Unlock()
	}

	// A later attach retains the normal first-paint reset path.
	replacement := &attachedClient{}
	h.ackReady.Store(true)
	h.rc.attach(replacement)
	h.rc.invalidateForAttachment(replacement, renderInvalidation{class: invalidateUrgent, reset: true, producer: "first paint"})
	timers := h.armedTimers(t)
	require.Len(t, timers, 1)
	timers[0].ch <- time.Time{}
	wake := awaitWake(t, h.wakes)
	require.True(t, wake.reset)
	require.Same(t, replacement, wake.lease.attachment)
}

func TestRenderCoordinatorPreviewLifecycleDropsStaleTargetWakes(t *testing.T) {
	cases := []struct {
		name       string
		transition func(*renderCoordinator, *attachedClient, *attachedClient)
	}{
		{"target detach", func(rc *renderCoordinator, target, _ *attachedClient) { rc.noteDetach(target) }},
		{"target replacement", func(rc *renderCoordinator, target, replacement *attachedClient) { rc.noteReplace(target, replacement) }},
		{"target teardown", func(rc *renderCoordinator, _ *attachedClient, _ *attachedClient) { rc.beginSessionTeardown().finish() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			target, replacement, viewer := &attachedClient{}, &attachedClient{}, &attachedClient{}
			h.rc.attach(target)
			h.rc.subscribePreviewFor(viewer, 1, func(w renderWake) { h.previews <- w })
			h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "target output"})
			stale := h.armedTimers(t)
			require.Len(t, stale, 1)

			tc.transition(h.rc, target, replacement)
			stale[0].ch <- time.Time{}
			requireNoWake(t, h.wakes)
			requireNoWake(t, h.previews)
		})
	}
}

func TestRenderCoordinatorPreviewSubscriptionsAreIndependent(t *testing.T) {
	h := newCoordinatorHarness(t)
	one, two := &attachedClient{}, &attachedClient{}
	first, second := make(chan renderWake, 1), make(chan renderWake, 1)
	h.rc.subscribePreviewFor(one, 1, func(w renderWake) { first <- w })
	h.rc.subscribePreviewFor(two, 1, func(w renderWake) { second <- w })
	h.rc.attach(&attachedClient{})
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "preview"})
	last := h.armedTimers(t)
	last[len(last)-1].ch <- time.Time{}
	awaitWake(t, first)
	awaitWake(t, second)

	h.rc.teardownPreviewFor(one, 1)
	h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "preview"})
	last = h.armedTimers(t)
	last[len(last)-1].ch <- time.Time{}
	requireNoWake(t, first)
	awaitWake(t, second)
}

func TestRenderCoordinatorLifecycleDropsStaleWakes(t *testing.T) {
	cases := []struct {
		name     string
		teardown func(rc *renderCoordinator, owner *attachedClient)
	}{
		{"detach", func(rc *renderCoordinator, owner *attachedClient) { rc.noteDetach(owner) }},
		{"park", func(rc *renderCoordinator, owner *attachedClient) { rc.notePark(owner) }},
		{"session teardown", func(rc *renderCoordinator, _ *attachedClient) { rc.beginSessionTeardown().finish() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			owner := &attachedClient{}
			h.rc.attach(owner)
			h.rc.invalidate(renderInvalidation{class: invalidateOutput})
			stale := h.armedTimers(t)
			require.NotEmpty(t, stale, "invalidate must arm a deadline timer")

			tc.teardown(h.rc, owner)
			for _, timer := range stale {
				timer.ch <- time.Time{}
			}
			requireNoWake(t, h.wakes)

			h.rc.invalidate(renderInvalidation{class: invalidateUrgent})
			for _, timer := range h.armedTimers(t) {
				timer.ch <- time.Time{}
			}
			requireNoWake(t, h.wakes)
		})
	}

	t.Run("replacement attachment wakes independently", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		owner := &attachedClient{}
		replacement := &attachedClient{}
		h.rc.attach(owner)
		h.rc.invalidate(renderInvalidation{class: invalidateOutput})
		stale := h.armedTimers(t)
		require.NotEmpty(t, stale, "invalidate must arm a deadline timer")

		h.rc.noteReplace(owner, replacement)
		h.rc.attach(replacement)
		for _, timer := range stale {
			timer.ch <- time.Time{}
		}
		requireNoWake(t, h.wakes)

		h.rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true})
		fresh := h.armedTimers(t)
		require.NotEmpty(t, fresh, "a replacement attachment must arm its own deadline")
		fresh[len(fresh)-1].ch <- time.Time{}
		w := awaitWake(t, h.wakes)
		require.True(t, w.reset, "the replacement starts from an independent full state")
		requireNoWake(t, h.wakes)
	})
}

// --- resize metadata ownership -----------------------------------------------------

func TestRenderCoordinatorResizeMetadata(t *testing.T) {
	sz := func(cols, rows int) domain.Size { return domain.Size{Cols: cols, Rows: rows} }
	cases := []struct {
		name string
		run  func(t *testing.T, rc *renderCoordinator, owner, replacement *attachedClient)
	}{
		{
			name: "latest request wins with strictly increasing epochs",
			run: func(t *testing.T, rc *renderCoordinator, owner, _ *attachedClient) {
				require.Equal(t, uint64(1), rc.recordResizeRequest(sz(100, 30), owner))
				require.Equal(t, uint64(2), rc.recordResizeRequest(sz(110, 32), owner))
				require.Equal(t, uint64(3), rc.recordResizeRequest(sz(120, 40), owner))
				snap := rc.resizeSnapshot()
				require.Equal(t, sz(120, 40), snap.size)
				require.Same(t, owner, snap.source)
				require.Equal(t, uint64(3), snap.epoch)

				// Resize state is exclusively coordinator-owned.
				require.Zero(t, snap.committed)
			},
		},
		{
			name: "duplicate sizes still advance the epoch deterministically",
			run: func(t *testing.T, rc *renderCoordinator, owner, _ *attachedClient) {
				require.Equal(t, uint64(1), rc.recordResizeRequest(sz(120, 40), owner))
				require.Equal(t, uint64(2), rc.recordResizeRequest(sz(120, 40), owner))
				snap := rc.resizeSnapshot()
				require.Equal(t, sz(120, 40), snap.size)
				require.Equal(t, uint64(2), snap.epoch)
			},
		},
		{
			name: "stale resize callbacks preserve latest metadata through lifecycle transitions",
			run: func(t *testing.T, rc *renderCoordinator, owner, replacement *attachedClient) {
				type lifecycleCase struct {
					name       string
					installNew bool
				}
				for _, tc := range []lifecycleCase{
					{name: "detach"},
					{name: "park"},
					{name: "session teardown"},
					{name: "replacement", installNew: true},
				} {
					t.Run(tc.name, func(t *testing.T) {
						h := newCoordinatorHarness(t)
						staleOwner := &attachedClient{}
						freshOwner := &attachedClient{}
						h.rc.attach(staleOwner)
						require.Equal(t, uint64(1), h.rc.recordResizeRequest(sz(120, 40), staleOwner))

						// This models a stale callback which captured the old
						// attachment before lifecycle ownership changed.
						staleCallback := func() uint64 {
							return h.rc.recordResizeRequest(sz(80, 20), staleOwner)
						}

						switch tc.name {
						case "detach":
							h.rc.noteDetach(staleOwner)
						case "park":
							h.rc.notePark(staleOwner)
						case "session teardown":
							h.rc.beginSessionTeardown().finish()
						case "replacement":
							h.rc.noteReplace(staleOwner, freshOwner)
							h.rc.attach(freshOwner)
							require.Equal(t, uint64(2), h.rc.recordResizeRequest(sz(100, 50), freshOwner))
						}

						require.Zero(t, staleCallback(), "stale callback must not advance the resize epoch")
						snap := h.rc.resizeSnapshot()
						if tc.installNew {
							require.Equal(t, sz(100, 50), snap.size)
							require.Equal(t, uint64(2), snap.epoch)
							require.Same(t, freshOwner, snap.source)
							return
						}
						require.Equal(t, sz(120, 40), snap.size)
						require.Equal(t, uint64(1), snap.epoch)
						require.Same(t, staleOwner, snap.source,
							"the stale callback must not replace the recorded attachment identity")
					})
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newCoordinatorHarness(t)
			owner := &attachedClient{}
			replacement := &attachedClient{}
			h.rc.attach(owner)
			tc.run(t, h.rc, owner, replacement)
		})
	}
}

// --- producer fan-in ------------------------------------------------------------

// producerFiles is the current production direct-paint inventory. Every file
// gets exactly one exercised state transition in TestProducerInvalidations.
var producerFiles = []string{
	"attention.go", "client.go", "copymode.go", "floating.go", "input.go",
	"palette.go", "pane_actions.go", "picker.go", "prompt.go", "render.go",
	"session.go", "session_back.go",
}

func TestProducerInvalidations(t *testing.T) {
	cases := []struct {
		file     string
		name     string
		tabs     int
		producer string
		run      func(t *testing.T, d *Daemon, sess *session, ac *attachedClient)
	}{
		{
			file: "attention.go",
			name: "attention repaint",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.repaintAttachedClients(sess)
			},
		},
		{
			file: "client.go",
			name: "client theme application",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.applyTheme(sess, ac, ports.Theme{TrueColor: true})
			},
		},
		{
			file: "copymode.go",
			name: "copy mode entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterCopyMode(sess, ac)
			},
		},
		{
			file: "floating.go",
			name: "floating toggle to visible",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				fp := newPaneWithStableID(layout.PaneID("floating"), "float-producer", newQuietPTY(), domain.Size{Cols: 40, Rows: 10})
				tb := testAttachmentTab(sess)
				tb.mu.Lock()
				tb.floating.pane = fp
				tb.floating.state = floatingHidden
				tb.floating.generation = 1
				tb.mu.Unlock()
				require.NoError(t, d.toggleFloating(sess, ac))
			},
		},
		{
			file:     "input.go",
			name:     "proxied jump attention switches a local tab",
			tabs:     2,
			producer: "input.go",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				ac.proxied = true
				sess.tabs[1].attention = true
				daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionJumpAttention, nil)
				require.Equal(t, 1, testAttachmentTabIndex(sess))
			},
		},
		{
			file: "palette.go",
			name: "palette entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterPalette(sess, ac)
			},
		},
		{
			file: "pane_actions.go",
			name: "pane split",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				require.NoError(t, d.splitPane(sess, ac, layout.Right))
			},
		},
		{
			file: "picker.go",
			name: "picker entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterPicker(sess, ac)
			},
		},
		{
			file: "prompt.go",
			name: "prompt entry",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.enterPrompt(sess, ac, "rename", "", func(string) error { return nil })
			},
		},
		{
			file: "render.go",
			name: "retained resize dispatch",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				clk := newCoordinatorMockClock(t, 2)
				// The coordinator owns deadline time; install the deterministic
				// clock on the already-attached coordinator rather than changing
				// Daemon's construction-time clock afterwards.
				sess.renderCoordinator().opts.clock = clk.clock
				d.resize(sess, ac, domain.Size{Cols: 100, Rows: 26})
				timer := awaitCoordinatorScheduledTimer(t, clk)
				done := captureResizeCallbackDone(t, sess.renderCoordinator())
				timer.ch <- time.Time{}
				awaitTestCompletion(t, done, "resize callback did not complete")
			},
		},
		{
			file: "session.go",
			name: "tab close repaint",
			tabs: 2,
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				sess.mu.Lock()
				tb := sess.tabs[1]
				sess.mu.Unlock()
				require.NoError(t, d.closeTab(sess, tb, true))
			},
		},
		{
			file: "session_back.go",
			name: "back session fallback without a target",
			run: func(t *testing.T, d *Daemon, sess *session, ac *attachedClient) {
				d.backSession(sess, ac)
				require.Same(t, sess, ac.currentSession())
			},
		},
	}

	exercised := make([]string, 0, len(cases))
	for _, tc := range cases {
		exercised = append(exercised, tc.file)
	}
	require.ElementsMatch(t, producerFiles, exercised,
		"every current direct-paint producer file needs exactly one exercised transition")

	for _, tc := range cases {
		t.Run(tc.file+"/"+tc.name, func(t *testing.T) {
			tabs := tc.tabs
			if tabs == 0 {
				tabs = 1
			}
			d, sess, ac, sends, releases := newManualTabSession(t, tabs)
			defer releaseAll(releases)
			invs := make(chan renderInvalidation, 8)
			sess.installRenderCoordinator(newRenderCoordinator(renderCoordinatorOptions{
				clock:        d.clock,
				wake:         func(renderWake) {},
				onInvalidate: func(inv renderInvalidation) { invs <- inv },
			}))

			tc.run(t, d, sess, ac)

			inv := awaitInvalidation(t, invs)
			if tc.producer != "" {
				require.Equal(t, tc.producer, inv.producer)
			}
			requireNoInvalidation(t, invs)
			requireNoCoordinatorOutputFrame(t, sends)
		})
	}
}

// TestProducerInvalidationInventory is a supplementary inventory guard only:
// it keeps producerFiles aligned with the daemon source layout so the
// behavioral table above cannot drift silently. It is never acceptance
// evidence for coordinator behavior.
func TestProducerInvalidationInventory(t *testing.T) {
	for _, name := range producerFiles {
		_, err := os.Stat(filepath.Join(".", name))
		require.NoErrorf(t, err, "producer file %s is gone; update TestProducerInvalidations", name)
	}
}

// --- coordinator resize dispatch ----------------------------------------------

func TestConcurrentPaintInitializesOverlayUnderSendOwnership(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	// Exercise the original seam: two fallback paints reach lazy initialization
	// together before either can compose.
	ac.overlays = nil
	ac.overlayOnce = sync.Once{}
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	for range 2 {
		go func() {
			defer wg.Done()
			<-start
			d.paint(sess, ac, true, nil)
		}()
	}
	close(start)
	wg.Wait()
	require.NotNil(t, ac.overlays)
}

func TestStartPaneGoroutinesAccountsForOneReader(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)
	require.NotNil(t, tb)
	d.startPaneGoroutines(sess, tb, tb.focusedPane())
	release()
	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(time.Second):
		t.Fatal("one reader must balance exactly one WaitGroup count")
	}
}

func requireWorkerExit(t *testing.T, done <-chan struct{}) {
	t.Helper()
	awaitTestCompletion(t, done, "cancelled coordinator timer worker did not exit")
}

func TestCoordinatorDeadlineCannotPaintPublishedReplacementBeforeOwnershipInstall(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, owner, ownerSends := newManualSessionWithPTYs(t, p)
	replacementTransport, replacementSends := newCapturingTransport(t)
	replacement := &attachedClient{tr: replacementTransport, output: newOutputStateStream(), size: owner.size}
	replacement.initOverlays()
	replacement.setSession(sess)

	clock := newCoordinatorMockClock(t, 2)
	d.clock = clock.clock
	rc := newRenderCoordinator(renderCoordinatorOptions{
		clock: clock.clock,
		wake: func(w renderWake) {
			// This is the production ownership boundary: composition must use
			// the coordinator's captured attachment, never sess.snapshotAttachments()[0].
			d.paint(sess, w.lease.attachment, w.reset, w.lease)
		},
		ackReady: func() bool { return true },
	})
	rc.attach(owner)
	sess.installRenderCoordinator(rc)
	rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true})
	timer := awaitCoordinatorScheduledTimer(t, clock)

	// Model attachClient's publication window exactly: sess.snapshotAttachments()[0] is new,
	// but coordinator replacement has not yet invalidated the old deadline.
	sess.mu.Lock()
	sess.registerAttachmentLocked(replacement)
	sess.mu.Unlock()
	timer.ch <- time.Time{}
	requireNoCoordinatorOutputFrame(t, replacementSends)
	requireNoCoordinatorOutputFrame(t, ownerSends)
}

func TestRenderCoordinatorResizeLaneRejectsStaleToken(t *testing.T) {
	rc := newRenderCoordinator(renderCoordinatorOptions{})
	newer := &timerToken{generation: 2, timer: &portsmocks.MockTimer{}, cancel: make(chan struct{}), done: make(chan struct{})}
	stale := &timerToken{generation: 1}

	rc.mu.Lock()
	rc.resizeLane.generation = 2
	rc.resizeLane.token = newer
	require.False(t, rc.resizeLane.clearLocked(stale))
	require.Same(t, newer, rc.resizeLane.token)
	require.True(t, rc.resizeLane.clearLocked(newer))
	require.Nil(t, rc.resizeLane.token)
	rc.mu.Unlock()
}

func TestRenderCoordinatorNilClockRetryRunsSynchronously(t *testing.T) {
	rc := newRenderCoordinator(renderCoordinatorOptions{})
	owner := &attachedClient{}
	rc.attach(owner)
	lease := rc.attachmentLease(owner)
	epoch := rc.recordResizeRequestForLease(domain.Size{Cols: 120, Rows: 40}, owner, lease)
	require.NotZero(t, epoch)
	require.True(t, rc.resizeCurrentForLease(epoch, owner, lease, true))

	called := false
	rc.scheduleResizeRetryForLease(epoch, owner, lease, func() { called = true })
	require.True(t, called, "a nil clock must run the retry without a timer")
	rc.mu.Lock()
	retryToken := rc.retryLane.token
	rc.mu.Unlock()
	require.Nil(t, retryToken, "a disabled retry lane must not retain stale completion ownership")
}

func TestRenderCoordinatorInertTimerFiresSynchronouslyWithoutWorker(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	clock.EXPECT().NewTimer(minOutputRenderDeadline).Return(timer).Once()
	timer.EXPECT().C().Return((<-chan time.Time)(nil)).Once()
	timer.EXPECT().Stop().Return(true).Once()

	wakes := make(chan renderWake, 1)
	rc := newRenderCoordinator(renderCoordinatorOptions{
		clock:    clock,
		ackReady: func() bool { return true },
		wake:     func(w renderWake) { wakes <- w },
	})
	rc.invalidate(renderInvalidation{class: invalidateOutput})

	require.Equal(t, renderWake{coalesced: 1}, <-wakes)
	require.Equal(t, renderCoordinatorBurstMetricsSnapshot{invalidations: 1, wakes: 1, coalesced: 1}, rc.burstMetricsSnapshot())
	rc.mu.Lock()
	require.Nil(t, rc.normalLane.token)
	rc.mu.Unlock()
}

func TestRenderCoordinatorSyncBatchSurvivesAttachmentLifecycle(t *testing.T) {
	t.Run("detach and park retain a gated headless preview until complete", func(t *testing.T) {
		for _, transition := range []struct {
			name string
			run  func(*renderCoordinator, *attachedClient)
		}{
			{"detach", func(rc *renderCoordinator, ac *attachedClient) { rc.noteDetach(ac) }},
			{"park", func(rc *renderCoordinator, ac *attachedClient) { rc.notePark(ac) }},
		} {
			t.Run(transition.name, func(t *testing.T) {
				h := newCoordinatorHarness(t)
				target, viewer, p := &attachedClient{}, &attachedClient{}, &pane{}
				h.rc.attach(target)
				h.rc.subscribePreviewFor(viewer, 1, func(w renderWake) { h.previews <- w })
				h.rc.noteSyncBegin(p, 1)
				watchdog := h.armedTimers(t)[0]
				transition.run(h.rc, target)

				// PTY-owned output remains visible to picker observers while the
				// target attachment is parked. Its deadline must retain the sync gate.
				h.rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "complete preview"})
				timers := h.armedTimers(t)
				require.Len(t, timers, 1)
				h.rc.mu.Lock()
				normalWorkerDone := h.rc.normalLane.token.done
				h.rc.mu.Unlock()
				require.NotNil(t, normalWorkerDone)
				timers[0].ch <- time.Time{}
				requireWorkerExit(t, normalWorkerDone)
				requireNoWake(t, h.previews)

				h.rc.mu.Lock()
				require.Same(t, h.rc.syncBatches[p].lane.token.timer, watchdog.mock)
				h.rc.mu.Unlock()
				h.rc.noteSyncEnd(p, 1)
				preview := awaitWake(t, h.previews)
				preview.lease = nil
				require.Equal(t, renderWake{urgent: true, coalesced: 1}, preview)
				requireNoWake(t, h.previews)
				requireNoWake(t, h.wakes)
			})
		}
	})

	t.Run("replacement first paint waits for active batch and rejects stale owner", func(t *testing.T) {
		h := newCoordinatorHarness(t)
		old, replacement, p := &attachedClient{}, &attachedClient{}, &pane{}
		h.rc.attach(old)
		h.rc.noteSyncBegin(p, 1)
		watchdog := h.armedTimers(t)[0]
		h.rc.noteReplace(old, replacement, false)
		require.False(t, h.rc.markAttachmentReady(h.rc.attachmentLease(old)), "old attachment Welcome is stale")
		require.True(t, h.rc.markAttachmentReady(h.rc.attachmentLease(replacement)))

		h.rc.invalidateForAttachment(old, renderInvalidation{class: invalidateUrgent, producer: "stale old attachment"})
		require.Empty(t, h.armedTimers(t))
		h.rc.invalidateForAttachment(replacement, renderInvalidation{class: invalidateUrgent, reset: true, producer: "replacement first paint"})
		timers := h.armedTimers(t)
		require.Len(t, timers, 1)
		h.rc.mu.Lock()
		normalWorkerDone := h.rc.normalLane.token.done
		h.rc.mu.Unlock()
		require.NotNil(t, normalWorkerDone)
		timers[0].ch <- time.Time{}
		requireWorkerExit(t, normalWorkerDone)
		requireNoWake(t, h.wakes)

		h.rc.mu.Lock()
		require.Same(t, h.rc.syncBatches[p].lane.token.timer, watchdog.mock)
		h.rc.mu.Unlock()
		h.rc.noteSyncEnd(p, 1)
		wake := awaitWake(t, h.wakes)
		require.True(t, wake.reset)
		require.True(t, wake.urgent)
		require.Same(t, replacement, wake.lease.attachment)
		requireNoWake(t, h.wakes)
	})

	t.Run("pane removal and session teardown cancel sync workers", func(t *testing.T) {
		for _, lifecycle := range []struct {
			name string
			end  func(*renderCoordinator, *pane)
		}{
			{"pane removal", func(rc *renderCoordinator, p *pane) { rc.noteSyncPaneRemoved(p) }},
			{"session teardown", func(rc *renderCoordinator, _ *pane) { rc.beginSessionTeardown().finish() }},
		} {
			t.Run(lifecycle.name, func(t *testing.T) {
				clk := newCoordinatorMockClock(t, 2)
				rc := newRenderCoordinator(renderCoordinatorOptions{clock: clk.clock})
				p := &pane{}
				rc.noteSyncBegin(p, 1)
				syncTimer := <-clk.timers
				rc.mu.Lock()
				syncDone := rc.syncBatches[p].lane.token.done
				rc.mu.Unlock()
				lifecycle.end(rc, p)
				syncTimer.mock.AssertNumberOfCalls(t, "Stop", 1)
				requireWorkerExit(t, syncDone)
				if lifecycle.name == "session teardown" {
					rc.waitForTimerWorkers()
				}
			})
		}
	})
}

func TestRenderCoordinatorResizeEpochDispatch(t *testing.T) {
	newResizeFixture := func(t *testing.T) (*Daemon, *session, *attachedClient, chan ports.Frame, *coordinatorMockClock, chan renderInvalidation) {
		t.Helper()
		p, releasePTY := newBlockingPTY(t)
		t.Cleanup(releasePTY)
		d, sess, ac, sends := newManualSessionWithPTYs(t, p)
		clk := newCoordinatorMockClock(t, 4)
		d.clock = clk.clock
		invs := make(chan renderInvalidation, 4)
		rc := newRenderCoordinator(renderCoordinatorOptions{
			clock:        clk.clock,
			wake:         func(renderWake) {},
			onInvalidate: func(inv renderInvalidation) { invs <- inv },
		})
		rc.attach(ac)
		sess.installRenderCoordinator(rc)
		return d, sess, ac, sends, clk, invs
	}

	t.Run("bounded timer records metadata and dispatches through the coordinator", func(t *testing.T) {
		d, sess, ac, sends, clk, invs := newResizeFixture(t)

		d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
		timer := awaitCoordinatorScheduledTimer(t, clk)
		require.GreaterOrEqual(t, timer.duration, minOutputRenderDeadline)
		require.LessOrEqual(t, timer.duration, maxOutputRenderDeadline)
		snap := sess.renderCoordinator().resizeSnapshot()
		require.Equal(t, domain.Size{Cols: 120, Rows: 24}, snap.size,
			"the coordinator must record the latest requested resize before delegating")
		require.Same(t, ac, snap.source)
		require.Equal(t, uint64(1), snap.epoch)
		requireNoCoordinatorOutputFrame(t, sends)

		done := captureResizeCallbackDone(t, sess.renderCoordinator())
		timer.ch <- time.Time{}
		awaitTestCompletion(t, done, "resize callback did not complete")
		inv := awaitInvalidation(t, invs)
		require.True(t, inv.reset, "the resize dispatch must request a full-redraw invalidation")
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)
	})

	t.Run("stale generations stay rejected", func(t *testing.T) {
		d, sess, ac, sends, clk, invs := newResizeFixture(t)

		d.resize(sess, ac, domain.Size{Cols: 100, Rows: 24})
		first := awaitCoordinatorScheduledTimer(t, clk)
		d.resize(sess, ac, domain.Size{Cols: 120, Rows: 24})
		latest := awaitCoordinatorScheduledTimer(t, clk)
		require.Equal(t, uint64(2), sess.renderCoordinator().resizeSnapshot().epoch,
			"every resize request must advance the coordinator epoch")

		done := captureResizeCallbackDone(t, sess.renderCoordinator())
		first.ch <- time.Time{}
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)

		latest.ch <- time.Time{}
		awaitTestCompletion(t, done, "latest resize callback did not complete")
		inv := awaitInvalidation(t, invs)
		require.True(t, inv.reset)
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)
		require.Equal(t, domain.Size{Cols: 120, Rows: 24}, sess.renderCoordinator().resizeSnapshot().size)
	})

	t.Run("cancellation still drops the pending dispatch", func(t *testing.T) {
		d, sess, ac, sends, clk, invs := newResizeFixture(t)

		d.resize(sess, ac, domain.Size{Cols: 90, Rows: 30})
		timer := awaitCoordinatorScheduledTimer(t, clk)
		done := captureResizeCallbackDone(t, sess.renderCoordinator())
		sess.renderCoordinator().noteDetach(ac)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, done, "cancelled resize callback did not complete")
		requireNoInvalidation(t, invs)
		requireNoCoordinatorOutputFrame(t, sends)
	})

	t.Run("request reports immediate completion and async schedule acceptance", func(t *testing.T) {
		t.Run("immediate reset invalidation", func(t *testing.T) {
			d, sess, ac, _, _, invs := newResizeFixture(t)

			require.True(t, d.requestTransactionalResize(sess, ac, domain.Size{Cols: 120, Rows: 24}, true))
			inv := awaitInvalidation(t, invs)
			require.True(t, inv.reset)
		})

		t.Run("superseded immediate transaction", func(t *testing.T) {
			resizeStarted := make(chan struct{})
			releaseResize := make(chan struct{})
			pty := &transactionalResizePTY{onResize: func() {
				close(resizeStarted)
				<-releaseResize
			}}
			d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
			clk := newCoordinatorMockClock(t, 2)
			d.clock = clk.clock
			invs := make(chan renderInvalidation, 1)
			rc := newRenderCoordinator(renderCoordinatorOptions{
				clock:        clk.clock,
				wake:         func(renderWake) {},
				onInvalidate: func(inv renderInvalidation) { invs <- inv },
			})
			rc.attach(ac)
			sess.installRenderCoordinator(rc)

			result := make(chan bool, 1)
			go func() {
				result <- d.requestTransactionalResize(sess, ac, domain.Size{Cols: 120, Rows: 24}, true)
			}()
			<-resizeStarted
			require.NotZero(t, rc.recordResizeRequest(domain.Size{Cols: 140, Rows: 30}, ac))
			close(releaseResize)

			require.False(t, <-result)
			requireNoInvalidation(t, invs)
		})

		t.Run("async schedule and teardown rejection", func(t *testing.T) {
			d, sess, ac, _, clk, _ := newResizeFixture(t)

			require.True(t, d.requestTransactionalResize(sess, ac, domain.Size{Cols: 120, Rows: 24}, false))
			awaitCoordinatorScheduledTimer(t, clk)

			// A torn-down coordinator cannot accept a stale attachment's schedule.
			sess.renderCoordinator().beginSessionTeardown().finish()
			require.False(t, d.requestTransactionalResize(sess, ac, domain.Size{Cols: 140, Rows: 30}, false))
		})
	})
}
