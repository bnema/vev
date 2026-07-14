package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestTimerLaneDetachRemovesCompleteToken(t *testing.T) {
	lane := timerLane{generation: 1}
	timer := portsmocks.NewMockTimer(t)
	token := lane.publishLocked(1, timer)
	require.NotNil(t, token)
	detached := lane.detachLocked()
	require.Same(t, token, detached)
	require.Nil(t, lane.token)
	select {
	case <-token.cancel:
	default:
		t.Fatal("detach did not cancel token")
	}
	require.Nil(t, lane.publishLocked(0, timer), "stale generation reacquired detached lane")
}

func TestRenderDeadlineCallbackCanInvalidateAgainWithoutJoiningItself(t *testing.T) {
	clock := newCoordinatorMockClock(t, 2)
	ac := &attachedClient{}
	wakeReturned := make(chan struct{})
	var rc *renderCoordinator
	rc = newRenderCoordinator(renderCoordinatorOptions{
		clock: clock.clock,
		wake: func(renderWake) {
			require.True(t, rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, producer: "reentrant-wake-test"}))
			close(wakeReturned)
		},
	})
	rc.attach(ac)
	t.Cleanup(func() {
		rc.beginSessionTeardown().finish()
		rc.waitForTimerWorkers()
	})
	require.True(t, rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, producer: "initial-wake-test"}))
	awaitCoordinatorScheduledTimer(t, clock).ch <- time.Time{}
	awaitTestCompletion(t, wakeReturned, "timer callback deadlocked while publishing its next invalidation")
}

func TestRenderTimerCallbacksCanReplaceTheirOwnLane(t *testing.T) {
	cases := []struct {
		name string
		run  func(*testing.T, *renderCoordinator, *coordinatorMockClock, *attachedClient)
	}{
		{
			name: "normal wake invalidates", run: func(t *testing.T, rc *renderCoordinator, clock *coordinatorMockClock, ac *attachedClient) {
				woke := make(chan struct{})
				rc.opts.wake = func(renderWake) {
					rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, producer: "table-normal"})
					close(woke)
				}
				rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, producer: "table-normal"})
				awaitCoordinatorScheduledTimer(t, clock).ch <- time.Time{}
				awaitTestCompletion(t, woke, "normal callback did not reenter invalidation")
			},
		},
		{
			name: "resize callback schedules resize", run: func(t *testing.T, rc *renderCoordinator, clock *coordinatorMockClock, ac *attachedClient) {
				lease := rc.attachmentLease(ac)
				rc.scheduleResizeForLease(domain.Size{Cols: 80, Rows: 24}, ac, lease, func(uint64) {
					rc.scheduleResizeForLease(domain.Size{Cols: 81, Rows: 24}, ac, lease, func(uint64) {})
				})
				timer := awaitCoordinatorScheduledTimer(t, clock)
				done := captureResizeCallbackDone(t, rc)
				timer.ch <- time.Time{}
				_ = awaitTestValue(t, clock.timers, "resize callback did not schedule replacement")
				awaitTestCompletion(t, done, "resize callback did not complete")
			},
		},
		{
			name: "retry callback schedules retry", run: func(t *testing.T, rc *renderCoordinator, clock *coordinatorMockClock, ac *attachedClient) {
				lease := rc.attachmentLease(ac)
				epoch := rc.recordResizeRequestForLease(domain.Size{Cols: 80, Rows: 24}, ac, lease)
				require.True(t, rc.resizeCurrentForLease(epoch, ac, lease, true))
				rc.scheduleResizeRetryForLease(epoch, ac, lease, func() { rc.scheduleResizeRetryForLease(epoch, ac, lease, func() {}) })
				awaitCoordinatorScheduledTimer(t, clock).ch <- time.Time{}
				_ = awaitTestValue(t, clock.timers, "retry callback did not schedule replacement")
			},
		},
		{
			name: "sync force transitions sync generation", run: func(t *testing.T, rc *renderCoordinator, clock *coordinatorMockClock, _ *attachedClient) {
				p := &pane{}
				rc.noteSyncBegin(p, 1, func() { rc.noteSyncBegin(p, 2) })
				awaitCoordinatorScheduledTimer(t, clock).ch <- time.Time{}
				_ = awaitTestValue(t, clock.timers, "sync force did not schedule replacement")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clock := newCoordinatorMockClock(t, 4)
			rc, ac := newRenderCoordinator(renderCoordinatorOptions{clock: clock.clock}), &attachedClient{}
			rc.attach(ac)
			tc.run(t, rc, clock, ac)
			rc.beginSessionTeardown().finish()
			rc.waitForTimerWorkers()
		})
	}
}

func TestRenderTimerScheduleRacesTerminalTeardown(t *testing.T) {
	for range 32 {
		clock := newCoordinatorMockClock(t, 2)
		rc, ac := newRenderCoordinator(renderCoordinatorOptions{clock: clock.clock}), &attachedClient{}
		rc.attach(ac)
		start := make(chan struct{})
		scheduled := make(chan struct{})
		go func() {
			<-start
			rc.invalidateForAttachment(ac, renderInvalidation{class: invalidateUrgent, producer: "race"})
			close(scheduled)
		}()
		close(start)
		cleanup := rc.beginSessionTeardown()
		awaitTestCompletion(t, scheduled, "schedule raced terminal teardown")
		cleanup.finish()
		rc.waitForTimerWorkers()
		rc.mu.Lock()
		require.True(t, rc.torndown)
		require.Nil(t, rc.normalLane.token)
		rc.mu.Unlock()
	}
}

func TestTimerSupervisorCancelsNeverFiringWorker(t *testing.T) {
	var lane timerLane
	generation, _ := lane.replaceLocked()
	token := lane.publishLocked(generation, portsmocks.NewMockTimer(t))
	var supervisor timerSupervisor
	supervisor.startLocked(token, nil, func() { t.Fatal("cancelled worker fired") })
	lane.detachLocked()
	supervisor.wait()
}
