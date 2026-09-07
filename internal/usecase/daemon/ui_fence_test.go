package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/require"
)

func TestUIFenceRegistrationAndCapturedThreshold(t *testing.T) {
	for _, scenario := range []string{"processed", "rebind", "detach", "teardown"} {
		t.Run(scenario, func(t *testing.T) {
			rc := newRenderCoordinator(renderCoordinatorOptions{})
			ac := &attachedClient{}
			lease := rc.attachWithReadiness(ac, true)
			t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
			before := rc.captureUIFence(lease)
			require.False(t, rc.registerUIFence(lease, 0, nil))
			require.True(t, rc.registerUIFence(lease, 7, nil))
			require.False(t, rc.registerUIFence(lease, 7, nil))
			require.False(t, rc.registerUIFence(lease, 8, nil))
			require.Nil(t, rc.retireUIFence(lease, before), "a capture already underway cannot confirm new input")
			captured := rc.captureUIFence(lease)
			require.Greater(t, captured, before)
			switch scenario {
			case "rebind":
				rc.mu.Lock()
				replacement := rc.rebindAttachmentWithReadinessLocked(ac, true)
				rc.mu.Unlock()
				require.Zero(t, rc.captureUIFence(replacement))
			case "detach":
				rc.beginDetach(ac).finish()
			case "teardown":
				rc.beginSessionTeardown().finish()
			}
			pending := rc.retireUIFence(lease, captured)
			if scenario != "processed" {
				require.Nil(t, pending)
				return
			}
			require.NotNil(t, pending)
			require.Equal(t, uint64(7), pending.actionID)
			require.Nil(t, rc.retireUIFence(lease, captured), "only one sender owns retirement")
			require.False(t, rc.registerUIFence(lease, 8, nil), "a selected receipt still owns the slot until accepted send")
			rc.finishUIFence(lease, pending)
			require.True(t, rc.registerUIFence(lease, 8, nil))
			require.Nil(t, rc.retireUIFence(lease, captured), "an older confirmed capture cannot complete the next action")
		})
	}
}

func TestUIFenceRegistrationDoesNotRunInvalidationCallbackInline(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(renderInvalidation) {
		close(entered)
		<-release
	}})
	lease := rc.attachWithReadiness(&attachedClient{}, true)
	t.Cleanup(func() { close(release); rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
	returned := make(chan bool, 1)
	go func() { returned <- rc.registerUIFence(lease, 1, nil) }()
	require.True(t, awaitTestValue(t, returned, "fence dispatch waited for render work"))
	awaitTestValue(t, entered, "invalidation work was not started")
}

func TestUIFenceExpiryAndCancellation(t *testing.T) {
	for _, scenario := range []string{"expire", "detach", "retire"} {
		t.Run(scenario, func(t *testing.T) {
			clock := portsmocks.NewMockClock(t)
			timer := portsmocks.NewMockTimer(t)
			deadline := make(chan time.Time, 1)
			armed, stopped := make(chan struct{}), make(chan struct{})
			clock.EXPECT().NewTimer(30 * time.Second).RunAndReturn(func(time.Duration) ports.Timer { return timer }).Once()
			timer.EXPECT().C().RunAndReturn(func() <-chan time.Time { close(armed); return deadline }).Once()
			timer.EXPECT().Stop().Run(func() { close(stopped) }).Return(true).Once()
			// Leave an invalidation pending without a timer so registration does
			// not need another clock expectation unrelated to fence expiry.
			rc := newRenderCoordinator(renderCoordinatorOptions{clock: clock})
			lease := rc.attachWithReadiness(&attachedClient{}, true)
			rc.pending, rc.pendingUrgent, rc.armed = true, true, true
			expired := make(chan uint64, 1)
			t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
			require.True(t, rc.registerUIFence(lease, 42, func(id uint64) { expired <- id }))
			awaitTestValue(t, armed, "expiry timer was not armed")
			switch scenario {
			case "expire":
				deadline <- time.Time{}
				require.Equal(t, uint64(42), awaitTestValue(t, expired, "fence did not expire"))
			case "detach":
				rc.beginDetach(lease.attachment).finish()
			case "retire":
				pending := rc.retireUIFence(lease, rc.captureUIFence(lease))
				require.NotNil(t, pending)
				rc.finishUIFence(lease, pending)
			}
			awaitTestValue(t, stopped, "fence timer was not stopped")
			rc.waitForTimerWorkers()
			require.Empty(t, expired)
			require.Nil(t, rc.retireUIFence(lease, ^uint64(0)))
		})
	}
}

func TestUIFenceThresholdExhaustion(t *testing.T) {
	rc := newRenderCoordinator(renderCoordinatorOptions{})
	lease := rc.attachWithReadiness(&attachedClient{}, true)
	lease.requestedUIFence = ^uint64(0)
	require.False(t, rc.registerUIFence(lease, 1, nil))
	require.False(t, rc.pending, "refusal must not reserve invalidation")
}
