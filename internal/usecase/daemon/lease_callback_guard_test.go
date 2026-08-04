package daemon

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestResizeAndRetryCallbacksRejectSameObjectLeaseReplacement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		schedule func(*renderCoordinator, *attachedClient, *attachmentLease, func())
		done     func(*testing.T, *renderCoordinator) <-chan struct{}
	}{
		{
			name: "resize",
			schedule: func(rc *renderCoordinator, ac *attachedClient, lease *attachmentLease, run func()) {
				rc.scheduleResizeForLease(domain.Size{Cols: 100, Rows: 30}, ac, lease, func(uint64) { run() })
			},
			done: captureResizeCallbackDone,
		},
		{
			name: "retry",
			schedule: func(rc *renderCoordinator, ac *attachedClient, lease *attachmentLease, run func()) {
				epoch := rc.recordResizeRequestForLease(domain.Size{Cols: 100, Rows: 30}, ac, lease)
				require.NotZero(t, epoch)
				require.True(t, rc.resizeCurrentForLease(epoch, ac, lease, true))
				rc.scheduleResizeRetryForLease(epoch, ac, lease, run)
			},
			done: func(t *testing.T, rc *renderCoordinator) <-chan struct{} {
				t.Helper()
				rc.mu.Lock()
				defer rc.mu.Unlock()
				if rc.retryLane.token == nil {
					t.Fatal("coordinator did not publish a retry callback completion")
				}
				return rc.retryLane.token.done
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := newCoordinatorMockClock(t, 1)
			rc := newRenderCoordinator(renderCoordinatorOptions{clock: clock.clock})
			ac := &attachedClient{}
			rc.attach(ac)
			lease := rc.attachmentLease(ac)
			ran := false
			tc.schedule(rc, ac, lease, func() { ran = true })
			timer := awaitCoordinatorScheduledTimer(t, clock)
			done := tc.done(t, rc)
			require.NotNil(t, done)

			rc.noteDetach(ac)
			rc.attach(ac) // resume the exact same object under a new incarnation
			timer.ch <- time.Time{}
			awaitTestCompletion(t, done, "stale callback did not complete")
			for range 32 {
				runtime.Gosched()
			}
			require.False(t, ran, "stale %s callback ran after lease replacement", tc.name)
		})
	}
}
