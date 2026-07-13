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
		done     func(*renderCoordinator) <-chan struct{}
	}{
		{
			name: "resize",
			schedule: func(rc *renderCoordinator, ac *attachedClient, lease *attachmentLease, run func()) {
				rc.scheduleResizeForLease(domain.Size{Cols: 100, Rows: 30}, ac, lease, func(uint64) { run() })
			},
			done: func(rc *renderCoordinator) <-chan struct{} { return rc.resizeCallbackDone() },
		},
		{
			name: "retry",
			schedule: func(rc *renderCoordinator, ac *attachedClient, lease *attachmentLease, run func()) {
				epoch := rc.recordResizeRequestForLease(domain.Size{Cols: 100, Rows: 30}, ac, lease)
				require.NotZero(t, epoch)
				require.True(t, rc.resizeCurrentForLease(epoch, ac, lease, true))
				rc.scheduleResizeRetryForLease(epoch, ac, lease, run)
			},
			done: func(rc *renderCoordinator) <-chan struct{} {
				rc.mu.Lock()
				defer rc.mu.Unlock()
				return rc.retryDone
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
			done := tc.done(rc)
			require.NotNil(t, done)

			rc.notePark(ac)
			rc.attach(ac) // resume the exact same object under a new incarnation
			timer.ch <- time.Time{}
			<-done
			for range 32 {
				runtime.Gosched()
			}
			require.False(t, ran, "stale %s callback ran after lease replacement", tc.name)
		})
	}
}
