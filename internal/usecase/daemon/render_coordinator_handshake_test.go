package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Fresh producer state may arrive after route has published an attachment but
// before the transport has accepted Welcome. It must remain pending until the
// exact attachment incarnation completes that handshake.
func TestRenderCoordinatorFreshWakeWaitsForWelcome(t *testing.T) {
	h := newCoordinatorHarness(t)
	ac := &attachedClient{}
	h.rc.attachWithReadiness(ac, false)
	lease := h.rc.attachmentLease(ac)
	require.NotNil(t, lease)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true, producer: "pty"})
	timer := awaitCoordinatorScheduledTimer(t, h.clk)
	h.rc.mu.Lock()
	workerDone := h.rc.normalLane.token.done
	h.rc.mu.Unlock()
	require.NotNil(t, workerDone)
	timer.ch <- time.Time{}
	requireWorkerExit(t, workerDone)
	requireNoWake(t, h.wakes)

	require.True(t, h.rc.markAttachmentReady(lease))
	h.rc.fireCurrent(false)
	wake := awaitWake(t, h.wakes)
	require.True(t, wake.reset)
	require.Equal(t, 1, wake.coalesced)
	require.Same(t, ac, wake.lease.attachment)
	requireNoWake(t, h.wakes)
}

// Parking and resuming the same client object is a new incarnation: a failed
// Welcome on the replacement transport must not revive readiness from before
// the park.
func TestRenderCoordinatorParkedResumeRequiresNewWelcome(t *testing.T) {
	h := newCoordinatorHarness(t)
	ac := &attachedClient{}
	h.rc.attachWithReadiness(ac, false)
	firstLease := h.rc.attachmentLease(ac)
	require.True(t, h.rc.markAttachmentReady(firstLease))
	h.rc.notePark(ac)
	h.rc.attachWithReadiness(ac, false)
	resumedLease := h.rc.attachmentLease(ac)
	require.NotNil(t, resumedLease)
	require.NotSame(t, firstLease, resumedLease)
	require.False(t, h.rc.markAttachmentReady(firstLease), "stale Welcome must not bless the reused attachment")

	h.rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "session"})
	timer := awaitCoordinatorScheduledTimer(t, h.clk)
	h.rc.mu.Lock()
	workerDone := h.rc.normalLane.token.done
	h.rc.mu.Unlock()
	require.NotNil(t, workerDone)
	timer.ch <- time.Time{}
	requireWorkerExit(t, workerDone)
	requireNoWake(t, h.wakes)

	require.True(t, h.rc.markAttachmentReady(resumedLease))
	h.rc.fireCurrent(false)
	wake := awaitWake(t, h.wakes)
	require.True(t, wake.reset)
	require.Equal(t, 1, wake.coalesced)
}
