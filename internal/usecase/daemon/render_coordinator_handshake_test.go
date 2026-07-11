package daemon

import (
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// awaitHandshakeWorker observes the worker completion channel rather than a
// scheduler-dependent no-wake window. The generated mock timer's channel is
// the only event that advances it.
func awaitHandshakeWorker(t *testing.T, done <-chan struct{}) {
	t.Helper()
	for range 1_000_000 {
		select {
		case <-done:
			return
		default:
			runtime.Gosched()
		}
	}
	t.Fatal("coordinator deadline worker did not complete")
}

// Fresh producer state may arrive after route has published an attachment but
// before the transport has accepted Welcome. It must remain pending until the
// exact attachment incarnation completes that handshake.
func TestRenderCoordinatorFreshWakeWaitsForWelcome(t *testing.T) {
	h := newCoordinatorHarness(t)
	ac := &attachedClient{}
	h.rc.attachWithReadiness(ac, false)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true, producer: "pty"})
	timer := awaitCoordinatorScheduledTimer(t, h.clk)
	h.rc.mu.Lock()
	workerDone := h.rc.normalWorkerDone
	h.rc.mu.Unlock()
	require.NotNil(t, workerDone)
	timer.ch <- time.Time{}
	requireWorkerExit(t, workerDone)
	requireNoWake(t, h.wakes)

	require.True(t, h.rc.markAttachmentReady(ac))
	h.rc.fireCurrent(false)
	wake := awaitWake(t, h.wakes)
	require.True(t, wake.reset)
	require.Equal(t, 1, wake.coalesced)
	require.Same(t, ac, wake.attachment)
	requireNoWake(t, h.wakes)
}

// Parking and resuming the same client object is a new incarnation: a failed
// Welcome on the replacement transport must not revive readiness from before
// the park.
func TestRenderCoordinatorParkedResumeRequiresNewWelcome(t *testing.T) {
	h := newCoordinatorHarness(t)
	ac := &attachedClient{}
	h.rc.attachWithReadiness(ac, false)
	require.True(t, h.rc.markAttachmentReady(ac))
	h.rc.notePark(ac)
	h.rc.attachWithReadiness(ac, false)

	h.rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "session"})
	timer := awaitCoordinatorScheduledTimer(t, h.clk)
	h.rc.mu.Lock()
	workerDone := h.rc.normalWorkerDone
	h.rc.mu.Unlock()
	require.NotNil(t, workerDone)
	timer.ch <- time.Time{}
	requireWorkerExit(t, workerDone)
	requireNoWake(t, h.wakes)

	require.True(t, h.rc.markAttachmentReady(ac))
	h.rc.fireCurrent(false)
	wake := awaitWake(t, h.wakes)
	require.True(t, wake.reset)
	require.Equal(t, 1, wake.coalesced)
}
