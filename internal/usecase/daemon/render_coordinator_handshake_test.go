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
	h.rc.attach(ac)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true, producer: "pty"})
	timers := h.armedTimers(t)
	require.Len(t, timers, 1)
	timers[0].ch <- time.Time{}
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
	h.rc.attach(ac)
	require.True(t, h.rc.markAttachmentReady(ac))
	h.rc.notePark(ac)
	h.rc.attach(ac)

	h.rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "session"})
	timers := h.armedTimers(t)
	require.Len(t, timers, 1)
	timers[0].ch <- time.Time{}
	requireNoWake(t, h.wakes)

	require.True(t, h.rc.markAttachmentReady(ac))
	h.rc.fireCurrent(false)
	wake := awaitWake(t, h.wakes)
	require.True(t, wake.reset)
	require.Equal(t, 1, wake.coalesced)
}
