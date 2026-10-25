package daemon

import (
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
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
	require.Nil(t, wake.lease, "shared wakes do not carry an attachment lease")
	requireNoWake(t, h.wakes)
}

// TestRenderCoordinatorFanoutDoesNotWaitForSlowTransport blocks only the first
// output send of one attachment. The paired healthy attachment must still
// receive its frame while this peer is stuck.
func TestRenderCoordinatorFanoutDoesNotWaitForSlowTransport(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, healthy, healthySends, _ := newManualTabSession(t, 1)
		gate := newSendGate()
		done := make(chan struct{})
		var doneOnce sync.Once
		slowTransport, _ := newTrackedConnection(t,
			withSendFunc(func(frame wire.Envelope, log *connLog) error {
				log.record(frame)
				if envelopeMessageName(nil, frame.Payload) == "Output" {
					gate.enter()
					gate.wait()
				}
				doneOnce.Do(func() { close(done) })
				return nil
			}),
			withCloseHook(gate.open),
		)
		slow := &attachedClient{
			tr:       slowTransport,
			output:   newOutputStateStream(),
			geometry: domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}},
		}
		slow.initOverlays()
		slow.setSession(sess)
		sess.core().mu.Lock()
		sess.core().attachments[slow] = struct{}{}
		sess.core().mu.Unlock()

		rc := d.ensureRenderCoordinator(sess)
		rc.opts.clock = nil // publish, then fire synchronously below
		rc.attach(healthy)
		rc.attach(slow)
		rc.invalidate(renderInvalidation{class: invalidateUrgent, reset: true, producer: "fanout"})
		rc.fireCurrent(false)

		frame := requireReceived(t, healthySends, "healthy attachment remained blocked behind the slow transport")
		require.Equal(t, "Output", envelopeMessageName(t, frame.Payload), "healthy attachment did not receive output")
		requireReceived(t, gate.entered, "slow transport did not enter its blocked output send")
		gate.open()
		requireReceived(t, done, "slow output send did not finish after release")
	})
}

func TestRenderCoordinatorWelcomeReadinessIsAttachmentScoped(t *testing.T) {
	h := newCoordinatorHarness(t)
	slow, healthy := &attachedClient{}, &attachedClient{}
	h.rc.attachWithReadiness(slow, false)
	h.rc.attachWithReadiness(healthy, true)
	slowLease := h.rc.attachmentLease(slow)
	require.NotNil(t, slowLease)

	h.rc.invalidate(renderInvalidation{class: invalidateOutput, reset: true, producer: "pty"})
	h.rc.fireCurrent(false)
	wake := awaitTestValue(t, h.wakes, "ready attachment did not receive output while peer handshakes")
	require.Contains(t, wake.attachmentLeases, healthy)
	_, slowSelected := wake.attachmentLeases[slow]
	require.False(t, slowSelected)
	h.rc.mu.Lock()
	require.True(t, h.rc.pending, "the handshaking peer must retain the shared mutation")
	h.rc.mu.Unlock()

	require.True(t, h.rc.markAttachmentReady(slowLease))
	h.rc.fireCurrentForLease(slowLease)
	wake = awaitTestValue(t, h.wakes, "handshaken attachment did not receive deferred output")
	require.Contains(t, wake.attachmentLeases, slow)
	_, healthySelected := wake.attachmentLeases[healthy]
	require.False(t, healthySelected, "a deferred peer must not replay output to a healthy peer")
	h.rc.mu.Lock()
	require.False(t, h.rc.pending, "the shared mutation must clear after the handshake catches up")
	h.rc.mu.Unlock()
}

// A wake captured for an old attachment generation must not become valid after
// the same attachment object is rebound to a replacement connection.
func TestRenderCoordinatorStaleGenerationWakeIsFenced(t *testing.T) {
	h := newCoordinatorHarness(t)
	ac := &attachedClient{}
	h.rc.attachWithReadiness(ac, true)
	oldLease := h.rc.attachmentLease(ac)
	require.NotNil(t, oldLease)
	h.rc.invalidateForLease(ac, oldLease, renderInvalidation{class: invalidateUrgent, reset: true, producer: "old"})
	h.rc.fireCurrentForLease(oldLease)
	wake := awaitTestValue(t, h.wakes, "old attachment wake was not published")
	require.True(t, h.rc.wakeCurrent(wake))

	h.rc.noteDetach(ac)
	h.rc.attachWithReadiness(ac, true)
	require.False(t, h.rc.wakeCurrent(wake), "a stale generation wake must not target the replacement connection")
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
	h.rc.noteDetach(ac)
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
