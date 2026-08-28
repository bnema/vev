package daemon

import (
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
)

// fanoutBlockingTransport blocks only the first output send. The paired
// healthy attachment must still receive its frame while this peer is stuck.
type fanoutBlockingTransport struct {
	sent       chan wire.Frame
	entered    chan struct{}
	release    chan struct{}
	done       chan struct{}
	enteredOne sync.Once
	releaseOne sync.Once
	doneOne    sync.Once
}

func newFanoutBlockingTransport() *fanoutBlockingTransport {
	return &fanoutBlockingTransport{
		sent:    make(chan wire.Frame, 8),
		entered: make(chan struct{}),
		release: make(chan struct{}),
		done:    make(chan struct{}),
	}
}

func (t *fanoutBlockingTransport) Send(frame wire.Frame) error {
	t.sent <- frame
	if frame.Type == wire.MsgOutput {
		t.enteredOne.Do(func() { close(t.entered) })
		<-t.release
	}
	t.doneOne.Do(func() { close(t.done) })
	return nil
}

func (t *fanoutBlockingTransport) Recv() (wire.Frame, error) { return wire.Frame{}, io.EOF }

func (t *fanoutBlockingTransport) Close() error {
	t.releaseOne.Do(func() { close(t.release) })
	return nil
}

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

func TestRenderCoordinatorFanoutDoesNotWaitForSlowTransport(t *testing.T) {
	d, sess, healthy, healthySends, _ := newManualTabSession(t, 1)
	slowTransport := newFanoutBlockingTransport()
	slow := &attachedClient{
		tr:     slowTransport,
		output: newOutputStateStream(),
		size:   domain.Size{Cols: 80, Rows: 24},
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

	select {
	case frame := <-healthySends:
		require.Equal(t, wire.MsgOutput, frame.Type, "healthy attachment did not receive output")
	case <-time.After(2 * time.Second):
		t.Fatal("healthy attachment remained blocked behind the slow transport")
	}
	select {
	case <-slowTransport.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("slow transport did not enter its blocked output send")
	}
	slowTransport.releaseOne.Do(func() { close(slowTransport.release) })
	select {
	case <-slowTransport.done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow output send did not finish after release")
	}
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
