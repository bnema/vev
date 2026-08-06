package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

func TestRemoteViewWarmExpiryRemovesOnlyDetachedExactView(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	view := registerRemoteWarmTestView(t, d)

	d.parkRemoteViewWarm(view)
	warm, timer := remoteWarmTimer(t, clock, view)
	require.Equal(t, remoteViewWarmTTL, timer.duration)
	timer.ch <- time.Now()
	awaitTestCompletion(t, warm.done, "remote warm expiry did not complete")

	d.mu.Lock()
	require.Nil(t, d.remoteViewByKeyLocked(view.key))
	d.mu.Unlock()
	view.mu.Lock()
	require.True(t, view.closed)
	require.Nil(t, view.link)
	view.mu.Unlock()
}

func TestRemoteViewWarmActivationFencesStaleExpiry(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	view := registerRemoteWarmTestView(t, d)

	d.parkRemoteViewWarm(view)
	warm, timer := remoteWarmTimer(t, clock, view)
	attachment := &attachedClient{}
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(attachment))
	view.mu.Unlock()
	d.activateRemoteView(view)
	awaitTestCompletion(t, warm.done, "activating remote view did not cancel warm expiry")
	// A timer event already queued by the clock belongs to the canceled
	// generation and cannot remove the newly active remote view.
	timer.ch <- time.Now()

	d.mu.Lock()
	require.Same(t, view, d.remoteViewByKeyLocked(view.key))
	d.mu.Unlock()
	view.mu.Lock()
	require.False(t, view.closed)
	view.mu.Unlock()
}

func TestShutdownAllCancelsRemoteViewWarmTimer(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	view := registerRemoteWarmTestView(t, d)

	d.parkRemoteViewWarm(view)
	warm, _ := remoteWarmTimer(t, clock, view)
	d.shutdownAll(ports.ReasonServerShutdown)
	awaitTestCompletion(t, warm.done, "shutdown did not cancel remote warm timer")
}

func registerRemoteWarmTestView(t *testing.T, d *Daemon) *remoteView {
	t.Helper()
	key, err := remoteViewKeyForTarget(remoteLinkTestTarget())
	require.NoError(t, err)
	view := &remoteView{key: key}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()
	return view
}

func remoteWarmTimer(t *testing.T, clock *signalClock, view *remoteView) (*remoteViewWarm, *signalTimer) {
	t.Helper()
	timer := awaitTestValue(t, clock.timers, "remote warm timer was not created")
	view.mu.Lock()
	warm := view.warm
	view.mu.Unlock()
	require.NotNil(t, warm)
	return warm, timer
}
