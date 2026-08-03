package daemon

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

type handshakeBlockingTransport struct {
	blockWelcome bool
	welcome      chan struct{}
	closed       chan struct{}
	closeOnce    sync.Once
}

func newHandshakeBlockingTransport(blockWelcome bool) *handshakeBlockingTransport {
	return &handshakeBlockingTransport{
		blockWelcome: blockWelcome,
		welcome:      make(chan struct{}),
		closed:       make(chan struct{}),
	}
}

func (t *handshakeBlockingTransport) Send(frame ports.Frame) error {
	if t.blockWelcome && frame.Type == ports.MsgWelcome {
		close(t.welcome)
		<-t.closed
		return io.ErrClosedPipe
	}
	return nil
}

func (t *handshakeBlockingTransport) Recv() (ports.Frame, error) {
	<-t.closed
	return ports.Frame{}, io.ErrClosedPipe
}

func (t *handshakeBlockingTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func TestBoundedHandshakeCompletionCancellationDoesNotDoubleReceive(t *testing.T) {
	ctx := &completionCancellationContext{}
	tr := &closeTrackingTransport{}
	done := make(chan error, 1)
	go func() {
		done <- boundedHandshakeOperation(ctx, tr, func() error {
			ctx.canceled = true
			return nil
		})
	}()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(testWaitTimeout):
		t.Fatal("handshake completion/cancellation race deadlocked")
	}
}

type completionCancellationContext struct{ canceled bool }

func (c *completionCancellationContext) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *completionCancellationContext) Done() <-chan struct{}       { return nil }
func (c *completionCancellationContext) Err() error {
	if c.canceled {
		return context.Canceled
	}
	return nil
}
func (*completionCancellationContext) Value(any) any { return nil }

func TestBoundedHandshakeCancellationDoesNotWaitForUninterruptibleOperation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := &closeTrackingTransport{}
	started := make(chan struct{})
	release := make(chan struct{})
	operationDone := make(chan struct{})
	result := make(chan error, 1)
	go func() {
		result <- boundedHandshakeOperation(ctx, tr, func() error {
			close(started)
			<-release
			close(operationDone)
			return nil
		})
	}()

	<-started
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(testWaitTimeout):
		t.Fatal("handshake cancellation waited for the transport operation")
	}
	close(release)
	awaitTestCompletion(t, operationDone, "handshake operation did not finish")
}

func TestHandshakeTimeoutClosesBlockedReceive(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	tr := newHandshakeBlockingTransport(false)
	done := make(chan struct{})
	go func() {
		d.handleConn(tr)
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, ports.HandshakeTimeout, timer.duration)
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "handshake receive did not stop after timeout")
	requireClosedHandshakeTransport(t, tr)
	d.mu.Lock()
	require.Empty(t, d.sessions)
	require.Empty(t, d.parked)
	d.mu.Unlock()
}

func TestFailedResumeHandshakeRestoresParkedCredential(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := newHandshakeBlockingTransport(false)
	sess, ac, err := d.route(helloResumeCapable(ports.IntentNew, "resume-timeout", 0), oldTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, oldTransport, false)
	require.Empty(t, sess.snapshotAttachments())

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	resumeTransport := newHandshakeBlockingTransport(true)
	done := make(chan struct{})
	go func() {
		d.handleHello(resumeTransport, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(helloResumeCapable(ports.IntentResume, sess.name, token))})
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, ports.HandshakeTimeout, timer.duration)
	<-resumeTransport.welcome
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "failed resume handshake did not finish")
	requireClosedHandshakeTransport(t, resumeTransport)
	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked, "failed pre-claim resume must restore the parked credential")
	require.Same(t, ac, parked.ac)
	require.Equal(t, token, ac.resumeToken)
	require.True(t, ac.parked)
	require.Empty(t, sess.snapshotAttachments())
}

func TestHandshakeTimeoutClosesBlockedWelcomeSend(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	clock := &signalClock{timers: make(chan *signalTimer, 4)}
	d := newTestDaemon(t, newFactory(t, pty), clock)
	tr := newHandshakeBlockingTransport(true)
	done := make(chan struct{})
	go func() {
		d.handleHello(tr, mustHello(ports.IntentNew, "timeout-send", domain.Size{Cols: 80, Rows: 24}))
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, ports.HandshakeTimeout, timer.duration)
	<-tr.welcome
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "handshake send did not stop after timeout")
	requireClosedHandshakeTransport(t, tr)
	d.mu.Lock()
	require.Empty(t, d.sessions, "a timed-out newly-created handshake must not leave an empty session")
	require.Empty(t, d.parked)
	d.mu.Unlock()
}

func requireClosedHandshakeTransport(t *testing.T, tr *handshakeBlockingTransport) {
	t.Helper()
	select {
	case <-tr.closed:
	default:
		t.Fatal("handshake timeout did not close transport")
	}
}

func TestHandshakeTimeoutCancelsRouteRestoreWait(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)
	restoreDone := make(chan struct{})
	d.mu.Lock()
	d.stopped["restoring"] = stoppedSession{name: "restoring", restoreDone: restoreDone}
	d.mu.Unlock()

	tr := newHandshakeBlockingTransport(false)
	done := make(chan struct{})
	go func() {
		d.handleHello(tr, mustHello(ports.IntentAttach, "restoring", domain.Size{Cols: 80, Rows: 24}))
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, ports.HandshakeTimeout, timer.duration)
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "route restore wait did not stop after handshake timeout")
	requireClosedHandshakeTransport(t, tr)
	d.mu.Lock()
	require.Empty(t, d.sessions)
	require.Empty(t, d.parked)
	d.mu.Unlock()
}

func TestHandshakeTimeoutRemovesRestoredEmptySession(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	d.mu.Lock()
	d.stopped["restored"] = stoppedSession{name: "restored", cwd: "/tmp"}
	d.mu.Unlock()

	clock := &signalClock{timers: make(chan *signalTimer, 4)}
	d.clock = clock
	tr := newHandshakeBlockingTransport(true)
	done := make(chan struct{})
	go func() {
		d.handleHello(tr, mustHello(ports.IntentAttach, "restored", domain.Size{Cols: 80, Rows: 24}))
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, ports.HandshakeTimeout, timer.duration)
	<-tr.welcome
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "timed-out restored attachment cleanup did not finish")
	requireClosedHandshakeTransport(t, tr)
	d.mu.Lock()
	require.Empty(t, d.sessions)
	_, retained := d.stopped["restored"]
	d.mu.Unlock()
	require.True(t, retained, "failed attach must retain the stopped session authority")
}

func TestHandshakeTimeoutPreservesUnrelatedAttachment(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	oldTransport := &closeTrackingTransport{}
	sess, old, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "shared", Size: domain.Size{Cols: 80, Rows: 24}}, oldTransport)
	require.NoError(t, err)

	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d.clock = clock
	tr := newHandshakeBlockingTransport(true)
	done := make(chan struct{})
	go func() {
		d.handleHello(tr, mustHello(ports.IntentAttach, "shared", domain.Size{Cols: 80, Rows: 24}))
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, ports.HandshakeTimeout, timer.duration)
	<-tr.welcome
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "timed-out attachment cleanup did not finish")
	requireClosedHandshakeTransport(t, tr)
	require.False(t, oldTransport.Closed(), "cleanup closed an unrelated attachment transport")
	require.Same(t, sess, old.currentAttachmentSession())
	require.Equal(t, []*attachedClient{old}, sess.snapshotAttachments())
	d.mu.Lock()
	require.Len(t, d.sessions, 1)
	require.Empty(t, d.parked)
	d.mu.Unlock()
	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
}

func TestSuccessfulRouteOutlivesStoppedHandshakeContext(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, pty), stubClock{})
	ctx, cancel := context.WithCancel(context.Background())
	sess, ac, err := d.routeWithContext(ctx, ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "lifetime", Size: domain.Size{Cols: 80, Rows: 24}}, &closeTrackingTransport{})
	require.NoError(t, err)
	cancel()
	select {
	case <-sess.ctx.Done():
		t.Fatal("session lifetime was parented to the handshake context")
	default:
	}
	require.NotNil(t, ac)
	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
}
