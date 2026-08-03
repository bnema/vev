package daemon

import (
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
	require.Len(t, d.sessions, 1)
	var sess *session
	for _, entry := range d.sessions {
		sess, _ = localSession(entry)
	}
	require.Empty(t, sess.snapshotAttachments())
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
