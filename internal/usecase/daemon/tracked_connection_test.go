package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/stretchr/testify/mock"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// connLog is a plain recorder for one tracked server connection. It implements
// no interface; the generated wire transport mock behind the connection feeds
// it through RunAndReturn closures.
type connLog struct {
	mu     sync.Mutex
	sends  []wire.Envelope
	closes int
	closed bool
	done   chan struct{}
}

func newConnLog() *connLog { return &connLog{done: make(chan struct{})} }

// record appends one sent envelope.
func (l *connLog) record(envelope wire.Envelope) {
	l.mu.Lock()
	l.sends = append(l.sends, envelope)
	l.mu.Unlock()
}

// markClosed records one Close call. Done is closed on the first call, after
// the closed flag is visible to Closed.
func (l *connLog) markClosed() {
	l.mu.Lock()
	l.closes++
	first := !l.closed
	l.closed = true
	l.mu.Unlock()
	if first {
		close(l.done)
	}
}

// Sends returns a copy of every envelope recorded so far.
func (l *connLog) Sends() []wire.Envelope {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]wire.Envelope(nil), l.sends...)
}

// Closed reports whether Close was called at least once.
func (l *connLog) Closed() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closed
}

// CloseCount reports how many times Close was called.
func (l *connLog) CloseCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.closes
}

// Done is closed on the first Close, after the closed flag is recorded.
func (l *connLog) Done() <-chan struct{} { return l.done }

// newTrackedConnection returns a server connection over a generated transport
// mock plus the log of its sends and closes. Options run first so their
// RunAndReturn expectations take precedence over the recording defaults
// registered afterwards (testify matches the first registered expectation).
func newTrackedConnection(t testing.TB, opts ...func(*mockServerConnection, *connLog)) (*mockServerConnection, *connLog) {
	t.Helper()
	conn := newMockServerConnection(t)
	log := newConnLog()
	for _, opt := range opts {
		opt(conn, log)
	}
	conn.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
		log.record(f)
		return nil
	}).Maybe()
	conn.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
		return wire.Envelope{}, errors.New("closed")
	}).Maybe()
	conn.EXPECT().Close().RunAndReturn(func() error {
		log.markClosed()
		return nil
	}).Maybe()
	return conn, log
}

// newQuietConnection is newTrackedConnection for call sites that never inspect
// the log.
func newQuietConnection(t testing.TB) *mockServerConnection {
	t.Helper()
	conn, _ := newTrackedConnection(t)
	return conn
}

// handshakeLog extends connLog with the signal that a Welcome send started.
type handshakeLog struct {
	*connLog
	welcome chan struct{}
}

// newHandshakeBlockingConnection returns a tracked connection whose Recv blocks
// until Close. When blockWelcome is set, the Welcome send signals
// handshakeLog.welcome and blocks until Close, then fails. Only the first
// message of a handshake may be scripted through first (nil for none): Recv
// yields it once before blocking.
func newHandshakeBlockingConnection(t testing.TB, blockWelcome bool, first ...wire.Envelope) (*mockServerConnection, *handshakeLog) {
	t.Helper()
	h := &handshakeLog{welcome: make(chan struct{})}
	var once sync.Once
	var mu sync.Mutex
	pending := append([]wire.Envelope(nil), first...)
	conn, log := newTrackedConnection(t, func(c *mockServerConnection, log *connLog) {
		h.connLog = log
		c.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
			if blockWelcome && envelopeMessageName(nil, f.Payload) == "Welcome" {
				once.Do(func() { close(h.welcome) })
				<-log.Done()
				return io.ErrClosedPipe
			}
			log.record(f)
			return nil
		}).Maybe()
		c.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
			mu.Lock()
			if len(pending) > 0 {
				f := pending[0]
				pending = pending[1:]
				mu.Unlock()
				return f, nil
			}
			mu.Unlock()
			<-log.Done()
			return wire.Envelope{}, io.ErrClosedPipe
		}).Maybe()
	})
	h.connLog = log
	return conn, h
}

// sendGate is a plain entered/release latch for tests that hold a send open.
type sendGate struct {
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newSendGate() *sendGate {
	return &sendGate{entered: make(chan struct{}), release: make(chan struct{})}
}

// enter signals that a send reached the gate.
func (g *sendGate) enter() { g.enteredOnce.Do(func() { close(g.entered) }) }

// wait blocks until open.
func (g *sendGate) wait() { <-g.release }

// open releases every blocked send; it is idempotent.
func (g *sendGate) open() { g.releaseOnce.Do(func() { close(g.release) }) }

// withSendFunc routes every Send through fn. fn owns recording: call
// log.record when the frame should be observable through Sends.
func withSendFunc(fn func(wire.Envelope, *connLog) error) func(*mockServerConnection, *connLog) {
	return func(c *mockServerConnection, log *connLog) {
		c.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error { return fn(f, log) }).Maybe()
	}
}

// withSendError records every frame and then fails the send with err.
func withSendError(err error) func(*mockServerConnection, *connLog) {
	return withSendFunc(func(f wire.Envelope, log *connLog) error {
		log.record(f)
		return err
	})
}

// withSendBlockedUntilClose makes the first send signal gate.entered and every
// send block until the connection is closed, then fail like a closed transport.
func withSendBlockedUntilClose(gate *sendGate) func(*mockServerConnection, *connLog) {
	return withSendFunc(func(_ wire.Envelope, log *connLog) error {
		gate.enter()
		<-log.Done()
		return errors.New("transport closed")
	})
}

// withCloseHook runs hook on every Close before the close is recorded.
func withCloseHook(hook func()) func(*mockServerConnection, *connLog) {
	return func(c *mockServerConnection, log *connLog) {
		c.EXPECT().Close().RunAndReturn(func() error {
			hook()
			log.markClosed()
			return nil
		}).Maybe()
	}
}

// withBlockingRecv makes Recv block until the connection is closed, then
// return io.EOF.
func withBlockingRecv() func(*mockServerConnection, *connLog) {
	return func(c *mockServerConnection, log *connLog) {
		c.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
			<-log.Done()
			return wire.Envelope{}, io.EOF
		}).Maybe()
	}
}

// newChannelConnection returns a tracked connection whose sends are published
// on the returned sends channel and whose Recv yields frames from the returned
// recv channel until it is closed (Close closes it) and then reports io.EOF.
func newChannelConnection(t testing.TB) (*mockServerConnection, chan wire.Envelope, chan wire.Envelope) {
	t.Helper()
	sends := make(chan wire.Envelope, 64)
	recv := make(chan wire.Envelope, 64)
	var once sync.Once
	conn, _ := newTrackedConnection(t, func(c *mockServerConnection, log *connLog) {
		c.EXPECT().Send(mock.Anything).RunAndReturn(func(f wire.Envelope) error {
			log.record(f)
			sends <- f
			return nil
		}).Maybe()
		c.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
			f, ok := <-recv
			if !ok {
				return wire.Envelope{}, io.EOF
			}
			return f, nil
		}).Maybe()
		c.EXPECT().Close().RunAndReturn(func() error {
			once.Do(func() { close(recv) })
			log.markClosed()
			return nil
		}).Maybe()
	})
	return conn, sends, recv
}

// typedFrames records the frames of a capability connection: sends carries
// SendServer/SendOutput frames, async carries SendServerAsync/SendOutputAsync
// frames, and recv feeds ReceiveClient until Close closes it.
type typedFrames struct {
	*connLog
	sends chan wire.Envelope
	async chan wire.Envelope
	recv  chan wire.Envelope
}

// newCapabilityConnection returns a generated ports.ServerConnection mock that
// reports caps, which the byte-level bridge cannot (the bridge derives
// capabilities from the raw transport type). Each send mode encodes its typed
// message to an envelope and publishes it on the matching channel; override
// registers extra expectations before the defaults so a test can replace one
// send mode.
func newCapabilityConnection(t testing.TB, caps protocol.ConnectionCapabilities, override ...func(*portsmocks.MockServerConnection, *typedFrames)) (*portsmocks.MockServerConnection, *typedFrames) {
	t.Helper()
	conn := portsmocks.NewMockServerConnection(t)
	frames := &typedFrames{
		connLog: newConnLog(),
		sends:   make(chan wire.Envelope, 64),
		async:   make(chan wire.Envelope, 64),
		recv:    make(chan wire.Envelope, 64),
	}
	for _, apply := range override {
		apply(conn, frames)
	}
	publish := func(ch chan wire.Envelope, message protocol.ServerMessage) error {
		envelope, err := testServerEnvelope(message)
		if err != nil {
			return err
		}
		frames.record(envelope)
		ch <- envelope
		return nil
	}
	conn.EXPECT().Capabilities().Return(caps).Maybe()
	conn.EXPECT().LinkState().Return(ports.LinkStateConnected).Maybe()
	conn.EXPECT().LinkEvents().Return(nil).Maybe()
	conn.EXPECT().SendServer(mock.Anything).RunAndReturn(func(m protocol.ServerMessage) error { return publish(frames.sends, m) }).Maybe()
	conn.EXPECT().SendServerSynchronous(mock.Anything).RunAndReturn(func(m protocol.ServerMessage) error { return publish(frames.sends, m) }).Maybe()
	conn.EXPECT().SendServerAsync(mock.Anything).RunAndReturn(func(m protocol.ServerMessage) error { return publish(frames.async, m) }).Maybe()
	conn.EXPECT().SendOutput(mock.Anything).RunAndReturn(func(o protocol.Output) error { return publish(frames.sends, o) }).Maybe()
	conn.EXPECT().SendOutputSynchronous(mock.Anything).RunAndReturn(func(o protocol.Output) error { return publish(frames.sends, o) }).Maybe()
	conn.EXPECT().SendOutputAsync(mock.Anything).RunAndReturn(func(o protocol.Output) error { return publish(frames.async, o) }).Maybe()
	conn.EXPECT().ReceiveClient().RunAndReturn(func() (protocol.ClientMessage, error) {
		envelope, ok := <-frames.recv
		if !ok {
			return nil, io.EOF
		}
		return sessionwire.DecodeClientEnvelope(envelope.Payload)
	}).Maybe()
	var once sync.Once
	conn.EXPECT().Close().RunAndReturn(func() error {
		once.Do(func() { close(frames.recv) })
		frames.markClosed()
		return nil
	}).Maybe()
	return conn, frames
}

// asyncCapabilities is what an async-capable carriage reports.
func asyncCapabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen, PreferredOutputWindow: protocol.MaxOutputWindow, AsyncSend: true}
}

// ownedSynchronousCapabilities is what an owned-synchronous carriage reports.
func ownedSynchronousCapabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen, PreferredOutputWindow: protocol.MaxOutputWindow, OwnedSynchronousSend: true}
}

// newFailingConnection returns a connection whose every send fails.
func newFailingConnection(t testing.TB) *mockServerConnection {
	t.Helper()
	conn, _ := newTrackedConnection(t, withSendError(errors.New("send failed")))
	return conn
}
