package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by
// every test that is not specifically exercising the debounce.
type stubClock struct{}

func (stubClock) Now() time.Time                     { return time.Time{} }
func (stubClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

type stubTimer struct{}

func (stubTimer) C() <-chan time.Time      { return nil }
func (stubTimer) Reset(time.Duration) bool { return false }
func (stubTimer) Stop() bool               { return true }

func newTestDaemon(t *testing.T, ptys ports.PTYFactory, clk ports.Clock) *Daemon {
	t.Helper()
	d := New(ptys, clk, slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.serveCtx, d.serveCancel = context.WithCancel(context.Background())
	d.hardCtx, d.hardCancel = context.WithCancel(context.Background())
	t.Cleanup(d.serveCancel)
	t.Cleanup(d.hardCancel)
	return d
}

// newBlockingPTY returns a MockPTY whose Read blocks until release is called,
// then reports io.EOF (the "child exited" signal).
func newBlockingPTY(t *testing.T) (*portsmocks.MockPTY, func()) {
	t.Helper()
	p := portsmocks.NewMockPTY(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		<-release
		return 0, io.EOF
	}).Maybe()
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) { return len(b), nil }).Maybe()
	p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	// Close unblocks a parked Read, exactly as the real PTY does (its Close
	// closes the master fd, failing the in-flight read).
	p.EXPECT().Close().RunAndReturn(func() error { unblock(); return nil }).Maybe()
	p.EXPECT().Pid().Return(4242).Maybe()
	return p, unblock
}

func newFactory(t *testing.T, p ports.PTY) *portsmocks.MockPTYFactory {
	t.Helper()
	f := portsmocks.NewMockPTYFactory(t)
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(p, nil).Maybe()
	return f
}

// newConn scripts a MockTransport: Recv yields first then more (in order),
// then blocks until the connection is released or Closed (returning io.EOF).
// Every Send is captured on the returned channel.
func newConn(t *testing.T, first ports.Frame, more ...ports.Frame) (*portsmocks.MockTransport, chan ports.Frame, func()) {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	sends := make(chan ports.Frame, 64)
	recvCh := make(chan ports.Frame, 1+len(more))
	recvCh <- first
	for _, f := range more {
		recvCh <- f
	}
	done := make(chan struct{})
	var once sync.Once
	closeDone := func() { once.Do(func() { close(done) }) }

	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case f := <-recvCh:
			return f, nil
		case <-done:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		sends <- f
		return nil
	}).Maybe()
	tr.EXPECT().Close().RunAndReturn(func() error { closeDone(); return nil }).Maybe()
	return tr, sends, closeDone
}

func mustHello(intent uint8, name string, sz domain.Size) ports.Frame {
	return ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
		Version: ports.ProtocolVersion, Intent: intent, Name: name, Size: sz, TermEnv: "xterm-256color",
	})}
}

// awaitFrame waits for the next frame of type typ on ch, failing on timeout.
func awaitFrame(t *testing.T, ch chan ports.Frame, typ ports.MsgType) ports.Frame {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case f := <-ch:
			if f.Type == typ {
				return f
			}
		case <-deadline:
			t.Fatalf("timed out waiting for frame type %d", typ)
		}
	}
}

func firstSession(d *Daemon) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, s := range d.sessions {
		return s
	}
	return nil
}

func sessionCount(d *Daemon) int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.sessions)
}

// --- handshake --------------------------------------------------------------

func TestHandshakeEphemeralHappy(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))

	var hg sync.WaitGroup
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(tr) }()

	w := awaitFrame(t, sends, ports.MsgWelcome)
	welcome, err := ports.UnmarshalWelcome(w.Payload)
	require.NoError(t, err)
	require.Equal(t, "0", welcome.SessionName)
	require.True(t, welcome.Ephemeral)
	awaitFrame(t, sends, ports.MsgOutput) // guaranteed first paint
	require.Equal(t, 1, sessionCount(d))

	releaseConn()
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
}

func TestHandshakeNewHappy(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))

	var hg sync.WaitGroup
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(tr) }()

	w := awaitFrame(t, sends, ports.MsgWelcome)
	welcome, err := ports.UnmarshalWelcome(w.Payload)
	require.NoError(t, err)
	require.Equal(t, "work", welcome.SessionName)
	require.False(t, welcome.Ephemeral)

	sess := firstSession(d)
	require.NotNil(t, sess)

	releaseConn()
	d.killSession(sess, ports.ReasonServerShutdown)
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
}

func TestHandshakeVersionMismatch(t *testing.T) {
	// No Open expectation: the factory must never be asked to spawn a PTY.
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	bad := ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
		Version: ports.ProtocolVersion + 99, Intent: ports.IntentEphemeral, Size: domain.Size{Cols: 80, Rows: 24},
	})}
	tr, sends, _ := newConn(t, bad)

	d.handleConn(tr) // returns after the rejection; no session, no goroutines

	e := awaitFrame(t, sends, ports.MsgError)
	em, err := ports.UnmarshalErrorMsg(e.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ErrVersionMismatch, em.Code)
	require.Equal(t, 0, sessionCount(d))
}

func TestHandshakeNameTaken(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	// Pre-populate a bare session (no goroutines) to collide with.
	d.sessions[domain.SessionID("x")] = &session{id: "x", name: "work"}

	tr, sends, _ := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
	d.handleConn(tr)

	e := awaitFrame(t, sends, ports.MsgError)
	em, _ := ports.UnmarshalErrorMsg(e.Payload)
	require.Equal(t, ports.ErrNameTaken, em.Code)
}

func TestHandshakeNoSuchSession(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	tr, sends, _ := newConn(t, mustHello(ports.IntentAttach, "ghost", domain.Size{Cols: 80, Rows: 24}))
	d.handleConn(tr)

	e := awaitFrame(t, sends, ports.MsgError)
	em, _ := ports.UnmarshalErrorMsg(e.Payload)
	require.Equal(t, ports.ErrNoSuchSession, em.Code)
}

// --- ephemeral numbering ----------------------------------------------------

func TestEphemeralNumberingReuse(t *testing.T) {
	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	// Emulate two ephemeral sessions "0" and "1".
	d.sessions["a"] = &session{id: "a", name: "0"}
	d.sessions["b"] = &session{id: "b", name: "1"}
	require.Equal(t, "2", d.allocEphemeralNameLocked())

	// Kill "0": the freed number is reused before "2".
	delete(d.sessions, "a")
	require.Equal(t, "0", d.allocEphemeralNameLocked())
}

// --- attach replace ---------------------------------------------------------

func TestAttachReplaceDetachesOld(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})

	// Client A creates and attaches to ephemeral "0".
	trA, sendsA, releaseA := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))
	var hg sync.WaitGroup
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(trA) }()
	awaitFrame(t, sendsA, ports.MsgWelcome)
	awaitFrame(t, sendsA, ports.MsgOutput)
	sess := firstSession(d)
	require.NotNil(t, sess)

	// Client B attaches to the same session, displacing A.
	trB, sendsB, releaseB := newConn(t, mustHello(ports.IntentAttach, "0", domain.Size{Cols: 80, Rows: 24}))
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(trB) }()
	awaitFrame(t, sendsB, ports.MsgWelcome)

	// A is notified it was detached.
	dA := awaitFrame(t, sendsA, ports.MsgDetached)
	det, _ := ports.UnmarshalDetached(dA.Payload)
	require.Equal(t, ports.ReasonDetach, det.Reason)

	// B is now the current client.
	sess.mu.Lock()
	require.NotNil(t, sess.client)
	sess.mu.Unlock()

	releaseA()
	releaseB()
	d.killSession(sess, ports.ReasonServerShutdown)
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
}

// --- detach semantics -------------------------------------------------------

func TestDetachKillsEphemeral(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, _ := newConn(t,
		mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}),
		ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})},
	)

	var hg sync.WaitGroup
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(tr) }()
	awaitFrame(t, sends, ports.MsgWelcome)

	require.Eventually(t, func() bool { return sessionCount(d) == 0 }, 2*time.Second, 5*time.Millisecond,
		"ephemeral session must die on client detach")

	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
}

func TestDetachKeepsNamed(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, _ := newConn(t,
		mustHello(ports.IntentNew, "keep", domain.Size{Cols: 80, Rows: 24}),
		ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})},
	)

	var hg sync.WaitGroup
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(tr) }()
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgDetached) // ack for explicit detach
	hg.Wait()                               // handler returns after detach

	require.Equal(t, 1, sessionCount(d), "named session survives detach")
	sess := firstSession(d)
	sess.mu.Lock()
	require.Nil(t, sess.client, "named session is headless after detach")
	sess.mu.Unlock()

	d.killSession(sess, ports.ReasonServerShutdown)
	releasePTY()
	d.sessWg.Wait()
}

// --- render scheduler debounce ----------------------------------------------

func TestSchedulerDebounceCoalesces(t *testing.T) {
	mc := portsmocks.NewMockClock(t)
	mt := portsmocks.NewMockTimer(t)
	timerCh := make(chan time.Time, 1)
	newTimerCalled := make(chan struct{}, 4)

	mc.EXPECT().NewTimer(debounceInterval).RunAndReturn(func(time.Duration) ports.Timer {
		select {
		case newTimerCalled <- struct{}{}:
		default:
		}
		return mt
	}).Maybe()
	mt.EXPECT().C().Return(timerCh).Maybe()
	mt.EXPECT().Stop().Return(true).Maybe()

	var outputs atomic.Int32
	gotOutput := make(chan struct{}, 1)
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgOutput {
			outputs.Add(1)
			select {
			case gotOutput <- struct{}{}:
			default:
			}
		}
		return nil
	}).Maybe()

	d := New(nil, mc, slog.New(slog.NewTextHandler(io.Discard, nil)))
	sctx, cancel := context.WithCancel(context.Background())
	win := &window{screen: vt.NewScreen(20, 5), dirty: make(chan struct{}, 1), size: domain.Size{Cols: 20, Rows: 5}}
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "s", name: "s", win: win, ctx: sctx, cancel: cancel, client: ac}

	win.mu.Lock()
	win.screen.Write([]byte("hi"))
	win.mu.Unlock()

	d.sessWg.Add(1)
	go d.scheduler(sess)

	// First dirty opens the debounce window.
	win.dirty <- struct{}{}
	<-newTimerCalled

	// A burst of further dirties inside the window must be absorbed.
	for i := 0; i < 5; i++ {
		select {
		case win.dirty <- struct{}{}:
		default:
		}
	}

	// Fire the timer once: exactly one render.
	timerCh <- time.Now()
	<-gotOutput

	cancel()
	d.sessWg.Wait()
	require.Equal(t, int32(1), outputs.Load(), "N dirties in one window must render exactly once")
}

// --- resize ordering --------------------------------------------------------

func TestResizeOrdersPTYBeforeScreen(t *testing.T) {
	newSize := domain.Size{Cols: 100, Rows: 30}

	p := portsmocks.NewMockPTY(t)
	var screenWidthAtResize int
	win := &window{screen: vt.NewScreen(80, 24), dirty: make(chan struct{}, 1), size: domain.Size{Cols: 80, Rows: 24}}
	p.EXPECT().Resize(newSize).RunAndReturn(func(sz domain.Size) error {
		// The screen must not yet be resized when the PTY is: proves order.
		screenWidthAtResize = win.screen.Frame.Width
		return nil
	}).Once()
	win.pty = p

	var gotOutput atomic.Bool
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgOutput {
			gotOutput.Store(true)
		}
		return nil
	}).Maybe()

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "s", name: "s", win: win, client: ac}

	d.resize(sess, ac, newSize)

	require.Equal(t, 80, screenWidthAtResize, "pty.Resize must run before screen.Resize")
	require.Equal(t, 100, win.screen.Frame.Width, "screen resized after pty")
	require.True(t, gotOutput.Load(), "resize forces a full redraw output")
}

// --- reader EOF -> registry-empty shutdown ----------------------------------

func TestReaderEOFRemovesSessionAndSignalsShutdown(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	// Read returns EOF immediately (child already gone).
	p.EXPECT().Read(mock.Anything).Return(0, io.EOF).Maybe()
	p.EXPECT().Close().Return(nil).Maybe()
	p.EXPECT().Pid().Return(1).Maybe()

	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, _ := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))

	var hg sync.WaitGroup
	hg.Add(1)
	go func() { defer hg.Done(); d.handleConn(tr) }()

	// The client is detached with ReasonSessionKilled when the child exits.
	det := awaitFrame(t, sends, ports.MsgDetached)
	dm, _ := ports.UnmarshalDetached(det.Payload)
	require.Equal(t, ports.ReasonSessionKilled, dm.Reason)

	select {
	case <-d.done:
	case <-time.After(2 * time.Second):
		t.Fatal("registry-empty shutdown was not signalled")
	}
	require.Equal(t, 0, sessionCount(d))

	hg.Wait()
	d.sessWg.Wait()
}

// --- full Serve lifecycle ---------------------------------------------------

func TestServeReturnsWhenLastSessionExits(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	tr, sends, _ := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))

	l := portsmocks.NewMockListener(t)
	connCh := make(chan ports.Transport, 1)
	connCh <- tr
	closed := make(chan struct{})
	var once sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (ports.Transport, error) {
		select {
		case c := <-connCh:
			return c, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	l.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	l.EXPECT().Addr().Return("mock").Maybe()

	d := New(newFactory(t, p), stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))

	served := make(chan error, 1)
	go func() { served <- d.Serve(context.Background(), l) }()

	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	// Child exits -> session removed -> registry empties -> Serve returns.
	releasePTY()

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after last session exited")
	}
}

func TestServeGracefulShutdownOnContextCancel(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	tr, sends, _ := newConn(t, mustHello(ports.IntentNew, "long", domain.Size{Cols: 80, Rows: 24}))

	l := portsmocks.NewMockListener(t)
	connCh := make(chan ports.Transport, 1)
	connCh <- tr
	closed := make(chan struct{})
	var once sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (ports.Transport, error) {
		select {
		case c := <-connCh:
			return c, nil
		case <-closed:
			return nil, io.EOF
		}
	}).Maybe()
	l.EXPECT().Close().RunAndReturn(func() error { once.Do(func() { close(closed) }); return nil }).Maybe()
	l.EXPECT().Addr().Return("mock").Maybe()

	d := New(newFactory(t, p), stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())

	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, l) }()

	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	cancel() // graceful shutdown

	det := awaitFrame(t, sends, ports.MsgDetached)
	dm, _ := ports.UnmarshalDetached(det.Payload)
	require.Equal(t, ports.ReasonServerShutdown, dm.Reason)

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Serve did not return after context cancel")
	}
}
