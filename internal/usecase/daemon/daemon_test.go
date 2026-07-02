package daemon

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
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

type signalClock struct {
	called chan struct{}
	once   sync.Once
}

func (c *signalClock) Now() time.Time { return time.Time{} }
func (c *signalClock) NewTimer(time.Duration) ports.Timer {
	c.once.Do(func() { close(c.called) })
	return stubTimer{}
}

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
	return newBlockingPTYWithWrites(t, nil)
}

func newBlockingPTYWithWrites(t *testing.T, writes chan<- []byte) (*portsmocks.MockPTY, func()) {
	t.Helper()
	p := portsmocks.NewMockPTY(t)
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		<-release
		return 0, io.EOF
	}).Maybe()
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) {
		if writes != nil {
			cp := append([]byte(nil), b...)
			writes <- cp
		}
		return len(b), nil
	}).Maybe()
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

func newFactorySeq(t *testing.T, ptys ...ports.PTY) *portsmocks.MockPTYFactory {
	t.Helper()
	f := portsmocks.NewMockPTYFactory(t)
	var mu sync.Mutex
	next := 0
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(string, []string, []string, domain.Size) (ports.PTY, error) {
			mu.Lock()
			defer mu.Unlock()
			if next >= len(ptys) {
				t.Fatalf("unexpected PTY open #%d", next+1)
			}
			p := ptys[next]
			next++
			return p, nil
		},
	).Maybe()
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

func frameInput(data []byte) ports.Frame {
	return ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{Data: data})}
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

func listSessions(t *testing.T, d *Daemon) ports.Sessions {
	t.Helper()
	tr, sends, _ := newConn(t, ports.Frame{Type: ports.MsgList, Payload: ports.MarshalList(ports.List{})})
	d.handleList(tr)
	f := awaitFrame(t, sends, ports.MsgSessions)
	sessions, err := ports.UnmarshalSessions(f.Payload)
	require.NoError(t, err)
	return sessions
}

func newCapturingTransport(t *testing.T) (*portsmocks.MockTransport, chan ports.Frame) {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	sends := make(chan ports.Frame, 64)
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		sends <- f
		return nil
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	return tr, sends
}

func newManualSessionWithPTYs(t *testing.T, ptys ...ports.PTY) (*Daemon, *session, *attachedClient, chan ports.Frame) {
	t.Helper()
	d := newTestDaemon(t, nil, stubClock{})
	tr, sends := newCapturingTransport(t)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{}), size: domain.Size{Cols: 80, Rows: 24}}
	sctx, cancel := context.WithCancel(d.serveCtx)
	windows := make([]*window, 0, len(ptys))
	for _, p := range ptys {
		wctx, wcancel := context.WithCancel(sctx)
		windows = append(windows, &window{pty: p, screen: vt.NewScreen(80, 23), dirty: make(chan struct{}, 1), size: domain.Size{Cols: 80, Rows: 23}, ctx: wctx, cancel: wcancel})
	}
	sess := &session{id: "manual", name: "work", ctx: sctx, cancel: cancel, windows: windows, client: ac}
	d.sessions[sess.id] = sess
	t.Cleanup(cancel)
	return d, sess, ac, sends
}

func newManualWindowSession(t *testing.T, n int) (*Daemon, *session, *attachedClient, chan ports.Frame, []func()) {
	t.Helper()
	ptys := make([]ports.PTY, 0, n)
	releases := make([]func(), 0, n)
	for range n {
		p, release := newBlockingPTY(t)
		ptys = append(ptys, p)
		releases = append(releases, release)
	}
	d, sess, ac, sends := newManualSessionWithPTYs(t, ptys...)
	return d, sess, ac, sends, releases
}

func activeWindowIndex(sess *session) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return sess.active
}

func testRow(text string) []renderer.Cell {
	cells := make([]renderer.Cell, 0, len(text))
	for _, r := range text {
		cells = append(cells, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	return cells
}

func TestCopyModeAltUInterceptsAndDoesNotForward(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.windows[0].scrollback = scopy.NewScrollback(4)
	sess.windows[0].screen.Write([]byte("live"))

	d.handleInput(sess, ac, []byte("\x1bu"))

	if ac.copyMode == nil {
		t.Fatal("copy mode not entered")
	}
	select {
	case got := <-writes:
		t.Fatalf("copy-mode binding forwarded to PTY: %q", got)
	default:
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if !strings.Contains(string(msg.Data), "[COPY]") {
		t.Fatalf("copy mode paint = %q, want [COPY] status", string(msg.Data))
	}
}

func TestCopyModeInputNotForwardedAndOSC52Copy(t *testing.T) {
	writes := make(chan []byte, 1)
	p, _ := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.windows[0].scrollback = scopy.NewScrollback(4)
	sess.windows[0].scrollback.Append(testRow("old1    "))
	sess.windows[0].scrollback.Append(testRow("old2    "))
	sess.windows[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte{'g', ' ', 'j', 'y'})

	select {
	case got := <-writes:
		t.Fatalf("copy-mode navigation forwarded to PTY: %q", got)
	default:
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if got, want := string(msg.Data), "\x1b]52;c;b2xkMQpvbGQy\x07"; got != want {
		t.Fatalf("OSC52 = %q, want %q", got, want)
	}
	if ac.copyMode != nil {
		t.Fatal("copy mode still active after yank")
	}
	live := awaitFrame(t, sends, ports.MsgOutput)
	liveMsg, err := ports.UnmarshalOutput(live.Payload)
	require.NoError(t, err)
	if strings.Contains(string(liveMsg.Data), "[COPY]") {
		t.Fatalf("live repaint still contains copy status: %q", string(liveMsg.Data))
	}
	if !strings.Contains(string(liveMsg.Data), "live") {
		t.Fatalf("live repaint = %q, want live screen", string(liveMsg.Data))
	}
}

func TestCopyModeEscapeRestoresLiveFullRepaint(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.windows[0].scrollback = scopy.NewScrollback(4)
	sess.windows[0].screen.Write([]byte("live"))

	d.enterCopyMode(sess, ac)
	awaitFrame(t, sends, ports.MsgOutput)
	d.handleInput(sess, ac, []byte("q"))

	if ac.copyMode != nil {
		t.Fatal("copy mode still active after q")
	}
	out := awaitFrame(t, sends, ports.MsgOutput)
	msg, err := ports.UnmarshalOutput(out.Payload)
	require.NoError(t, err)
	if strings.Contains(string(msg.Data), "[COPY]") || !strings.Contains(string(msg.Data), "live") {
		t.Fatalf("exit repaint = %q, want live full repaint without copy status", string(msg.Data))
	}
}

func TestCopyModeEnterExitConcurrentWithPaintRace(t *testing.T) {
	p, _ := newBlockingPTY(t)
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	sess.windows[0].scrollback = scopy.NewScrollback(4)
	sess.windows[0].screen.Write([]byte("live"))

	done := make(chan struct{})
	var drain sync.WaitGroup
	drain.Go(func() {
		for {
			select {
			case <-sends:
			case <-done:
				return
			}
		}
	})

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			d.enterCopyMode(sess, ac)
			d.handleInput(sess, ac, []byte("q"))
		})
		wg.Go(func() {
			d.paint(sess, ac, true)
		})
	}
	wg.Wait()
	close(done)
	drain.Wait()
}

func windowCount(sess *session) int {
	sess.mu.Lock()
	defer sess.mu.Unlock()
	return len(sess.windows)
}

// --- handshake --------------------------------------------------------------

func TestHandshakeEphemeralHappy(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })

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
	hg.Go(func() { d.handleConn(tr) })

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
	d.waitNotifies()
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

func TestAltDigitSwitchesBetweenThreeWindows(t *testing.T) {
	writes1 := make(chan []byte, 1)
	writes2 := make(chan []byte, 1)
	writes3 := make(chan []byte, 1)
	p1, releasePTY1 := newBlockingPTYWithWrites(t, writes1)
	p2, releasePTY2 := newBlockingPTYWithWrites(t, writes2)
	p3, releasePTY3 := newBlockingPTYWithWrites(t, writes3)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2, p3), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}),
		frameInput([]byte("\x1bc")),
		frameInput([]byte("\x1bc")),
		frameInput([]byte("\x1b1")),
		frameInput([]byte("A")),
		frameInput([]byte("\x1b2")),
		frameInput([]byte("B")),
		frameInput([]byte("\x1b3")),
		frameInput([]byte("C")),
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	require.Eventually(t, func() bool {
		sessions := listSessions(t, d)
		return len(sessions.Sessions) == 1 && sessions.Sessions[0].Windows == 3
	}, 2*time.Second, 5*time.Millisecond)
	require.Eventually(t, func() bool { return len(writes1) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("A"), <-writes1)
	require.Eventually(t, func() bool { return len(writes2) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("B"), <-writes2)
	require.Eventually(t, func() bool { return len(writes3) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("C"), <-writes3)

	releaseConn()
	releasePTY1()
	releasePTY2()
	releasePTY3()
	hg.Wait()
	d.sessWg.Wait()
}

func TestAltCCreatesSecondActiveWindow(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}),
		frameInput([]byte("\x1bc")),
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	require.Eventually(t, func() bool {
		sessions := listSessions(t, d)
		return len(sessions.Sessions) == 1 && sessions.Sessions[0].Windows == 2
	}, 2*time.Second, 5*time.Millisecond)

	releaseConn()
	releasePTY1()
	releasePTY2()
	hg.Wait()
	d.sessWg.Wait()
}

func TestAltNextPreviousSwitchActiveWindow(t *testing.T) {
	cases := []struct {
		name      string
		start     int
		input     []byte
		wantIndex int
	}{
		{name: "next advances", start: 0, input: []byte("\x1bn"), wantIndex: 1},
		{name: "next wraps", start: 2, input: []byte("\x1bn"), wantIndex: 0},
		{name: "previous moves back", start: 2, input: []byte("\x1bp"), wantIndex: 1},
		{name: "previous wraps", start: 0, input: []byte("\x1bp"), wantIndex: 2},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualWindowSession(t, 3)
			defer func() {
				for _, release := range releases {
					release()
				}
			}()
			sess.active = tc.start

			d.handleInput(sess, ac, tc.input)

			require.Equal(t, tc.wantIndex, activeWindowIndex(sess))
		})
	}
}

func TestAltXClosesActiveWindowAndSelectsRemaining(t *testing.T) {
	writes := make(chan []byte, 1)
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	p3, releasePTY3 := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2, p3)
	sess.active = 1

	d.handleInput(sess, ac, []byte("\x1bx"))

	require.Equal(t, 1, sessionCount(d))
	require.Len(t, sess.windows, 2)
	require.Equal(t, 1, activeWindowIndex(sess), "closing middle window selects the next remaining window")
	d.handleInput(sess, ac, []byte("Z"))
	require.Eventually(t, func() bool { return len(writes) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, []byte("Z"), <-writes)

	releasePTY1()
	releasePTY2()
	releasePTY3()
}

func waitGroupDone(wg *sync.WaitGroup) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	return done
}

func TestAltXClosesNonFinalWindowScheduler(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	clk := &signalClock{called: make(chan struct{})}
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
	d.clock = clk
	defer releasePTY1()
	defer releasePTY2()

	d.sessWg.Add(1)
	go d.scheduler(sess, sess.windows[1])
	sess.windows[1].dirty <- struct{}{}
	<-clk.called

	d.handleInput(sess, ac, []byte("\x1b2"))
	d.handleInput(sess, ac, []byte("\x1bx"))

	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler for removed window did not exit")
	}
	require.Equal(t, 1, sessionCount(d))
}

func TestPTYEOFClosesNonFinalWindowScheduler(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	clk := &signalClock{called: make(chan struct{})}
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	d.clock = clk
	defer releasePTY1()
	defer releasePTY2()

	d.sessWg.Add(2)
	go d.scheduler(sess, sess.windows[1])
	go d.ptyReader(sess, sess.windows[1])
	sess.windows[1].dirty <- struct{}{}
	<-clk.called
	releasePTY2()

	select {
	case <-waitGroupDone(&d.sessWg):
	case <-time.After(2 * time.Second):
		t.Fatal("scheduler for EOF-removed window did not exit")
	}
	require.Equal(t, 1, sessionCount(d))
}

func TestAltXClosesFinalWindowAndDetaches(t *testing.T) {
	d, sess, ac, sends, releases := newManualWindowSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1bx"))

	require.Equal(t, 0, sessionCount(d))
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonSessionKilled, det.Reason)
}

func TestPTYEOFClosesActiveNonFinalWindowAndRepaintsRemaining(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY2()
	sess.active = 0
	sess.windows[1].screen.Write([]byte("remaining"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.windows[0])
	releasePTY1()

	require.Eventually(t, func() bool { return windowCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, activeWindowIndex(sess))
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	data := string(out.Data)
	require.Contains(t, data, "remaining")
	require.Contains(t, data, "work")
	require.Contains(t, data, ";7m")

	d.sessWg.Wait()
}

func TestPTYEOFClosesInactiveNonFinalWindowAndRepaintsStatus(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, _, sends := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	sess.active = 0
	sess.windows[0].screen.Write([]byte("active"))

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.windows[1])
	releasePTY2()

	require.Eventually(t, func() bool { return windowCount(sess) == 1 }, 2*time.Second, 5*time.Millisecond)
	require.Equal(t, 1, sessionCount(d))
	require.Equal(t, 0, activeWindowIndex(sess))
	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	data := string(out.Data)
	require.Contains(t, data, "active")
	require.Contains(t, data, "work")
	require.NotContains(t, data, "  2 ")

	d.sessWg.Wait()
}

func TestPTYEOFFinalWindowKillsSessionAndDetaches(t *testing.T) {
	d, sess, _, sends, releases := newManualWindowSession(t, 1)

	d.sessWg.Add(1)
	go d.ptyReader(sess, sess.windows[0])
	releases[0]()

	require.Eventually(t, func() bool { return sessionCount(d) == 0 }, 2*time.Second, 5*time.Millisecond)
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonSessionKilled, det.Reason)

	d.sessWg.Wait()
}

func TestAltDDetachesCurrentClient(t *testing.T) {
	d, sess, ac, sends, releases := newManualWindowSession(t, 1)
	defer releases[0]()

	d.handleInput(sess, ac, []byte("\x1bd"))

	require.Nil(t, sess.client)
	f := awaitFrame(t, sends, ports.MsgDetached)
	det, err := ports.UnmarshalDetached(f.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonDetach, det.Reason)
}

func TestAltRPromotesEphemeralSessionPromptlessly(t *testing.T) {
	d, sess, ac, sends, releases := newManualWindowSession(t, 1)
	defer releases[0]()
	sess.ephemeral = true
	sess.name = "0"

	d.handleInput(sess, ac, []byte("\x1br"))

	require.False(t, sess.ephemeral)
	require.Equal(t, "0", sess.name)
	awaitFrame(t, sends, ports.MsgOutput)
}

func TestStatusCompositionGolden(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	sess.active = 1
	sess.name = "work"

	win := sess.activeWindow()
	win.screen = vt.NewScreen(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	win.screen.Write([]byte("hello"))

	frame, damage := composeClientFrame(sess, win, true)

	require.Equal(t, 12, frame.Width)
	require.Equal(t, 3, frame.Height)
	require.Equal(t, "hello       ", rowText(frame.Row(0)))
	require.Equal(t, " work  1  2 ", rowText(frame.Row(2)))
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	for i, c := range frame.Row(2) {
		if i >= len(" work  1 ") && i < len(" work  1  2 ") {
			require.True(t, c.Style.Inverse, "active window segment cell %d should be inverse", i)
		}
	}
}

func TestStatusRepaintsOnCreateSwitchAndResize(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 20, Rows: 5}),
		frameInput([]byte("\x1bc")),
		frameInput([]byte("\x1b1")),
		ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: domain.Size{Cols: 22, Rows: 6}})},
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	first := awaitFrame(t, sends, ports.MsgOutput)
	created := awaitFrame(t, sends, ports.MsgOutput)
	switched := awaitFrame(t, sends, ports.MsgOutput)
	resized := awaitFrame(t, sends, ports.MsgOutput)

	for _, f := range []ports.Frame{first, created, switched, resized} {
		out, err := ports.UnmarshalOutput(f.Payload)
		require.NoError(t, err)
		require.Contains(t, string(out.Data), "work")
		require.Contains(t, string(out.Data), ";7m", "active status window should be inverse-highlighted")
	}

	releaseConn()
	releasePTY1()
	releasePTY2()
	hg.Wait()
	d.sessWg.Wait()
}

func rowText(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, c := range row {
		runes[i] = c.Rune
	}
	return string(runes)
}

func TestAttachReplaceDetachesOld(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})

	// Client A creates and attaches to ephemeral "0".
	trA, sendsA, releaseA := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(trA) })
	awaitFrame(t, sendsA, ports.MsgWelcome)
	awaitFrame(t, sendsA, ports.MsgOutput)
	sess := firstSession(d)
	require.NotNil(t, sess)

	// Client B attaches to the same session, displacing A.
	trB, sendsB, releaseB := newConn(t, mustHello(ports.IntentAttach, "0", domain.Size{Cols: 80, Rows: 24}))
	hg.Go(func() { d.handleConn(trB) })
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
	d.waitNotifies()
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
	hg.Go(func() { d.handleConn(tr) })
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
	hg.Go(func() { d.handleConn(tr) })
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
	d.waitNotifies()
}

// --- render scheduler debounce ----------------------------------------------

func TestSchedulerDebounceCoalesces(t *testing.T) {
	mc := portsmocks.NewMockClock(t)
	mc.EXPECT().Now().Return(time.Now()).Maybe()
	mt := portsmocks.NewMockTimer(t)
	timerCh := make(chan time.Time, 1)
	newTimerCalled := make(chan struct{}, 4)

	mc.EXPECT().NewTimer(mock.Anything).RunAndReturn(func(time.Duration) ports.Timer {
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
	sess := &session{id: "s", name: "s", windows: []*window{win}, ctx: sctx, cancel: cancel, client: ac}

	win.mu.Lock()
	win.screen.Write([]byte("hi"))
	win.mu.Unlock()

	d.sessWg.Add(1)
	go d.scheduler(sess, win)

	// First dirty opens the debounce window.
	win.dirty <- struct{}{}
	<-newTimerCalled

	// A burst of further dirties inside the window must be absorbed.
	for range 5 {
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

func TestSchedulerAdaptiveDebounceFloodAndIdle(t *testing.T) {
	delay := nextDebounceDelay(minDebounceInterval, 3)
	require.Greater(t, delay, minDebounceInterval, "sustained flood should widen debounce")
	delay = nextDebounceDelay(delay, 3)
	require.Greater(t, delay, minDebounceInterval+debounceStep, "continued flood should keep adapting")
}

func TestSchedulerAdaptiveDebounceResetsAfterQuietPeriod(t *testing.T) {
	delay := nextDebounceDelay(minDebounceInterval, 2)
	require.Greater(t, delay, minDebounceInterval)
	require.Equal(t, minDebounceInterval, nextDebounceDelay(delay, 0), "isolated update after quiet window should restore idle latency")
}

// --- resize ordering --------------------------------------------------------

func TestResizeOrdersPTYBeforeScreen(t *testing.T) {
	newSize := domain.Size{Cols: 100, Rows: 30}

	p := portsmocks.NewMockPTY(t)
	var screenWidthAtResize int
	win := &window{screen: vt.NewScreen(80, 24), dirty: make(chan struct{}, 1), size: domain.Size{Cols: 80, Rows: 24}}
	p.EXPECT().Resize(domain.Size{Cols: 100, Rows: 29}).RunAndReturn(func(sz domain.Size) error {
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
	sess := &session{id: "s", name: "s", windows: []*window{win}, client: ac}

	d.resize(sess, ac, newSize)

	require.Equal(t, 80, screenWidthAtResize, "pty.Resize must run before screen.Resize")
	require.Equal(t, 100, win.screen.Frame.Width, "screen resized after pty")
	require.Equal(t, 29, win.screen.Frame.Height, "screen reserves bottom status row")
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
	hg.Go(func() { d.handleConn(tr) })

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
	d.waitNotifies()
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

// --- shutdown/create interlock ------------------------------------------------

// TestHelloRacingShutdownIsRejected covers the interlock between the
// registry-empty shutdown decision and session creation: once killSession has
// removed the last session (making shutdown irreversible via doneOnce), a
// Hello landing at any later instant must be rejected cleanly — never spawn a
// child that no teardown pass would reap. The interleaving is deterministic:
// killSession runs to completion before the racing Hellos are handled, which
// models the worst case (insertion attempt after the shutdown snapshot).
func TestHelloRacingShutdownIsRejected(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	f := portsmocks.NewMockPTYFactory(t)
	// Exactly one child may ever be spawned; a second Open call (a leaked
	// child) fails the test via mock expectations.
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(p, nil).Once()
	d := newTestDaemon(t, f, stubClock{})

	tr1, sends1, release1 := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr1) })
	awaitFrame(t, sends1, ports.MsgWelcome)
	sess := firstSession(d)
	require.NotNil(t, sess)

	// The last session dies: shutdown begins irreversibly.
	d.killSession(sess, ports.ReasonSessionKilled)
	select {
	case <-d.done:
	default:
		t.Fatal("registry-empty shutdown must be signalled synchronously by killSession")
	}

	// Racing Hellos — both creation intents must be rejected with a clean
	// typed error, and nothing may be inserted into the registry.
	for _, intent := range []struct {
		name   string
		intent uint8
		sess   string
	}{
		{"ephemeral", ports.IntentEphemeral, ""},
		{"named", ports.IntentNew, "latecomer"},
	} {
		tr2, sends2, _ := newConn(t, mustHello(intent.intent, intent.sess, domain.Size{Cols: 80, Rows: 24}))
		d.handleConn(tr2)
		e := awaitFrame(t, sends2, ports.MsgError)
		em, err := ports.UnmarshalErrorMsg(e.Payload)
		require.NoError(t, err)
		require.Equal(t, ports.ErrServerShutdown, em.Code, "%s hello racing shutdown must be rejected", intent.name)
	}
	require.Equal(t, 0, sessionCount(d), "no session may be inserted after shutdown began")

	release1()
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
}

// --- wedged-client teardown ---------------------------------------------------

// notifyClock drives the daemon with a stub debounce timer (schedulers park)
// but a short real timer for the detach-notify deadline, so a wedged client's
// transport is force-closed quickly and deterministically.
type notifyClock struct{}

func (notifyClock) Now() time.Time { return time.Now() }
func (notifyClock) NewTimer(d time.Duration) ports.Timer {
	if d == debounceInterval {
		return stubTimer{}
	}
	return realTimer{t: time.NewTimer(5 * time.Millisecond)}
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time        { return r.t.C }
func (r realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r realTimer) Stop() bool                 { return r.t.Stop() }

// TestServeReturnsDespiteWedgedClientOnShutdown: killSession's teardown must
// not be gated behind the Detached notice. The client transport wedges every
// MsgDetached send until the transport is closed (mirroring a full kernel
// send buffer: only Close fails the in-flight write). Serve must still tear
// down the PTY and return.
func TestServeReturnsDespiteWedgedClientOnShutdown(t *testing.T) {
	p, _ := newBlockingPTY(t) // Close unblocks the parked reader

	tr := portsmocks.NewMockTransport(t)
	sends := make(chan ports.Frame, 64)
	recvCh := make(chan ports.Frame, 1)
	recvCh <- mustHello(ports.IntentNew, "wedge", domain.Size{Cols: 80, Rows: 24})
	connDone := make(chan struct{})
	var connOnce sync.Once
	closeConn := func() { connOnce.Do(func() { close(connDone) }) }

	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case f := <-recvCh:
			return f, nil
		case <-connDone:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Send(mock.Anything).RunAndReturn(func(f ports.Frame) error {
		if f.Type == ports.MsgDetached {
			// Wedged: blocks until the transport is force-closed.
			<-connDone
			return io.ErrClosedPipe
		}
		sends <- f
		return nil
	}).Maybe()
	tr.EXPECT().Close().RunAndReturn(func() error { closeConn(); return nil }).Maybe()

	l := portsmocks.NewMockListener(t)
	connCh := make(chan ports.Transport, 1)
	connCh <- tr
	lnClosed := make(chan struct{})
	var lnOnce sync.Once
	l.EXPECT().Accept().RunAndReturn(func() (ports.Transport, error) {
		select {
		case c := <-connCh:
			return c, nil
		case <-lnClosed:
			return nil, io.EOF
		}
	}).Maybe()
	l.EXPECT().Close().RunAndReturn(func() error { lnOnce.Do(func() { close(lnClosed) }); return nil }).Maybe()
	l.EXPECT().Addr().Return("mock").Maybe()

	d := New(newFactory(t, p), notifyClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())

	served := make(chan error, 1)
	go func() { served <- d.Serve(ctx, l) }()

	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)

	cancel() // shutdown with the wedged client still attached

	select {
	case err := <-served:
		require.NoError(t, err)
	case <-time.After(3 * time.Second):
		t.Fatal("Serve hung on a wedged client during teardown")
	}
	require.Equal(t, 0, sessionCount(d), "session must be torn down despite the wedged client")
}

// --- send-error kills ephemeral -----------------------------------------------

// TestSendErrorKillsEphemeral: a failed output send detaches the client, and —
// like every other detach path — that kills an ephemeral session rather than
// leaving it headless-but-alive forever.
func TestSendErrorKillsEphemeral(t *testing.T) {
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Close().Return(nil).Maybe()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(io.ErrClosedPipe).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	d := newTestDaemon(t, nil, stubClock{})
	win := &window{pty: p, screen: vt.NewScreen(20, 5), dirty: make(chan struct{}, 1), size: domain.Size{Cols: 20, Rows: 5}}
	sctx, cancel := context.WithCancel(context.Background())
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "e", name: "0", ephemeral: true, windows: []*window{win}, ctx: sctx, cancel: cancel, client: ac}
	d.sessions[sess.id] = sess

	win.mu.Lock()
	win.screen.Write([]byte("x"))
	win.mu.Unlock()

	d.paint(sess, ac, true) // send fails -> detach -> ephemeral killed

	require.Equal(t, 0, sessionCount(d), "ephemeral session must die when its client's send fails")
	d.waitNotifies()
}

func TestSchedulerDefersPendingDirtyTimerDuringSynchronizedUpdate(t *testing.T) {
	mc := portsmocks.NewMockClock(t)
	mc.EXPECT().Now().Return(time.Now()).Maybe()
	mt := portsmocks.NewMockTimer(t)
	timerCh := make(chan time.Time, 1)
	mc.EXPECT().NewTimer(mock.Anything).Return(mt).Maybe()
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
	win := &window{screen: vt.NewScreen(20, 5), dirty: make(chan struct{}, 1), flush: make(chan struct{}, 1), size: domain.Size{Cols: 20, Rows: 5}, ctx: sctx, cancel: cancel}
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "sync", name: "sync", windows: []*window{win}, ctx: sctx, cancel: cancel, client: ac}

	win.mu.Lock()
	win.screen.Write([]byte("before"))
	win.mu.Unlock()

	d.sessWg.Add(1)
	go d.scheduler(sess, win)
	win.dirty <- struct{}{}

	win.mu.Lock()
	win.screen.Write([]byte("\x1b[?2026hafter"))
	win.mu.Unlock()
	timerCh <- time.Now()

	select {
	case <-gotOutput:
		t.Fatal("scheduler rendered while synchronized update was active")
	case <-time.After(20 * time.Millisecond):
	}
	require.Equal(t, int32(0), outputs.Load())

	win.mu.Lock()
	win.screen.Write([]byte(" done\x1b[?2026l"))
	win.mu.Unlock()
	win.flush <- struct{}{}
	select {
	case <-gotOutput:
	case <-time.After(time.Second):
		t.Fatal("sync end did not flush deferred damage")
	}

	cancel()
	d.sessWg.Wait()
}

func TestPTYReaderSameReadSynchronizedUpdateStartEndFlushesImmediately(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b[?2026hhello\x1b[?2026l")}
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Close().Return(nil).Maybe()

	sctx, cancel := context.WithCancel(context.Background())
	win := &window{pty: p, screen: vt.NewScreen(20, 5), dirty: make(chan struct{}, 1), flush: make(chan struct{}, 1), size: domain.Size{Cols: 20, Rows: 5}, ctx: sctx, cancel: cancel}
	sess := &session{id: "sync", name: "sync", windows: []*window{win}, ctx: sctx, cancel: cancel}
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win)

	select {
	case <-win.dirty:
		t.Fatal("same-read synchronized update end should request flush, not dirty")
	default:
	}
	select {
	case <-win.flush:
	case <-time.After(time.Second):
		t.Fatal("same-read synchronized update end did not request immediate flush")
	}
}

func TestPTYReaderDefersDirtyDuringSynchronizedUpdateAndFlushesAtEnd(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := portsmocks.NewMockPTY(t)
	chunks := [][]byte{[]byte("\x1b[?2026hhello"), []byte(" world\x1b[?2026l")}
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		chunks = chunks[1:]
		return n, nil
	})
	p.EXPECT().Close().Return(nil).Maybe()

	sctx, cancel := context.WithCancel(context.Background())
	win := &window{pty: p, screen: vt.NewScreen(20, 5), dirty: make(chan struct{}, 1), flush: make(chan struct{}, 1), size: domain.Size{Cols: 20, Rows: 5}, ctx: sctx, cancel: cancel}
	sess := &session{id: "sync", name: "sync", windows: []*window{win}, ctx: sctx, cancel: cancel}
	d.sessions[sess.id] = sess
	d.sessWg.Add(1)
	d.ptyReader(sess, win)

	select {
	case <-win.dirty:
		t.Fatal("dirty signaled while synchronized update was active")
	default:
	}
	select {
	case <-win.flush:
	case <-time.After(time.Second):
		t.Fatal("sync end did not request immediate flush")
	}
}
