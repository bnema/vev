package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/persist"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/picker"
)

// --- test doubles -----------------------------------------------------------
func expectFloatingPrewarmOpen(factory *portsmocks.MockPTYFactory, normalSize domain.Size, floating ports.PTY) {
	factory.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.MatchedBy(func(got domain.Size) bool {
		return got != normalSize && got.Valid()
	})).Return(floating, nil).Maybe()
}

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestRoutePropagatesHelloCwdAndTabsInheritIt(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	first, releaseFirst := newBlockingPTY(t)
	defer releaseFirst()
	second, releaseSecond := newBlockingPTY(t)
	defer releaseSecond()
	f := portsmocks.NewMockPTYFactory(t)
	var dirs []string
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, dir string, _ domain.Size) (ports.PTY, error) {
			dirs = append(dirs, dir)
			if len(dirs) == 1 {
				return first, nil
			}
			return second, nil
		},
	).Twice()
	floating := newQuietPTY()
	expectFloatingPrewarmOpen(f, normalSize, floating)

	d := newTestDaemon(t, f, stubClock{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	sess, ac, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentNew,
		Name:    "work",
		Size:    sz,
		TermEnv: "xterm-256color",
		Cwd:     "/tmp/work",
	}, tr)
	require.NoError(t, err)
	require.NotNil(t, ac)
	require.Equal(t, "/tmp/work", sess.cwd)

	require.NoError(t, d.createTab(sess, sz))
	require.Equal(t, []string{"/tmp/work", "/tmp/work"}, dirs)
	requireFloatingInitialized(t, sess.activeTab())
	_ = d.killSession(sess, ports.ReasonSessionKilled, false)
	releaseFirst()
	releaseSecond()
	d.sessWg.Wait()
	// Teardown may cancel a queued prewarm before it enters Open; an opened
	// prewarm is closed by teardownFloating.
}

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
	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
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

func TestHandshakeOldHelloLayoutReportsVersionMismatch(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	oldLayout := []byte{
		0x00, byte(ports.ProtocolVersion - 1),
		ports.IntentEphemeral,
		0x00, 0x00,
		0x00, 0x50,
		0x00, 0x18,
		0x00, 0x00,
	}
	tr, sends, _ := newConn(t, ports.Frame{Type: ports.MsgHello, Payload: oldLayout})
	d.handleConn(tr)

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

func TestKillAllEmptyDaemonSignalsShutdown(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	tr, _, _ := newConn(t, ports.Frame{Type: ports.MsgKill, Payload: ports.MarshalKill(ports.Kill{All: true})})
	d.handleConn(tr)

	select {
	case <-d.done:
	case <-time.After(time.Second):
		t.Fatal("kill all on empty daemon did not signal shutdown")
	}
}

func TestCreateTabClosesPTYIfSessionKilledDuringOpen(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2 := portsmocks.NewMockPTY(t)
	openCtx := make(chan context.Context, 1)
	releaseOpen := make(chan struct{})
	closed := make(chan struct{})
	p2.EXPECT().Close().RunAndReturn(func() error {
		close(closed)
		return nil
	}).Once()
	p2.EXPECT().Pid().Return(4242).Maybe()

	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: 80, Rows: 22}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).Return(p1, nil).Once()
	floating := newQuietPTY()
	expectFloatingPrewarmOpen(f, normalSize, floating)
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(ctx context.Context, _ string, _ []string, _ []string, _ string, _ domain.Size) (ports.PTY, error) {
			openCtx <- ctx
			select {
			case <-ctx.Done():
			case <-releaseOpen:
			}
			return p2, nil
		},
	).Once()

	d := newTestDaemon(t, f, stubClock{})
	tr, sends, releaseConn := newConn(t, mustHello(ports.IntentNew, "work", domain.Size{Cols: 80, Rows: 24}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgOutput)
	sess := firstSession(d)
	require.NotNil(t, sess)

	errCh := make(chan error, 1)
	go func() { errCh <- d.createTab(sess, domain.Size{Cols: 80, Rows: 24}) }()
	ctx := <-openCtx
	_ = d.killSession(sess, ports.ReasonSessionKilled, false)

	cancelled := true
	select {
	case <-ctx.Done():
	case <-time.After(time.Second):
		cancelled = false
	}
	close(releaseOpen)

	if !cancelled {
		t.Error("killSession did not cancel the context passed to PTYFactory.Open")
	}
	require.Error(t, <-errCh)
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("new PTY was not closed after session died during tab creation")
	}
	require.Equal(t, 0, sessionCount(d))
	releaseConn()
	hg.Wait()
	d.sessWg.Wait()
	// A queued prewarm may be cancelled before Open during teardown.
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
	require.Equal(t, ports.ReasonReplaced, det.Reason)

	// B is now the current client.
	sess.mu.Lock()
	require.NotNil(t, sess.client)
	sess.mu.Unlock()

	releaseA()
	releaseB()
	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
}

// --- detach semantics -------------------------------------------------------

func TestDetachKeepsEphemeralHeadless(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	tr, sends, _ := newConn(t,
		mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}),
		ports.Frame{Type: ports.MsgDetach, Payload: ports.MarshalDetach(ports.Detach{})},
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	awaitFrame(t, sends, ports.MsgDetached)
	hg.Wait()

	require.Equal(t, 1, sessionCount(d), "ephemeral session survives explicit detach")
	sess := firstSession(d)
	require.NotNil(t, sess)
	sess.mu.Lock()
	require.True(t, sess.ephemeral)
	require.Nil(t, sess.client, "ephemeral session is headless after detach")
	sess.mu.Unlock()

	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
	releasePTY()
	d.sessWg.Wait()
	d.waitNotifies()
}

func TestListShowsDetachedEphemeralSession(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})

	tr := &closeTrackingTransport{}
	sess, ac, err := d.route(helloResumeCapable(ports.IntentEphemeral, "", 0), tr)
	require.NoError(t, err)
	d.clientGone(sess, ac, ac.transport(), true)

	got := listSessions(t, d)
	require.Len(t, got.Sessions, 1)
	require.Equal(t, sess.name, got.Sessions[0].Name)
	require.True(t, got.Sessions[0].Ephemeral)
	require.False(t, got.Sessions[0].Attached)

	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
	d.sessWg.Wait()
	d.waitNotifies()
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

	_ = d.killSession(sess, ports.ReasonServerShutdown, false)
	releasePTY()
	d.sessWg.Wait()
	d.waitNotifies()
}

// --- render scheduler debounce ----------------------------------------------

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
// registry-empty shutdown decision and session creation. In particular, the
// old `.Once()` factory expectation was wrong: firstPaint legitimately queues
// a floating prewarm, so an Open that starts before killSession is valid work,
// not a leaked post-shutdown child. The factory markers classify timing at the
// Open boundary: this test first observes that valid prewarm, then verifies
// that d.done (closed synchronously by killSession) is never followed by an
// Open start.
func TestHelloRacingShutdownIsRejected(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	floating := newQuietPTY()
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: 80, Rows: 22}
	preShutdownFloatingOpen := make(chan struct{}, 1)
	var opensAfterShutdown atomic.Int32
	d := newTestDaemon(t, f, stubClock{})
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, _ []string, _ string, size domain.Size) (ports.PTY, error) {
			select {
			case <-d.done:
				opensAfterShutdown.Add(1)
			default:
			}
			if size == normalSize {
				return p, nil
			}
			select {
			case preShutdownFloatingOpen <- struct{}{}:
			default:
			}
			return floating, nil
		},
	).Maybe()

	tr1, sends1, release1 := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr1) })
	awaitFrame(t, sends1, ports.MsgWelcome)
	// firstPaint starts the background prewarm asynchronously. It is valid for
	// this Open to occur before shutdown, so do not mistake it for the racing
	// Hello's forbidden launch.
	awaitFrame(t, sends1, ports.MsgOutput)
	select {
	case <-preShutdownFloatingOpen:
	case <-time.After(time.Second):
		t.Fatal("legitimate pre-shutdown floating prewarm did not start")
	}
	sess := firstSession(d)
	require.NotNil(t, sess)

	// The last session dies: shutdown begins irreversibly.
	_ = d.killSession(sess, ports.ReasonSessionKilled, false)
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
	require.Zero(t, opensAfterShutdown.Load(), "no PTYFactory.Open may start after d.done/shutdown completion")

	release1()
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
	require.Zero(t, opensAfterShutdown.Load(), "racing Hellos must not start a PTY after shutdown")
}

// --- wedged-client teardown ---------------------------------------------------

// notifyClock drives the daemon with a stub debounce timer (schedulers park)
// but a short real timer for the detach-notify deadline, so a wedged client's

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

type mockStoreState struct {
	mu     sync.Mutex
	data   map[string][]byte
	sets   int
	dels   []string
	syncs  int
	closed bool
}

func newMockStore(t *testing.T) (*portsmocks.MockStore, *mockStoreState) {
	t.Helper()
	state := &mockStoreState{data: make(map[string][]byte)}
	store := portsmocks.NewMockStore(t)
	store.EXPECT().Get(mock.Anything).RunAndReturn(func(k []byte) ([]byte, bool) {
		state.mu.Lock()
		defer state.mu.Unlock()
		v, ok := state.data[string(k)]
		return append([]byte(nil), v...), ok
	}).Maybe()
	store.EXPECT().Set(mock.Anything, mock.Anything).RunAndReturn(func(k, v []byte) error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.sets++
		state.data[string(k)] = append([]byte(nil), v...)
		return nil
	}).Maybe()
	store.EXPECT().Delete(mock.Anything).RunAndReturn(func(k []byte) error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.dels = append(state.dels, string(k))
		delete(state.data, string(k))
		return nil
	}).Maybe()
	store.EXPECT().Range(mock.Anything).Run(func(fn func(k, v []byte) bool) {
		state.mu.Lock()
		defer state.mu.Unlock()
		for k, v := range state.data {
			if !fn([]byte(k), append([]byte(nil), v...)) {
				return
			}
		}
	}).Maybe()
	store.EXPECT().Sync().RunAndReturn(func() error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.syncs++
		return nil
	}).Maybe()
	store.EXPECT().Close().RunAndReturn(func() error {
		state.mu.Lock()
		defer state.mu.Unlock()
		state.closed = true
		return nil
	}).Maybe()
	return store, state
}

func (s *mockStoreState) has(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data[name]
	return ok
}

func (s *mockStoreState) record(t *testing.T, name string) persist.Record {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	records, err := persist.New(newStaticStore(s.data)).LoadAll()
	require.NoError(t, err)
	for _, r := range records {
		if r.Name == name {
			return r
		}
	}
	t.Fatalf("record %q not found", name)
	return persist.Record{}
}

type newStaticStore map[string][]byte

func (s newStaticStore) Get(key []byte) ([]byte, bool) {
	v, ok := s[string(key)]
	return append([]byte(nil), v...), ok
}
func (s newStaticStore) Set(_, _ []byte) error { return nil }
func (s newStaticStore) Delete(_ []byte) error { return nil }
func (s newStaticStore) Range(fn func(k, v []byte) bool) {
	for k, v := range s {
		if !fn([]byte(k), append([]byte(nil), v...)) {
			return
		}
	}
}
func (s newStaticStore) Sync() error  { return nil }
func (s newStaticStore) Close() error { return nil }

func TestDaemonLoadsPersistedSessionsAsStopped(t *testing.T) {
	store, _ := newMockStore(t)
	seed := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStore(store))
	require.NoError(t, seed.persist.Save(persist.Record{Name: "work", Cwd: "/tmp/work", CreatedAt: 7, UpdatedAt: 8, LastUsedSeq: 9}))

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStore(store))
	d.mu.Lock()
	stopped := d.stopped["work"]
	d.mu.Unlock()
	require.Equal(t, stoppedSession{name: "work", cwd: "/tmp/work", createdAt: 7, lastUsedSeq: 9}, stopped)
	require.Equal(t, uint64(9), d.mruSeq.Load())
}

func TestTouchMRUConcurrentUpdatesRemainMonotonic(t *testing.T) {
	d := &Daemon{}
	sess := &session{}
	const goroutines = 16
	const iterations = 1000
	start := make(chan struct{})
	done := make(chan struct{})
	var decreased atomic.Bool
	var wg sync.WaitGroup
	var observer sync.WaitGroup

	observer.Go(func() {
		previous := sess.mruAt.Load()
		for {
			select {
			case <-done:
				return
			default:
			}
			current := sess.mruAt.Load()
			if current < previous {
				decreased.Store(true)
				return
			}
			previous = current
		}
	})

	for range goroutines {
		wg.Go(func() {
			<-start
			for range iterations {
				d.touchMRU(sess)
			}
		})
	}
	close(start)
	wg.Wait()
	close(done)
	observer.Wait()

	require.False(t, decreased.Load())
	require.Equal(t, d.mruSeq.Load(), sess.mruAt.Load())
}

func TestTouchMRUPersistsNamedButNotEphemeral(t *testing.T) {
	store, state := newMockStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithStore(store)(d)
	named := &session{name: "work", tabs: []*tab{{}}, createdAt: 1}
	require.NoError(t, d.persist.Save(persist.Record{Name: "work", Cwd: "/work", CreatedAt: 1, UpdatedAt: 1}))

	d.touchMRU(named)
	require.Equal(t, named.mruAt.Load(), state.record(t, "work").LastUsedSeq)
	setsAfterNamed := state.sets

	ephemeral := &session{name: "0", ephemeral: true, tabs: []*tab{{}}}
	d.touchMRU(ephemeral)
	require.False(t, state.has("0"))
	require.Equal(t, setsAfterNamed, state.sets)
}

func TestCreateSessionSeedsMRUFromStopped(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/tmp/work", createdAt: 1, lastUsedSeq: 42}
	d.mruSeq.Store(42)

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.Equal(t, uint64(42), sess.mruAt.Load())
	require.Equal(t, uint64(42), state.record(t, "work").LastUsedSeq)
}

func TestCreateRenameKillPersistenceLifecycle(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	WithStore(store)(d)

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.True(t, state.has("work"))
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "shell"))
	require.NoError(t, d.createTab(sess, sz))
	require.NoError(t, d.renameTab(sess, sess.tabs[1], "logs"))

	require.NoError(t, d.renameSession(sess, "renamed"))
	require.False(t, state.has("work"))
	require.True(t, state.has("renamed"))
	require.Equal(t, []string{"shell", "logs"}, state.record(t, "renamed").TabNames)

	require.NoError(t, d.killSession(sess, ports.ReasonSessionKilled, true))
	require.False(t, state.has("renamed"))
}

func TestRenameTabPersistsForNamedSession(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, _ := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "shell"))

	records, err := d.persist.LoadAll()
	require.NoError(t, err)
	require.Len(t, records, 1)
	require.Equal(t, "work", records[0].Name)
	require.Equal(t, "/tmp/work", records[0].Cwd)
	require.Equal(t, sess.createdAt, records[0].CreatedAt)
	require.GreaterOrEqual(t, records[0].UpdatedAt, records[0].CreatedAt)
	require.Equal(t, []string{"shell"}, records[0].TabNames)
}

func TestTabNamePersistenceTracksTabIndexShifts(t *testing.T) {
	tests := []struct {
		name       string
		tabNames   []string
		closeIndex int
		want       []string
	}{
		{name: "close before named tab", tabNames: []string{"shell", "", "logs"}, closeIndex: 0, want: []string{"", "logs"}},
		{name: "close after named tab", tabNames: []string{"shell", "logs", ""}, closeIndex: 2, want: []string{"shell", "logs"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ptys := make([]ports.PTY, 3)
			for i := range ptys {
				p, release := newBlockingPTY(t)
				ptys[i] = p
				t.Cleanup(release)
			}
			store, _ := newMockStore(t)
			d := newTestDaemon(t, newFactorySeq(t, ptys...), stubClock{})
			WithStore(store)(d)
			sz := domain.Size{Cols: 80, Rows: 24}

			sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
			require.NoError(t, err)
			require.NoError(t, d.createTab(sess, sz))
			require.NoError(t, d.createTab(sess, sz))
			for i, name := range tt.tabNames {
				require.NoError(t, d.renameTab(sess, sess.tabs[i], name))
			}

			d.closeTab(sess, sess.tabs[tt.closeIndex], false)
			records, err := d.persist.LoadAll()
			require.NoError(t, err)
			require.Len(t, records, 1)
			require.Equal(t, tt.want, records[0].TabNames)
		})
	}
}

func TestCloseActiveTabActivatesDestinationFloatingPane(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 2)
	sess.mu.Lock()
	sess.client = ac
	sess.mu.Unlock()
	d.ptys = newBlockingOpenFactory(t, d)
	defer releases[0]()
	defer releases[1]()
	sess.mu.Lock()
	first, closing := sess.tabs[0], sess.tabs[1]
	sess.active = 1
	sess.mu.Unlock()
	first.mu.Lock()
	stale := first.takeFloatingLocked()
	first.mu.Unlock()
	closeFloatingPane(stale)

	d.closeTab(sess, closing, false)

	require.Same(t, first, sess.activeTab())
	requireFloatingInitialized(t, first)
}

func TestRenameTabDoesNotPersistForEphemeralSession(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)

	sess, err := d.createSessionLocked("0", true, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "shell"))
	require.Equal(t, "shell", sess.tabs[0].name)

	require.False(t, state.has("0"))
	records, err := d.persist.LoadAll()
	require.NoError(t, err)
	require.Empty(t, records)
}

func TestAttachRestoresPersistedTabNames(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	store, _ := newMockStore(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	WithStore(store)(d)
	require.NoError(t, d.persist.Save(persist.Record{Name: "work", Cwd: "/tmp/work", CreatedAt: 7, UpdatedAt: 8, TabNames: []string{"shell", "logs"}}))
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/tmp/work", createdAt: 7, tabNames: []string{"shell", "logs"}}
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	sess, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: sz}, tr)
	require.NoError(t, err)
	require.NotNil(t, ac)
	require.Len(t, sess.tabs, 2)
	require.Equal(t, "shell", sess.tabs[0].name)
	require.Equal(t, "logs", sess.tabs[1].name)
}

func TestEphemeralRenamePromotesAndStoppedCollisionRejected(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)
	d.stopped["taken"] = stoppedSession{name: "taken", cwd: "/tmp", createdAt: 1}

	sess, err := d.createSessionLocked("0", true, "/tmp/e", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.False(t, state.has("0"))
	require.EqualError(t, d.renameSession(sess, "taken"), "name already in use")
	require.NoError(t, d.renameSession(sess, "named"))
	require.True(t, state.has("named"))
}

func TestEphemeralPromotionAssignsAndPersistsLifecycleIdentity(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	clock := &lifecycleClock{nows: []time.Time{time.Unix(0, 100)}}
	d := newTestDaemon(t, newFactory(t, p), clock)
	WithStore(store)(d)

	sess, err := d.createSessionLocked("0", true, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.Zero(t, sess.createdAt, "regression fixture: ephemerals have no lifecycle identity")

	require.NoError(t, d.renameSession(sess, "named"))

	require.False(t, sess.ephemeral)
	require.Equal(t, "named", sess.name)
	require.Equal(t, int64(100), sess.createdAt)
	require.Equal(t, sess.createdAt, state.record(t, "named").CreatedAt)
}

func TestEphemeralPromotionLifecyclePreventsStaleSameNamePaletteTarget(t *testing.T) {
	fromPTY, releaseFrom := newBlockingPTY(t)
	firstPTY, releaseFirst := newBlockingPTY(t)
	secondPTY, releaseSecond := newBlockingPTY(t)
	defer releaseFrom()
	defer releaseFirst()
	defer releaseSecond()

	d, from, ac, _ := newManualSessionWithPTYs(t, fromPTY)
	store, _ := newMockStore(t)
	clock := &lifecycleClock{nows: []time.Time{time.Unix(0, 100), time.Unix(0, 100)}}
	d.clock = clock
	d.ptys = newFactorySeq(t, firstPTY, secondPTY)
	WithStore(store)(d)

	first, err := d.createSessionLocked("0", true, "/tmp", ac.size, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.NoError(t, d.renameSession(first, "named"))
	staleCreatedAt := first.createdAt
	require.NotZero(t, staleCreatedAt)
	require.NoError(t, d.killSession(first, ports.ReasonSessionKilled, true))
	d.sessWg.Wait()

	second, err := d.createSessionLocked("0", true, "/tmp", ac.size, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.NoError(t, d.renameSession(second, "named"))
	require.NotEqual(t, staleCreatedAt, second.createdAt)

	require.Error(t, d.switchToTarget(from, ac, picker.Target{Name: "named", TabIndex: -1, ExpectedCreatedAt: &staleCreatedAt}))
	require.Same(t, from, ac.currentSession())
	releaseSecond()
	d.sessWg.Wait()
}

func TestEphemeralPromotionLifecycleFailuresLeaveStateRollbackSafe(t *testing.T) {
	t.Run("allocator exhaustion", func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		store, state := newMockStore(t)
		d := newTestDaemon(t, newFactory(t, p), stubClock{})
		WithStore(store)(d)
		d.lastAllocatedCreatedAt = math.MaxInt64

		sess, err := d.createSessionLocked("0", true, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
		require.NoError(t, err)
		require.EqualError(t, d.renameSession(sess, "named"), "daemon: lifecycle identities exhausted")
		require.Equal(t, "0", sess.name)
		require.True(t, sess.ephemeral)
		require.Zero(t, sess.createdAt)
		require.Equal(t, int64(math.MaxInt64), d.lastAllocatedCreatedAt)
		require.False(t, state.has("named"))
	})

	t.Run("persistence failure", func(t *testing.T) {
		p, release := newBlockingPTY(t)
		defer release()
		store := portsmocks.NewMockStore(t)
		var attempted map[string][]byte
		store.EXPECT().Set(mock.Anything, mock.Anything).RunAndReturn(func(key, value []byte) error {
			attempted = map[string][]byte{string(key): append([]byte(nil), value...)}
			return errors.New("disk full")
		}).Once()
		clock := &lifecycleClock{nows: []time.Time{time.Unix(0, 100), time.Unix(0, 100)}}
		d := newTestDaemon(t, newFactory(t, p), clock)
		WithStore(store)(d)

		sess, err := d.createSessionLocked("0", true, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
		require.NoError(t, err)
		require.EqualError(t, d.renameSession(sess, "named"), "disk full")
		require.Equal(t, "0", sess.name)
		require.True(t, sess.ephemeral)
		require.Zero(t, sess.createdAt)
		require.Equal(t, int64(100), d.lastAllocatedCreatedAt, "a failed durable write may be ambiguous, so its lifecycle identity remains reserved")
		records, err := persist.New(newStaticStore(attempted)).LoadAll()
		require.NoError(t, err)
		require.Len(t, records, 1)
		require.Equal(t, int64(100), records[0].CreatedAt, "a promotion must never attempt to persist a zero lifecycle identity")

		store.EXPECT().Set(mock.Anything, mock.Anything).Return(nil).Once()
		store.EXPECT().Sync().Return(nil).Once()
		require.NoError(t, d.renameSession(sess, "named"))
		require.Equal(t, int64(101), sess.createdAt, "the high-water mark must remain monotonic after a failed promotion")
	})
}

func TestRefreshSessionCwdTouchesOnlyOnChange(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)
	cwd := "/tmp/work"
	WithCwdReader(func(int) (string, error) { return cwd, nil })(d)

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	state.mu.Lock()
	setsAfterCreate := state.sets
	state.mu.Unlock()

	d.refreshSessionCwd(sess)
	state.mu.Lock()
	require.Equal(t, setsAfterCreate, state.sets)
	state.mu.Unlock()

	cwd = "/tmp/next"
	d.refreshSessionCwd(sess)
	state.mu.Lock()
	require.Equal(t, setsAfterCreate+1, state.sets)
	state.mu.Unlock()
}

func TestAttachResumesStoppedSessionFromStoredCwd(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, _ := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)
	cwd := t.TempDir()
	d.stopped["work"] = stoppedSession{name: "work", cwd: cwd, createdAt: 1}
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	sess, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: sz}, tr)
	require.NoError(t, err)
	require.NotNil(t, ac)
	require.Equal(t, "work", sess.name)
	require.Equal(t, cwd, sess.cwd)
	d.mu.Lock()
	_, stillStopped := d.stopped["work"]
	d.mu.Unlock()
	require.False(t, stillStopped)
}

func TestAttachStoppedMissingCwdFallsBackToHome(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, _ := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)
	d.stopped["work"] = stoppedSession{name: "work", cwd: "/definitely/missing/vev", createdAt: 1}
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()

	sess, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: sz}, tr)
	require.NoError(t, err)
	require.NotEqual(t, "/definitely/missing/vev", sess.cwd)
}

func TestPickerStoppedTargetKillPurges(t *testing.T) {
	store, state := newMockStore(t)
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithStore(store)(d)
	require.NoError(t, d.persist.Save(persist.Record{Name: "old", Cwd: "/tmp", CreatedAt: 1, UpdatedAt: 1}))
	d.stopped["old"] = stoppedSession{name: "old", cwd: "/tmp", createdAt: 1}
	require.NoError(t, d.killPickerTarget(picker.Target{Name: "old", Stopped: true}))
	require.False(t, state.has("old"))
	d.mu.Lock()
	_, ok := d.stopped["old"]
	d.mu.Unlock()
	require.False(t, ok)
}

func TestChildEnvEscapesLegacySessionName(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})

	got := d.childEnv("legacy,name=value", "t_alpha", "p_beta", terminalEnv{TrueColor: true})

	require.Contains(t, got, "VEV=session=legacy%2Cname%3Dvalue,tab=t_alpha,pane=p_beta")
}

func TestNewSessionAssignsStableIDsAndChildEnv(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()

	var gotEnv []string
	f := portsmocks.NewMockPTYFactory(t)
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			gotEnv = append([]string(nil), env...)
			return p, nil
		},
	).Once()
	d := newTestDaemon(t, f, stubClock{})
	d.baseEnv = []string{"KEEP=1", "TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old"}

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{TrueColor: true}, d.baseEnv)
	require.NoError(t, err)
	defer func() {
		_ = d.killSession(sess, ports.ReasonServerShutdown, false)
		releasePTY()
		d.sessWg.Wait()
	}()

	sess.mu.Lock()
	require.Len(t, sess.tabs, 1)
	tb := sess.tabs[0]
	sess.mu.Unlock()
	tb.mu.Lock()
	paneStableID := tb.panes["pane-1"].stableID
	tabStableID := tb.stableID
	tb.mu.Unlock()

	require.True(t, strings.HasPrefix(tabStableID, "t_"), tabStableID)
	require.True(t, strings.HasPrefix(paneStableID, "p_"), paneStableID)
	require.NotEqual(t, tabStableID, paneStableID)
	require.Contains(t, gotEnv, "KEEP=1")
	require.NotContains(t, gotEnv, "TERM=old")
	require.NotContains(t, gotEnv, "COLORTERM=old")
	require.NotContains(t, gotEnv, "TERM_PROGRAM=old")
	require.NotContains(t, gotEnv, "VEV=old")
	require.Contains(t, gotEnv, "TERM=xterm-direct")
	require.Contains(t, gotEnv, "COLORTERM=truecolor")
	require.Contains(t, gotEnv, "TERM_PROGRAM=vev")
	require.Contains(t, gotEnv, "VEV=session=work,tab="+tabStableID+",pane="+paneStableID)
}

func TestIntentNewStoppedNameRejected(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	d.stopped["taken"] = stoppedSession{name: "taken", cwd: "/tmp", createdAt: 1}
	tr := portsmocks.NewMockTransport(t)
	_, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "taken", Size: domain.Size{Cols: 80, Rows: 24}}, tr)
	require.ErrorContains(t, err, "name already in use")
}

func TestIntentNewUnsafeNameRejected(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	tr := portsmocks.NewMockTransport(t)
	_, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "my work", Size: domain.Size{Cols: 80, Rows: 24}}, tr)
	require.ErrorContains(t, err, domain.ErrInvalidSessionName.Error())
}

func TestCreateSessionAndSwitchUnsafeNameRejected(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	require.ErrorIs(t, d.createSessionAndSwitch(nil, nil, "my work"), domain.ErrInvalidSessionName)
}

func TestRenameSessionUnsafeNameRejected(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.ErrorIs(t, d.renameSession(sess, "my work"), domain.ErrInvalidSessionName)
}

func TestCreateTabPtyFailureIsUserError(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)

	cause := errors.New("fork/exec: no such file")
	failing := portsmocks.NewMockPTYFactory(t)
	failing.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, cause).Once()
	d.ptys = failing

	err = d.createTab(sess, sz)

	var ue *domain.UserError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, domain.NoticeTabSpawn, ue.Code)
	require.Equal(t, domain.NoticeError, ue.Severity)
	require.Equal(t, "couldn't open tab: shell failed to start", ue.Msg)
	require.ErrorIs(t, err, cause)
	require.NotContains(t, ue.Msg, cause.Error())
}

func TestNaturalExitStoppedButExplicitKillPurges(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	WithStore(store)(d)
	WithCwdReader(func(int) (string, error) { return "/tmp/latest", nil })(d)

	natural, err := d.createSessionLocked("natural", false, "/tmp/old", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	other, err := d.createSessionLocked("other", false, "/tmp/other", sz, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	_ = d.killSession(natural, ports.ReasonSessionKilled, false)
	require.True(t, state.has("natural"))
	d.mu.Lock()
	stopped := d.stopped["natural"]
	d.mu.Unlock()
	require.Equal(t, "/tmp/latest", stopped.cwd)

	_ = d.killSession(other, ports.ReasonSessionKilled, true)
	require.False(t, state.has("other"))
}

func TestChildEnvTrueColorCapability(t *testing.T) {
	tests := []struct {
		name           string
		baseEnv        []string
		term           terminalEnv
		wantContain    []string
		wantNotContain []string
	}{
		{
			name:    "exports truecolor",
			baseEnv: []string{"KEEP=1", "TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old"},
			term:    terminalEnv{TrueColor: true},
			wantContain: []string{
				"KEEP=1",
				"TERM=xterm-direct",
				"COLORTERM=truecolor",
				"TERM_PROGRAM=vev",
				"VEV=session=work,tab=t_alpha,pane=p_beta",
			},
			wantNotContain: []string{"TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old"},
		},
		{
			name:           "omits truecolor when unsupported",
			baseEnv:        []string{"COLORTERM=old"},
			term:           terminalEnv{},
			wantContain:    []string{"TERM=xterm-256color", "TERM_PROGRAM=vev"},
			wantNotContain: []string{"TERM=xterm-direct", "COLORTERM=truecolor"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			d.baseEnv = tt.baseEnv

			got := d.childEnv("work", "t_alpha", "p_beta", tt.term)

			for _, want := range tt.wantContain {
				require.Contains(t, got, want)
			}
			for _, notWant := range tt.wantNotContain {
				require.NotContains(t, got, notWant)
			}
		})
	}
}

func TestAttachUpdatesFutureChildEnvTrueColor(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()

	var opens [][]string
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			opens = append(opens, append([]string(nil), env...))
			if len(opens) == 1 {
				return p1, nil
			}
			return p2, nil
		},
	).Twice()
	floating := newQuietPTY()
	expectFloatingPrewarmOpen(f, normalSize, floating)

	d := newTestDaemon(t, f, stubClock{})
	d.baseEnv = []string{"KEEP=1", "COLORTERM=old"}
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	sess, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: sz, TrueColor: true}, tr)
	require.NoError(t, err)
	defer func() {
		_ = d.killSession(sess, ports.ReasonServerShutdown, false)
		d.sessWg.Wait()
	}()

	require.Contains(t, opens[0], "TERM=xterm-direct")
	require.Contains(t, opens[0], "COLORTERM=truecolor")
	require.Contains(t, opens[0], "TERM_PROGRAM=vev")
	require.NoError(t, d.createTab(sess, ac.size))
	require.Contains(t, opens[1], "TERM=xterm-direct")
	require.Contains(t, opens[1], "COLORTERM=truecolor")
	require.Contains(t, opens[1], "TERM_PROGRAM=vev")
}

func TestLiveAttachUpdatesFutureChildEnvTrueColor(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()

	var opens [][]string
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			opens = append(opens, append([]string(nil), env...))
			if len(opens) == 1 {
				return p1, nil
			}
			return p2, nil
		},
	).Twice()
	floating := newQuietPTY()
	expectFloatingPrewarmOpen(f, normalSize, floating)

	d := newTestDaemon(t, f, stubClock{})
	tr1 := portsmocks.NewMockTransport(t)
	tr1.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr1.EXPECT().Close().Return(nil).Maybe()
	tr2 := portsmocks.NewMockTransport(t)
	tr2.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr2.EXPECT().Close().Return(nil).Maybe()
	sess, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: sz, TrueColor: false}, tr1)
	require.NoError(t, err)
	_, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: sz, TrueColor: true}, tr2)
	require.NoError(t, err)
	defer func() {
		_ = d.killSession(sess, ports.ReasonServerShutdown, false)
		d.sessWg.Wait()
	}()

	require.Contains(t, opens[0], "TERM=xterm-256color")
	require.NotContains(t, opens[0], "COLORTERM=truecolor")
	require.NoError(t, d.createTab(sess, ac.size))
	require.Contains(t, opens[1], "TERM=xterm-direct")
	require.Contains(t, opens[1], "COLORTERM=truecolor")
	require.Contains(t, opens[1], "TERM_PROGRAM=vev")
}

func TestCreateSessionAndSwitchInheritsTerminalEnv(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()
	var opens [][]string
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			opens = append(opens, append([]string(nil), env...))
			if len(opens) == 1 {
				return p1, nil
			}
			return p2, nil
		},
	).Twice()
	floating := newQuietPTY()
	expectFloatingPrewarmOpen(f, normalSize, floating)
	d := newTestDaemon(t, f, stubClock{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	sess, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: sz, TrueColor: true}, tr)
	require.NoError(t, err)
	// The source coordinator is deliberately made pending so the switch must
	// invalidate it rather than letting its stale callback render for ac.
	sourceCoordinator := sess.renderCoordinator()
	require.NotNil(t, sourceCoordinator)
	sourceCoordinator.invalidate(renderInvalidation{class: invalidateOutput})
	ac.sendMu.Lock()
	ac.output.next = 3
	ac.output.acked = 1
	ac.sendMu.Unlock()

	require.NoError(t, d.createSessionAndSwitch(sess, ac, "next"))
	got := ac.sess.Get()
	require.NotNil(t, got)
	sourceCoordinator.mu.Lock()
	require.NotNil(t, sourceCoordinator.lease)
	require.False(t, sourceCoordinator.lease.active, "source callbacks must be stale after handoff")
	sourceCoordinator.mu.Unlock()
	destinationCoordinator := got.renderCoordinator()
	require.NotNil(t, destinationCoordinator, "destination coordinator precedes first paint")
	destinationCoordinator.mu.Lock()
	require.Same(t, ac, destinationCoordinator.lease.attachment)
	destinationCoordinator.mu.Unlock()
	ac.sendMu.Lock()
	require.Equal(t, uint64(1), ac.output.next-ac.output.acked, "only the destination first paint may follow the rebase")
	ac.sendMu.Unlock()
	got.mu.Lock()
	require.True(t, got.terminal.TrueColor)
	got.mu.Unlock()
	require.Len(t, opens, 2)
	require.Contains(t, opens[1], "TERM=xterm-direct")
	require.Contains(t, opens[1], "COLORTERM=truecolor")
	require.Contains(t, opens[1], "TERM_PROGRAM=vev")
	_ = d.killSession(got, ports.ReasonSessionKilled, false)
	release1()
	release2()
	d.sessWg.Wait()
}

func TestAttachEnvironmentReplacesFuturePTYInputs(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p1, release1 := newBlockingPTY(t)
	defer release1()
	p2, release2 := newBlockingPTY(t)
	defer release2()

	var commands []string
	var envs [][]string
	f := portsmocks.NewMockPTYFactory(t)
	normalSize := domain.Size{Cols: sz.Cols, Rows: sz.Rows - 2}
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, normalSize).RunAndReturn(
		func(_ context.Context, command string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
			commands = append(commands, command)
			envs = append(envs, append([]string(nil), env...))
			if len(envs) == 1 {
				return p1, nil
			}
			return p2, nil
		},
	).Twice()
	floating := newQuietPTY()
	expectFloatingPrewarmOpen(f, normalSize, floating)
	d := newTestDaemon(t, f, stubClock{})
	tr1 := portsmocks.NewMockTransport(t)
	tr1.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr1.EXPECT().Close().Return(nil).Maybe()
	tr2 := portsmocks.NewMockTransport(t)
	tr2.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	tr2.EXPECT().Close().Return(nil).Maybe()

	first := []string{"SECRET=first", "TERM=bad", "TERM_PROGRAM_extra=keep", "SHELL=/usr/bin/fish", "A=a=b"}
	sess, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "work", Size: sz, Env: first}, tr1)
	require.NoError(t, err)
	second := []string{"SECRET=second", "TERM=bad", "TERM_PROGRAM_extra=keep", "SHELL=/bin/bash", "A=a=b"}
	_, ac, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentAttach, Name: "work", Size: sz, Env: second}, tr2)
	require.NoError(t, err)
	defer func() {
		_ = d.killSession(sess, ports.ReasonServerShutdown, false)
		d.sessWg.Wait()
	}()

	require.Equal(t, "/usr/bin/fish", commands[0])
	require.Equal(t, []string{"SECRET=first", "TERM_PROGRAM_extra=keep", "SHELL=/usr/bin/fish", "A=a=b", "TERM=xterm-256color", "TERM_PROGRAM=vev", "VEV=session=work,tab=" + sess.tabs[0].stableID + ",pane=" + sess.tabs[0].panes["pane-1"].stableID}, envs[0])
	require.NoError(t, d.createTab(sess, ac.size))
	require.Equal(t, "/bin/bash", commands[1])
	require.Equal(t, []string{"SECRET=second", "TERM_PROGRAM_extra=keep", "SHELL=/bin/bash", "A=a=b", "TERM=xterm-256color", "TERM_PROGRAM=vev", "VEV=session=work,tab=" + sess.tabs[1].stableID + ",pane=" + sess.tabs[1].panes["pane-1"].stableID}, envs[1])
}

func TestChildEnvFromPreservesNonReservedEntriesVerbatimAndInOrder(t *testing.T) {
	env := []string{"SECRET=a=b=c", "TERM_PROGRAM_extra=keep", "TERM", "TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old", "EMPTY="}
	got := childEnvFrom(env, "work", "tab", "pane", terminalEnv{})
	require.Equal(t, []string{
		"SECRET=a=b=c", "TERM_PROGRAM_extra=keep", "TERM", "EMPTY=",
		"TERM=xterm-256color", "TERM_PROGRAM=vev", "VEV=session=work,tab=tab,pane=pane",
	}, got)
}

func TestShellFromEnvironmentFallsBackOnlyWhenAbsentOrEmpty(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		want string
	}{
		{name: "absent", want: "/bin/sh"},
		{name: "empty", env: []string{"SHELL="}, want: "/bin/sh"},
		{name: "set", env: []string{"SHELL=/usr/bin/fish"}, want: "/usr/bin/fish"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, shellFromEnvironment(tt.env))
		})
	}
}

type lifecycleClock struct {
	nows []time.Time
	next int
}

func (c *lifecycleClock) Now() time.Time {
	i := min(c.next, len(c.nows)-1)
	c.next++
	return c.nows[i]
}
func (*lifecycleClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

func TestNamedSessionLifecycleTimestampsAreMonotonicAcrossClockRegression(t *testing.T) {
	ptys := make([]ports.PTY, 3)
	var releases []func()
	for i := range ptys {
		p, release := newBlockingPTY(t)
		ptys[i] = p
		releases = append(releases, release)
	}
	defer releaseAll(releases)
	clock := &lifecycleClock{nows: []time.Time{time.Unix(0, 100), time.Unix(0, 100), time.Unix(0, 50)}}
	d := newTestDaemon(t, newFactorySeq(t, ptys...), clock)
	sz := domain.Size{Cols: 80, Rows: 24}

	var got []int64
	for _, name := range []string{"one", "two", "three"} {
		sess, err := d.createSessionLocked(name, false, "/tmp", sz, terminalEnv{}, d.baseEnv)
		require.NoError(t, err)
		got = append(got, sess.createdAt)
	}
	require.Equal(t, []int64{100, 101, 102}, got)
}

func TestNamedSessionLifecycleExhaustionDoesNotMutateSessionState(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.lastAllocatedCreatedAt = math.MaxInt64
	d.nextID = 17
	d.stopped["retained"] = stoppedSession{name: "retained", cwd: "/tmp", createdAt: 9}

	sess, err := d.createSessionLocked("new", false, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)

	require.ErrorContains(t, err, "lifecycle identities exhausted")
	require.Nil(t, sess)
	require.Empty(t, d.sessions)
	require.Equal(t, uint64(17), d.nextID)
	require.Equal(t, int64(math.MaxInt64), d.lastAllocatedCreatedAt)
	require.Equal(t, stoppedSession{name: "retained", cwd: "/tmp", createdAt: 9}, d.stopped["retained"])

	_, _, err = d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "routed", Size: domain.Size{Cols: 80, Rows: 24}}, nil)
	require.ErrorContains(t, err, "lifecycle identities exhausted")
	require.Empty(t, d.sessions)
	require.Equal(t, uint64(17), d.nextID)
	require.Equal(t, stoppedSession{name: "retained", cwd: "/tmp", createdAt: 9}, d.stopped["retained"])
}

func TestNamedSessionLifecycleTimestampStartsAfterPersistedHighWaterMark(t *testing.T) {
	store, _ := newMockStore(t)
	seed := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStore(store))
	require.NoError(t, seed.persist.Save(persist.Record{Name: "old", Cwd: "/tmp", CreatedAt: 900, UpdatedAt: 900}))

	p, release := newBlockingPTY(t)
	defer release()
	clock := &lifecycleClock{nows: []time.Time{time.Unix(0, 100)}}
	// Constructing with persistence must establish the lifecycle high-water mark.
	d := New(newFactory(t, p), clock, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStore(store))
	d.serveCtx, d.serveCancel = context.WithCancel(context.Background())
	t.Cleanup(d.serveCancel)

	sess, err := d.createSessionLocked("new", false, "/tmp", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, d.baseEnv)
	require.NoError(t, err)
	require.Equal(t, int64(901), sess.createdAt)
}

func TestResumingStoppedSessionPreservesLifecycleIdentityInPersistence(t *testing.T) {
	fromPTY, releaseFrom := newBlockingPTY(t)
	targetPTY, releaseTarget := newBlockingPTY(t)
	defer releaseFrom()
	defer releaseTarget()
	d, from, ac, _ := newManualSessionWithPTYs(t, fromPTY)
	store, state := newMockStore(t)
	WithStore(store)(d)
	d.ptys = newFactory(t, targetPTY)
	d.stopped["stopped"] = stoppedSession{name: "stopped", cwd: "/tmp", createdAt: 77}

	require.True(t, d.resumeStoppedAndSwitch(from, ac, picker.Target{Name: "stopped", Stopped: true}))
	resumed := ac.currentSession()
	require.Equal(t, int64(77), resumed.createdAt)
	require.Equal(t, int64(77), state.record(t, "stopped").CreatedAt)
}

func TestLifecycleExpectedTargetChecksAreAtomicAcrossStateTransitions(t *testing.T) {
	t.Run("active replacement is rejected", func(t *testing.T) {
		d, from, ac, _, releases := newRecentNavigationTestSessions(t)
		defer releaseAll(releases)
		target := d.sessions["recent"]
		target.createdAt = 22
		expected := int64(21)

		require.Error(t, d.switchToTarget(from, ac, picker.Target{Session: target.id, Name: "recent", TabIndex: 0, ExpectedCreatedAt: &expected}))
		require.Same(t, from, ac.currentSession())
	})

	t.Run("active target that stopped resumes same lifecycle", func(t *testing.T) {
		fromPTY, releaseFrom := newBlockingPTY(t)
		targetPTY, releaseTarget := newBlockingPTY(t)
		defer releaseFrom()
		defer releaseTarget()
		d, from, ac, _ := newManualSessionWithPTYs(t, fromPTY)
		d.ptys = newFactory(t, targetPTY)
		expected := int64(31)
		d.stopped["target"] = stoppedSession{name: "target", cwd: "/tmp", createdAt: expected}

		require.NoError(t, d.switchToTarget(from, ac, picker.Target{Session: "old-active-id", Name: "target", TabIndex: 0, ExpectedCreatedAt: &expected}))
		require.Equal(t, "target", ac.currentSession().name)
		require.Equal(t, expected, ac.currentSession().createdAt)
	})

	t.Run("stopped target that became active switches same lifecycle", func(t *testing.T) {
		d, from, ac, _, releases := newRecentNavigationTestSessions(t)
		defer releaseAll(releases)
		target := d.sessions["recent"]
		target.createdAt = 41
		expected := int64(41)

		require.NoError(t, d.switchToTarget(from, ac, picker.Target{Session: "stopped:recent", Name: "recent", TabIndex: 0, Stopped: true, ExpectedCreatedAt: &expected}))
		require.Same(t, target, ac.currentSession())
	})

	t.Run("stopped deletion and recreation is rejected", func(t *testing.T) {
		fromPTY, releaseFrom := newBlockingPTY(t)
		defer releaseFrom()
		d, from, ac, _ := newManualSessionWithPTYs(t, fromPTY)
		d.stopped["target"] = stoppedSession{name: "target", cwd: "/tmp", createdAt: 52}
		expected := int64(51)

		require.Error(t, d.switchToTarget(from, ac, picker.Target{Name: "target", TabIndex: 0, Stopped: true, ExpectedCreatedAt: &expected}))
		require.Same(t, from, ac.currentSession())
	})
}
