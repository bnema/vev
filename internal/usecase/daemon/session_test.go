package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
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
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(_ string, _ []string, _ []string, dir string, _ domain.Size) (ports.PTY, error) {
			dirs = append(dirs, dir)
			if len(dirs) == 1 {
				return first, nil
			}
			return second, nil
		},
	).Twice()

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
	opened := make(chan struct{})
	releaseOpen := make(chan struct{})
	closed := make(chan struct{})
	p2.EXPECT().Close().RunAndReturn(func() error {
		close(closed)
		return nil
	}).Once()
	p2.EXPECT().Pid().Return(4242).Maybe()

	f := portsmocks.NewMockPTYFactory(t)
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(p1, nil).Once()
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(string, []string, []string, string, domain.Size) (ports.PTY, error) {
			close(opened)
			<-releaseOpen
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
	<-opened
	_ = d.killSession(sess, ports.ReasonSessionKilled, false)
	close(releaseOpen)

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
	require.Equal(t, ports.ReasonDetach, det.Reason)

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
	f.EXPECT().Open(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(p, nil).Once()
	d := newTestDaemon(t, f, stubClock{})

	tr1, sends1, release1 := newConn(t, mustHello(ports.IntentEphemeral, "", domain.Size{Cols: 80, Rows: 24}))
	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr1) })
	awaitFrame(t, sends1, ports.MsgWelcome)
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

	release1()
	releasePTY()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
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

// --- send-error kills ephemeral -----------------------------------------------

// TestSendErrorKillsEphemeral: a failed output send detaches the client, and —
// like every other detach path — that kills an ephemeral session rather than

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

func TestDaemonLoadsPersistedSessionsAsStopped(t *testing.T) {
	store, _ := newMockStore(t)
	seed := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStore(store))
	require.NoError(t, seed.persist.Save(persist.Record{Name: "work", Cwd: "/tmp/work", CreatedAt: 7, UpdatedAt: 8}))

	d := New(nil, stubClock{}, slog.New(slog.NewTextHandler(io.Discard, nil)), WithStore(store))
	d.mu.Lock()
	stopped := d.stopped["work"]
	d.mu.Unlock()
	require.Equal(t, stoppedSession{name: "work", cwd: "/tmp/work", createdAt: 7}, stopped)
}

func TestCreateRenameKillPersistenceLifecycle(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, nil)
	require.NoError(t, err)
	require.True(t, state.has("work"))

	require.NoError(t, d.renameSession(sess, "renamed"))
	require.False(t, state.has("work"))
	require.True(t, state.has("renamed"))

	_ = d.killSession(sess, ports.ReasonSessionKilled, true)
	require.False(t, state.has("renamed"))
}

func TestRenameTabPersistsForNamedSession(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, _ := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, nil)
	require.NoError(t, err)
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "shell"))

	records, err := d.persist.LoadAll()
	require.NoError(t, err)
	require.Equal(t, []persist.Record{{Name: "work", Cwd: "/tmp/work", CreatedAt: sess.createdAt, UpdatedAt: records[0].UpdatedAt, TabNames: []string{"shell"}}}, records)
}

func TestRenameTabDoesNotPersistForEphemeralSession(t *testing.T) {
	sz := domain.Size{Cols: 80, Rows: 24}
	p, release := newBlockingPTY(t)
	defer release()
	store, state := newMockStore(t)
	d := newTestDaemon(t, newFactory(t, p), stubClock{})
	WithStore(store)(d)

	sess, err := d.createSessionLocked("0", true, "/tmp/work", sz, nil)
	require.NoError(t, err)
	require.NoError(t, d.renameTab(sess, sess.tabs[0], "shell"))

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

	sess, err := d.createSessionLocked("0", true, "/tmp/e", sz, nil)
	require.NoError(t, err)
	require.False(t, state.has("0"))
	require.EqualError(t, d.renameSession(sess, "taken"), "name already in use")
	require.NoError(t, d.renameSession(sess, "named"))
	require.True(t, state.has("named"))
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

	sess, err := d.createSessionLocked("work", false, "/tmp/work", sz, nil)
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
	d.killPickerTarget(picker.Target{Name: "old", Stopped: true})
	require.False(t, state.has("old"))
	d.mu.Lock()
	_, ok := d.stopped["old"]
	d.mu.Unlock()
	require.False(t, ok)
}

func TestIntentNewStoppedNameRejected(t *testing.T) {
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	d.stopped["taken"] = stoppedSession{name: "taken", cwd: "/tmp", createdAt: 1}
	tr := portsmocks.NewMockTransport(t)
	_, _, err := d.route(ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "taken", Size: domain.Size{Cols: 80, Rows: 24}}, tr)
	require.ErrorContains(t, err, "name already in use")
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

	natural, err := d.createSessionLocked("natural", false, "/tmp/old", sz, nil)
	require.NoError(t, err)
	other, err := d.createSessionLocked("other", false, "/tmp/other", sz, nil)
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
