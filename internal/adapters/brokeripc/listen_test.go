package brokeripc

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
)

// gatedAuthority blocks admission until a test releases it, so a listener Close
// can race an admission that has already reached the authority.
type gatedAuthority struct {
	inner   ports.BrokerAuthority
	entered chan struct{}
	gate    chan struct{}
	once    sync.Once
}

func (a *gatedAuthority) AdmitClient(ctx context.Context) (ports.BrokerCoreService, error) {
	a.once.Do(func() { close(a.entered) })
	<-a.gate
	return a.inner.AdmitClient(ctx)
}

// ctxAuthority blocks admission until its context is canceled, so a test can
// prove Listener.Close cancels an already-accepted client parked in admission
// instead of waiting out HandshakeTimeout.
type ctxAuthority struct {
	entered chan struct{}
	once    sync.Once
}

func (a *ctxAuthority) AdmitClient(ctx context.Context) (ports.BrokerCoreService, error) {
	a.once.Do(func() { close(a.entered) })
	<-ctx.Done()
	return nil, ctx.Err()
}

// TestCloseRacingBlockedAdmissionDoesNotHang proves a Close that races a blocked
// admission never calls session.Close before the run loop starts, which would
// wait forever on an unstarted done channel and hang listener shutdown.
func TestCloseRacingBlockedAdmissionDoesNotHang(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x51)
	path := testSocketPath(t)
	gated := &gatedAuthority{
		inner:   newFakeAuthority(epoch, ports.BrokerSnapshot{}),
		entered: make(chan struct{}),
		gate:    make(chan struct{}),
	}
	bound, err := listen(path, epoch, gated, Config{MaxClients: 2, HandshakeTimeout: 30 * time.Second}, ipc.SameUserPeerVerifier())
	require.NoError(t, err)

	// Complete the preamble so the connection reaches admission, then leave it
	// blocked there.
	_ = rawDial(t, &endpoint{t: t, path: path, epoch: epoch}, brokerwire.DefaultCeilings())
	select {
	case <-gated.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("admission was never reached")
	}

	closed := make(chan error, 1)
	go func() { closed <- bound.Close() }()

	// Release admission only once the listener is committed to closing, so the
	// admission observes a closed listener before its run loop would start.
	require.Eventually(t, func() bool { return bound.(*listener).isClosed() }, 5*time.Second, time.Millisecond)
	close(gated.gate)

	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close must not wait on a session whose run loop never started")
	}
	require.Zero(t, bound.(*listener).liveSessions())
}

// TestCloseCancelsStalledPreamble proves a client that connects but never sends
// its preamble is dropped by Listener.Close immediately: Close returns well
// before HandshakeTimeout, the accepted connection closes so blocked framing
// I/O unblocks, and the slot and admission goroutine are released.
func TestCloseCancelsStalledPreamble(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x51)
	path := testSocketPath(t)
	authority := newFakeAuthority(epoch, ports.BrokerSnapshot{})
	bound, err := Listen(path, epoch, authority, Config{MaxClients: 2, HandshakeTimeout: 30 * time.Second})
	require.NoError(t, err)
	l := bound.(*listener)

	// A raw client connects and never sends its preamble, so its accepted
	// carriage is parked in framing I/O for the whole handshake budget.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stalled, err := ipc.DialMuxContext(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stalled.Close() })

	require.Eventually(t, func() bool { return l.inFlight() == 1 }, 5*time.Second, time.Millisecond,
		"the stalled connection must be accepted and parked before the close")

	closed := make(chan error, 1)
	go func() { closed <- bound.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close must not wait out HandshakeTimeout on a stalled preamble")
	}

	// The accepted connection is closed, so the stalled client's framing read
	// unblocks instead of hanging until the peer gives up.
	readErr := make(chan error, 1)
	go func() {
		_, err := stalled.RecvBounded(brokerwire.BrokerPreambleLimit)
		readErr <- err
	}()
	select {
	case err := <-readErr:
		require.Error(t, err, "the accepted connection must be closed")
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled client's framing read must unblock on Close")
	}

	require.Zero(t, l.inFlight(), "no accepted carriage may stay pending")
	require.Zero(t, l.liveSessions())
	require.Eventually(t, func() bool { return len(l.slots) == 0 }, 5*time.Second, time.Millisecond,
		"the client slot must be released")
	require.ErrorIs(t, l.refusal(), ErrListenerClosed, "a close-induced drop is ErrListenerClosed")
}

// TestCloseCancelsClientStalledInAdmission proves Listener.Close cancels a
// client that completed its preamble and is parked in an authority honoring
// ctx, so Close returns promptly and the accepted carriage is closed.
func TestCloseCancelsClientStalledInAdmission(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x51)
	path := testSocketPath(t)
	stall := &ctxAuthority{entered: make(chan struct{})}
	bound, err := Listen(path, epoch, stall, Config{MaxClients: 2, HandshakeTimeout: 30 * time.Second})
	require.NoError(t, err)
	l := bound.(*listener)

	// Complete the preamble so the connection reaches admission, then leave the
	// authority holding it until the listener is closed.
	_ = rawDial(t, &endpoint{t: t, path: path, epoch: epoch}, brokerwire.DefaultCeilings())
	select {
	case <-stall.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("admission was never reached")
	}
	require.Equal(t, 1, l.inFlight())

	closed := make(chan error, 1)
	go func() { closed <- bound.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close must cancel an admission stalled in the authority")
	}

	require.Zero(t, l.inFlight(), "the canceled carriage must be released")
	require.Zero(t, l.liveSessions())
	require.Eventually(t, func() bool { return len(l.slots) == 0 }, 5*time.Second, time.Millisecond,
		"the client slot must be released")
	require.ErrorIs(t, l.refusal(), ErrListenerClosed, "a close-induced drop is ErrListenerClosed")
}

// TestStalledPreambleKeepsHandshakeTimeout proves a stalled preamble with the
// listener still open keeps its normal handshake-timeout classification rather
// than being reported as a listener close.
func TestStalledPreambleKeepsHandshakeTimeout(t *testing.T) {
	const epoch = ports.BrokerEpoch(0x51)
	path := testSocketPath(t)
	authority := newFakeAuthority(epoch, ports.BrokerSnapshot{})
	bound, err := Listen(path, epoch, authority, Config{MaxClients: 2, HandshakeTimeout: 100 * time.Millisecond})
	require.NoError(t, err)
	t.Cleanup(func() { _ = bound.Close() })
	l := bound.(*listener)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stalled, err := ipc.DialMuxContext(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stalled.Close() })

	require.Eventually(t, func() bool { return errors.Is(l.refusal(), context.DeadlineExceeded) }, 5*time.Second, 5*time.Millisecond,
		"a stalled preamble must keep its handshake-timeout classification")
	require.False(t, errors.Is(l.refusal(), ErrListenerClosed), "an open listener must not report a close")
	require.Zero(t, l.inFlight())
	require.Zero(t, l.liveSessions())
}

// TestAcceptTerminalCauseSurvivesInFlightRefusal proves the terminal carriage
// failure Accept reports is split from the diagnostic last refusal, so an
// in-flight refusal can never overwrite the cause that ended acceptance.
func TestAcceptTerminalCauseSurvivesInFlightRefusal(t *testing.T) {
	fatal := errors.New("carriage accept failed")
	refusal := ipc.ErrMuxPeerRejected

	t.Run("refusal after fatal", func(t *testing.T) {
		l := &listener{fatal: make(chan struct{})}
		l.failAccept(fatal)
		l.confirmRefusal(refusal)
		_, err := l.Accept()
		require.ErrorIs(t, err, fatal)
		require.ErrorIs(t, l.refusal(), refusal, "the diagnostic refusal is still recorded")
	})

	t.Run("fatal after refusal", func(t *testing.T) {
		l := &listener{fatal: make(chan struct{})}
		l.confirmRefusal(refusal)
		l.failAccept(fatal)
		_, err := l.Accept()
		require.ErrorIs(t, err, fatal)
	})

	t.Run("concurrent order", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			l := &listener{fatal: make(chan struct{})}
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); l.failAccept(fatal) }()
			go func() { defer wg.Done(); l.confirmRefusal(refusal) }()
			wg.Wait()
			_, err := l.Accept()
			require.ErrorIs(t, err, fatal, "an in-flight refusal must never replace the terminal cause")
		}
	})
}

// TestListenCreatesOwnerOnlyDirectoryAndSocket proves the endpoint is placed in
// an owner-only directory and that the bound socket itself is tightened, so the
// private carriage is never exposed through filesystem modes.
func TestListenCreatesOwnerOnlyDirectoryAndSocket(t *testing.T) {
	path := testSocketPath(t)
	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	listener, err := Listen(path, 1, authority, Config{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	dirInfo, err := os.Lstat(filepath.Dir(path))
	require.NoError(t, err)
	require.True(t, dirInfo.IsDir())
	require.Equal(t, os.FileMode(0o700), dirInfo.Mode().Perm())

	socketInfo, err := os.Lstat(path)
	require.NoError(t, err)
	require.NotZero(t, socketInfo.Mode()&os.ModeSocket, "endpoint must be a Unix socket")
	require.Equal(t, os.FileMode(0o600), socketInfo.Mode().Perm())
	require.Equal(t, path, listener.Addr())
}

// TestListenRecoversStaleSocket proves an endpoint left behind by a dead owner
// is recovered rather than refused.
func TestListenRecoversStaleSocket(t *testing.T) {
	path := testSocketPath(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	addr, err := net.ResolveUnixAddr("unix", path)
	require.NoError(t, err)
	stale, err := net.ListenUnix("unix", addr)
	require.NoError(t, err)
	stale.SetUnlinkOnClose(false)
	require.NoError(t, stale.Close())
	_, err = os.Lstat(path)
	require.NoError(t, err, "stale socket file must still exist")

	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	listener, err := Listen(path, 1, authority, Config{})
	require.NoError(t, err)
	require.NoError(t, listener.Close())
}

// TestListenRefusesLiveOwner proves a listener never evicts a live endpoint.
func TestListenRefusesLiveOwner(t *testing.T) {
	running := startEndpoint(t, Config{})
	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	_, err := Listen(running.path, 1, authority, Config{})
	require.ErrorIs(t, err, ipc.ErrDaemonRunning)
}

// TestListenRefusesForeignPathWithoutRemovingIt proves a path that is not a
// socket is refused and never unlinked.
func TestListenRefusesForeignPathWithoutRemovingIt(t *testing.T) {
	path := testSocketPath(t)
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o700))
	require.NoError(t, os.WriteFile(path, []byte("foreign"), 0o600))

	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	_, err := Listen(path, 1, authority, Config{})
	require.ErrorIs(t, err, ipc.ErrMuxForeignPath)
	content, readErr := os.ReadFile(path)
	require.NoError(t, readErr)
	require.Equal(t, "foreign", string(content))
}

// TestCloseRemovesOnlyItsOwnSocket proves Close unlinks the inode it created and
// preserves a replacement at the same path.
func TestCloseRemovesOnlyItsOwnSocket(t *testing.T) {
	path := testSocketPath(t)
	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	listener, err := Listen(path, 1, authority, Config{})
	require.NoError(t, err)

	require.NoError(t, os.Remove(path))
	require.NoError(t, os.WriteFile(path, []byte("replacement"), 0o600))
	require.NoError(t, listener.Close())

	content, err := os.ReadFile(path)
	require.NoError(t, err, "Close must preserve a path it did not create")
	require.Equal(t, "replacement", string(content))

	require.NoError(t, listener.Close(), "Close is idempotent")
}

// TestListenRejectsInvalidConfig proves a missing epoch or authority is refused.
func TestListenRejectsInvalidConfig(t *testing.T) {
	path := testSocketPath(t)
	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	_, err := Listen(path, 0, authority, Config{})
	require.ErrorIs(t, err, ErrConfig)
	_, err = Listen(path, 1, nil, Config{})
	require.ErrorIs(t, err, ErrConfig)
}

// TestAcceptRefusesRejectedPeerAndKeepsServing proves a peer refused by the
// carriage is isolated: the refusal is recorded, no session is created, and the
// listener keeps admitting later same-user clients.
func TestAcceptRefusesRejectedPeerAndKeepsServing(t *testing.T) {
	path := testSocketPath(t)
	authority := newFakeAuthority(1, ports.BrokerSnapshot{})
	var calls atomic.Int32
	verify := func(conn *net.UnixConn) error {
		if calls.Add(1) == 1 {
			return ipc.ErrMuxPeerRejected
		}
		return ipc.SameUserPeerVerifier()(conn)
	}
	mux, err := listen(path, 1, authority, Config{}, verify)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mux.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	refused, err := dial(ctx, path, Config{HandshakeTimeout: time.Second}, ipc.SameUserPeerVerifier())
	require.Error(t, err, "a refused peer must never be admitted")
	require.Nil(t, refused)

	client, err := Dial(ctx, path, Config{HandshakeTimeout: time.Second})
	require.NoError(t, err, "the listener must keep serving after a refusal")
	t.Cleanup(func() { _ = client.Close() })
	session, err := mux.Accept()
	require.NoError(t, err)
	require.Equal(t, client.ConnectionID(), session.ConnectionID())
	require.ErrorIs(t, mux.(*listener).refusal(), ipc.ErrMuxPeerRejected)
}

// TestDialRefusesRejectedPeer proves the client adapter fails closed when the
// carriage refuses the peer instead of admitting an unverified connection.
func TestDialRefusesRejectedPeer(t *testing.T) {
	e := startEndpoint(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	refusing := func(*net.UnixConn) error { return ipc.ErrMuxPeerRejected }
	client, err := dial(ctx, e.path, Config{}, refusing)
	require.ErrorIs(t, err, ipc.ErrMuxPeerRejected)
	require.Nil(t, client)
}

// TestBoundedConcurrentClients proves the listener admits at most MaxClients
// concurrent connections and recovers a slot when a client disconnects.
func TestBoundedConcurrentClients(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 1, HandshakeTimeout: 300 * time.Millisecond})
	first, firstSession := e.pair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := Dial(ctx, e.path, Config{HandshakeTimeout: 250 * time.Millisecond})
	require.Error(t, err, "a second client must not be admitted past the bound")

	// Closing the accepted connection releases its slot, and the client slot is
	// reusable for a fresh client.
	require.NoError(t, first.Close())
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		client, err := Dial(ctx, e.path, Config{HandshakeTimeout: 500 * time.Millisecond})
		if err != nil {
			return false
		}
		_ = client.Close()
		return true
	}, 5*time.Second, 20*time.Millisecond)
	require.NotEqual(t, ports.BrokerConnectionID{}, firstSession.ConnectionID())
}

// TestStalledHandshakeDoesNotBlockOtherClients proves a peer that connects but
// never completes its preamble occupies only its own slot and goroutine: another
// client is admitted while it is still stalled.
func TestStalledHandshakeDoesNotBlockOtherClients(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 4, HandshakeTimeout: 400 * time.Millisecond})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stalled, err := ipc.DialMuxContext(ctx, e.path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stalled.Close() })

	client, err := Dial(ctx, e.path, Config{HandshakeTimeout: 2 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	session, err := e.listener.Accept()
	require.NoError(t, err)
	require.Equal(t, client.ConnectionID(), session.ConnectionID())
}

// TestConcurrentDialAndClose proves concurrent clients may dial, register, run
// one operation, and close without corrupting listener or session state.
func TestConcurrentDialAndClose(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 8, HandshakeTimeout: 2 * time.Second})

	sessions := make(chan ports.BrokerCoreService, 8)
	go func() {
		defer close(sessions)
		for i := 0; i < cap(sessions); i++ {
			service, err := e.listener.Accept()
			if err != nil {
				return
			}
			sessions <- service
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := Dial(ctx, e.path, Config{HandshakeTimeout: 2 * time.Second})
			if err != nil {
				t.Errorf("Dial: %v", err)
				return
			}
			if err := connectionDelegates(ctx, client); err != nil {
				t.Errorf("connection refused a membership round trip: %v", err)
			}
			_ = client.Close()
		}()
	}
	wg.Wait()
	for service := range sessions {
		require.NoError(t, service.Close())
	}
}
