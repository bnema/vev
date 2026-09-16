package brokeripc

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/ipc"
)

// Fully bounded broker setup (P3.4 slice A). These tests drive the real AF_UNIX
// carriage, never an in-memory replacement, so the only way to interrupt a peer
// parked in framing I/O is the behavior under test: the client closing the
// carriage when its single setup deadline expires, and the server enforcing the
// accept-time registration budget on the pre-Register phase.
//
// The client half: one Dial context/deadline bounds the Unix dial, the preamble
// exchange, the Register send, and the wait for Registered; a peer that stalls
// after a successful preamble is interrupted by closing the carriage with every
// setup worker joined; and a successful connection detaches from the setup
// context after Registered. The server half: after ports.BrokerAuthority
// admission a client must send Register within the same accept-time handshake
// budget, or the session, its admitted core lease, and its listener slot are
// released, while Listener.Close stays immediate.

// silentBrokerPeer is one real AF_UNIX peer that completes the broker preamble
// and then never answers Register, so a client's registration wait is observable
// and interruptible without sleeping on a race.
type silentBrokerPeer struct {
	mux          ipc.MuxListener
	preambleSeen chan struct{}
	release      chan struct{}
	done         chan struct{}
	once         sync.Once
}

// startSilentBrokerPeer binds one real same-user endpoint whose only peer
// completes the preamble and then stays silent.
func startSilentBrokerPeer(t *testing.T, path string) *silentBrokerPeer {
	t.Helper()
	mux, err := ipc.ListenMux(path)
	require.NoError(t, err)
	peer := &silentBrokerPeer{
		mux:          mux,
		preambleSeen: make(chan struct{}),
		release:      make(chan struct{}),
		done:         make(chan struct{}),
	}
	go func() {
		defer close(peer.done)
		conn, err := mux.Accept()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if _, err := runServerPreamble(context.Background(), conn, brokerwire.DefaultCeilings()); err != nil {
			return
		}
		close(peer.preambleSeen)
		<-peer.release
	}()
	t.Cleanup(peer.stop)
	return peer
}

// stop releases the silent peer and joins it. It is idempotent.
func (p *silentBrokerPeer) stop() {
	p.once.Do(func() { close(p.release) })
	_ = p.mux.Close()
	<-p.done
}

// awaitPreamble fails unless the peer observed the client's preamble.
func (p *silentBrokerPeer) awaitPreamble(t *testing.T) {
	t.Helper()
	select {
	case <-p.preambleSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("the peer never observed the broker preamble")
	}
}

// TestClientDialSilentAfterPreambleIsBoundedAndJoined proves a peer that
// completes the preamble and never answers Register cannot park Dial past the
// configured handshake budget: Dial returns a deadline failure, the carriage was
// closed to interrupt the blocked wait, and the setup workers were joined
// instead of being abandoned.
func TestClientDialSilentAfterPreambleIsBoundedAndJoined(t *testing.T) {
	path := testSocketPath(t)
	peer := startSilentBrokerPeer(t, path)

	baseline := runtime.NumGoroutine()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	started := time.Now()
	client, err := Dial(ctx, path, Config{HandshakeTimeout: 200 * time.Millisecond})
	elapsed := time.Since(started)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, client)
	require.Less(t, elapsed, 5*time.Second, "the setup deadline must interrupt the silent registration wait")
	peer.awaitPreamble(t)

	peer.stop()
	require.Eventually(t, func() bool { return runtime.NumGoroutine() <= baseline+2 }, 5*time.Second, 5*time.Millisecond,
		"a bounded Dial must not abandon a setup goroutine (baseline=%d now=%d)", baseline, runtime.NumGoroutine())
}

// TestClientDialEverySetupStepSharesTheOneDeadline proves the caller's context
// deadline, not only HandshakeTimeout, bounds the whole client setup: a deadline
// far tighter than HandshakeTimeout still interrupts the registration wait.
func TestClientDialEverySetupStepSharesTheOneDeadline(t *testing.T) {
	path := testSocketPath(t)
	peer := startSilentBrokerPeer(t, path)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	client, err := Dial(ctx, path, Config{HandshakeTimeout: 30 * time.Second})
	elapsed := time.Since(started)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, client)
	require.Less(t, elapsed, 5*time.Second, "the caller's deadline must bound the setup")
	peer.awaitPreamble(t)
	peer.stop()
}

// TestClientDialCancellationInterruptsRegistrationWait proves an explicit
// cancellation, not just a deadline, interrupts the wait for Registered and is
// reported as cancellation rather than being masked by a carriage failure.
func TestClientDialCancellationInterruptsRegistrationWait(t *testing.T) {
	path := testSocketPath(t)
	peer := startSilentBrokerPeer(t, path)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := Dial(ctx, path, Config{HandshakeTimeout: 30 * time.Second})
		result <- err
	}()
	peer.awaitPreamble(t)
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled, "cancellation must interrupt the registration wait")
	case <-time.After(5 * time.Second):
		t.Fatal("Dial must return on cancellation instead of waiting out HandshakeTimeout")
	}
	peer.stop()
}

// TestClientConnectionOutlivesSetupContext proves a successful connection
// detaches from the setup context after Registered: once Dial returns, the
// caller's setup deadline may elapse without disturbing the established
// connection.
func TestClientConnectionOutlivesSetupContext(t *testing.T) {
	e := startEndpoint(t, Config{HandshakeTimeout: 5 * time.Second})

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancelSetup()
	client, err := Dial(setupCtx, e.path, Config{HandshakeTimeout: 5 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })
	session := e.accept()
	require.Equal(t, session.ConnectionID(), client.ConnectionID())

	// Let the setup deadline elapse; the connection was registered in time and
	// must keep serving on its own connection-lived context.
	time.Sleep(400 * time.Millisecond)
	require.ErrorIs(t, setupCtx.Err(), context.DeadlineExceeded, "the setup context must have expired")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.AddHost(ctx, "after@setup:22"), "a registered connection must outlive its setup context")
}

// TestServerRegistrationBudgetSettlesSilentPeer proves a same-user peer cannot
// hold an admitted core lease or a listener slot indefinitely: after
// ports.BrokerAuthority admission the client must send Register within the
// accept-time handshake budget, or the session closes the core service and
// releases the slot, and the listener keeps serving.
func TestServerRegistrationBudgetSettlesSilentPeer(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 1, HandshakeTimeout: 200 * time.Millisecond})
	l := e.listener.(*listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport, err := ipc.DialMuxContext(ctx, e.path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close() })
	_, err = runClientPreamble(ctx, transport, brokerwire.DefaultCeilings())
	require.NoError(t, err)

	session := e.accept()
	core := e.authority.last()
	require.NotNil(t, core)

	require.Eventually(t, func() bool {
		select {
		case <-core.done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 5*time.Millisecond, "a silent unregistered peer must not hold the admitted core lease")
	require.Eventually(t, func() bool { return l.liveSessions() == 0 }, 5*time.Second, 5*time.Millisecond,
		"a settled session must leave the listener's live set")

	// The timeout is the peer's departure, not a session failure: the settled
	// session reports an orderly end even though its terminal cause still records
	// the registration budget.
	require.NoError(t, session.Close())
	require.ErrorIs(t, session.(*serverSession).terminalErr(), ErrRegistrationTimeout)
	require.ErrorIs(t, session.(*serverSession).terminalErr(), context.DeadlineExceeded)

	// The released slot is reusable even at the bound of one: the listener
	// admits a fresh same-user client after the timeout.
	fresh := e.dial()
	freshSession := e.accept()
	require.Equal(t, fresh.ConnectionID(), freshSession.ConnectionID())

	// A clean listener shutdown must not report a peer timeout as its own
	// failure, for the settled session or for the live one it ends.
	require.NoError(t, e.listener.Close())
}

// TestRegistrationDeadlineBoundaryMarker proves the pre-Register guard treats
// the registered marker, not timer ordering, as the deadline boundary: a session
// whose Register handling already began is never settled with
// ErrRegistrationTimeout, while a session with no Register is. The deadline is
// already in the past so the guard observes the boundary deterministically.
func TestRegistrationDeadlineBoundaryMarker(t *testing.T) {
	tests := []struct {
		name        string
		registered  bool
		wantTimeout bool
	}{
		{name: "register handling begun at the deadline", registered: true},
		{name: "no register by the deadline", registered: false, wantTimeout: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			core := newTestCore(0x51)
			session, err := newServerSession(0x51, newRecordingTransport(), brokerwire.DefaultCeilings(), core, Config{}, func() {}, time.Now().Add(-time.Second))
			require.NoError(t, err)
			session.registered.Store(tc.registered)

			stop := session.armRegistrationDeadline()
			if tc.wantTimeout {
				require.Eventually(t, func() bool { return errors.Is(session.terminalErr(), ErrRegistrationTimeout) }, time.Second, time.Millisecond)
			}
			stop()

			if tc.wantTimeout {
				require.ErrorIs(t, session.terminalErr(), ErrRegistrationTimeout)
				require.ErrorIs(t, session.terminalErr(), context.DeadlineExceeded)
				return
			}
			require.NoError(t, session.terminalErr(), "a Register already being handled must not be settled as a peer timeout")
		})
	}
}

// TestServerRegistrationBudgetLeavesRegisteredSessionHealthy proves the
// registration budget bounds only the pre-Register phase: a session that
// registered in time keeps serving long after the accept-time deadline has
// passed.
func TestServerRegistrationBudgetLeavesRegisteredSessionHealthy(t *testing.T) {
	e := startEndpoint(t, Config{HandshakeTimeout: 150 * time.Millisecond})
	client, _ := e.pair()
	core := e.authority.last()

	// Outlive the accept-time registration budget.
	time.Sleep(400 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, client.AddHost(ctx, "after@budget:22"))
	select {
	case <-core.done:
		t.Fatal("a session that registered in time must not be settled by the registration budget")
	default:
	}
}

// TestListenerCloseDuringRegistrationWindowIsImmediate proves Close remains
// immediate for a session that is inside its registration window with a
// deliberately long budget: it does not wait out HandshakeTimeout, and it
// releases the admitted core service and the slot.
func TestListenerCloseDuringRegistrationWindowIsImmediate(t *testing.T) {
	e := startEndpoint(t, Config{MaxClients: 2, HandshakeTimeout: 30 * time.Second})
	l := e.listener.(*listener)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport, err := ipc.DialMuxContext(ctx, e.path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = transport.Close() })
	_, err = runClientPreamble(ctx, transport, brokerwire.DefaultCeilings())
	require.NoError(t, err)

	_ = e.accept()
	core := e.authority.last()
	require.NotNil(t, core)

	closed := make(chan error, 1)
	go func() { closed <- e.listener.Close() }()
	select {
	case err := <-closed:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Close must not wait out the registration budget for a silent session")
	}
	select {
	case <-core.done:
	case <-time.After(5 * time.Second):
		t.Fatal("Close must end the admitted core service")
	}
	require.Zero(t, l.liveSessions())
	require.Eventually(t, func() bool { return len(l.slots) == 0 }, 5*time.Second, 5*time.Millisecond,
		"Close must release the client slot")
}
