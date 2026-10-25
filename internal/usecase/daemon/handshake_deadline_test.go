package daemon

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// newAcceptedDeadlineConnection returns a handshake-blocking connection that
// also exposes the optional accepted absolute handshake deadline seam through a
// generated provider mock. A non-nil first becomes the scripted first client
// message. Ordinary connections keep the fresh-budget behavior because they do
// not implement the seam.
func newAcceptedDeadlineConnection(t testing.TB, blockWelcome bool, deadline time.Time, first protocol.ClientMessage) (*deadlineConnection, *handshakeLog) {
	t.Helper()
	var scripted []wire.Envelope
	if first != nil {
		scripted = append(scripted, mustClientEnvelope(first))
	}
	base, h := newHandshakeBlockingConnection(t, blockWelcome, scripted...)
	provider := portsmocks.NewMockHandshakeDeadlineProvider(t)
	provider.EXPECT().HandshakeDeadline().Return(deadline).Maybe()
	return &deadlineConnection{mockServerConnection: base, provider: provider}, h
}

func TestAcceptedHandshakeDeadlineSeamResolution(t *testing.T) {
	plain, _ := newHandshakeBlockingConnection(t, false)
	require.True(t, acceptedHandshakeDeadline(plain).IsZero(),
		"a connection without the optional seam must keep the fresh 15s budget")

	fixed := time.Unix(1_700_000_000, 0)
	seamed, _ := newAcceptedDeadlineConnection(t, false, fixed, nil)
	require.Equal(t, fixed, acceptedHandshakeDeadline(seamed))
}

func TestNewHandshakeContextConsumesAcceptedQueueTime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 2)
		d := newTestDaemon(t, nil, clock)

		// signalClock.Now is the zero time, so a deadline 4s after it leaves only
		// that much of the accepted budget instead of restarting 15s.
		ctx, timedOut, finish := d.newHandshakeContext(context.Background(), time.Time{}.Add(4*time.Second))
		defer finish()

		timer := <-clockTimers
		require.Equal(t, 4*time.Second, timer.duration, "queue time must be consumed from the accepted budget")
		require.NoError(t, ctx.Err())
		select {
		case <-timedOut:
			t.Fatal("handshake timed out before the accepted budget elapsed")
		default:
		}

		timer.ch <- time.Time{}
		requireReceived(t, ctx.Done(), "accepted budget did not cancel the handshake context")
		requireReceived(t, timedOut, "accepted budget did not publish the timeout signal")
	})
}

func TestNewHandshakeContextExpiredAcceptedDeadlineFailsPromptly(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 1)
		d := newTestDaemon(t, nil, clock)

		ctx, timedOut, finish := d.newHandshakeContext(context.Background(), time.Time{}.Add(-time.Second))
		require.ErrorIs(t, ctx.Err(), context.Canceled)
		select {
		case <-timedOut:
		default:
			t.Fatal("an already-expired accepted deadline did not publish the timeout signal")
		}
		select {
		case timer := <-clockTimers:
			t.Fatalf("an already-expired accepted deadline started a new %s budget", timer.duration)
		default:
		}
		// finish stays safe to call repeatedly on the immediate-expiry path.
		finish()
		finish()
	})
}

// TestAcceptedHandshakeDeadlineSpansFirstFrameThroughWelcome proves handleConn
// adopts the accepted absolute deadline as the single budget covering the first
// ReceiveClient and the blocked Welcome send, so no second 15s budget is
// started and the accepted budget alone ends the handshake.
func TestAcceptedHandshakeDeadlineSpansFirstFrameThroughWelcome(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		pty, release := newBlockingPTY(t)
		defer release()
		clock, clockTimers := newSignalClock(t, 4)
		d := newTestDaemon(t, newFactory(t, pty), clock)

		conn, connH := newAcceptedDeadlineConnection(t, true, time.Time{}.Add(9*time.Second), protocol.Hello{
			Version: protocol.Version,
			Intent:  protocol.IntentNew,
			Name:    "accepted-deadline",
			Size:    domain.Size{Cols: 80, Rows: 24},
			TermEnv: "xterm-256color",
		})
		done := make(chan struct{})
		go func() {
			d.handleConn(conn)
			close(done)
		}()

		timer := <-clockTimers
		require.Equal(t, 9*time.Second, timer.duration,
			"handleConn must adopt the accepted deadline rather than start 15s")
		<-connH.welcome
		timer.ch <- time.Time{}
		requireReceived(t, done, "accepted handshake did not stop after its budget elapsed")
		requireClosedHandshakeTransport(t, connH)
		d.mu.Lock()
		require.Empty(t, d.sessions, "a budget-expired newly-created handshake must not leave an empty session")
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
	})
}

// TestExpiredAcceptedHandshakeDeadlineClosesConn proves handleConn fails an
// already-expired accepted connection promptly: it closes the transport,
// returns, and never starts a fresh 15s receive budget.
func TestExpiredAcceptedHandshakeDeadlineClosesConn(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 1)
		d := newTestDaemon(t, nil, clock)

		conn, connH := newAcceptedDeadlineConnection(t, false, time.Time{}.Add(-time.Second), nil)
		done := make(chan struct{})
		go func() {
			d.handleConn(conn)
			close(done)
		}()

		requireReceived(t, done, "an expired accepted deadline did not fail promptly")
		requireClosedHandshakeTransport(t, connH)
		select {
		case timer := <-clockTimers:
			t.Fatalf("an expired accepted deadline started a fresh %s budget", timer.duration)
		default:
		}
		d.mu.Lock()
		require.Empty(t, d.sessions)
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
	})
}

// TestHandleHelloAdoptsExpiredAcceptedDeadline proves the direct handleHello
// entry point honors the same optional seam as handleConn.
func TestHandleHelloAdoptsExpiredAcceptedDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		clock, clockTimers := newSignalClock(t, 1)
		d := newTestDaemon(t, nil, clock)

		conn, connH := newAcceptedDeadlineConnection(t, false, time.Time{}.Add(-time.Second), nil)
		done := make(chan struct{})
		go func() {
			d.handleHello(conn, protocol.Hello{
				Version: protocol.Version,
				Intent:  protocol.IntentNew,
				Name:    "direct-expired",
				Size:    domain.Size{Cols: 80, Rows: 24},
				TermEnv: "xterm-256color",
			})
			close(done)
		}()

		requireReceived(t, done, "handleHello did not fail on an expired accepted deadline")
		requireClosedHandshakeTransport(t, connH)
		select {
		case timer := <-clockTimers:
			t.Fatalf("handleHello started a fresh %s budget for an expired accepted deadline", timer.duration)
		default:
		}
		d.mu.Lock()
		require.Empty(t, d.sessions)
		require.Empty(t, d.resume.parked)
		d.mu.Unlock()
	})
}
