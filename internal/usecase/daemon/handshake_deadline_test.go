package daemon

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// acceptedDeadlineConn is a typed daemon connection that yields one scripted
// first message, then blocks on its embedded transport, while exposing the
// optional accepted absolute handshake deadline seam. Ordinary test transports
// keep the fresh-budget behavior because they do not implement the seam.
type acceptedDeadlineConn struct {
	*handshakeBlockingTransport
	deadline time.Time
	first    protocol.ClientMessage
}

func (c *acceptedDeadlineConn) ReceiveClient() (protocol.ClientMessage, error) {
	if c.first != nil {
		message := c.first
		c.first = nil
		return message, nil
	}
	return c.handshakeBlockingTransport.ReceiveClient()
}

func (c *acceptedDeadlineConn) HandshakeDeadline() time.Time { return c.deadline }

func TestAcceptedHandshakeDeadlineSeamResolution(t *testing.T) {
	plain := newHandshakeBlockingTransport(false)
	require.True(t, acceptedHandshakeDeadline(plain).IsZero(),
		"a connection without the optional seam must keep the fresh 15s budget")

	fixed := time.Unix(1_700_000_000, 0)
	seamed := &acceptedDeadlineConn{
		handshakeBlockingTransport: newHandshakeBlockingTransport(false),
		deadline:                   fixed,
	}
	require.Equal(t, fixed, acceptedHandshakeDeadline(seamed))
}

func TestNewHandshakeContextConsumesAcceptedQueueTime(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 2)}
	d := newTestDaemon(t, nil, clock)

	// signalClock.Now is the zero time, so a deadline 4s after it leaves only
	// that much of the accepted budget instead of restarting 15s.
	ctx, timedOut, finish := d.newHandshakeContext(context.Background(), time.Time{}.Add(4*time.Second))
	defer finish()

	timer := <-clock.timers
	require.Equal(t, 4*time.Second, timer.duration, "queue time must be consumed from the accepted budget")
	require.NoError(t, ctx.Err())
	select {
	case <-timedOut:
		t.Fatal("handshake timed out before the accepted budget elapsed")
	default:
	}

	timer.ch <- time.Time{}
	awaitTestCompletion(t, ctx.Done(), "accepted budget did not cancel the handshake context")
	awaitTestCompletion(t, timedOut, "accepted budget did not publish the timeout signal")
}

func TestNewHandshakeContextExpiredAcceptedDeadlineFailsPromptly(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)

	ctx, timedOut, finish := d.newHandshakeContext(context.Background(), time.Time{}.Add(-time.Second))
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	select {
	case <-timedOut:
	default:
		t.Fatal("an already-expired accepted deadline did not publish the timeout signal")
	}
	select {
	case timer := <-clock.timers:
		t.Fatalf("an already-expired accepted deadline started a new %s budget", timer.duration)
	default:
	}
	// finish stays safe to call repeatedly on the immediate-expiry path.
	finish()
	finish()
}

// TestAcceptedHandshakeDeadlineSpansFirstFrameThroughWelcome proves handleConn
// adopts the accepted absolute deadline as the single budget covering the first
// ReceiveClient and the blocked Welcome send, so no second 15s budget is
// started and the accepted budget alone ends the handshake.
func TestAcceptedHandshakeDeadlineSpansFirstFrameThroughWelcome(t *testing.T) {
	pty, release := newBlockingPTY(t)
	defer release()
	clock := &signalClock{timers: make(chan *signalTimer, 4)}
	d := newTestDaemon(t, newFactory(t, pty), clock)

	conn := &acceptedDeadlineConn{
		handshakeBlockingTransport: newHandshakeBlockingTransport(true),
		deadline:                   time.Time{}.Add(9 * time.Second),
		first: protocol.Hello{
			Version: protocol.Version,
			Intent:  protocol.IntentNew,
			Name:    "accepted-deadline",
			Size:    domain.Size{Cols: 80, Rows: 24},
			TermEnv: "xterm-256color",
		},
	}
	done := make(chan struct{})
	go func() {
		d.handleConn(conn)
		close(done)
	}()

	timer := <-clock.timers
	require.Equal(t, 9*time.Second, timer.duration,
		"handleConn must adopt the accepted deadline rather than start 15s")
	<-conn.welcome
	timer.ch <- time.Time{}
	awaitTestCompletion(t, done, "accepted handshake did not stop after its budget elapsed")
	requireClosedHandshakeTransport(t, conn.handshakeBlockingTransport)
	d.mu.Lock()
	require.Empty(t, d.sessions, "a budget-expired newly-created handshake must not leave an empty session")
	require.Empty(t, d.parked)
	d.mu.Unlock()
}

// TestExpiredAcceptedHandshakeDeadlineClosesConn proves handleConn fails an
// already-expired accepted connection promptly: it closes the transport,
// returns, and never starts a fresh 15s receive budget.
func TestExpiredAcceptedHandshakeDeadlineClosesConn(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)

	conn := &acceptedDeadlineConn{
		handshakeBlockingTransport: newHandshakeBlockingTransport(false),
		deadline:                   time.Time{}.Add(-time.Second),
	}
	done := make(chan struct{})
	go func() {
		d.handleConn(conn)
		close(done)
	}()

	awaitTestCompletion(t, done, "an expired accepted deadline did not fail promptly")
	requireClosedHandshakeTransport(t, conn.handshakeBlockingTransport)
	select {
	case timer := <-clock.timers:
		t.Fatalf("an expired accepted deadline started a fresh %s budget", timer.duration)
	default:
	}
	d.mu.Lock()
	require.Empty(t, d.sessions)
	require.Empty(t, d.parked)
	d.mu.Unlock()
}

// TestHandleHelloAdoptsExpiredAcceptedDeadline proves the direct handleHello
// entry point honors the same optional seam as handleConn.
func TestHandleHelloAdoptsExpiredAcceptedDeadline(t *testing.T) {
	clock := &signalClock{timers: make(chan *signalTimer, 1)}
	d := newTestDaemon(t, nil, clock)

	conn := &acceptedDeadlineConn{
		handshakeBlockingTransport: newHandshakeBlockingTransport(false),
		deadline:                   time.Time{}.Add(-time.Second),
	}
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

	awaitTestCompletion(t, done, "handleHello did not fail on an expired accepted deadline")
	requireClosedHandshakeTransport(t, conn.handshakeBlockingTransport)
	select {
	case timer := <-clock.timers:
		t.Fatalf("handleHello started a fresh %s budget for an expired accepted deadline", timer.duration)
	default:
	}
	d.mu.Lock()
	require.Empty(t, d.sessions)
	require.Empty(t, d.parked)
	d.mu.Unlock()
}
