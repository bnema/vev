package sessionwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// TestServerHandshakeCompletionSeam proves the accepted absolute deadline is
// stable for the connection's lifetime and completion is published exactly once
// to early and late subscribers, without restarting a deadline the caller
// already fixed.
func TestServerHandshakeCompletionSeam(t *testing.T) {
	fixed := time.Now().Add(protocol.HandshakeTimeout)
	raw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustEncodeClient(t, protocol.Ping{}))}
	conn := NewServerConnectionWithDeadline(raw, fixed).(*serverConnection)

	require.Equal(t, fixed, conn.HandshakeDeadline(), "the accepted deadline is adopted verbatim")
	require.Equal(t, fixed, conn.HandshakeDeadline(), "the resolved deadline is stable")

	select {
	case <-conn.HandshakeDone():
		t.Fatal("HandshakeDone closed before the handshake ran")
	default:
	}

	early := make(chan struct{}, 1)
	conn.OnHandshakeComplete(func() { early <- struct{}{} })
	select {
	case <-early:
		t.Fatal("completion ran before the handshake")
	default:
	}

	// The first receive runs the lazy preamble, which publishes completion.
	message, err := conn.ReceiveClient()
	require.NoError(t, err)
	require.Equal(t, protocol.Ping{}, message)

	select {
	case <-early:
	case <-time.After(5 * time.Second):
		t.Fatal("registered completion hook did not run")
	}
	select {
	case <-conn.HandshakeDone():
	case <-time.After(5 * time.Second):
		t.Fatal("HandshakeDone did not close")
	}

	// A hook registered after completion runs immediately.
	late := make(chan struct{}, 1)
	conn.OnHandshakeComplete(func() { late <- struct{}{} })
	select {
	case <-late:
	case <-time.After(5 * time.Second):
		t.Fatal("late completion hook did not run")
	}
}

// TestServerHandshakeElapsedDeadlineFailsPreamble proves a deadline the caller
// already spent fails the lazy preamble immediately instead of starting a
// second 15 s budget, and that completion is still published for the failed
// handshake.
func TestServerHandshakeElapsedDeadlineFailsPreamble(t *testing.T) {
	raw := &scriptedTransport{recv: mustServerPreambleQueue(t, mustEncodeClient(t, protocol.Ping{}))}
	conn := NewServerConnectionWithDeadline(raw, time.Now().Add(-time.Second)).(*serverConnection)

	completed := make(chan struct{}, 1)
	conn.OnHandshakeComplete(func() { completed <- struct{}{} })

	_, err := conn.ReceiveClient()
	require.Error(t, err)
	require.ErrorIs(t, err, ErrPreambleTimeout)
	select {
	case <-completed:
	case <-time.After(5 * time.Second):
		t.Fatal("a failed handshake did not publish completion")
	}
}

// TestServerFinishHandshakeNilSafe proves finishPreamble and runHandshakeHooks
// tolerate a connection literal that never allocated its done channel, and that
// the hook bookkeeping still completes.
func TestServerFinishHandshakeNilSafe(t *testing.T) {
	conn := &serverConnection{}
	require.NotPanics(t, func() {
		conn.finishPreamble()
		conn.runHandshakeHooks()
	})

	ran := false
	conn.OnHandshakeComplete(func() { ran = true })
	require.True(t, ran, "a hook registered after completion runs immediately")
}

// TestServerHandshakeHookReentersConnection proves a completion hook that calls
// back into the connection does not re-enter the preamble's sync.Once and
// deadlock: the hook runs after the Once body has returned, so it can receive a
// message of its own before the outer receive continues.
func TestServerHandshakeHookReentersConnection(t *testing.T) {
	// A preamble followed by two messages: one for the re-entrant hook and one
	// for the outer receive.
	raw := &scriptedTransport{recv: []wire.Envelope{
		mustPreambleRequest(t),
		{Payload: mustEncodeClient(t, protocol.Ping{})},
		{Payload: mustEncodeClient(t, protocol.Ping{})},
	}}
	conn := NewServerConnection(raw).(*serverConnection)

	hookErr := make(chan error, 1)
	hookMessage := make(chan protocol.ClientMessage, 1)
	conn.OnHandshakeComplete(func() {
		// A hook that calls a connection method must not re-enter the preamble's
		// sync.Once and block the handshake forever.
		message, err := conn.ReceiveClient()
		if err != nil {
			hookErr <- err
			return
		}
		hookMessage <- message
	})

	message, err := conn.ReceiveClient()
	require.NoError(t, err)
	require.Equal(t, protocol.Ping{}, message)
	select {
	case err := <-hookErr:
		require.NoError(t, err)
	case got := <-hookMessage:
		require.Equal(t, protocol.Ping{}, got)
	case <-time.After(5 * time.Second):
		t.Fatal("handshake hook re-entered the connection and deadlocked")
	}
}
