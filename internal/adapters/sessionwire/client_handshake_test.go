package sessionwire

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// TestClientHandshakeCompletionSeam proves the handshake deadline is stable for
// the connection's lifetime and completion is published exactly once to early
// and late subscribers.
func TestClientHandshakeCompletionSeam(t *testing.T) {
	raw := &scriptedTransport{recv: []wire.Envelope{mustPreambleResponse(t)}}
	conn := NewClientConnection(raw).(*clientConnection)

	deadline := conn.HandshakeDeadline()
	require.WithinDuration(t, time.Now().Add(protocol.HandshakeTimeout), deadline, 5*time.Second)
	require.Equal(t, deadline, conn.HandshakeDeadline(), "the resolved deadline is stable")

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

	// The first application send runs the lazy preamble, which publishes
	// completion.
	require.NoError(t, conn.SendClient(protocol.Ping{}))
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

// TestClientFinishHandshakeNilSafe proves finishPreamble and runHandshakeHooks
// tolerate a connection literal that never allocated its done channel, and that
// the hook bookkeeping still completes.
func TestClientFinishHandshakeNilSafe(t *testing.T) {
	conn := &clientConnection{}
	require.NotPanics(t, func() {
		conn.finishPreamble()
		conn.runHandshakeHooks()
	})

	ran := false
	conn.OnHandshakeComplete(func() { ran = true })
	require.True(t, ran, "a hook registered after completion runs immediately")
}

// TestClientHandshakeHookReentersConnection proves a completion hook that calls
// back into the connection does not re-enter the preamble's sync.Once and
// deadlock: the hook runs after the Once body has returned.
func TestClientHandshakeHookReentersConnection(t *testing.T) {
	raw := &scriptedTransport{recv: mustClientPreambleQueue(t, mustEncodeServer(t, protocol.Pong{}))}
	conn := NewClientConnection(raw).(*clientConnection)

	sent := make(chan error, 1)
	conn.OnHandshakeComplete(func() {
		// A hook that calls a connection method must not re-enter the preamble's
		// sync.Once and block the handshake forever.
		sent <- conn.SendClient(protocol.Ping{})
	})

	require.NoError(t, conn.SendClient(protocol.Ping{}))
	select {
	case err := <-sent:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handshake hook re-entered the connection and deadlocked")
	}
}
