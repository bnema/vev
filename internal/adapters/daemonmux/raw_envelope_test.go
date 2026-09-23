package daemonmux

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// TestLogicalRawEnvelopeRelay proves a logical connection carries opaque
// envelopes byte-for-byte in both directions without decoding them.
func TestLogicalRawEnvelopeRelay(t *testing.T) {
	broker, daemon := newPairedPumps(t, DefaultMuxCeilings())
	acceptor := newDaemonAcceptor(daemon, 0)
	acceptor.startBare(1, time.Now().Add(5*time.Second))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	conn, err := mustConnector(t, broker).Open(ctx, controlRequest(1))
	require.NoError(t, err)
	defer conn.Close()
	require.NoError(t, acceptor.wait(1, time.Now().Add(5*time.Second)))

	payload := []byte{0xff, 0, 0x80, 7}
	require.NoError(t, conn.SendEnvelope(payload))
	status, ok := daemon.Engine().Status(conn.Ref().Physical)
	require.True(t, ok)
	peer := newMuxTransport(newMuxStreamPipe(daemon, conn.Ref().Physical, daemon.Ceilings().StreamChunkLimit, status.Done))
	defer peer.Close()
	got, err := peer.Recv()
	require.NoError(t, err)
	require.Equal(t, payload, got.Payload)

	large := bytes.Repeat([]byte{0xfe}, 2048)
	require.NoError(t, peer.Send(wire.Envelope{Payload: large}))
	gotPayload, err := conn.RecvEnvelope()
	require.NoError(t, err)
	require.Equal(t, large, gotPayload)
}
