package dgram

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// This must stay byte-for-byte equal to the carriage transcript in the stream
// transport adapters. Datagram framing is not a rendering operation.
func replayTranscript() []ports.Frame {
	return []ports.Frame{
		{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("\x1b[2J\x1b[Hone\r\ntwo")})},
		{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 1, NewStateNum: 2, EchoAck: 7, Data: []byte("\x1b[2;1HTWO")})},
	}
}

func TestTransportReplay(t *testing.T) {
	link := newSimulatedLink(fixedClock{}, packetPolicy{})
	leftPC, rightPC := newPairWithCapacity(link, 32)
	left, err := NewTransport(leftPC, testAddr("b"), key(), 1, 2)
	require.NoError(t, err)
	right, err := NewTransport(rightPC, testAddr("a"), key(), 2, 1)
	require.NoError(t, err)
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })

	for _, want := range replayTranscript() {
		require.NoError(t, left.Send(want))
		link.flush(rightPC)
		got, err := right.Recv()
		require.NoError(t, err)
		require.Equal(t, want.Type, got.Type)
		require.Equal(t, want.Payload, got.Payload)
		out, err := ports.UnmarshalOutput(got.Payload)
		require.NoError(t, err)
		wantOut, err := ports.UnmarshalOutput(want.Payload)
		require.NoError(t, err)
		require.Equal(t, wantOut, out)
	}
}
