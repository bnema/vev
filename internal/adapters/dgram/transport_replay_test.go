package dgram

import (
	"testing"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/testutil/replaytest"
	"github.com/stretchr/testify/require"
)

func TestTransportReplay(t *testing.T) {
	replaytest.Run(t, func(t *testing.T, frames []wire.Frame) []wire.Frame {
		link := newSimulatedLink(fixedClock{}, packetPolicy{})
		leftPC, rightPC := newPairWithCapacity(link, 32)
		left, err := NewTransport(leftPC, testAddr("b"), key(), 1, 2)
		require.NoError(t, err)
		right, err := NewTransport(rightPC, testAddr("a"), key(), 2, 1)
		require.NoError(t, err)
		t.Cleanup(func() { _ = left.Close(); _ = right.Close() })

		got := make([]wire.Frame, 0, len(frames))
		for _, frame := range frames {
			require.NoError(t, left.Send(frame))
			link.flush(rightPC)
			received, recvErr := right.Recv()
			require.NoError(t, recvErr)
			got = append(got, received)
		}
		return got
	})
}
