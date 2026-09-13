package ipc

import (
	"net"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/testutil/replaytest"
)

func TestTransportReplay(t *testing.T) {
	replaytest.Run(t, func(t *testing.T, envelopes []wire.Envelope) []wire.Envelope {
		left, right := net.Pipe()
		t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
		sender, receiver := NewTransport(left), NewTransport(right)

		var wg sync.WaitGroup
		wg.Go(func() {
			for _, frame := range envelopes {
				if err := sender.Send(frame); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		})
		got := make([]wire.Envelope, 0, len(envelopes))
		for range envelopes {
			frame, err := receiver.Recv()
			if err != nil {
				t.Errorf("Recv: %v", err)
				_ = receiver.Close()
				break
			}
			got = append(got, frame)
		}
		wg.Wait()
		return got
	})
}
