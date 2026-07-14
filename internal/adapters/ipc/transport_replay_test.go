package ipc

import (
	"net"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/testutil/replaytest"
)

func TestTransportReplay(t *testing.T) {
	replaytest.Run(t, func(t *testing.T, frames []ports.Frame) []ports.Frame {
		left, right := net.Pipe()
		t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
		sender, receiver := NewTransport(left), NewTransport(right)

		var wg sync.WaitGroup
		wg.Go(func() {
			for _, frame := range frames {
				if err := sender.Send(frame); err != nil {
					t.Errorf("Send: %v", err)
					return
				}
			}
		})
		got := make([]ports.Frame, 0, len(frames))
		for range frames {
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
