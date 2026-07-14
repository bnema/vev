package sshstdio

import (
	"io"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/testutil/replaytest"
)

func TestTransportReplay(t *testing.T) {
	replaytest.Run(t, func(t *testing.T, frames []ports.Frame) []ports.Frame {
		clientRead, serverWrite := io.Pipe()
		serverRead, clientWrite := io.Pipe()
		sender := NewTransport(clientRead, clientWrite, func() error { return clientWrite.Close() })
		receiver := NewTransport(serverRead, serverWrite, func() error { return serverRead.Close() })
		t.Cleanup(func() { _ = sender.Close(); _ = receiver.Close() })

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
