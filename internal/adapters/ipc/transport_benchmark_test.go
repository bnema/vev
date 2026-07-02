package ipc

import (
	"net"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func BenchmarkTransportRecvReuse(b *testing.B) {
	c1, c2 := net.Pipe()
	defer func() { _ = c1.Close() }()
	defer func() { _ = c2.Close() }()
	recv := NewTransport(c2)
	send := NewTransport(c1)
	payload := []byte("payload payload payload")
	frames := make(chan ports.Frame, 1024)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for frame := range frames {
			if err := send.Send(frame); err != nil {
				return
			}
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		frames <- ports.Frame{Type: ports.MsgOutput, Payload: payload}
		if _, err := recv.Recv(); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	close(frames)
	<-done
}
