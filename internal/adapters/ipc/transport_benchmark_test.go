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
	tr := NewTransport(c2)
	payload := []byte("payload payload payload")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < b.N; i++ {
			_ = NewTransport(c1).Send(ports.Frame{Type: ports.MsgOutput, Payload: payload})
		}
	}()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := tr.Recv(); err != nil {
			b.Fatal(err)
		}
	}
	<-done
}
