package sessionwire

import (
	"net"
	"testing"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// Baseline typed-conversation benchmark frozen in P1.1: one Hello/Welcome
// plus one Input/Output exchange over net.Pipe IPC transports. AC8 compares
// the equivalent Protobuf conversation against this median.

func BenchmarkTypedClientServerRoundTrip(b *testing.B) {
	clientRaw, serverRaw := net.Pipe()
	defer func() {
		_ = clientRaw.Close()
		_ = serverRaw.Close()
	}()
	client := NewClientConnection(ipc.NewTransport(clientRaw))
	server := NewServerConnection(ipc.NewTransport(serverRaw))

	hello := protocol.Hello{Version: protocol.Version, Intent: protocol.IntentAttach, Size: domain.Size{Cols: 80, Rows: 24}}
	welcome := protocol.Welcome{SessionID: "sess-1", SessionName: "work"}
	input := protocol.Input{InputSeq: 1, Data: []byte("x")}
	context := testUIOutputContext()
	output := protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: context, Data: []byte("row")}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		done := make(chan error, 1)
		go func() {
			if err := client.SendClient(hello); err != nil {
				done <- err
				return
			}
			if _, err := client.ReceiveServer(); err != nil {
				done <- err
				return
			}
			if err := client.SendClient(input); err != nil {
				done <- err
				return
			}
			_, err := client.ReceiveServer()
			done <- err
		}()
		if _, err := server.ReceiveClient(); err != nil {
			b.Fatal(err)
		}
		if err := server.SendServer(welcome); err != nil {
			b.Fatal(err)
		}
		if _, err := server.ReceiveClient(); err != nil {
			b.Fatal(err)
		}
		if err := server.SendServer(output); err != nil {
			b.Fatal(err)
		}
		if err := <-done; err != nil {
			b.Fatal(err)
		}
	}
}
