package sessionwire

import (
	"io"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol/wire"
)

// failingTransport fails every receive, so the lazy preamble settles quickly
// while still writing the ceilings it negotiated.
type failingTransport struct{}

func (failingTransport) Send(wire.Envelope) error     { return nil }
func (failingTransport) Recv() (wire.Envelope, error) { return wire.Envelope{}, io.EOF }
func (failingTransport) Close() error                 { return nil }

// TestCapabilitiesReadIsSafeDuringPreamble proves a caller may ask for
// capabilities while another goroutine is establishing the preamble.
//
// The preamble is lazy: whoever first uses the connection runs it, and it
// publishes the negotiated ceilings. A capabilities read does not go through
// that once, so it needs the same lock the publication takes; without it the
// read races with the write, which is exactly what a real offline composition
// reported under the race detector.
func TestCapabilitiesReadIsSafeDuringPreamble(t *testing.T) {
	connection := NewClientConnection(failingTransport{})
	require.NotNil(t, connection)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = connection.ReceiveServer()
	}()

	// Read capabilities for the whole window in which the preamble publishes.
	for {
		select {
		case <-done:
			return
		default:
			_ = connection.Capabilities()
		}
	}
}

// TestServerCapabilitiesReadIsSafeDuringPreamble is the server-side twin: the
// daemon-facing connection has the same lazy publication and the same unlocked
// capabilities read.
func TestServerCapabilitiesReadIsSafeDuringPreamble(t *testing.T) {
	connection, ok := NewServerConnection(failingTransport{}).(*serverConnection)
	require.True(t, ok)

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = connection.ReceiveClient()
	}()

	for {
		select {
		case <-done:
			return
		default:
			_ = connection.Capabilities()
		}
	}
}
