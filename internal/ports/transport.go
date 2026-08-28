package ports

import (
	"context"

	"github.com/bnema/vev/internal/protocol/wire"
)

// Transport is a framed message channel over a single connection. Close must
// be safe to call concurrently with Send and Recv, and must unblock active
// Send and Recv calls.
type Transport interface {
	Send(wire.Frame) error
	Recv() (wire.Frame, error) // blocking; io.EOF on close
	Close() error
}

// DatagramTransport marks transports backed by a datagram link. It lets
// usecases negotiate a conservative output window without importing adapters.
type DatagramTransport interface {
	Transport
	DatagramTransport()
}

// AsyncTransport accepts frames for ordered background transmission. SendAsync
// returns once the adapter owns the frame; Send retains its synchronous wire-
// attempt contract. Daemon paint output may use this capability to pipeline.
type AsyncTransport interface {
	SendAsync(wire.Frame) error
}

// OwnedSynchronousTransport owns the complete bounded synchronous operation,
// including adapter queues, pacing, write deadlines, and close cancellation.
type OwnedSynchronousTransport interface {
	SendSynchronous(wire.Frame) error
}

// Dialer establishes outbound Transport connections.
type Dialer interface {
	Dial(ctx context.Context) (Transport, error)
}

// Listener accepts incoming Transport connections.
type Listener interface {
	Accept() (Transport, error)
	Close() error
	Addr() string
}
