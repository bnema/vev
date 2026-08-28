package wire

import "context"

// Transport is a framed message channel over a single connection. Close must
// be safe to call concurrently with Send and Recv, and must unblock active
// Send and Recv calls.
type Transport interface {
	Send(Frame) error
	Recv() (Frame, error) // blocking; io.EOF on close
	Close() error
}

// DatagramTransport marks transports backed by a datagram link. Typed session
// adapters translate this carriage detail into semantic capabilities.
type DatagramTransport interface {
	Transport
	DatagramTransport()
}

// AsyncTransport accepts frames for ordered background transmission. SendAsync
// returns once the adapter owns the frame; Send retains its synchronous wire-
// attempt contract. Daemon paint output may use this capability to pipeline.
type AsyncTransport interface {
	SendAsync(Frame) error
}

// OwnedSynchronousTransport owns the complete bounded synchronous operation,
// including adapter queues, pacing, write deadlines, and close cancellation.
type OwnedSynchronousTransport interface {
	SendSynchronous(Frame) error
}

// Dialer establishes outbound raw carriage connections.
type Dialer interface {
	Dial(context.Context) (Transport, error)
}

// Listener accepts incoming raw carriage connections.
type Listener interface {
	Accept() (Transport, error)
	Close() error
	Addr() string
}
