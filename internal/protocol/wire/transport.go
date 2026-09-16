package wire

import "context"

// Envelope is the unit of exchange over a Transport: one complete
// serialized Protobuf envelope (client, server, or preamble), without any
// legacy type byte. The payload is owned by the caller on Send and by the
// receiver on Recv; Transports copy across queue boundaries so either side
// may retain it.
type Envelope struct {
	Payload []byte
}

// Transport is an envelope channel over a single connection. Close must be
// safe to call concurrently with Send and Recv, and must unblock active
// Send and Recv calls.
type Transport interface {
	Send(Envelope) error
	Recv() (Envelope, error) // blocking; io.EOF on close
	Close() error
}

// DatagramTransport marks transports backed by a datagram link. Typed session
// adapters translate this carriage detail into semantic capabilities.
type DatagramTransport interface {
	Transport
	DatagramTransport()
}

// AsyncTransport accepts envelopes for ordered background transmission.
// SendAsync returns once the adapter owns the envelope; Send retains its
// synchronous wire-attempt contract. Daemon paint output may use this
// capability to pipeline.
type AsyncTransport interface {
	SendAsync(Envelope) error
}

// OwnedSynchronousTransport owns the complete bounded synchronous operation,
// including adapter queues, pacing, write deadlines, and close cancellation.
type OwnedSynchronousTransport interface {
	SendSynchronous(Envelope) error
}

// BoundedTransport is a Transport whose receive path accepts a caller-supplied
// ceiling: RecvBounded validates the framed length prefix against limit before
// allocating or reading the body, so a peer cannot force a large allocation by
// advertising a huge frame. The effective bound is the smaller of limit and the
// transport's own framed ceiling, so it can only narrow a carriage that already
// enforces one. Every vev stream carriage (IPC, QUIC, SSH stdio) implements it;
// callers that need a bounded read assert this optional capability, leaving the
// base Transport contract unchanged.
type BoundedTransport interface {
	Transport
	RecvBounded(limit uint64) (Envelope, error)
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
