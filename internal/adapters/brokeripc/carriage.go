package brokeripc

import (
	"github.com/bnema/vev/internal/adapters/streamframe"
	"github.com/bnema/vev/internal/protocol/wire"
)

// carriage adapts one bounded streamPipe to the wire.Transport contract the
// sessionwire typed connection frames over: Send writes one complete envelope
// (4-byte big-endian length then payload) into the pipe, Recv reads one
// complete envelope back out, and Close settles the pipe. Framing, the bounded
// egress queue, and close-unblocks-IO behavior all come from the shared
// streamframe implementation; this adapter owns no frame format of its own.
type carriage struct {
	framer *streamframe.Framer
}

var _ wire.BoundedTransport = (*carriage)(nil)

// newCarriage builds one envelope carriage over a stream pipe. The pipe carries
// opaque chunks and sessionwire applies the negotiated per-category ceilings
// above it, so the carriage ceiling is the protocol absolute limit.
func newCarriage(pipe *streamPipe) *carriage {
	return &carriage{
		framer: streamframe.NewFramer(pipe, pipe, func() error { pipe.closeWith(nil); return nil }, streamframe.WithMaxEnvelope(wire.AbsoluteEnvelopeLimit)),
	}
}

// Send queues one envelope for the sole writer and waits for its pipe write.
func (c *carriage) Send(envelope wire.Envelope) error { return c.framer.Send(envelope.Payload) }

// Recv reads one complete envelope.
func (c *carriage) Recv() (wire.Envelope, error) {
	payload, err := c.framer.Recv()
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: payload}, nil
}

// RecvBounded reads one complete envelope bounded to limit before allocation.
func (c *carriage) RecvBounded(limit uint64) (wire.Envelope, error) {
	payload, err := c.framer.RecvBounded(limit)
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: payload}, nil
}

// Close settles the pipe and joins the carriage writer. It is idempotent and
// concurrent-safe.
func (c *carriage) Close() error { return c.framer.Close() }
