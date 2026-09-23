// Raw-carrier bridge.
//
// One daemonmux physical connection rides an existing raw framed transport:
// IPC, QUIC, or SSH stdio, each of which already owns vev's shared 4-byte
// big-endian length framing and a caller-bounded receive (wire.BoundedTransport).
// This file adapts that raw carriage to the private FramedCarrier the pump
// (pump.go) and the physical preamble handshake (handshake.go) drive. It
// introduces no socket, no framing of its own, no listener, and no sessionwire.
//
// The bridge is narrow and bounded. Send is synchronous: it hands one envelope
// to the raw transport and returns the raw write result, so no error means the
// carriage accepted the frame. Receive is bounded to wire.PreambleLimit while
// the connection is in its preamble phase, and the explicit, one-way Negotiate
// transition raises that bound to the daemonmux envelope ceiling the physical
// preamble negotiated: a receive never allocates a body larger than the bound
// that applied when it started, and an oversized application frame after
// negotiation is refused rather than accepted. Negotiate must run before the
// pump that reads application frames; a receive already in flight keeps the
// bound it started with.
//
// Close is idempotent and concurrent-safe: it marks the bridge closed, closes
// the raw transport through the adapter prompt-close contract so a blocked
// Send or RecvBounded releases, and then waits for every bridge operation that
// was already admitted. Send and Receive honour their context the same way:
// cancelling it closes the raw transport to release the blocked call - a
// carriage that can no longer make progress is terminal - and joins the one
// worker it started, so no goroutine is abandoned. Because Close joins the
// operations it admitted and each operation joins its own worker, the bridge
// owns no long-lived goroutine.
package daemonmux

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/protocol/wire"
)

var (
	// ErrCarrierState reports an invalid construction or state transition: a
	// nil raw transport, or a second Negotiate once the preamble phase ended.
	ErrCarrierState = errors.New("daemonmux: invalid carrier state")
	// ErrCarrierClosed reports Send or Receive on a bridge that was closed.
	ErrCarrierClosed = errors.New("daemonmux: carrier closed")
)

// RawFramedTransport is the narrow raw framed carriage the bridge adapts: Send
// writes one complete envelope synchronously, RecvBounded reads one complete
// envelope bounded to limit before allocating or reading its body, and Close
// releases blocked I/O. Every wire.BoundedTransport satisfies it; the bridge
// deliberately does not require the unbounded Recv it never calls.
type RawFramedTransport interface {
	Send(envelope wire.Envelope) error
	RecvBounded(limit uint64) (wire.Envelope, error)
	Close() error
}

// Every raw wire.BoundedTransport satisfies the bridge's narrow capability.
var _ RawFramedTransport = (wire.BoundedTransport)(nil)

// FramedCarrierBridge adapts a RawFramedTransport to the private FramedCarrier
// contract. It is safe for concurrent use: Send and Receive may run from
// different goroutines, Close is idempotent and concurrent-safe, and the
// receive bound is read atomically so Negotiate never races a receive.
type FramedCarrierBridge struct {
	raw RawFramedTransport

	// limit is the receive bound the next Receive applies: wire.PreambleLimit
	// until Negotiate records the negotiated envelope ceiling.
	limit      atomic.Uint64
	negotiated atomic.Bool

	// mu guards closed and closeErr and serializes operation admission against
	// Close, so ops is never Add-ed concurrently with Wait.
	mu       sync.Mutex
	closed   bool
	closeErr error

	// done is closed exactly once by Close so a blocked operation wakes.
	done chan struct{}

	// ops counts admitted operations between admission and return. Close waits
	// for it after closing the raw transport, which is what guarantees no
	// bridge worker outlives Close.
	ops sync.WaitGroup

	rawCloseOnce sync.Once
	closeOnce    sync.Once
}

var _ FramedCarrier = (*FramedCarrierBridge)(nil)

// NewPreambleCarrier wraps raw in its preamble phase: every Receive is bounded
// to wire.PreambleLimit until Negotiate raises the bound to the negotiated
// daemonmux envelope ceiling. A nil raw transport is refused with
// ErrCarrierState.
func NewPreambleCarrier(raw RawFramedTransport) (*FramedCarrierBridge, error) {
	if raw == nil {
		return nil, ErrCarrierState
	}
	c := &FramedCarrierBridge{raw: raw, done: make(chan struct{})}
	c.limit.Store(wire.PreambleLimit)
	return c, nil
}

// Negotiate records the connection's negotiated ceilings and raises the receive
// bound from wire.PreambleLimit to ceilings.MaxReceiveEnvelopeBytes. It is the
// explicit, one-way state transition out of the preamble phase: an invalid
// advertisement, a second call, or a call after Close is refused rather than
// silently widening the bound. The transition must precede the pump that reads
// application frames; a receive already in flight keeps its old bound.
func (c *FramedCarrierBridge) Negotiate(ceilings MuxCeilings) error {
	if c == nil || c.raw == nil {
		return ErrCarrierState
	}
	if err := ceilings.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCarrierClosed
	}
	if c.negotiated.Load() {
		return ErrCarrierState
	}
	c.limit.Store(ceilings.MaxReceiveEnvelopeBytes)
	c.negotiated.Store(true)
	return nil
}

// ReceiveLimit returns the bound the next Receive applies: wire.PreambleLimit
// before Negotiate and the negotiated envelope ceiling afterwards.
func (c *FramedCarrierBridge) ReceiveLimit() uint64 {
	if c == nil {
		return 0
	}
	return c.limit.Load()
}

// Preamble reports whether the carrier is still bounded to wire.PreambleLimit.
func (c *FramedCarrierBridge) Preamble() bool {
	if c == nil {
		return false
	}
	return !c.negotiated.Load()
}

// Send hands one envelope to the raw transport and returns its write result, so
// it is synchronous: a nil error means the raw write attempt completed. A
// closed bridge reports ErrCarrierClosed. Cancelling ctx closes the raw
// transport to release a blocked write and joins the worker before returning,
// so a send that cannot finish never strands a goroutine.
func (c *FramedCarrierBridge) Send(ctx context.Context, payload []byte) error {
	if c == nil || c.raw == nil {
		return ErrCarrierState
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.enter(); err != nil {
		return err
	}
	defer c.ops.Done()
	result := make(chan error, 1)
	go func() { result <- c.raw.Send(wire.Envelope{Payload: payload}) }()
	select {
	case err := <-result:
		return err
	case <-c.done:
		<-result
		return ErrCarrierClosed
	case <-ctx.Done():
		c.closeRaw()
		<-result
		return ctx.Err()
	}
}

// Receive blocks for the next complete envelope, validated against the
// carrier's current bound (wire.PreambleLimit in the preamble phase, the
// negotiated envelope ceiling afterwards) before its body is allocated. A
// closed bridge reports ErrCarrierClosed. Cancelling ctx closes the raw
// transport to release the blocked read and joins the worker before returning.
func (c *FramedCarrierBridge) Receive(ctx context.Context) ([]byte, error) {
	if c == nil || c.raw == nil {
		return nil, ErrCarrierState
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := c.enter(); err != nil {
		return nil, err
	}
	defer c.ops.Done()
	limit := c.limit.Load()
	result := make(chan carrierRecv, 1)
	go func() {
		envelope, err := c.raw.RecvBounded(limit)
		result <- carrierRecv{payload: envelope.Payload, err: err}
	}()
	select {
	case r := <-result:
		return r.payload, r.err
	case <-c.done:
		<-result
		return nil, ErrCarrierClosed
	case <-ctx.Done():
		c.closeRaw()
		<-result
		return nil, ctx.Err()
	}
}

// Close interrupts a blocked Send or Receive and is idempotent and
// concurrent-safe. It marks the bridge closed, closes the raw transport
// through the adapter prompt-close contract, then waits for every admitted
// operation, so it never returns with a bridge worker still running. It returns
// the raw transport's close error; every caller observes the same result.
func (c *FramedCarrierBridge) Close() error {
	if c == nil || c.raw == nil {
		return ErrCarrierState
	}
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
		c.closeRaw()
		c.ops.Wait()
	})
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closeErr
}

// enter admits one operation unless the bridge already closed. It holds mu so
// an admission can never race Close's Wait.
func (c *FramedCarrierBridge) enter() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return ErrCarrierClosed
	}
	c.ops.Add(1)
	return nil
}

// closeRaw closes the raw transport exactly once through the adapter
// prompt-close contract, unblocking a blocked Send or RecvBounded. The first
// close's error is retained for Close.
func (c *FramedCarrierBridge) closeRaw() {
	c.rawCloseOnce.Do(func() {
		err := c.raw.Close()
		c.mu.Lock()
		c.closeErr = err
		c.mu.Unlock()
	})
}

// carrierRecv is one bounded receive result delivered from a bridge worker.
type carrierRecv struct {
	payload []byte
	err     error
}
