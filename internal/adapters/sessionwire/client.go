package sessionwire

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

type clientConnection struct {
	raw      wire.Transport
	ceilings protoCeilings
	// ceilingsMu guards ceilings. The lazy preamble publishes the negotiated
	// values from whichever goroutine first uses the connection, while another
	// goroutine may already be asking for capabilities or starting a send, so
	// the once-guarded write is not by itself a happens-before edge for them.
	ceilingsMu sync.Mutex

	preambleOnce sync.Once
	preambleErr  error
	deadline     time.Time

	// preambleDone closes exactly once when the handshake has run, whether it
	// succeeded or failed. It backs the handshake completion hook that a logical
	// mux connection exposes; the absolute deadline is never restarted.
	preambleDone chan struct{}

	hooksMu       sync.Mutex
	handshakeDone bool
	hooksRun      bool
	hooks         []func()
}

var _ ports.ClientConnection = (*clientConnection)(nil)

// NewClientConnection wraps one raw client connection incarnation. The
// preamble runs lazily on first use, bounded by the handshake deadline
// started here; prefer NewClientDialer, which runs it inside Dial.
func NewClientConnection(raw wire.Transport) ports.ClientConnection {
	if raw == nil {
		return nil
	}
	return &clientConnection{raw: raw, ceilings: defaultProtoCeilings(), deadline: time.Now().Add(protocol.HandshakeTimeout), preambleDone: make(chan struct{})}
}

// HandshakeDeadline returns the absolute local deadline of the session
// handshake this connection started with. It is the one accepted deadline the
// preamble, Hello/Welcome, and first committed publication share; the value is
// fixed for the connection's lifetime and is never restarted.
func (c *clientConnection) HandshakeDeadline() time.Time { return c.deadline }

// HandshakeDone returns a channel that is closed exactly once when the
// handshake has run, whether it succeeded or failed.
func (c *clientConnection) HandshakeDone() <-chan struct{} { return c.preambleDone }

// OnHandshakeComplete registers fn to run exactly once when the handshake has
// run. Registering after completion runs fn immediately; a nil fn is ignored.
func (c *clientConnection) OnHandshakeComplete(fn func()) {
	if fn == nil {
		return
	}
	c.hooksMu.Lock()
	if c.handshakeDone {
		c.hooksMu.Unlock()
		fn()
		return
	}
	c.hooks = append(c.hooks, fn)
	c.hooksMu.Unlock()
}

// finishPreamble publishes handshake completion from inside the preamble's
// sync.Once: it closes the done channel and marks the handshake complete so a
// later registration runs immediately. It deliberately runs no registered hook,
// because a hook that calls back into the connection would re-enter sync.Once
// and deadlock; runHandshakeHooks runs the hooks after the Once body returns.
// It tolerates a zero-value connection that never allocated the done channel
// (test literals and seam-only construction), so closing is nil-safe.
func (c *clientConnection) finishPreamble() {
	if c.preambleDone != nil {
		close(c.preambleDone)
	}
	c.hooksMu.Lock()
	c.handshakeDone = true
	c.hooksMu.Unlock()
}

// runHandshakeHooks runs the completion hooks registered before the handshake
// finished exactly once, after the preamble's sync.Once body has returned.
// Running them outside the Once is what lets a hook call back into the
// connection - for example to send a message - without re-entering sync.Once
// and deadlocking. Every ensurePreamble caller invokes it; only the first
// drains the hooks, and a registrant that arrives after completion observes the
// completed flag and runs itself.
func (c *clientConnection) runHandshakeHooks() {
	c.hooksMu.Lock()
	if c.hooksRun {
		c.hooksMu.Unlock()
		return
	}
	c.hooksRun = true
	hooks := c.hooks
	c.hooks = nil
	c.hooksMu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

func (c *clientConnection) ensurePreamble() error {
	c.preambleOnce.Do(func() {
		ctx, cancel := context.WithDeadline(context.Background(), c.deadline)
		defer cancel()
		next, err := runProtoClientPreamble(ctx, c.raw, c.limits())
		c.publishLimits(next)
		c.preambleErr = err
		if err != nil {
			_ = c.raw.Close()
		}
		c.finishPreamble()
	})
	c.runHandshakeHooks()
	return c.preambleErr
}

// limits returns the negotiated ceilings under the lock the preamble publishes
// them through.
func (c *clientConnection) limits() protoCeilings {
	c.ceilingsMu.Lock()
	defer c.ceilingsMu.Unlock()
	return c.ceilings
}

// publishLimits records the ceilings one preamble negotiation produced.
func (c *clientConnection) publishLimits(next protoCeilings) {
	c.ceilingsMu.Lock()
	c.ceilings = next
	c.ceilingsMu.Unlock()
}

func (c *clientConnection) SendClient(message protocol.ClientMessage) error {
	if err := c.ensurePreamble(); err != nil {
		return err
	}
	envelope, err := encodeProtoClient(message)
	if err != nil {
		return err
	}
	raw, err := proto.Marshal(envelope)
	if err != nil {
		return err
	}
	if err := checkCategoryCeiling(raw, clientEnvelopeCategory(raw), c.limits().maxReceiveEnvelopeBytes); err != nil {
		return err
	}
	return c.raw.Send(wire.Envelope{Payload: raw})
}

func (c *clientConnection) ReceiveServer() (protocol.ServerMessage, error) {
	if err := c.ensurePreamble(); err != nil {
		return nil, err
	}
	envelope, err := c.raw.Recv()
	if err != nil {
		return nil, err
	}
	limits := c.limits()
	message, decodeErr := decodeServerEnvelopeWithin(envelope.Payload, limits.maxReceiveEnvelopeBytes, limits.outputDataLimit)
	if decodeErr == nil {
		return message, nil
	}
	return nil, decodeErr
}

func (c *clientConnection) Capabilities() protocol.ConnectionCapabilities {
	return rawCapabilities(c.raw, c.limits().outputDataLimit)
}

func (c *clientConnection) LinkState() ports.LinkState         { return rawLinkState(c.raw) }
func (c *clientConnection) LinkEvents() <-chan ports.LinkEvent { return rawLinkEvents(c.raw) }
func (c *clientConnection) Close() error                       { return c.raw.Close() }

type clientDialer struct{ raw wire.Dialer }

var _ ports.ClientDialer = (*clientDialer)(nil)

// NewClientDialer wraps every dialed raw connection in a stable typed adapter.
func NewClientDialer(raw wire.Dialer) ports.ClientDialer {
	if raw == nil {
		return nil
	}
	return &clientDialer{raw: raw}
}

func (d *clientDialer) Dial(ctx context.Context) (ports.ClientConnection, error) {
	raw, err := d.raw.Dial(ctx)
	if err != nil {
		return nil, err
	}
	connection := &clientConnection{raw: raw, ceilings: defaultProtoCeilings(), deadline: time.Now().Add(protocol.HandshakeTimeout), preambleDone: make(chan struct{})}
	if err := connection.ensurePreambleWith(ctx); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return connection, nil
}

func (c *clientConnection) ensurePreambleWith(ctx context.Context) error {
	c.preambleOnce.Do(func() {
		ctx, cancel := context.WithDeadline(ctx, c.deadline)
		defer cancel()
		next, err := runProtoClientPreamble(ctx, c.raw, c.limits())
		c.publishLimits(next)
		c.preambleErr = err
		c.finishPreamble()
	})
	c.runHandshakeHooks()
	return c.preambleErr
}

// DecodeServerEnvelope unwraps one scanned server envelope payload into
// its semantic message for hidden one-shot carriages.
func DecodeServerEnvelope(payload []byte) (protocol.ServerMessage, error) {
	return decodeServerEnvelope(payload)
}

func decodeServerEnvelope(payload []byte) (protocol.ServerMessage, error) {
	return decodeServerEnvelopeWithin(payload, wire.AbsoluteEnvelopeLimit, uint64(protocol.MaxOutputDataLen))
}

// decodeServerEnvelopeWithin unwraps one server envelope under the
// negotiated receive ceilings: the serialized envelope must fit
// maxReceiveEnvelopeBytes and any Output must declare uncompressed data at
// or below outputDataLimit. The one-shot exported decoder uses the absolute
// ceilings because no preamble was negotiated there.
func decodeServerEnvelopeWithin(payload []byte, envelopeLimit, outputDataLimit uint64) (protocol.ServerMessage, error) {
	envelope := &wire.ServerEnvelope{}
	if err := checkCategoryCeiling(payload, serverEnvelopeCategory(payload), envelopeLimit); err != nil {
		return nil, classifyEnvelopeError(err, envelope)
	}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		return nil, classifyEnvelopeError(err, envelope)
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return nil, &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err}
	}
	if err := checkOutputDataCeiling(envelope, outputDataLimit); err != nil {
		return nil, &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err}
	}
	message, err := decodeProtoServer(envelope)
	if err != nil {
		return nil, classifyEnvelopeError(err, envelope)
	}
	return message, nil
}

func classifyEnvelopeError(err error, _ *wire.ServerEnvelope) *protocol.DecodeFailure {
	failure := &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err}
	if errors.Is(err, wire.ErrScanUnknown) {
		failure.Category = protocol.DecodeUnknownType
	} else if errors.Is(err, ErrWrongDirection) {
		failure.Category = protocol.DecodeWrongDirection
	}
	return failure
}
