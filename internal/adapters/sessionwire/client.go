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

	preambleOnce sync.Once
	preambleErr  error
	deadline     time.Time
}

var _ ports.ClientConnection = (*clientConnection)(nil)

// NewClientConnection wraps one raw client connection incarnation. The
// preamble runs lazily on first use, bounded by the handshake deadline
// started here; prefer NewClientDialer, which runs it inside Dial.
func NewClientConnection(raw wire.Transport) ports.ClientConnection {
	if raw == nil {
		return nil
	}
	return &clientConnection{raw: raw, ceilings: defaultProtoCeilings(), deadline: time.Now().Add(protocol.HandshakeTimeout)}
}

func (c *clientConnection) ensurePreamble() error {
	c.preambleOnce.Do(func() {
		ctx, cancel := context.WithDeadline(context.Background(), c.deadline)
		defer cancel()
		c.ceilings, c.preambleErr = runProtoClientPreamble(ctx, c.raw, c.ceilings)
		if c.preambleErr != nil {
			_ = c.raw.Close()
		}
	})
	return c.preambleErr
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
	if err := checkCategoryCeiling(raw, clientEnvelopeCategory(raw), c.ceilings.maxReceiveEnvelopeBytes); err != nil {
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
	message, decodeErr := decodeServerEnvelopeWithin(envelope.Payload, c.ceilings.maxReceiveEnvelopeBytes, c.ceilings.outputDataLimit)
	if decodeErr == nil {
		return message, nil
	}
	return nil, decodeErr
}

func (c *clientConnection) Capabilities() protocol.ConnectionCapabilities {
	return rawCapabilities(c.raw, c.ceilings.outputDataLimit)
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
	connection := &clientConnection{raw: raw, ceilings: defaultProtoCeilings(), deadline: time.Now().Add(protocol.HandshakeTimeout)}
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
		c.ceilings, c.preambleErr = runProtoClientPreamble(ctx, c.raw, c.ceilings)
	})
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
