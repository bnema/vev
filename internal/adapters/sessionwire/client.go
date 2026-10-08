package sessionwire

import (
	"errors"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

type clientConnection struct {
	handshake
}

var _ ports.ClientConnection = (*clientConnection)(nil)

// NewClientConnection wraps one raw client connection incarnation. The
// preamble runs lazily on first use, bounded by the handshake deadline
// started here.
func NewClientConnection(raw wire.Transport) ports.ClientConnection {
	if raw == nil {
		return nil
	}
	return &clientConnection{handshake: newHandshake(raw, time.Now().Add(protocol.HandshakeTimeout))}
}

func (c *clientConnection) ensurePreamble() error {
	return c.handshake.ensurePreamble(runProtoClientPreamble)
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
