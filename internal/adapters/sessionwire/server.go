// Package sessionwire adapts typed session messages to Protobuf envelopes.
package sessionwire

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

var (
	ErrWrongDirection  = errors.New("sessionwire: wrong message direction")
	ErrInvalidMessage  = errors.New("sessionwire: invalid message")
	ErrUnsupportedSend = errors.New("sessionwire: send mode is unsupported")
)

type serverConnection struct {
	raw      wire.Transport
	ceilings protoCeilings

	preambleOnce sync.Once
	preambleErr  error
	deadline     time.Time
}

var _ ports.ServerConnection = (*serverConnection)(nil)

// NewServerConnection wraps one raw connection incarnation exactly once. The
// server preamble runs on the first ReceiveClient, bounded by the handshake
// deadline started here; the daemon's transport watcher closes the link on
// timeout, which unblocks the preamble read.
func NewServerConnection(raw wire.Transport) ports.ServerConnection {
	if raw == nil {
		return nil
	}
	return &serverConnection{raw: raw, ceilings: defaultProtoCeilings(), deadline: time.Now().Add(protocol.HandshakeTimeout)}
}

func (c *serverConnection) ensurePreamble() error {
	c.preambleOnce.Do(func() {
		ctx, cancel := context.WithDeadline(context.Background(), c.deadline)
		defer cancel()
		c.ceilings, c.preambleErr = runProtoServerPreamble(ctx, c.raw, c.ceilings)
		if c.preambleErr != nil {
			_ = c.raw.Close()
		}
	})
	return c.preambleErr
}

func (c *serverConnection) ReceiveClient() (protocol.ClientMessage, error) {
	if err := c.ensurePreamble(); err != nil {
		return nil, c.preambleDecodeFailure(err)
	}
	envelope, err := c.raw.Recv()
	if err != nil {
		return nil, err
	}
	message, decodeErr := decodeClientEnvelopeWithin(envelope.Payload, c.ceilings.maxReceiveEnvelopeBytes)
	if decodeErr == nil {
		return message, nil
	}
	return nil, decodeErr
}

// preambleDecodeFailure maps a preamble refusal to the typed compatibility
// responses the daemon sends before any mutation. Only Hello carries a
// version the daemon can answer; other first-frame kinds are handled after
// a successful preamble by the normal decode path.
func (c *serverConnection) preambleDecodeFailure(err error) *protocol.DecodeFailure {
	return &protocol.DecodeFailure{
		Category: protocol.DecodeMalformed,
		Kind:     protocol.DecodeMessageHello,
		Version:  uint16(protocol.Version),
		Err:      err,
	}
}

func (c *serverConnection) SendServer(message protocol.ServerMessage) error {
	envelope, err := encodeProtoServer(message)
	if err != nil {
		return err
	}
	return c.sendEnvelope(envelope)
}

func (c *serverConnection) SendServerAsync(message protocol.ServerMessage) error {
	transport, ok := c.raw.(wire.AsyncTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	envelope, err := encodeProtoServer(message)
	if err != nil {
		return err
	}
	raw, err := sendEnvelopeWithin(envelope, c.ceilings)
	if err != nil {
		return err
	}
	return transport.SendAsync(raw)
}

func (c *serverConnection) SendServerSynchronous(message protocol.ServerMessage) error {
	transport, ok := c.raw.(wire.OwnedSynchronousTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	envelope, err := encodeProtoServer(message)
	if err != nil {
		return err
	}
	raw, err := sendEnvelopeWithin(envelope, c.ceilings)
	if err != nil {
		return err
	}
	return transport.SendSynchronous(raw)
}

func (c *serverConnection) SendOutput(output protocol.Output) error {
	envelope, err := encodeProtoServer(output)
	if err != nil {
		return err
	}
	return c.sendEnvelope(envelope)
}

func (c *serverConnection) SendOutputAsync(output protocol.Output) error {
	transport, ok := c.raw.(wire.AsyncTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	envelope, err := encodeProtoServer(output)
	if err != nil {
		return err
	}
	raw, err := sendEnvelopeWithin(envelope, c.ceilings)
	if err != nil {
		return err
	}
	return transport.SendAsync(raw)
}

func (c *serverConnection) SendOutputSynchronous(output protocol.Output) error {
	transport, ok := c.raw.(wire.OwnedSynchronousTransport)
	if !ok {
		return ErrUnsupportedSend
	}
	envelope, err := encodeProtoServer(output)
	if err != nil {
		return err
	}
	raw, err := sendEnvelopeWithin(envelope, c.ceilings)
	if err != nil {
		return err
	}
	return transport.SendSynchronous(raw)
}

func (c *serverConnection) sendEnvelope(envelope *wire.ServerEnvelope) error {
	raw, err := sendEnvelopeWithin(envelope, c.ceilings)
	if err != nil {
		return err
	}
	return c.raw.Send(raw)
}

func marshalServerEnvelope(envelope *wire.ServerEnvelope) (wire.Envelope, error) {
	raw, err := (proto.Marshal(envelope))
	if err != nil {
		return wire.Envelope{}, err
	}
	return wire.Envelope{Payload: raw}, nil
}

// DecodeClientEnvelope unwraps one scanned client envelope payload into its
// semantic message. App composition uses it for hidden one-shot carriages
// (remote preview) that validate a request before dialing the daemon.
func DecodeClientEnvelope(payload []byte) (protocol.ClientMessage, error) {
	return decodeClientEnvelope(payload)
}

// EncodePreambleResponseForTest marshals the canned server preamble
// acceptance exchanged before application traffic. Tests own their
// preamble fixtures; production always negotiates through the handshake.
func EncodePreambleResponseForTest() ([]byte, error) {
	return proto.Marshal(preambleResponseToWire(true, defaultProtoCeilings(), 0))
}

// EncodePreambleRequestForTest marshals the canned client preamble request
// exchanged before application traffic.
func EncodePreambleRequestForTest() ([]byte, error) {
	return proto.Marshal(preambleRequestToWire(defaultProtoCeilings()))
}

// DecodePreambleAcceptanceForTest reports whether a raw preamble response
// payload is a server acceptance.
func DecodePreambleAcceptanceForTest(payload []byte) bool {
	var response wire.PreambleResponse
	if serr := wire.ScanEnvelope(&response, payload); serr != nil {
		return false
	}
	if uerr := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, &response)); uerr != nil {
		return false
	}
	_, err := checkPreambleResponse(&response)
	return err == nil
}

// EncodeClientMessage wraps one semantic client message in its serialized
// directional envelope for hidden one-shot carriages.
func EncodeClientMessage(message protocol.ClientMessage) ([]byte, error) {
	envelope, err := encodeProtoClient(message)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(envelope)
}

// EncodeServerMessage wraps one semantic server message in its serialized
// directional envelope for hidden one-shot carriages.
func EncodeServerMessage(message protocol.ServerMessage) ([]byte, error) {
	envelope, err := encodeProtoServer(message)
	if err != nil {
		return nil, err
	}
	return proto.Marshal(envelope)
}

func decodeClientEnvelope(payload []byte) (protocol.ClientMessage, error) {
	return decodeClientEnvelopeWithin(payload, wire.AbsoluteEnvelopeLimit)
}

// decodeClientEnvelopeWithin unwraps one client envelope under the
// negotiated receive envelope ceiling. The one-shot exported decoder uses
// the absolute ceiling because no preamble was negotiated there.
func decodeClientEnvelopeWithin(payload []byte, envelopeLimit uint64) (protocol.ClientMessage, error) {
	envelope := &wire.ClientEnvelope{}
	if err := checkCategoryCeiling(payload, clientEnvelopeCategory(payload), envelopeLimit); err != nil {
		return nil, &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err, Kind: clientVariantKind(payload)}
	}
	if err := wire.ScanEnvelope(envelope, payload); err != nil {
		failure := &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err}
		if errors.Is(err, wire.ErrScanUnknown) {
			failure.Category = protocol.DecodeUnknownType
		}
		// Scan-level malformation (truncation, trailing data) still
		// carries the envelope variant in its leading tag. Recovering
		// the kind preserves first-frame compatibility routing: the
		// daemon answers malformed Command/Kill/Hello frames with typed
		// refusals instead of a generic hello error.
		failure.Kind = clientVariantKind(payload)
		return nil, failure
	}
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}.Unmarshal(payload, envelope)); err != nil {
		return nil, &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err}
	}
	message, err := decodeProtoClient(envelope)
	if err != nil {
		return nil, clientFailureFor(envelope, err)
	}
	return message, nil
}

// clientVariantKind recovers the envelope variant from the leading tag of
// a payload that failed strict scanning. Only the kinds with typed
// first-frame refusals (Hello/Command/Kill/RemotePreview/Navigation
// Inventory) are reported; anything else stays Unknown so the daemon
// answers with its generic hello error.
// ClientVariantKindForTest exposes scan-failure kind recovery to
// integration probes. Production uses it through decodeClientEnvelope.
func ClientVariantKindForTest(payload []byte) protocol.DecodeMessageKind {
	return clientVariantKind(payload)
}

func clientVariantKind(payload []byte) protocol.DecodeMessageKind {
	number, wireType, length := protowire.ConsumeTag(payload)
	if wireType != protowire.BytesType {
		return protocol.DecodeMessageUnknown
	}
	// ConsumeTag reports errCodeFieldNumber (-1..-4) for bad tags;
	// only accept a positively consumed tag.
	if length <= 0 {
		return protocol.DecodeMessageUnknown
	}
	switch number {
	case 1:
		return protocol.DecodeMessageHello
	case 7:
		return protocol.DecodeMessageKill
	case 12:
		return protocol.DecodeMessageCommand
	case 14:
		return protocol.DecodeMessageRemotePreview
	case 21:
		return protocol.DecodeMessageNavigationInventory
	default:
		return protocol.DecodeMessageUnknown
	}
}

// clientFailureFor preserves the daemon's first-frame compatibility routing:
// Hello/Command/Kill/RemotePreview/NavigationInventory/PickerControl kinds
// select the typed refusal a malformed first frame receives.
func clientFailureFor(envelope *wire.ClientEnvelope, err error) *protocol.DecodeFailure {
	failure := &protocol.DecodeFailure{Category: protocol.DecodeMalformed, Err: err}
	if errors.Is(err, ErrWrongDirection) {
		failure.Category = protocol.DecodeWrongDirection
		return failure
	}
	switch envelope.Payload.(type) {
	case *wire.ClientEnvelope_Hello:
		failure.Kind = protocol.DecodeMessageHello
		if payload, ok := envelope.Payload.(*wire.ClientEnvelope_Hello); ok && payload.Hello != nil {
			failure.Version = uint16(payload.Hello.GetVersion())
		}
	case *wire.ClientEnvelope_CommandRequest:
		failure.Kind = protocol.DecodeMessageCommand
		if payload, ok := envelope.Payload.(*wire.ClientEnvelope_CommandRequest); ok && payload.CommandRequest != nil {
			failure.Version = uint16(payload.CommandRequest.GetVersion())
			failure.RequestID = payload.CommandRequest.GetRequestId()
			failure.HasRequestID = true
		}
	case *wire.ClientEnvelope_Kill:
		failure.Kind = protocol.DecodeMessageKill
	case *wire.ClientEnvelope_RemotePreviewRequest:
		failure.Kind = protocol.DecodeMessageRemotePreview
	case *wire.ClientEnvelope_NavigationInventoryRequest:
		failure.Kind = protocol.DecodeMessageNavigationInventory
		if payload, ok := envelope.Payload.(*wire.ClientEnvelope_NavigationInventoryRequest); ok && payload.NavigationInventoryRequest != nil {
			failure.Version = uint16(payload.NavigationInventoryRequest.GetVersion())
			failure.RequestID = payload.NavigationInventoryRequest.GetRequestId()
			failure.HasRequestID = true
		}
	case *wire.ClientEnvelope_PickerControlRequest:
		if payload, ok := envelope.Payload.(*wire.ClientEnvelope_PickerControlRequest); ok && payload.PickerControlRequest != nil {
			failure.Version = uint16(payload.PickerControlRequest.GetVersion())
		}
	}
	return failure
}

func (c *serverConnection) Capabilities() protocol.ConnectionCapabilities {
	return rawCapabilities(c.raw, c.ceilings.outputDataLimit)
}

func (c *serverConnection) LinkState() ports.LinkState         { return rawLinkState(c.raw) }
func (c *serverConnection) LinkEvents() <-chan ports.LinkEvent { return rawLinkEvents(c.raw) }

func rawCapabilities(raw wire.Transport, outputDataLimit uint64) protocol.ConnectionCapabilities {
	_, datagram := raw.(wire.DatagramTransport)
	_, async := raw.(wire.AsyncTransport)
	_, synchronous := raw.(wire.OwnedSynchronousTransport)
	_, linkState := raw.(ports.LinkStateReporter)
	window := uint8(protocol.MaxOutputWindow)
	if datagram {
		window = 1
	}
	return protocol.ConnectionCapabilities{
		OutputDataLimit:       int(outputDataLimit),
		PreferredOutputWindow: window,
		AsyncSend:             async,
		OwnedSynchronousSend:  synchronous,
		LinkState:             linkState,
	}
}

func rawLinkState(raw wire.Transport) ports.LinkState {
	if reporter, ok := raw.(ports.LinkStateReporter); ok {
		return reporter.LinkState()
	}
	return ports.LinkStateConnected
}

func rawLinkEvents(raw wire.Transport) <-chan ports.LinkEvent {
	if reporter, ok := raw.(ports.LinkStateReporter); ok {
		return reporter.LinkEvents()
	}
	return nil
}

func (c *serverConnection) Close() error { return c.raw.Close() }

type serverListener struct{ raw wire.Listener }

var _ ports.ServerListener = (*serverListener)(nil)

// NewServerListener wraps every accepted raw connection in a stable typed adapter.
func NewServerListener(raw wire.Listener) ports.ServerListener {
	if raw == nil {
		return nil
	}
	return &serverListener{raw: raw}
}

func (l *serverListener) Accept() (ports.ServerConnection, error) {
	raw, err := l.raw.Accept()
	if err != nil {
		return nil, err
	}
	return NewServerConnection(raw), nil
}

func (l *serverListener) Close() error { return l.raw.Close() }
func (l *serverListener) Addr() string { return l.raw.Addr() }
