package sessionwire

import (
	"io"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// BrokerCodec translates a raw broker envelope stream into the typed session
// protocol for the broker's own observation and preview operations.
// Attachment traffic stays opaque and never passes through this codec.
type BrokerCodec struct{}

var _ ports.SessionCodec = BrokerCodec{}

// Client returns the typed client view of stream, or nil for a nil stream.
func (BrokerCodec) Client(stream ports.BrokerEnvelopeStream) ports.ClientConnection {
	if stream == nil {
		return nil
	}
	return NewClientConnection(brokerEnvelopeTransport{stream: stream})
}

// brokerEnvelopeTransport carries complete envelopes over a broker stream.
type brokerEnvelopeTransport struct{ stream ports.BrokerEnvelopeStream }

var _ wire.Transport = brokerEnvelopeTransport{}

func (t brokerEnvelopeTransport) Send(e wire.Envelope) error { return t.stream.SendEnvelope(e.Payload) }

func (t brokerEnvelopeTransport) Recv() (wire.Envelope, error) {
	data, err := t.stream.RecvEnvelope()
	if err != nil {
		return wire.Envelope{}, err
	}
	if data == nil {
		return wire.Envelope{}, io.EOF
	}
	return wire.Envelope{Payload: data}, nil
}

func (t brokerEnvelopeTransport) Close() error { return t.stream.Close() }
