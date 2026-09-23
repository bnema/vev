package daemonmux

import (
	"context"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
)

// typedStream adapts one raw mux stream to the typed session port the way the
// client side does, while keeping the raw stream's lifecycle observable.
type typedStream struct {
	ports.ClientConnection
	ports.BrokerEnvelopeStream
}

var _ ports.BrokerLogicalConnection = typedStream{}

func (s typedStream) Close() error { return s.ClientConnection.Close() }

// asTyped builds the one typed view of stream. The codec owns the session
// preamble, so each raw stream must get exactly one view.
func asTyped(stream ports.BrokerEnvelopeStream) typedStream {
	return typedStream{ClientConnection: sessionwire.BrokerCodec{}.Client(stream), BrokerEnvelopeStream: stream}
}

// typedLogical is a typed view of one concrete logical connection.
type typedLogical struct {
	typedStream
	*LogicalConnection
}

func (c typedLogical) Close() error { return c.typedStream.Close() }

func newTypedLogical(connection *LogicalConnection) typedLogical {
	return typedLogical{typedStream: asTyped(connection), LogicalConnection: connection}
}

type envelopeOpener interface {
	OpenStream(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error)
}

// openTyped opens one raw stream and returns its typed view; nil on failure.
func openTyped(opener envelopeOpener, ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	stream, err := opener.OpenStream(ctx, request)
	if err != nil {
		return nil, err
	}
	return asTyped(stream), nil
}
