package daemonmux

import (
	"context"
	"sync"

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

// typedViews keeps one session codec per raw stream: the codec owns the
// preamble, so a second codec on the same stream would restart it.
var typedViews sync.Map // ports.BrokerEnvelopeStream -> typedStream

func asTyped(stream ports.BrokerEnvelopeStream) typedStream {
	if view, ok := typedViews.Load(stream); ok {
		return view.(typedStream)
	}
	view, _ := typedViews.LoadOrStore(stream, typedStream{ClientConnection: sessionwire.BrokerCodec{}.Client(stream), BrokerEnvelopeStream: stream})
	return view.(typedStream)
}

// typedLogical is a typed view of one concrete logical connection.
type typedLogical struct {
	typedStream
	*LogicalConnection
}

func (c typedLogical) Close() error { return c.typedStream.Close() }

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
