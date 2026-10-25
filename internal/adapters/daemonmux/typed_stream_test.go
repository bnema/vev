package daemonmux

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
)

// asTyped forwards through the real session codec while exposing the raw
// stream's lifecycle through the generated logical-connection mock.
func asTyped(t *testing.T, stream ports.BrokerEnvelopeStream) *portsmocks.MockBrokerLogicalConnection {
	t.Helper()
	typed := sessionwire.BrokerCodec{}.Client(stream)
	conn := portsmocks.NewMockBrokerLogicalConnection(t)
	conn.EXPECT().SendClient(mock.Anything).RunAndReturn(typed.SendClient).Maybe()
	conn.EXPECT().ReceiveServer().RunAndReturn(typed.ReceiveServer).Maybe()
	conn.EXPECT().Capabilities().RunAndReturn(typed.Capabilities).Maybe()
	conn.EXPECT().LinkState().RunAndReturn(typed.LinkState).Maybe()
	conn.EXPECT().LinkEvents().RunAndReturn(typed.LinkEvents).Maybe()
	conn.EXPECT().Close().RunAndReturn(typed.Close).Maybe()
	conn.EXPECT().Done().RunAndReturn(stream.Done).Maybe()
	conn.EXPECT().Err().RunAndReturn(stream.Err).Maybe()
	return conn
}

func newTypedLogical(t *testing.T, connection *LogicalConnection) *portsmocks.MockBrokerLogicalConnection {
	t.Helper()
	return asTyped(t, connection)
}

type envelopeOpener interface {
	OpenStream(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error)
}

func openTyped(t *testing.T, opener envelopeOpener, ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	t.Helper()
	stream, err := opener.OpenStream(ctx, request)
	if err != nil {
		return nil, err
	}
	return asTyped(t, stream), nil
}
