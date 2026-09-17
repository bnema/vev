package brokeripc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

// awaitDone waits for one broker service to become terminal.
func awaitDone(t *testing.T, service ports.BrokerService) {
	t.Helper()
	select {
	case <-service.Done():
	case <-time.After(5 * time.Second):
		t.Fatal("broker service did not become terminal")
	}
}

// TestConnectorConnectReturnsIndependentService proves the ports.BrokerConnector
// implementation dials one connection per Connect and hands ownership to the
// caller.
func TestConnectorConnectReturnsIndependentService(t *testing.T) {
	e := startEndpoint(t, Config{})
	connector := NewConnector(e.path, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	first, err := connector.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, first)

	second, err := connector.Connect(ctx)
	require.NoError(t, err)
	require.NotNil(t, second)
	require.NotEqual(t, first.ConnectionID(), second.ConnectionID(), "each Connect admits an independent connection")

	// An orderly local Close reports no terminal cause and is stable.
	require.NoError(t, first.Close())
	awaitDone(t, first)
	require.NoError(t, first.Err())
	require.NoError(t, first.Err(), "Err is stable after Done")
	require.False(t, func() bool {
		select {
		case <-second.Done():
			return true
		default:
			return false
		}
	}(), "closing one connection must not close another")

	require.NoError(t, second.Close())
	awaitDone(t, second)
}

// TestConnectorReportsCarriageLoss proves the client service surfaces a broker
// loss through Done and Err while it holds no logical stream.
func TestConnectorReportsCarriageLoss(t *testing.T) {
	e := startEndpoint(t, Config{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	service, err := NewConnector(e.path, Config{}).Connect(ctx)
	require.NoError(t, err)

	// The broker side is torn down: the client observes a non-nil terminal
	// cause instead of a silent close.
	require.NoError(t, e.accept().Close())
	awaitDone(t, service)
	require.Error(t, service.Err())

	require.NoError(t, service.Close())
	require.Error(t, service.Err(), "the first terminal cause is never overwritten by a local Close")
}

func TestConnectorRefusesInvalidConfiguration(t *testing.T) {
	var absent *Connector
	_, err := absent.Connect(context.Background())
	require.ErrorIs(t, err, ErrConfig)

	_, err = NewConnector("", Config{}).Connect(context.Background())
	require.ErrorIs(t, err, ErrConfig)
}

// TestConnectorCancellationInterruptsStalledRegistration proves the
// ports.BrokerConnector seam honors cancellation while a peer stalls in the
// registration exchange: Connect returns the caller's cancellation instead of
// waiting out the handshake budget. The supervisor's cancellation lifetime
// depends on exactly this contract.
func TestConnectorCancellationInterruptsStalledRegistration(t *testing.T) {
	path := testSocketPath(t)
	peer := startSilentBrokerPeer(t, path)

	connector := NewConnector(path, Config{HandshakeTimeout: 30 * time.Second})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		_, err := connector.Connect(ctx)
		result <- err
	}()
	peer.awaitPreamble(t)
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled, "Connect must return the caller's cancellation, not wait out the handshake budget")
	case <-time.After(5 * time.Second):
		t.Fatal("Connect must return on cancellation instead of waiting out HandshakeTimeout")
	}
	peer.stop()
}

// TestConnectorServiceOutlivesSetupContext pins the ports.BrokerConnector
// contract that a successful service is independent of the setup context:
// cancelling that context after Connect returns must not close or disturb the
// established connection.
func TestConnectorServiceOutlivesSetupContext(t *testing.T) {
	e := startEndpoint(t, Config{})
	ctx, cancel := context.WithCancel(context.Background())
	service, err := NewConnector(e.path, Config{}).Connect(ctx)
	require.NoError(t, err)
	cancel()

	select {
	case <-service.Done():
		t.Fatal("cancelling the setup context must not close the established service")
	case <-time.After(100 * time.Millisecond):
	}

	// The detached connection still serves its own scope and can be closed by
	// its owner, which reports an orderly end.
	require.NotEqual(t, ports.BrokerConnectionID{}, service.ConnectionID())
	require.NoError(t, service.Close())
	awaitDone(t, service)
	require.NoError(t, service.Err())
}
