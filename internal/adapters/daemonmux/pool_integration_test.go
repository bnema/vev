package daemonmux

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/broker"
)

// p3cResolver is a test BrokerRouteAuthority: it returns one authenticated
// endpoint per request, keyed on the request's registration endpoint so two
// compatible aliases resolve to the same (identity, policy) physical key with
// distinct adapter routes.
type p3cResolver func(context.Context, ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error)

func (f p3cResolver) ResolveDialTarget(ctx context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
	return f(ctx, request)
}

type p3cBinder struct{}

func (p3cBinder) BindAuthenticatedIdentity(_ context.Context, request ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
	return request.Identity, nil
}

// p3cRemoteRequest builds one valid remote control-purpose request.
func p3cRemoteRequest(connection ports.BrokerConnectionID, endpoint string, stream uint64, policy ports.BrokerPolicy) ports.BrokerOpenStreamRequest {
	registration := domain.RemoteRegistration{
		Endpoint:    endpoint,
		Incarnation: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		Generation:  1,
	}
	return ports.BrokerOpenStreamRequest{
		Epoch:        1,
		Purpose:      ports.BrokerStreamControl,
		Local:        false,
		Connection:   connection,
		Stream:       ports.BrokerStreamID(stream),
		Endpoint:     endpoint,
		Registration: registration,
		Policy:       policy,
		StartMode:    ports.BrokerDaemonStartIfNeeded,
	}
}

// p3cPoolLimits bounds one pool with room for two aliases and two streams.
func p3cPoolLimits() broker.PoolLimits {
	return broker.PoolLimits{Physical: 2, Clients: 2, Streams: 4, StreamsPerClient: 4, Warm: 2, Idle: time.Hour}
}

// TestPoolSharesCompatibleAliasesOverOnePhysical proves the pool pools two
// compatible aliases over exactly one daemonmux physical connection: only one
// carriage is dialed, and both logical streams flow over it.
func TestPoolSharesCompatibleAliasesOverOnePhysical(t *testing.T) {
	binding := mustRemoteServerBinding(t)
	policy := binding.Policy()
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()
	resolver := p3cResolver(func(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return muxEndpoint(binding.Identity(), policy, "raw://"+request.Endpoint), nil
	})

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	pool, err := broker.NewPool(1, resolver, p3cBinder{}, connector, newListenerClock(time.Now()), p3cPoolLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	client, err := pool.RegisterClient()
	require.NoError(t, err)

	first, err := pool.OpenStream(context.Background(), p3cRemoteRequest(client, "aliasA@host:22", 1, policy))
	require.NoError(t, err)
	require.NotNil(t, first)
	t.Cleanup(func() { _ = first.Close() })

	second, err := pool.OpenStream(context.Background(), p3cRemoteRequest(client, "aliasB@host:22", 2, policy))
	require.NoError(t, err)
	require.NotNil(t, second)
	t.Cleanup(func() { _ = second.Close() })

	require.Equal(t, int64(1), server.dials.Load(), "compatible aliases must share one physical carriage")
	server.awaitAccepted(t, 2)

	for _, stream := range []ports.BrokerLogicalConnection{first, second} {
		require.NoError(t, stream.SendClient(protocol.Ping{}))
		message, err := stream.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, protocol.Pong{}, message)
	}
}

// TestPoolRefusesConflictingPolicyWithoutDial proves the pool rejects a request
// whose policy conflicts with the resolved endpoint policy before any carriage
// is dialed.
func TestPoolRefusesConflictingPolicyWithoutDial(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	resolver := p3cResolver(func(_ context.Context, _ ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return muxEndpoint(binding.Identity(), binding.Policy(), "raw://conflict"), nil
	})

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	pool, err := broker.NewPool(1, resolver, p3cBinder{}, connector, newListenerClock(time.Now()), p3cPoolLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	client, err := pool.RegisterClient()
	require.NoError(t, err)
	_, err = pool.OpenStream(context.Background(), p3cRemoteRequest(client, "aliasA@host:22", 1, alternatePolicy(binding.Policy())))
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)
	require.Zero(t, server.dials.Load(), "a conflicting policy never dials a carriage")
}

// TestPoolSurfacesPhysicalLossPerStream proves a carriage loss is published
// per affected logical stream through the pool: the pooled stream terminates
// with a broker stream-lost error once the accepted physical carriage dies.
func TestPoolSurfacesPhysicalLossPerStream(t *testing.T) {
	binding := mustRemoteServerBinding(t)
	policy := binding.Policy()
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()
	resolver := p3cResolver(func(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerDialTarget, error) {
		return muxEndpoint(binding.Identity(), policy, "raw://"+request.Endpoint), nil
	})

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	pool, err := broker.NewPool(1, resolver, p3cBinder{}, connector, newListenerClock(time.Now()), p3cPoolLimits())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pool.Close() })

	client, err := pool.RegisterClient()
	require.NoError(t, err)
	stream, err := pool.OpenStream(context.Background(), p3cRemoteRequest(client, "aliasA@host:22", 1, policy))
	require.NoError(t, err)
	server.awaitAccepted(t, 1)

	// Lose the daemon carriage: the supervised child is closed, so the broker
	// pump publishes the physical loss and the pool settles the stream.
	server.supervisor.mu.Lock()
	children := make([]*supervisedPhysical, 0, len(server.supervisor.children))
	for child := range server.supervisor.children {
		children = append(children, child)
	}
	server.supervisor.mu.Unlock()
	require.Len(t, children, 1)
	require.NoError(t, children[0].Close())

	select {
	case <-stream.Done():
	case <-time.After(p3cTestDeadline):
		t.Fatal("the pooled logical stream never observed the physical loss")
	}
	var lost ports.BrokerStreamLost
	require.ErrorAs(t, stream.Err(), &lost)
	require.Equal(t, ports.BrokerStreamID(1), lost.Stream)
	require.NotEqual(t, domain.RemoteFailureNone, lost.Cause)
}
