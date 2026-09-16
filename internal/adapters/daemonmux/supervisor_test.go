package daemonmux

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// connectRaw builds one broker PhysicalConnection over an already established
// raw carriage whose daemon end was handed to a supervisor.
func connectRaw(t *testing.T, raw RawFramedTransport, endpoint ports.BrokerResolvedEndpoint) ports.BrokerPhysicalConnection {
	t.Helper()
	connector, err := NewEndpointConnector(func(context.Context, string) (RawFramedTransport, error) {
		return raw, nil
	}, DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), endpoint)
	require.NoError(t, err)
	require.NotNil(t, physical)
	t.Cleanup(func() { _ = physical.Close() })
	return physical
}

// awaitHandshakeBridge performs the broker side of the daemonmux physical
// preamble over raw and returns the negotiated carrier, ready for a pump.
func awaitHandshakeBridge(t *testing.T, raw RawFramedTransport, endpoint ports.BrokerResolvedEndpoint) (FramedCarrier, ClientHandshakeResult) {
	t.Helper()
	bridge, err := NewPreambleCarrier(raw)
	require.NoError(t, err)
	result, err := RunClientHandshake(context.Background(), bridge, endpoint, DefaultMuxCeilings())
	require.NoError(t, err)
	require.NoError(t, bridge.Negotiate(result.Ceilings))
	return bridge, result
}

// TestNewServerSupervisorValidation proves the supervisor refuses a nil
// aggregate, an invalid binding, an invalid ceiling advertisement, and a nil
// carriage.
func TestNewServerSupervisorValidation(t *testing.T) {
	binding := mustServerBinding(t)

	_, err := NewServerSupervisor(nil, binding, DefaultMuxCeilings(), 0)
	require.ErrorIs(t, err, ErrSupervisorConfig)

	_, err = NewServerSupervisor(NewAggregateListener(), ServerBinding{}, DefaultMuxCeilings(), 0)
	require.ErrorIs(t, err, ErrSupervisorConfig)

	_, err = NewServerSupervisor(NewAggregateListener(), binding, MuxCeilings{}, 0)
	require.ErrorIs(t, err, ErrSupervisorConfig)

	supervisor, err := NewServerSupervisor(NewAggregateListener(), binding, DefaultMuxCeilings(), 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = supervisor.Close() })
	require.ErrorIs(t, supervisor.Adopt(context.Background(), nil), ErrSupervisorConfig)
}

// TestServerSupervisorFailedHandshakeRegistersNoChild proves a preamble that
// the binding refuses closes the carriage, admits no stream, and registers no
// owned child with the aggregate.
func TestServerSupervisorFailedHandshakeRegistersNoChild(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)

	clientRaw, serverRaw := rawCarriagePair(t)
	server.adopt(context.Background(), serverRaw)

	bridge, err := NewPreambleCarrier(clientRaw)
	require.NoError(t, err)
	t.Cleanup(func() { _ = bridge.Close() })
	_, err = RunClientHandshake(context.Background(), bridge, muxEndpoint(binding.Identity(), alternatePolicy(binding.Policy()), "raw://refused"), DefaultMuxCeilings())
	require.ErrorIs(t, err, ErrHandshakeRejected)

	require.ErrorIs(t, server.awaitAdoption(t), ErrHandshakeRejected)
	require.Zero(t, server.supervisor.Children(), "a refused handshake admits no physical child")
	require.Zero(t, server.acceptedCount(), "a refused handshake admits no stream")
}

// TestServerSupervisorEnforcesAcceptedPolicyBeforeAdmission proves the accepted
// physical policy is enforced on every inbound Open before fresh stream
// admission: a broker that completed the handshake at the accepted policy but
// names a different policy in an Open is refused on that stream alone, while a
// matching Open is admitted to the daemon's accept stream.
func TestServerSupervisorEnforcesAcceptedPolicyBeforeAdmission(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()
	endpoint := muxEndpoint(binding.Identity(), binding.Policy(), "raw://stream-policy")

	clientRaw, serverRaw := rawCarriagePair(t)
	server.adopt(context.Background(), serverRaw)
	carrier, result := awaitHandshakeBridge(t, clientRaw, endpoint)
	require.NoError(t, server.awaitAdoption(t))

	pump, err := NewPump(carrier, DirectionServer, result.Ceilings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	pump.Start(context.Background())
	logical, err := NewLogicalConnector(pump)
	require.NoError(t, err)

	_, err = logical.Open(context.Background(), muxOpenRequest(1, alternatePolicy(binding.Policy())))
	require.ErrorIs(t, err, ports.BrokerAdmissionInvalid)
	require.Zero(t, server.acceptedCount(), "a policy-mismatched open never reaches the accept stream")

	connection, err := logical.Open(context.Background(), muxOpenRequest(2, binding.Policy()))
	require.NoError(t, err)
	require.NotNil(t, connection)
	server.awaitAccepted(t, 1)
	require.NoError(t, connection.SendClient(protocol.Ping{}))
	message, err := connection.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
}

// TestServerSupervisorServerLossIsolated proves one lost physical child never
// stops the aggregate: a surviving child keeps accepting while the lost child
// is reaped and removed.
func TestServerSupervisorServerLossIsolated(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()
	endpoint := muxEndpoint(binding.Identity(), binding.Policy(), "raw://isolation")

	clientA, serverA := rawCarriagePair(t)
	server.adopt(context.Background(), serverA)
	physicalA := connectRaw(t, clientA, endpoint)
	require.NoError(t, server.awaitAdoption(t))

	clientB, serverB := rawCarriagePair(t)
	server.adopt(context.Background(), serverB)
	physicalB := connectRaw(t, clientB, endpoint)
	require.NoError(t, server.awaitAdoption(t))

	logicalA, err := physicalA.OpenStream(context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	logicalB, err := physicalB.OpenStream(context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	server.awaitAccepted(t, 2)
	require.Equal(t, 2, server.supervisor.Children())

	// Lose child A's carriage entirely.
	_ = serverA.Close()
	require.Eventually(t, func() bool { return channelClosed(physicalA.Done()) }, p3cTestDeadline, time.Millisecond,
		"the lost physical connection never reached its terminal outcome")
	require.Eventually(t, func() bool { return server.supervisor.Children() == 1 }, p3cTestDeadline, time.Millisecond,
		"the lost child was never reaped")

	// The surviving child still accepts and the aggregate never failed.
	logicalB2, err := physicalB.OpenStream(context.Background(), muxOpenRequest(2, binding.Policy()))
	require.NoError(t, err)
	require.NotNil(t, logicalB2)
	server.awaitAccepted(t, 3)

	// The lost physical refuses a fresh stream while its published logical
	// stream reports the physical loss.
	_, err = physicalA.OpenStream(context.Background(), muxOpenRequest(2, binding.Policy()))
	require.Error(t, err)
	require.True(t, channelClosed(logicalA.Done()))
	require.False(t, channelClosed(logicalB.Done()))
}

// TestServerSupervisorCloseTearsDownOwnedChildren proves Close removes and
// closes every owned child, refuses later carriage, and leaves the aggregate
// open.
func TestServerSupervisorCloseTearsDownOwnedChildren(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	endpoint := muxEndpoint(binding.Identity(), binding.Policy(), "raw://close")

	clientA, serverA := rawCarriagePair(t)
	server.adopt(context.Background(), serverA)
	physicalA := connectRaw(t, clientA, endpoint)
	require.NoError(t, server.awaitAdoption(t))

	clientB, serverB := rawCarriagePair(t)
	server.adopt(context.Background(), serverB)
	physicalB := connectRaw(t, clientB, endpoint)
	require.NoError(t, server.awaitAdoption(t))
	require.Equal(t, 2, server.supervisor.Children())

	require.NoError(t, server.supervisor.Close())
	require.Zero(t, server.supervisor.Children())
	require.Eventually(t, func() bool {
		return channelClosed(physicalA.Done()) && channelClosed(physicalB.Done())
	}, p3cTestDeadline, time.Millisecond, "Close did not tear down every owned carriage")

	// A closed supervisor refuses later carriage and closes it.
	lateClient, lateServer := rawCarriagePair(t)
	t.Cleanup(func() { _ = lateClient.Close() })
	require.ErrorIs(t, server.supervisor.Adopt(context.Background(), lateServer), ErrSupervisorClosed)

	// Close is idempotent.
	require.NoError(t, server.supervisor.Close())
}

// TestServerSupervisorBoundsConcurrentAdmissions proves admission is bounded: a
// second carriage waits while the first holds the only admission slot in its
// handshake, and proceeds once the first finishes.
func TestServerSupervisorBoundsConcurrentAdmissions(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 1, nil)
	endpoint := muxEndpoint(binding.Identity(), binding.Policy(), "raw://bounds")

	clientA, serverA := rawCarriagePair(t)
	server.adopt(context.Background(), serverA)
	require.Eventually(t, func() bool { return len(server.supervisor.slots) == 1 }, p3cTestDeadline, time.Millisecond,
		"the first admission never took the only slot")

	clientB, serverB := rawCarriagePair(t)
	admittedB := make(chan error, 1)
	go func() { admittedB <- server.supervisor.Adopt(context.Background(), serverB) }()
	select {
	case err := <-admittedB:
		t.Fatalf("the admission bound did not hold the second carriage: %v", err)
	case <-time.After(150 * time.Millisecond):
	}

	// Finish the first handshake; the slot is released and the second proceeds.
	bridgeA, _ := awaitHandshakeBridge(t, clientA, endpoint)
	t.Cleanup(func() { _ = bridgeA.Close() })
	require.NoError(t, server.awaitAdoption(t))

	bridgeB, _ := awaitHandshakeBridge(t, clientB, endpoint)
	t.Cleanup(func() { _ = bridgeB.Close() })
	select {
	case err := <-admittedB:
		require.NoError(t, err)
	case <-time.After(p3cTestDeadline):
		t.Fatal("the second carriage never proceeded after the first released its slot")
	}
	require.Equal(t, 2, server.supervisor.Children())
}
