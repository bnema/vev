package daemonmux

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/clock"
	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// p3cTestDeadline bounds every p3c integration wait. It is deliberately
// generous wall time: the only timing under test is a real carriage loss or an
// admission handshake, never a handshake budget.
const p3cTestDeadline = 5 * time.Second

// rawCarriagePair returns two independent real raw framed transports joined by
// an in-memory duplex pipe. Each end frames through the shared streamframe
// carriage via the IPC adapter, so the bridge, physical preamble, pump, and
// sessionwire all run over real framing rather than an in-memory envelope
// channel.
func rawCarriagePair(t *testing.T) (RawFramedTransport, RawFramedTransport) {
	t.Helper()
	client, server := net.Pipe()
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return rawFramedEnd(t, client), rawFramedEnd(t, server)
}

// rawFramedEnd adapts one net.Conn to the raw framed carriage the bridge and
// supervisor consume.
func rawFramedEnd(t *testing.T, conn net.Conn) RawFramedTransport {
	t.Helper()
	raw, ok := ipc.NewTransport(conn).(RawFramedTransport)
	require.True(t, ok, "ipc transport must satisfy the raw framed carriage")
	return raw
}

// alternatePolicy returns a valid policy that differs from base in exactly one
// field, so a mismatch is never a validation failure.
func alternatePolicy(base ports.BrokerPolicy) ports.BrokerPolicy {
	other := base
	other.Transport = base.Transport + "-alt"
	return other
}

// muxOpenRequest builds one valid local control-purpose open request under
// policy, so a client can open a stream without a remote registration.
func muxOpenRequest(stream uint64, policy ports.BrokerPolicy) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{
		Epoch:      1,
		Purpose:    ports.BrokerStreamControl,
		Local:      true,
		Connection: ports.BrokerConnectionID{1},
		Stream:     ports.BrokerStreamID(stream),
		Policy:     policy,
	}
}

// muxEndpoint builds the resolved endpoint the broker verifies against.
func muxEndpoint(identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy, address string) ports.BrokerResolvedEndpoint {
	return ports.BrokerResolvedEndpoint{Identity: identity, Policy: policy, Address: address}
}

// superviseServer is a test daemon: an AggregateListener plus a
// ServerSupervisor, with an optional accept loop that records and serves every
// typed connection the aggregate delivers. Its dial method produces the broker
// end of one real carriage and hands the daemon end to the supervisor.
type superviseServer struct {
	t          *testing.T
	aggregate  *AggregateListener
	supervisor *ServerSupervisor
	dials      atomic.Int64
	adoptions  chan error

	mu       sync.Mutex
	accepted []ports.ServerConnection
	serveOne sync.Once
}

func newSuperviseServer(t *testing.T, binding ServerBinding, ceilings MuxCeilings, limit int, timeSource ports.Clock) *superviseServer {
	t.Helper()
	if ceilings == (MuxCeilings{}) {
		ceilings = DefaultMuxCeilings()
	}
	if timeSource == nil {
		timeSource = clock.New()
	}
	aggregate := NewAggregateListener()
	supervisor, err := newServerSupervisor(aggregate, binding, ceilings, limit, timeSource)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = supervisor.Close()
		_ = aggregate.Close()
	})
	return &superviseServer{
		t:          t,
		aggregate:  aggregate,
		supervisor: supervisor,
		adoptions:  make(chan error, 16),
	}
}

// dial returns a RawCarrierDialer that produces one fresh real carriage per
// call and adopts its daemon end.
func (s *superviseServer) dial() RawCarrierDialer {
	return func(_ context.Context, _ string) (RawFramedTransport, error) {
		s.dials.Add(1)
		client, server := rawCarriagePair(s.t)
		go func() { s.adoptions <- s.supervisor.Adopt(context.Background(), server) }()
		return client, nil
	}
}

// adopt hands one already built carriage to the supervisor and reports the
// outcome through the shared adoption channel.
func (s *superviseServer) adopt(ctx context.Context, server RawFramedTransport) {
	go func() { s.adoptions <- s.supervisor.Adopt(ctx, server) }()
}

// awaitAdoption returns the next Adopt outcome.
func (s *superviseServer) awaitAdoption(t *testing.T) error {
	t.Helper()
	select {
	case err := <-s.adoptions:
		return err
	case <-time.After(p3cTestDeadline):
		t.Fatal("supervisor: Adopt did not return")
		return nil
	}
}

// serve starts the accept loop: every delivered typed connection is recorded
// and answered with Pong per Ping until its stream ends.
func (s *superviseServer) serve() {
	s.serveOne.Do(func() {
		go func() {
			for {
				connection, err := s.aggregate.Accept()
				if err != nil {
					return
				}
				s.mu.Lock()
				s.accepted = append(s.accepted, connection)
				s.mu.Unlock()
				go func() { _ = servePongs(connection) }()
			}
		}()
	})
}

// acceptedCount reports how many typed connections the aggregate delivered.
func (s *superviseServer) acceptedCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.accepted)
}

func (s *superviseServer) awaitAccepted(t *testing.T, count int) {
	t.Helper()
	require.Eventually(t, func() bool { return s.acceptedCount() >= count }, p3cTestDeadline, time.Millisecond,
		"server accepted %d of %d streams", s.acceptedCount(), count)
}

// TestNewEndpointConnectorValidation proves a connector refuses a nil dial
// function or an invalid ceiling advertisement, and Connect refuses an invalid
// resolved endpoint.
func TestNewEndpointConnectorValidation(t *testing.T) {
	_, err := NewEndpointConnector(nil, DefaultMuxCeilings())
	require.ErrorIs(t, err, ErrConnectorConfig)

	_, err = NewEndpointConnector(func(context.Context, string) (RawFramedTransport, error) { return nil, nil }, MuxCeilings{})
	require.ErrorIs(t, err, ErrConnectorConfig)

	connector, err := NewEndpointConnector(func(context.Context, string) (RawFramedTransport, error) {
		t.Fatal("dial must not run for an invalid endpoint")
		return nil, nil
	}, DefaultMuxCeilings())
	require.NoError(t, err)

	_, err = connector.Connect(context.Background(), ports.BrokerResolvedEndpoint{})
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorIncompatible, typed.Code)
}

// TestEndpointConnectorRefusesPolicyMismatch proves the resolved endpoint stays
// authoritative: a daemon that does not accept the endpoint policy is refused,
// no physical connection is published, and the daemon admits no child.
func TestEndpointConnectorRefusesPolicyMismatch(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), muxEndpoint(binding.Identity(), alternatePolicy(binding.Policy()), "raw://mismatch"))
	require.Error(t, err)
	require.Nil(t, physical)
	require.ErrorIs(t, err, ErrHandshakeRejected)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)

	require.ErrorIs(t, server.awaitAdoption(t), ErrHandshakeRejected)
	require.Zero(t, server.supervisor.Children(), "a refused handshake admits no physical child")
	require.Equal(t, int64(1), server.dials.Load())
}

// TestEndpointConnectorExposesAuthorityAndPolicyGuard proves one successful
// connection exposes the immutable accepted authority, delegates its terminal
// results to the pump, refuses a mismatched Open without touching the carriage,
// and admits a matching Open over the real carriage.
func TestEndpointConnectorExposesAuthorityAndPolicyGuard(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), muxEndpoint(binding.Identity(), binding.Policy(), "raw://authority"))
	require.NoError(t, err)
	require.NotNil(t, physical)
	t.Cleanup(func() { _ = physical.Close() })
	require.NoError(t, server.awaitAdoption(t))

	require.Equal(t, binding.Identity(), physical.Identity())
	require.Equal(t, binding.Incarnation(), physical.Incarnation())
	require.Equal(t, binding.Policy(), physical.Policy())
	require.False(t, channelClosed(physical.Done()))
	require.NoError(t, physical.Err())
	require.Equal(t, domain.RemoteFailureNone, physical.FailureKind())

	// A mismatched request policy is refused before the wire is touched.
	_, err = physical.OpenStream(context.Background(), muxOpenRequest(1, alternatePolicy(binding.Policy())))
	require.Error(t, err)
	var typed ports.BrokerError
	require.ErrorAs(t, err, &typed)
	require.Equal(t, ports.BrokerErrorConflictingPolicy, typed.Code)
	require.Zero(t, server.acceptedCount(), "a policy-mismatched open is never admitted")

	// The matching open is admitted and usable over the real carriage.
	logical, err := physical.OpenStream(context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	server.awaitAccepted(t, 1)
	require.NoError(t, logical.SendClient(protocol.Ping{}))
	message, err := logical.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)

	require.NoError(t, logical.Close())
	require.NoError(t, physical.Close())
	require.True(t, channelClosed(physical.Done()))
	require.NoError(t, physical.Err(), "an orderly local Close is not a failure")
	require.Equal(t, domain.RemoteFailureNone, physical.FailureKind())
}

// TestEndpointConnectorDetachesSetupContext proves a successful connection is
// not coupled to the caller's setup context: cancelling it after Connect
// returned does not stop the pooled physical connection.
func TestEndpointConnectorDetachesSetupContext(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	setupCtx, cancel := context.WithCancel(context.Background())
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), "raw://detach"))
	require.NoError(t, err)
	require.NoError(t, server.awaitAdoption(t))
	t.Cleanup(func() { _ = physical.Close() })

	cancel()
	// The setup context ended; the pooled connection must still work.
	require.False(t, channelClosed(physical.Done()))
	logical, err := physical.OpenStream(context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	server.awaitAccepted(t, 1)
	require.NoError(t, logical.SendClient(protocol.Ping{}))
	message, err := logical.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.NoError(t, logical.Close())
}

// TestPhysicalDoneBeforeLogicalLoss proves a carriage loss publishes the
// physical terminal outcome before the affected logical stream is terminalized:
// by the time the logical Done fires, the physical Done is already closed, and
// both carry the loss cause.
func TestPhysicalDoneBeforeLogicalLoss(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()

	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), muxEndpoint(binding.Identity(), binding.Policy(), "raw://loss"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })
	require.NoError(t, server.awaitAdoption(t))

	logical, err := physical.OpenStream(context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	server.awaitAccepted(t, 1)
	require.False(t, channelClosed(physical.Done()))

	ordered := make(chan bool, 1)
	go func() {
		select {
		case <-logical.Done():
		case <-time.After(p3cTestDeadline):
			ordered <- false
			return
		}
		select {
		case <-physical.Done():
			ordered <- true
		case <-time.After(p3cTestDeadline):
			ordered <- false
		}
	}()

	// Kill the daemon's carriage: the broker pump observes the carrier error.
	server.supervisor.mu.Lock()
	children := make([]*supervisedPhysical, 0, len(server.supervisor.children))
	for child := range server.supervisor.children {
		children = append(children, child)
	}
	server.supervisor.mu.Unlock()
	require.Len(t, children, 1)
	require.NoError(t, children[0].Close())

	require.True(t, <-ordered, "the logical Done fired before the physical Done")
	require.True(t, channelClosed(physical.Done()))
	require.Error(t, physical.Err())
	require.NotEqual(t, domain.RemoteFailureNone, physical.FailureKind())
	require.Error(t, logical.Err())
}

// TestEndpointConnectorStableIdentityDifferentIncarnationRestart proves a
// restart behind a stable authenticated identity surfaces a fresh incarnation:
// the identity and policy stay equal, the incarnation changes, and each
// connection reports its own captured authority.
func TestEndpointConnectorStableIdentityDifferentIncarnationRestart(t *testing.T) {
	first, err := NewServerBinding(testIdentity(), testIncarnation(), testPolicy())
	require.NoError(t, err)
	second, err := NewServerBinding(testIdentity(), ports.BrokerDaemonIncarnation{9, 8, 7, 6, 5, 4, 3, 2, 1, 0, 1, 2, 3, 4, 5, 6}, testPolicy())
	require.NoError(t, err)
	require.Equal(t, first.Identity(), second.Identity())
	require.NotEqual(t, first.Incarnation(), second.Incarnation())

	serverA := newSuperviseServer(t, first, DefaultMuxCeilings(), 0, nil)
	serverB := newSuperviseServer(t, second, DefaultMuxCeilings(), 0, nil)

	connectorA, err := NewEndpointConnector(serverA.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	physicalA, err := connectorA.Connect(context.Background(), muxEndpoint(first.Identity(), first.Policy(), "raw://restart-a"))
	require.NoError(t, err)
	require.NoError(t, serverA.awaitAdoption(t))
	require.Equal(t, first.Incarnation(), physicalA.Incarnation())
	require.NoError(t, physicalA.Close())

	connectorB, err := NewEndpointConnector(serverB.dial(), DefaultMuxCeilings())
	require.NoError(t, err)
	physicalB, err := connectorB.Connect(context.Background(), muxEndpoint(second.Identity(), second.Policy(), "raw://restart-b"))
	require.NoError(t, err)
	require.NoError(t, serverB.awaitAdoption(t))
	t.Cleanup(func() { _ = physicalB.Close() })

	require.Equal(t, physicalA.Identity(), physicalB.Identity(), "the authenticated identity is stable across a restart")
	require.Equal(t, physicalA.Policy(), physicalB.Policy())
	require.NotEqual(t, physicalA.Incarnation(), physicalB.Incarnation(), "a restart carries a fresh incarnation")
}
