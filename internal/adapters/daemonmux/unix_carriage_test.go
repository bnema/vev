// Integration coverage for the real private Unix daemonmux carriage (P3.2d).
//
// These tests exercise the daemonmux multiplexer over an actual AF_UNIX socket
// bound by ipc.ListenMux and dialed by ipc.DialMuxContext, not over the
// in-memory carrier used by the unit tests. The physical connection is the one
// real private mux carriage; every logical stream rides it. The full daemon
// path (EndpointConnector -> ServerSupervisor -> AggregateListener -> typed
// Listener) is exercised for the two-then-hundred admissions test, and the raw
// pump path is exercised for the blocked-consumer and reset-isolation checks so
// the stream accounting is asserted directly.
package daemonmux

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// muxCarriagePath returns an owner-only temporary directory and one mux.sock
// path inside it. The parent is created 0700 (os.MkdirTemp's default) and the
// path stays well inside the cross-platform AF_UNIX limit.
func muxCarriagePath(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "m")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return filepath.Join(root, "mux.sock")
}

// unixRawCarriagePair binds a real private mux carriage at a temporary path and
// returns its two ends as raw framed transports, joined by an actual AF_UNIX
// SOCK_STREAM connection.
func unixRawCarriagePair(t *testing.T) (RawFramedTransport, RawFramedTransport) {
	t.Helper()
	path := muxCarriagePath(t)
	listener, err := ipc.ListenMux(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	type acceptOutcome struct {
		raw RawFramedTransport
		err error
	}
	accepted := make(chan acceptOutcome, 1)
	go func() {
		raw, acceptErr := listener.Accept()
		if acceptErr != nil {
			accepted <- acceptOutcome{err: acceptErr}
			return
		}
		accepted <- acceptOutcome{raw: raw}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), p3cTestDeadline)
	defer cancel()
	dialed, err := ipc.DialMuxContext(ctx, path)
	require.NoError(t, err)

	outcome := <-accepted
	require.NoError(t, outcome.err)

	client, server := dialed, outcome.raw
	t.Cleanup(func() {
		_ = client.Close()
		_ = server.Close()
	})
	return client, server
}

// unixDialer returns a RawCarrierDialer that establishes the real mux carriage
// at path through ipc.DialMuxContext (which also verifies same-user peer
// credentials).
func unixDialer(path string) RawCarrierDialer {
	return func(ctx context.Context, _ string) (RawFramedTransport, error) {
		raw, err := ipc.DialMuxContext(ctx, path)
		if err != nil {
			return nil, err
		}
		return raw, nil
	}
}

// serveUnix adopts every accepted real mux carriage into the test supervisor
// until the listener closes.
func (s *superviseServer) serveUnix(t *testing.T, listener ipc.MuxListener) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			raw, err := listener.Accept()
			if err != nil {
				return
			}
			s.adopt(ctx, raw)
		}
	}()
}

// soleChildPump returns the pump of the supervisor's one owned physical child,
// so a test can assert the accepted stream accounting of the real carriage.
func (s *superviseServer) soleChildPump(t *testing.T) *Pump {
	t.Helper()
	s.supervisor.mu.Lock()
	defer s.supervisor.mu.Unlock()
	require.Len(t, s.supervisor.children, 1, "expected exactly one physical mux child")
	for child := range s.supervisor.children {
		return child.pump
	}
	return nil
}

// TestUnixCarriageSupervisorTwoThenHundredTypedAdmissions drives the full
// daemon path over one real Unix physical connection: EndpointConnector dials
// the private carriage, ServerSupervisor adopts it, the AggregateListener
// delivers independent typed daemon admissions, and every stream's traffic is
// isolated. Only one physical connection and one physical child exist for all
// 102 streams.
func TestUnixCarriageSupervisorTwoThenHundredTypedAdmissions(t *testing.T) {
	binding := mustServerBinding(t)
	policy := binding.Policy()
	path := muxCarriagePath(t)

	listener, err := ipc.ListenMux(path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })

	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()
	server.serveUnix(t, listener)

	connector, err := NewEndpointConnector(unixDialer(path), DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), muxEndpoint(binding.Identity(), policy, path))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })
	require.NoError(t, server.awaitAdoption(t))

	// Two independent typed admissions over the one physical carriage.
	first, err := physical.OpenStream(context.Background(), muxOpenRequest(1, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	second, err := physical.OpenStream(context.Background(), muxOpenRequest(2, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	server.awaitAccepted(t, 2)
	for _, stream := range []ports.BrokerLogicalConnection{first, second} {
		require.NoError(t, stream.SendClient(protocol.Ping{}))
		message, err := stream.ReceiveServer()
		require.NoError(t, err)
		require.Equal(t, protocol.Pong{}, message)
	}

	// One hundred more independently admitted typed streams, opened
	// concurrently over the same physical carriage.
	const extra = 100
	connections := make([]ports.BrokerLogicalConnection, extra)
	openErrs := make([]error, extra)
	var wg sync.WaitGroup
	for i := 0; i < extra; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			connections[i], openErrs[i] = physical.OpenStream(context.Background(), muxOpenRequest(uint64(3+i), policy))
		}(i)
	}
	wg.Wait()
	for i, openErr := range openErrs {
		require.NoError(t, openErr, "stream %d", i+3)
	}
	for i, connection := range connections {
		require.NotNil(t, connection, "stream %d", i+3)
		t.Cleanup(func() { _ = connection.Close() })
	}

	server.awaitAccepted(t, 2+extra)
	for i, connection := range connections {
		require.NoError(t, connection.SendClient(protocol.Ping{}), "stream %d", i+3)
		message, err := connection.ReceiveServer()
		require.NoError(t, err, "stream %d", i+3)
		require.Equal(t, protocol.Pong{}, message)
	}

	// All 102 streams share exactly one physical child and one physical
	// connection, and the daemon engine accounts for every admitted stream.
	require.Equal(t, 1, server.supervisor.Children(), "all streams must share one physical Unix connection")
	pump := server.soleChildPump(t)
	require.Eventually(t, func() bool { return pump.Engine().Live() == 2+extra }, p3cTestDeadline, time.Millisecond,
		"daemon engine admitted %d of %d streams", pump.Engine().Live(), 2+extra)

	all := make([]ports.BrokerLogicalConnection, 0, 2+extra)
	all = append(all, first, second)
	all = append(all, connections...)
	for _, connection := range all {
		require.NoError(t, connection.Close())
	}
	require.Eventually(t, func() bool { return pump.Engine().Live() == 0 }, p3cTestDeadline, time.Millisecond,
		"daemon engine still holds %d live streams after close", pump.Engine().Live())
	require.False(t, channelClosed(physical.Done()), "closing every stream must leave the physical carriage open")
}

// unixDaemonPump builds a daemon-side pump over the server end of a real mux
// carriage and returns the raw client end for direct frame injection, so a test
// can drive deterministic inbound traffic over the actual socket.
func unixDaemonPump(t *testing.T) (*Pump, RawFramedTransport) {
	t.Helper()
	client, server := unixRawCarriagePair(t)
	carrier, err := NewPreambleCarrier(server)
	require.NoError(t, err)
	require.NoError(t, carrier.Negotiate(DefaultMuxCeilings()))
	pump, err := NewPump(carrier, DirectionClient, DefaultMuxCeilings())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	pump.Start(context.Background())
	return pump, client
}

// injectUnixClientFrame writes one client frame over the real Unix carriage.
func injectUnixClientFrame(t *testing.T, client RawFramedTransport, message ClientMessage) {
	t.Helper()
	require.NoError(t, client.Send(wire.Envelope{Payload: mustEncodeClient(t, message)}))
}

// nextUnixServerFrame reads one daemon frame from the real Unix carriage.
func nextUnixServerFrame(t *testing.T, client RawFramedTransport) []byte {
	t.Helper()
	envelope, err := client.RecvBounded(testEnvelopeCeiling)
	require.NoError(t, err)
	return envelope.Payload
}

// TestUnixCarriageBlockedConsumerSiblingProgress proves, over a real Unix
// carriage, that a logical consumer which never drains one stream's inbound
// queue neither blocks the reader nor a sibling: the stalled stream is reset by
// its own bound while the sibling keeps receiving.
func TestUnixCarriageBlockedConsumerSiblingProgress(t *testing.T) {
	pump, client := unixDaemonPump(t)

	const siblings = 2
	for i := 1; i <= siblings; i++ {
		injectUnixClientFrame(t, client, openFor(PhysicalStreamID(i)))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == siblings })
	for i := 1; i <= siblings; i++ {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(PhysicalStreamID(i))}))
	}

	// The consumer never Takes stream 1: its bounded queue fills.
	for i := 0; i < MaxMuxStreamQueueChunks; i++ {
		injectUnixClientFrame(t, client, Data{Physical: 1, Data: []byte{byte(i)}})
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.QueuedChunks == MaxMuxStreamQueueChunks
	})

	// One more chunk overflows stream 1's own bound; a sibling chunk still
	// lands, so neither the reader nor the sibling was blocked.
	injectUnixClientFrame(t, client, Data{Physical: 1, Data: []byte{'x'}})
	injectUnixClientFrame(t, client, Data{Physical: 2, Data: []byte("sibling")})

	taken, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), taken)

	stalled := mustStatus(t, pump.Engine(), 1)
	require.Equal(t, StreamTerminal, stalled.State)
	require.ErrorIs(t, stalled.Err, ErrStreamQueueFull)
	require.Equal(t, domain.RemoteFailureTransport, stalled.FailureKind)
	require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))

	reset := decodeReset(t, nextUnixServerFrame(t, client), DirectionServer)
	require.Equal(t, PhysicalStreamID(1), reset.Physical)
	require.Equal(t, domain.RemoteFailureTransport, reset.Error.FailureKind)
}

// TestUnixCarriageResetIsolation proves, over a real Unix carriage, that one
// stream-local refusal schedules exactly one Reset for its own stream and
// leaves the physical connection and every sibling healthy.
func TestUnixCarriageResetIsolation(t *testing.T) {
	pump, client := unixDaemonPump(t)

	for _, id := range []PhysicalStreamID{1, 2, 3} {
		injectUnixClientFrame(t, client, openFor(id))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == 3 })
	for _, id := range []PhysicalStreamID{2, 3} {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
	}

	// Stream 1 is still opening: inbound data is a stream-local state refusal.
	injectUnixClientFrame(t, client, Data{Physical: 1, Data: []byte("early")})
	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.State == StreamTerminal
	})
	settled := mustStatus(t, pump.Engine(), 1)
	require.ErrorContains(t, settled.Err, "invalid stream state")
	require.Equal(t, domain.RemoteFailureInvalidResponse, settled.FailureKind)

	injectUnixClientFrame(t, client, Data{Physical: 2, Data: []byte("sibling")})
	taken, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), taken)

	reset := decodeReset(t, nextUnixServerFrame(t, client), DirectionServer)
	require.Equal(t, PhysicalStreamID(1), reset.Physical)
	require.True(t, reset.HasError)
	require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}
