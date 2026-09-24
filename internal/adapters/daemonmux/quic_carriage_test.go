// Integration coverage for the real private QUIC daemonmux carriage (P3.2e).
//
// These tests exercise the daemonmux multiplexer over an actual loopback QUIC
// connection authenticated by quic.NewServer's ephemeral certificate plus
// one-time token/nonce and dialed by quic.DialMuxContext, not over the
// in-memory carrier used by the unit tests or the Unix socket of P3.2d. One
// QUIC connection is one physical daemonmux connection: every logical stream
// rides its single bidirectional stream, and no logical attachment is ever
// mapped to a native QUIC stream.
//
// The full daemon path (EndpointConnector -> ServerSupervisor ->
// AggregateListener -> typed Listener) is exercised for the two-then-hundred
// admissions and physical-failure tests, and the raw pump path is exercised for
// the blocked-consumer and reset-isolation checks so the stream accounting is
// asserted directly.
package daemonmux

import (
	"context"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// quicCarriageDeadline bounds every real loopback QUIC mux carriage wait. QUIC
// pays a TLS handshake and a pinned dial per physical carriage, so the parity
// waits stay deliberately generous.
const quicCarriageDeadline = 10 * time.Second

// quicCarriageEndpoint mints one ephemeral bootstrap endpoint and returns it
// with its readiness record and the dial address composed from loopback plus
// the readiness port. The one-time token and nonce stay in the readiness
// record: daemonmux only ever sees the host-independent address.
func quicCarriageEndpoint(t *testing.T) (*quic.Server, quic.Readiness, string) {
	t.Helper()
	server, readiness, err := quic.NewServer()
	require.NoError(t, err)
	t.Cleanup(func() { _ = server.Close() })
	return server, readiness, net.JoinHostPort("127.0.0.1", strconv.Itoa(readiness.Port))
}

// quicDialer returns a RawCarrierDialer that establishes the real QUIC mux
// carriage through quic.DialMuxContext - exact ephemeral certificate pin plus
// single-use token/nonce - and hands the raw bounded transport to the connector
// unmodified.
func quicDialer(readiness quic.Readiness) RawCarrierDialer {
	return func(ctx context.Context, target ports.BrokerDialTarget) (RawFramedTransport, error) {
		raw, err := quic.DialMuxContext(ctx, target.Address, readiness, quic.Config{}, quicCarriageDeadline)
		if err != nil {
			return nil, err
		}
		return raw, nil
	}
}

// quicDialCarriage establishes the broker end of one real QUIC mux carriage.
func quicDialCarriage(t *testing.T, ctx context.Context, addr string, readiness quic.Readiness) RawFramedTransport {
	t.Helper()
	raw, err := quicDialer(readiness)(ctx, ports.BrokerDialTarget{Address: addr})
	require.NoError(t, err)
	t.Cleanup(func() { _ = raw.Close() })
	return raw
}

// serveQUIC adopts every carriage the bootstrap endpoint admits into the test
// supervisor until the endpoint closes. There is exactly one: the credential is
// one-time.
func (s *superviseServer) serveQUIC(t *testing.T, server *quic.Server) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() {
		for {
			raw, err := server.AcceptMux(ctx)
			if err != nil {
				return
			}
			s.adopt(ctx, raw)
		}
	}()
}

// serveQUICCapturing adopts the bootstrap endpoint's one authenticated carriage
// and publishes the accepted daemon carriage, so a test can fail that physical
// connection deterministically.
func (s *superviseServer) serveQUICCapturing(t *testing.T, server *quic.Server) <-chan RawFramedTransport {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	captured := make(chan RawFramedTransport, 1)
	go func() {
		raw, err := server.AcceptMux(ctx)
		if err != nil {
			return
		}
		captured <- raw
		s.adopt(ctx, raw)
	}()
	return captured
}

// awaitQUICAccepted waits for the aggregate to deliver count typed admissions.
func (s *superviseServer) awaitQUICAccepted(t *testing.T, count int) {
	t.Helper()
	require.Eventually(t, func() bool { return s.acceptedCount() >= count }, quicCarriageDeadline, time.Millisecond,
		"server accepted %d of %d streams", s.acceptedCount(), count)
}

// quicDaemonPump builds a daemon-side pump over the daemon end of a real QUIC
// mux carriage and returns the raw broker end for direct frame injection, so a
// test can drive deterministic inbound traffic over the actual QUIC stream.
func quicDaemonPump(t *testing.T) (*Pump, RawFramedTransport) {
	t.Helper()
	server, readiness, addr := quicCarriageEndpoint(t)
	ctx, cancel := context.WithTimeout(context.Background(), quicCarriageDeadline)
	t.Cleanup(cancel)

	type acceptOutcome struct {
		raw RawFramedTransport
		err error
	}
	accepted := make(chan acceptOutcome, 1)
	go func() {
		raw, err := server.AcceptMux(ctx)
		accepted <- acceptOutcome{raw: raw, err: err}
	}()

	client := quicDialCarriage(t, ctx, addr, readiness)

	var daemon RawFramedTransport
	select {
	case outcome := <-accepted:
		require.NoError(t, outcome.err)
		require.NotNil(t, outcome.raw)
		daemon = outcome.raw
		t.Cleanup(func() { _ = daemon.Close() })
	case <-time.After(quicCarriageDeadline):
		t.Fatal("AcceptMux did not return")
	}

	carrier, err := NewPreambleCarrier(daemon)
	require.NoError(t, err)
	require.NoError(t, carrier.Negotiate(DefaultMuxCeilings()))
	pump, err := NewPump(carrier, DirectionClient, DefaultMuxCeilings())
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	pump.Start(context.Background())
	return pump, client
}

// injectQUICClientFrame writes one client frame over the real QUIC carriage.
func injectQUICClientFrame(t *testing.T, client RawFramedTransport, message ClientMessage) {
	t.Helper()
	require.NoError(t, client.Send(wire.Envelope{Payload: mustEncodeClient(t, message)}))
}

// nextQUICServerFrame reads one daemon frame from the real QUIC carriage.
func nextQUICServerFrame(t *testing.T, client RawFramedTransport) []byte {
	t.Helper()
	envelope, err := client.RecvBounded(testEnvelopeCeiling)
	require.NoError(t, err)
	return envelope.Payload
}

// TestQUICCarriageSupervisorTwoThenHundredTypedAdmissions drives the full daemon
// path over one real QUIC physical connection: EndpointConnector dials the
// authenticated carriage, ServerSupervisor adopts it, the AggregateListener
// delivers independent typed daemon admissions, and every stream's traffic is
// isolated. Only one physical connection, one physical child, and one QUIC
// stream exist for all 102 streams.
func TestQUICCarriageSupervisorTwoThenHundredTypedAdmissions(t *testing.T) {
	binding := mustServerBinding(t)
	policy := binding.Policy()
	server, readiness, addr := quicCarriageEndpoint(t)

	daemon := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	daemon.serve()
	daemon.serveQUIC(t, server)

	connector, err := NewEndpointConnector(quicDialer(readiness), DefaultMuxCeilings())
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), muxEndpoint(binding.Identity(), policy, addr))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physical.Close() })
	require.NoError(t, daemon.awaitAdoption(t))

	// Two independent typed admissions over the one physical carriage.
	first, err := openTyped(physical, context.Background(), muxOpenRequest(1, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = first.Close() })
	second, err := openTyped(physical, context.Background(), muxOpenRequest(2, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = second.Close() })
	daemon.awaitQUICAccepted(t, 2)
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
			connections[i], openErrs[i] = openTyped(physical, context.Background(), muxOpenRequest(uint64(3+i), policy))
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

	daemon.awaitQUICAccepted(t, 2+extra)
	for i, connection := range connections {
		require.NoError(t, connection.SendClient(protocol.Ping{}), "stream %d", i+3)
		message, err := connection.ReceiveServer()
		require.NoError(t, err, "stream %d", i+3)
		require.Equal(t, protocol.Pong{}, message)
	}

	// All 102 streams share exactly one physical child and one physical QUIC
	// connection, and the daemon engine accounts for every admitted stream.
	require.Equal(t, 1, daemon.supervisor.Children(), "all streams must share one physical QUIC connection")
	pump := daemon.soleChildPump(t)
	require.Eventually(t, func() bool { return pump.Engine().Live() == 2+extra }, quicCarriageDeadline, time.Millisecond,
		"daemon engine admitted %d of %d streams", pump.Engine().Live(), 2+extra)

	all := make([]ports.BrokerLogicalConnection, 0, 2+extra)
	all = append(all, first, second)
	all = append(all, connections...)
	for _, connection := range all {
		require.NoError(t, connection.Close())
	}
	require.Eventually(t, func() bool { return pump.Engine().Live() == 0 }, quicCarriageDeadline, time.Millisecond,
		"daemon engine still holds %d live streams after close", pump.Engine().Live())
	require.False(t, channelClosed(physical.Done()), "closing every stream must leave the physical carriage open")
}

// TestQUICCarriageBlockedConsumerSiblingProgress proves, over a real QUIC
// carriage, that a logical consumer which never drains one stream's inbound
// queue neither blocks the reader nor a sibling: the stalled stream is reset by
// its own bound while the sibling keeps receiving.
func TestQUICCarriageBlockedConsumerSiblingProgress(t *testing.T) {
	pump, client := quicDaemonPump(t)

	const siblings = 2
	for i := 1; i <= siblings; i++ {
		injectQUICClientFrame(t, client, openFor(PhysicalStreamID(i)))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == siblings })
	for i := 1; i <= siblings; i++ {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(PhysicalStreamID(i))}))
	}

	// A peer ignores flow control and fills stream 1's whole window while the
	// consumer never Takes it.
	chunk, full := floodWindow(pump.Ceilings())
	for i := 0; i < full; i++ {
		injectQUICClientFrame(t, client, Data{Physical: 1, Data: chunk})
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.QueuedChunks == full
	})

	// One more chunk exceeds the granted window; a sibling chunk still lands,
	// so neither the reader nor the sibling was blocked.
	injectQUICClientFrame(t, client, Data{Physical: 1, Data: chunk})
	injectQUICClientFrame(t, client, Data{Physical: 2, Data: []byte("sibling")})

	taken, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), taken)

	stalled := mustStatus(t, pump.Engine(), 1)
	require.Equal(t, StreamTerminal, stalled.State)
	require.ErrorIs(t, stalled.Err, ErrStreamQueueFull)
	require.Equal(t, domain.RemoteFailureInvalidResponse, stalled.FailureKind)
	require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))

	reset := decodeReset(t, nextQUICServerFrame(t, client), DirectionServer)
	require.Equal(t, PhysicalStreamID(1), reset.Physical)
	require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)
}

// TestQUICCarriageResetIsolation proves, over a real QUIC carriage, that one
// stream-local refusal schedules exactly one Reset for its own stream and
// leaves the physical connection and every sibling healthy.
func TestQUICCarriageResetIsolation(t *testing.T) {
	pump, client := quicDaemonPump(t)

	for _, id := range []PhysicalStreamID{1, 2, 3} {
		injectQUICClientFrame(t, client, openFor(id))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == 3 })
	for _, id := range []PhysicalStreamID{2, 3} {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
	}

	// Stream 1 is still opening: inbound data is a stream-local state refusal.
	injectQUICClientFrame(t, client, Data{Physical: 1, Data: []byte("early")})
	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.State == StreamTerminal
	})
	settled := mustStatus(t, pump.Engine(), 1)
	require.ErrorContains(t, settled.Err, "invalid stream state")
	require.Equal(t, domain.RemoteFailureInvalidResponse, settled.FailureKind)

	injectQUICClientFrame(t, client, Data{Physical: 2, Data: []byte("sibling")})
	taken, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), taken)

	reset := decodeReset(t, nextQUICServerFrame(t, client), DirectionServer)
	require.Equal(t, PhysicalStreamID(1), reset.Physical)
	require.True(t, reset.HasError)
	require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}

// TestQUICCarriagePhysicalFailureFansOutAndIsIsolated proves a lost QUIC
// physical carriage publishes its terminal outcome before fanning out to every
// logical stream it carried, while a sibling physical QUIC connection on the
// same supervisor is untouched and a fresh stream on the lost physical is
// refused.
func TestQUICCarriagePhysicalFailureFansOutAndIsIsolated(t *testing.T) {
	binding := mustServerBinding(t)
	policy := binding.Policy()
	serverA, readinessA, addrA := quicCarriageEndpoint(t)
	serverB, readinessB, addrB := quicCarriageEndpoint(t)

	daemon := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	daemon.serve()
	carriageA := daemon.serveQUICCapturing(t, serverA)
	daemon.serveQUIC(t, serverB)

	connectorA, err := NewEndpointConnector(quicDialer(readinessA), DefaultMuxCeilings())
	require.NoError(t, err)
	physicalA, err := connectorA.Connect(context.Background(), muxEndpoint(binding.Identity(), policy, addrA))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physicalA.Close() })

	connectorB, err := NewEndpointConnector(quicDialer(readinessB), DefaultMuxCeilings())
	require.NoError(t, err)
	physicalB, err := connectorB.Connect(context.Background(), muxEndpoint(binding.Identity(), policy, addrB))
	require.NoError(t, err)
	t.Cleanup(func() { _ = physicalB.Close() })

	require.NoError(t, daemon.awaitAdoption(t))
	require.NoError(t, daemon.awaitAdoption(t))
	require.Equal(t, 2, daemon.supervisor.Children())

	lost := make([]ports.BrokerLogicalConnection, 3)
	for i := range lost {
		lost[i], err = openTyped(physicalA, context.Background(), muxOpenRequest(uint64(i+1), policy))
		require.NoError(t, err, "stream %d", i+1)
		t.Cleanup(func() { _ = lost[i].Close() })
	}
	survivor, err := openTyped(physicalB, context.Background(), muxOpenRequest(1, policy))
	require.NoError(t, err)
	t.Cleanup(func() { _ = survivor.Close() })
	daemon.awaitQUICAccepted(t, 4)

	// The physical terminal outcome is ordered before its logical fan-out.
	ordered := make(chan bool, 1)
	go func() {
		select {
		case <-lost[0].Done():
		case <-time.After(quicCarriageDeadline):
			ordered <- false
			return
		}
		select {
		case <-physicalA.Done():
			ordered <- true
		case <-time.After(quicCarriageDeadline):
			ordered <- false
		}
	}()

	// Lose the daemon end of physical A's QUIC carriage entirely.
	rawA := <-carriageA
	require.NoError(t, rawA.Close())

	require.True(t, <-ordered, "the logical Done fired before the physical Done")
	require.True(t, channelClosed(physicalA.Done()))
	require.Error(t, physicalA.Err())
	require.NotEqual(t, domain.RemoteFailureNone, physicalA.FailureKind())
	// The physical outcome fans out to every logical stream it carried. Each
	// stream publishes on its own watcher goroutine, so the fan-out is
	// asynchronous even though it can never precede the physical Done.
	for i, stream := range lost {
		require.Eventually(t, func() bool {
			return channelClosed(stream.Done()) && stream.Err() != nil
		}, quicCarriageDeadline, time.Millisecond, "lost stream %d never received the physical failure", i+1)
	}

	// The sibling physical QUIC connection is unaffected.
	require.False(t, channelClosed(physicalB.Done()))
	require.False(t, channelClosed(survivor.Done()))
	require.NoError(t, survivor.SendClient(protocol.Ping{}))
	message, err := survivor.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.Eventually(t, func() bool { return daemon.supervisor.Children() == 1 }, quicCarriageDeadline, time.Millisecond,
		"the lost QUIC physical child was never reaped")

	// The lost physical refuses a fresh stream.
	_, err = openTyped(physicalA, context.Background(), muxOpenRequest(9, policy))
	require.Error(t, err)
}

// TestQUICCarriageSetupContextDetachedFromPhysicalLifetime proves the setup
// context covers only setup: once EndpointConnector.Connect returned over a real
// QUIC carriage, cancelling the setup context never stops the pooled physical
// connection, which Close alone owns.
func TestQUICCarriageSetupContextDetachedFromPhysicalLifetime(t *testing.T) {
	binding := mustServerBinding(t)
	server, readiness, addr := quicCarriageEndpoint(t)

	daemon := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	daemon.serve()
	daemon.serveQUIC(t, server)

	connector, err := NewEndpointConnector(quicDialer(readiness), DefaultMuxCeilings())
	require.NoError(t, err)
	setupCtx, cancel := context.WithCancel(context.Background())
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), addr))
	require.NoError(t, err)
	require.NoError(t, daemon.awaitAdoption(t))
	t.Cleanup(func() { _ = physical.Close() })

	cancel()
	require.False(t, channelClosed(physical.Done()))
	logical, err := openTyped(physical, context.Background(), muxOpenRequest(1, binding.Policy()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = logical.Close() })
	daemon.awaitQUICAccepted(t, 1)
	require.NoError(t, logical.SendClient(protocol.Ping{}))
	message, err := logical.ReceiveServer()
	require.NoError(t, err)
	require.Equal(t, protocol.Pong{}, message)
	require.False(t, channelClosed(physical.Done()))
}

// TestQUICCarriageCanceledSetupAdmitsNothing proves one setup context covers
// bootstrap, dial, auth, and the mux handshake: a canceled setup admits no
// physical connection, no child, and no stream.
func TestQUICCarriageCanceledSetupAdmitsNothing(t *testing.T) {
	binding := mustServerBinding(t)
	server, readiness, addr := quicCarriageEndpoint(t)

	daemon := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	daemon.serveQUIC(t, server)

	connector, err := NewEndpointConnector(quicDialer(readiness), DefaultMuxCeilings())
	require.NoError(t, err)
	setupCtx, cancel := context.WithCancel(context.Background())
	cancel()
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), addr))
	require.ErrorIs(t, err, context.Canceled)
	require.Nil(t, physical)
	require.Zero(t, daemon.supervisor.Children())
	require.Zero(t, daemon.acceptedCount())
}

// TestQUICCarriageSetupDeadlineBoundsTheMuxHandshake proves the setup deadline
// covers the daemonmux physical handshake over a real QUIC carriage: a daemon
// that never answers the preamble fails the whole Connect within the setup
// deadline and publishes no physical connection.
func TestQUICCarriageSetupDeadlineBoundsTheMuxHandshake(t *testing.T) {
	binding := mustServerBinding(t)
	_, readiness, addr := quicCarriageEndpoint(t)

	connector, err := NewEndpointConnector(quicDialer(readiness), DefaultMuxCeilings())
	require.NoError(t, err)
	setupCtx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	physical, err := connector.Connect(setupCtx, muxEndpoint(binding.Identity(), binding.Policy(), addr))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Nil(t, physical)
}
