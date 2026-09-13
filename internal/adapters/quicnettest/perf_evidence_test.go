// This file holds the QUIC performance-measurement harness and its bounded
// evidence test. It is deliberately an external test package: it drives the
// real internal/adapters/quic transport through the adverse-network proxy and a
// real sessionwire conversation, exactly like quic_integration_test.go, while
// keeping the simulator package itself free of production dependencies.
//
// The harness measures one representative typed exchange, "Input -> valid
// Output -> Ack", under two profiles:
//
//   - "clean": the proxy forwards with no impairment,
//   - "train": the deterministic 4G/train profile ([trainProfile]).
//
// It reports a reproducible evidence line per profile: per-exchange latency
// percentiles (p50/p95/p99/max), process CPU per exchange (getrusage
// RUSAGE_SELF), allocations and bytes per exchange, GC count, retained heap,
// goroutine count, proxy-carried bytes per exchange, and recovery latency after
// a blackout and after a NAT rebinding.
//
// The test asserts only structural invariants (every exchange round-trips
// byte-for-byte, percentiles are monotone, the proxy accounting settles, and
// recovery stays inside a generous liveness ceiling). It deliberately makes no
// latency-threshold assertion, so it does not become flaky on a loaded machine.
// Higher-sample measurements live in quic_performance_benchmark_test.go.
package quicnettest_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"net"
	"runtime"
	"sort"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	quicadapter "github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/adapters/quicnettest"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// Bounds keep the evidence test cheap enough to run under -race while still
// exercising the adverse paths. Callers that want a tighter distribution should
// use the benchmarks with -benchtime.
const (
	perfExchangeTimeout = 25 * time.Second
	perfRecoveryCeiling = 8 * time.Second
	perfBlackoutWindow  = 100 * time.Millisecond
	perfBlackoutSamples = 2
	perfRebindSamples   = 2
	perfWarmupExchanges = 1

	perfInputPayload  = 512
	perfOutputPayload = 512
	// perfCleanExchanges and perfTrainExchanges cap the measured transcript.
	// The train profile pays roughly 3 impaired link legs per exchange; the
	// clean profile is near-free, so it takes the larger sample for a more
	// meaningful p99. Both keep the bounded test well under a minute -race.
	perfCleanExchanges = 100
	perfTrainExchanges = 30
)

// perfProfile pairs a link profile with the bounded measured transcript size.
type perfProfile struct {
	name      string
	link      quicnettest.LinkConfig
	exchanges int
}

// perfProfiles returns the two representative profiles.
func perfProfiles() []perfProfile {
	return []perfProfile{
		{name: "clean", link: perfCleanProfile(), exchanges: perfCleanExchanges},
		{name: "train", link: trainProfile(), exchanges: perfTrainExchanges},
	}
}

// perfCleanProfile is the unimpaired reference link: the proxy still carries
// every packet, but adds no latency, loss, duplication, or reordering.
func perfCleanProfile() quicnettest.LinkConfig { return quicnettest.LinkConfig{} }

// perfFixture is a real QUIC listener behind a [quicnettest.Proxy] plus the
// atomically accumulated proxy carriage counters used for bytes/work.
type perfFixture struct {
	proxy       *quicnettest.Proxy
	listener    *quicadapter.Listener
	fingerprint []byte

	packets      atomic.Uint64
	wireToServer atomic.Uint64
	wireToClient atomic.Uint64
}

// wireBytes is the total datagram bytes the proxy ingested in both directions.
func (f *perfFixture) wireBytes() uint64 {
	return f.wireToServer.Load() + f.wireToClient.Load()
}

// startPerfFixture starts the real QUIC listener and proxy, with a fixed seed so
// the train profile's impairment decisions are reproducible.
func startPerfFixture(tb testing.TB, link quicnettest.LinkConfig) *perfFixture {
	tb.Helper()
	cert, fingerprint, err := quicadapter.GenerateEphemeralCert()
	if err != nil {
		tb.Fatalf("generate cert: %v", err)
	}
	listener, err := quicadapter.ListenConfig("127.0.0.1:0", cert, quicadapter.Config{}, 4)
	if err != nil {
		tb.Fatalf("listen quic: %v", err)
	}
	tb.Cleanup(func() { _ = listener.Close() })

	serverAddr, err := net.ResolveUDPAddr("udp", listener.Addr())
	if err != nil {
		tb.Fatalf("resolve listener address %q: %v", listener.Addr(), err)
	}
	fixture := &perfFixture{listener: listener, fingerprint: fingerprint}
	proxy, err := quicnettest.New(quicnettest.Config{
		ServerAddr:    serverAddr,
		Seed:          20260913,
		QueueCapacity: 2048,
		ToServer:      link,
		ToClient:      link,
		// The sink only accumulates atomics, so it never blocks a reader.
		OnPacket: func(event quicnettest.Event) {
			fixture.packets.Add(1)
			if event.Direction == quicnettest.ToServer {
				fixture.wireToServer.Add(uint64(event.Bytes))
				return
			}
			fixture.wireToClient.Add(uint64(event.Bytes))
		},
	})
	if err != nil {
		tb.Fatalf("new proxy: %v", err)
	}
	tb.Cleanup(func() { _ = proxy.Close() })
	fixture.proxy = proxy
	return fixture
}

// perfServerReceive carries one typed server-side receive result.
type perfServerReceive struct {
	message protocol.ClientMessage
	err     error
}

// startPerfConversation dials the real QUIC carriage through the proxy and
// completes the typed sessionwire preamble (Hello/Welcome).
func startPerfConversation(tb testing.TB, fx *perfFixture) (ports.ClientConnection, ports.ServerConnection) {
	tb.Helper()
	accepted := make(chan struct {
		transport wire.Transport
		err       error
	}, 1)
	go func() {
		transport, err := fx.listener.Accept()
		accepted <- struct {
			transport wire.Transport
			err       error
		}{transport: transport, err: err}
	}()

	dialer := quicadapter.DialConfig(fx.proxy.Addr().String(), "vev-bootstrap", fx.fingerprint, quicadapter.Config{}, 15*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), perfExchangeTimeout)
	defer cancel()
	clientRaw, err := dialer.Dial(ctx)
	if err != nil {
		tb.Fatalf("dial through proxy: %v", err)
	}
	tb.Cleanup(func() { _ = clientRaw.Close() })

	var serverRaw wire.Transport
	select {
	case result := <-accepted:
		if result.err != nil {
			tb.Fatalf("accept: %v", result.err)
		}
		serverRaw = result.transport
	case <-time.After(perfExchangeTimeout):
		tb.Fatalf("timed out waiting for the QUIC listener to accept")
	}
	tb.Cleanup(func() { _ = serverRaw.Close() })

	client := sessionwire.NewClientConnection(clientRaw)
	server := sessionwire.NewServerConnection(serverRaw)
	tb.Cleanup(func() { _ = client.Close() })
	tb.Cleanup(func() { _ = server.Close() })

	hello := protocol.Hello{
		Version:  protocol.Version,
		ClientID: [16]byte{0x70, 0x65, 0x72, 0x66, 0x2d, 0x71, 0x75, 0x69},
		Name:     "perf-quic",
		Size:     domain.Size{Cols: 80, Rows: 24},
		TermEnv:  "xterm-256color",
	}
	got, err := perfSendClientMessage(client, server, hello)
	if err != nil {
		tb.Fatalf("handshake hello: %v", err)
	}
	if gotHello, ok := got.(protocol.Hello); !ok || gotHello.Name != hello.Name || gotHello.Version != hello.Version {
		tb.Fatalf("server received %#v, want %#v", got, hello)
	}
	reply, err := perfSendServerMessage(client, server, protocol.Welcome{SessionID: "perf-quic", SessionName: "perf", Capabilities: 1})
	if err != nil {
		tb.Fatalf("handshake welcome: %v", err)
	}
	welcome, ok := reply.(protocol.Welcome)
	if !ok || welcome.SessionID != "perf-quic" {
		tb.Fatalf("client reply is %#v, want Welcome{SessionID: perf-quic}", reply)
	}
	return client, server
}

// perfSendClientMessage sends one typed client message while the server accepts
// it, and returns the message the server observed.
func perfSendClientMessage(client ports.ClientConnection, server ports.ServerConnection, message protocol.ClientMessage) (protocol.ClientMessage, error) {
	received := make(chan perfServerReceive, 1)
	go func() {
		got, err := server.ReceiveClient()
		received <- perfServerReceive{message: got, err: err}
	}()
	if err := client.SendClient(message); err != nil {
		return nil, fmt.Errorf("send %T: %w", message, err)
	}
	select {
	case result := <-received:
		if result.err != nil {
			return nil, fmt.Errorf("receive after %T: %w", message, result.err)
		}
		return result.message, nil
	case <-time.After(perfExchangeTimeout):
		return nil, fmt.Errorf("timed out receiving after %T", message)
	}
}

// perfSendServerMessage sends one typed server message while the client accepts
// it, and returns the message the client observed.
func perfSendServerMessage(client ports.ClientConnection, server ports.ServerConnection, message protocol.ServerMessage) (protocol.ServerMessage, error) {
	sent := make(chan error, 1)
	go func() { sent <- server.SendServer(message) }()
	reply, err := client.ReceiveServer()
	if err != nil {
		return nil, fmt.Errorf("receive %T: %w", message, err)
	}
	if err := <-sent; err != nil {
		return nil, fmt.Errorf("send %T: %w", message, err)
	}
	return reply, nil
}

// perfExchange runs one representative typed exchange: the client sends an
// Input, the server answers with a semantically valid Output, and the client
// acknowledges it. It returns the wall-clock round-trip duration of the whole
// exchange and any structural error, so it is safe to call from a goroutine.
func perfExchange(client ports.ClientConnection, server ports.ServerConnection, seq uint64) (time.Duration, error) {
	start := time.Now()
	data := perfPayload(seq, perfInputPayload)

	receivedInput, err := perfSendClientMessage(client, server, protocol.Input{InputSeq: seq, ActionID: seq, Data: data})
	if err != nil {
		return 0, err
	}
	input, ok := receivedInput.(protocol.Input)
	if !ok {
		return 0, fmt.Errorf("server received %T, want protocol.Input", receivedInput)
	}
	if input.InputSeq != seq || input.ActionID != seq {
		return 0, fmt.Errorf("server input seq/action = %d/%d, want %d/%d", input.InputSeq, input.ActionID, seq, seq)
	}
	if !bytes.Equal(input.Data, data) {
		return 0, fmt.Errorf("server input %d payload differs from the sent bytes", seq)
	}

	reply, err := perfSendServerMessage(client, server, perfOutput(seq))
	if err != nil {
		return 0, err
	}
	output, ok := reply.(protocol.Output)
	if !ok {
		return 0, fmt.Errorf("client received %T, want protocol.Output", reply)
	}
	if err := protocol.ValidateOutput(output); err != nil {
		return 0, fmt.Errorf("client output is invalid: %w", err)
	}

	ack := protocol.Ack{Epoch: output.Epoch, State: output.New}
	receivedAck, err := perfSendClientMessage(client, server, ack)
	if err != nil {
		return 0, err
	}
	gotAck, ok := receivedAck.(protocol.Ack)
	if !ok {
		return 0, fmt.Errorf("server received %T, want protocol.Ack", receivedAck)
	}
	if gotAck != ack {
		return 0, fmt.Errorf("server ack = %#v, want %#v", gotAck, ack)
	}
	return time.Since(start), nil
}

// perfOutput builds a semantically valid full-screen Output carrying a payload,
// so the reverse direction exercises real bytes rather than an empty heartbeat.
func perfOutput(seq uint64) protocol.Output {
	return protocol.Output{
		Epoch:   1,
		Base:    0,
		New:     seq,
		Full:    true,
		Size:    domain.Size{Cols: 80, Rows: 24},
		Context: perfViewContext(),
		Data:    perfPayload(seq, perfOutputPayload),
	}
}

func perfViewContext() *protocol.ViewContext {
	return &protocol.ViewContext{
		Publication: 3,
		Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{
			LifecycleID: domain.SessionLifecycleID{1},
			SessionName: "work",
		}},
		TabID:         "tab-1",
		FocusedPaneID: "pane-1",
	}
}

func perfPayload(seq uint64, size int) []byte {
	payload := make([]byte, size)
	binary.BigEndian.PutUint64(payload, seq)
	for i := 8; i < len(payload); i++ {
		payload[i] = byte(seq)
	}
	return payload
}

// perfLatency is a nearest-rank latency summary over measured exchanges.
type perfLatency struct {
	Count              int
	P50, P95, P99, Max time.Duration
}

func perfLatencyFor(samples []time.Duration) perfLatency {
	if len(samples) == 0 {
		return perfLatency{}
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	return perfLatency{
		Count: len(sorted),
		P50:   perfRank(sorted, 0.50),
		P95:   perfRank(sorted, 0.95),
		P99:   perfRank(sorted, 0.99),
		Max:   sorted[len(sorted)-1],
	}
}

// perfRank returns the nearest-rank quantile of a sorted sample.
func perfRank(sorted []time.Duration, quantile float64) time.Duration {
	index := int(math.Ceil(quantile*float64(len(sorted)))) - 1
	if index < 0 {
		index = 0
	}
	if index >= len(sorted) {
		index = len(sorted) - 1
	}
	return sorted[index]
}

// perfCPUSample is a process CPU snapshot from getrusage(RUSAGE_SELF).
//
// getrusage reports process-wide CPU, so the CPU-per-exchange figure is stable
// when the package is run in isolation (for example with -run or a benchmark
// filter) rather than alongside other tests.
type perfCPUSample struct {
	user time.Duration
	sys  time.Duration
}

func (s perfCPUSample) total() time.Duration { return s.user + s.sys }

func readPerfCPU() perfCPUSample {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return perfCPUSample{}
	}
	return perfCPUSample{
		user: time.Duration(usage.Utime.Sec)*time.Second + time.Duration(usage.Utime.Usec)*time.Microsecond,
		sys:  time.Duration(usage.Stime.Sec)*time.Second + time.Duration(usage.Stime.Usec)*time.Microsecond,
	}
}

// perfMemSample captures the runtime allocation counters used for
// allocations/work, GC count, retained heap, and goroutines.
type perfMemSample struct {
	totalAlloc uint64
	mallocs    uint64
	heapAlloc  uint64
	numGC      uint32
	goroutines int
}

func readPerfMem() perfMemSample {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return perfMemSample{
		totalAlloc: stats.TotalAlloc,
		mallocs:    stats.Mallocs,
		heapAlloc:  stats.HeapAlloc,
		numGC:      stats.NumGC,
		goroutines: runtime.NumGoroutine(),
	}
}

// perfBlackoutRecoveryOnce runs one recovery sample: an exchange is put in
// flight, the proxy drops every packet for perfBlackoutWindow, and the returned
// duration is measured from the moment the blackout clears until that exchange
// completes.
func perfBlackoutRecoveryOnce(fx *perfFixture, client ports.ClientConnection, server ports.ServerConnection, seq uint64) (time.Duration, error) {
	if _, err := perfExchange(client, server, seq); err != nil {
		return 0, fmt.Errorf("pre-blackout baseline: %w", err)
	}
	fx.proxy.SetBlackout(true)
	result := make(chan error, 1)
	go func() {
		_, err := perfExchange(client, server, seq+1_000_000)
		result <- err
	}()
	time.Sleep(perfBlackoutWindow)
	start := time.Now()
	fx.proxy.SetBlackout(false)
	select {
	case err := <-result:
		if err != nil {
			return 0, fmt.Errorf("in-flight exchange after blackout: %w", err)
		}
		return time.Since(start), nil
	case <-time.After(perfRecoveryCeiling):
		return 0, fmt.Errorf("blackout recovery exceeded %s", perfRecoveryCeiling)
	}
}

// perfRebindRecoveryOnce runs one recovery sample: the proxy changes its
// upstream source address, and the returned duration is measured from the
// rebinding until the next exchange completes.
func perfRebindRecoveryOnce(fx *perfFixture, client ports.ClientConnection, server ports.ServerConnection, seq uint64) (time.Duration, error) {
	if _, err := perfExchange(client, server, seq); err != nil {
		return 0, fmt.Errorf("pre-rebind baseline: %w", err)
	}
	if err := fx.proxy.Rebind(); err != nil {
		return 0, fmt.Errorf("rebind: %w", err)
	}
	start := time.Now()
	if _, err := perfExchange(client, server, seq+2_000_000); err != nil {
		return 0, fmt.Errorf("exchange after rebind: %w", err)
	}
	return time.Since(start), nil
}

// perfWaitAccounting polls the proxy counters until every accepted packet is
// either sent or dropped, then returns the settled snapshot.
func perfWaitAccounting(tb testing.TB, fx *perfFixture, timeout time.Duration) quicnettest.Stats {
	tb.Helper()
	deadline := time.Now().Add(timeout)
	for {
		stats := fx.proxy.Stats()
		if perfAccounted(stats.ToServer) && perfAccounted(stats.ToClient) {
			return stats
		}
		if time.Now().After(deadline) {
			tb.Fatalf("proxy accounting did not settle within %s: %s", timeout, stats)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func perfAccounted(link quicnettest.LinkStats) bool {
	drops := link.LossDrops + link.BlackoutDrops + link.OverflowDrops
	return link.Received+link.Duplicates == link.Sent+drops
}

// perfBoundedCleanup closes every resource with a deadline and requires the
// goroutine count to return near its baseline.
func perfBoundedCleanup(tb testing.TB, fx *perfFixture, client ports.ClientConnection, server ports.ServerConnection, baseline int) {
	tb.Helper()
	closed := make(chan error, 1)
	go func() {
		closed <- errors.Join(client.Close(), server.Close(), fx.listener.Close(), fx.proxy.Close())
	}()
	select {
	case err := <-closed:
		if err != nil {
			tb.Fatalf("cleanup: %v", err)
		}
	case <-time.After(perfExchangeTimeout):
		tb.Fatalf("cleanup did not complete within %s", perfExchangeTimeout)
	}
	deadline := time.Now().Add(5 * time.Second)
	const slack = 8
	for {
		if runtime.NumGoroutine() <= baseline+slack {
			return
		}
		if time.Now().After(deadline) {
			tb.Fatalf("goroutines did not drain: baseline %d, still %d", baseline, runtime.NumGoroutine())
		}
		time.Sleep(25 * time.Millisecond)
	}
}

// TestRealQUICPerformanceEvidence produces one bounded, reproducible evidence
// record per profile. It asserts only structural invariants; the measured
// values are emitted with t.Logf and are meant to be read with -v, e.g.
//
//	go test ./internal/adapters/quicnettest -run TestRealQUICPerformanceEvidence -v
func TestRealQUICPerformanceEvidence(t *testing.T) {
	if testing.Short() {
		t.Skip("real QUIC performance evidence is skipped in short mode")
	}
	for _, profile := range perfProfiles() {
		t.Run(profile.name, func(t *testing.T) {
			perfEvidenceRun(t, profile)
		})
	}
}

func perfEvidenceRun(t *testing.T, profile perfProfile) {
	t.Helper()
	baselineGoroutines := runtime.NumGoroutine()
	fx := startPerfFixture(t, profile.link)

	handshakeStart := time.Now()
	client, server := startPerfConversation(t, fx)
	handshakeLatency := time.Since(handshakeStart)

	for i := 1; i <= perfWarmupExchanges; i++ {
		if _, err := perfExchange(client, server, uint64(i)); err != nil {
			t.Fatalf("warmup exchange %d: %v", i, err)
		}
	}

	cpuBefore := readPerfCPU()
	memBefore := readPerfMem()
	wireBefore := fx.wireBytes()
	latencies := make([]time.Duration, 0, profile.exchanges)
	measureStart := time.Now()
	for i := 1; i <= profile.exchanges; i++ {
		duration, err := perfExchange(client, server, uint64(1000+i))
		if err != nil {
			t.Fatalf("exchange %d: %v", i, err)
		}
		latencies = append(latencies, duration)
	}
	measureWall := time.Since(measureStart)
	cpuAfter := readPerfCPU()
	memAfter := readPerfMem()
	wireAfter := fx.wireBytes()

	latency := perfLatencyFor(latencies)

	blackouts := make([]time.Duration, 0, perfBlackoutSamples)
	for i := 0; i < perfBlackoutSamples; i++ {
		duration, err := perfBlackoutRecoveryOnce(fx, client, server, uint64(2000+i))
		if err != nil {
			t.Fatalf("blackout recovery sample %d: %v", i, err)
		}
		blackouts = append(blackouts, duration)
	}
	rebinds := make([]time.Duration, 0, perfRebindSamples)
	for i := 0; i < perfRebindSamples; i++ {
		duration, err := perfRebindRecoveryOnce(fx, client, server, uint64(3000+i))
		if err != nil {
			t.Fatalf("rebind recovery sample %d: %v", i, err)
		}
		rebinds = append(rebinds, duration)
	}

	runtime.GC()
	retained := readPerfMem()

	stats := perfWaitAccounting(t, fx, 10*time.Second)

	blackoutLatency := perfLatencyFor(blackouts)
	rebindLatency := perfLatencyFor(rebinds)
	cpuTotal := cpuAfter.total() - cpuBefore.total()
	cpuPerExchange := cpuTotal / time.Duration(profile.exchanges)
	allocsPerExchange := float64(memAfter.mallocs-memBefore.mallocs) / float64(profile.exchanges)
	bytesPerExchange := float64(memAfter.totalAlloc-memBefore.totalAlloc) / float64(profile.exchanges)
	gcDelta := int64(memAfter.numGC) - int64(memBefore.numGC)
	appBytesPerExchange := int64(perfInputPayload + perfOutputPayload)
	wireBytesPerExchange := float64(wireAfter-wireBefore) / float64(profile.exchanges)
	wireAmplification := wireBytesPerExchange / float64(appBytesPerExchange)

	// Structural invariants only: no latency threshold, so the evidence test
	// stays valid on a loaded or virtualized host.
	if latency.Count != profile.exchanges {
		t.Fatalf("latency sample count = %d, want %d", latency.Count, profile.exchanges)
	}
	if !(latency.P50 <= latency.P95 && latency.P95 <= latency.P99 && latency.P99 <= latency.Max) {
		t.Fatalf("latency percentiles are not monotone: %+v", latency)
	}
	for _, duration := range append(append([]time.Duration(nil), blackouts...), rebinds...) {
		if duration <= 0 || duration > perfRecoveryCeiling {
			t.Fatalf("recovery latency %s is outside the liveness window (0,%s]", duration, perfRecoveryCeiling)
		}
	}
	if stats.ToServer.DeliveryErrors != 0 || stats.ToClient.DeliveryErrors != 0 {
		t.Fatalf("proxy delivery failed: %s", stats)
	}
	if stats.ToServer.OverflowDrops != 0 || stats.ToClient.OverflowDrops != 0 {
		t.Fatalf("bounded queue overflowed: %s", stats)
	}
	if stats.ToServer.Received == 0 || stats.ToServer.Sent == 0 || stats.ToClient.Sent == 0 {
		t.Fatalf("proxy carried no traffic: %s", stats)
	}
	if stats.ToServer.BlackoutDrops == 0 {
		t.Fatalf("blackout did not drop packets: %s", stats)
	}
	if stats.Rebinds != uint64(perfRebindSamples) {
		t.Fatalf("rebinds = %d, want %d", stats.Rebinds, perfRebindSamples)
	}

	t.Logf("PERF profile=%s exchanges=%d handshake=%s measured_wall=%s", profile.name, profile.exchanges, handshakeLatency, measureWall)
	t.Logf("PERF latency profile=%s p50=%s p95=%s p99=%s max=%s", profile.name, latency.P50, latency.P95, latency.P99, latency.Max)
	t.Logf("PERF cost profile=%s cpu_total=%s cpu_per_exchange=%s allocs_per_exchange=%.1f bytes_per_exchange=%.0f gc_delta=%d goroutines=%d heap_retained_bytes=%d",
		profile.name, cpuTotal, cpuPerExchange, allocsPerExchange, bytesPerExchange, gcDelta, retained.goroutines, retained.heapAlloc)
	t.Logf("PERF bytes profile=%s app_bytes_per_exchange=%d wire_bytes_per_exchange=%.1f wire_amplification=%.2f proxy_packets=%d",
		profile.name, appBytesPerExchange, wireBytesPerExchange, wireAmplification, fx.packets.Load())
	t.Logf("PERF recovery profile=%s blackout_samples=%d blackout_p50=%s blackout_max=%s rebind_samples=%d rebind_p50=%s rebind_max=%s",
		profile.name, blackoutLatency.Count, blackoutLatency.P50, blackoutLatency.Max, rebindLatency.Count, rebindLatency.P50, rebindLatency.Max)
	t.Logf("PERF proxy profile=%s stats=%s", profile.name, stats)

	perfBoundedCleanup(t, fx, client, server, baselineGoroutines)
}
