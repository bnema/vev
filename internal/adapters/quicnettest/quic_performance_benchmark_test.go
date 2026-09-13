// This file holds the documentable QUIC performance benchmarks. They reuse the
// harness in perf_evidence_test.go: the real internal/adapters/quic transport
// behind a [quicnettest.Proxy] and a real sessionwire conversation whose
// representative exchange is "Input -> valid Output -> Ack".
//
// Each benchmark reports latency percentiles (p50/p95/p99/max) in ns/op,
// allocation counters via -benchmem, process CPU per exchange (getrusage),
// proxy-carried datagram bytes per exchange, retained heap, and goroutines. Run
// them isolated so the process-wide CPU figure stays meaningful:
//
//	go test ./internal/adapters/quicnettest -run '^$' -bench BenchmarkRealQUICExchange -benchmem -benchtime 50x
//	go test ./internal/adapters/quicnettest -run '^$' -bench BenchmarkRealQUICBlackoutRecovery -benchtime 10x
package quicnettest_test

import (
	"runtime"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/quicnettest"
	"github.com/bnema/vev/internal/ports"
)

// BenchmarkRealQUICExchange measures the representative typed exchange for both
// representative profiles.
func BenchmarkRealQUICExchange(b *testing.B) {
	for _, profile := range perfProfiles() {
		b.Run(profile.name, func(b *testing.B) {
			benchmarkPerfExchange(b, profile.link)
		})
	}
}

func benchmarkPerfExchange(b *testing.B, link quicnettest.LinkConfig) {
	if testing.Short() {
		b.Skip("real QUIC benchmark is skipped in short mode")
	}
	baselineGoroutines := runtime.NumGoroutine()
	fx := startPerfFixture(b, link)
	client, server := startPerfConversation(b, fx)
	if _, err := perfExchange(client, server, 1); err != nil {
		b.Fatalf("warmup exchange: %v", err)
	}

	latencies := make([]time.Duration, 0, b.N)
	var appBytes uint64
	wireBefore := fx.wireBytes()
	cpuBefore := readPerfCPU()
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		duration, err := perfExchange(client, server, uint64(1000+i))
		if err != nil {
			b.Fatalf("exchange %d: %v", i, err)
		}
		latencies = append(latencies, duration)
		appBytes += perfInputPayload + perfOutputPayload
	}
	cpuAfter := readPerfCPU()
	wireAfter := fx.wireBytes()

	runtime.GC()
	retained := readPerfMem()

	reportPerfBenchmarkMetrics(b, latencies, cpuAfter.total()-cpuBefore.total(), wireAfter-wireBefore, appBytes)
	b.ReportMetric(float64(retained.heapAlloc), "heap-bytes")
	b.ReportMetric(float64(retained.goroutines), "goroutines")

	perfBoundedCleanup(b, fx, client, server, baselineGoroutines)
}

// BenchmarkRealQUICBlackoutRecovery measures the latency from a blackout
// clearing until an exchange that was in flight during the blackout completes,
// on the train profile. Run with a small -benchtime; each iteration waits for
// perfBlackoutWindow plus QUIC recovery.
func BenchmarkRealQUICBlackoutRecovery(b *testing.B) {
	benchmarkPerfRecovery(b, perfBlackoutRecoveryOnce)
}

// BenchmarkRealQUICNATRebindRecovery measures the latency from a proxy NAT
// rebinding until the next exchange completes, on the train profile.
func BenchmarkRealQUICNATRebindRecovery(b *testing.B) {
	benchmarkPerfRecovery(b, perfRebindRecoveryOnce)
}

func benchmarkPerfRecovery(b *testing.B, recoverOnce func(*perfFixture, ports.ClientConnection, ports.ServerConnection, uint64) (time.Duration, error)) {
	if testing.Short() {
		b.Skip("real QUIC recovery benchmark is skipped in short mode")
	}
	baselineGoroutines := runtime.NumGoroutine()
	fx := startPerfFixture(b, trainProfile())
	client, server := startPerfConversation(b, fx)
	if _, err := perfExchange(client, server, 1); err != nil {
		b.Fatalf("warmup exchange: %v", err)
	}

	latencies := make([]time.Duration, 0, b.N)
	b.ReportAllocs()
	for i := 0; b.Loop(); i++ {
		duration, err := recoverOnce(fx, client, server, uint64(1000+i))
		if err != nil {
			b.Fatalf("recovery %d: %v", i, err)
		}
		latencies = append(latencies, duration)
	}
	reportPerfBenchmarkMetrics(b, latencies, 0, 0, 0)

	perfBoundedCleanup(b, fx, client, server, baselineGoroutines)
}

// reportPerfBenchmarkMetrics emits the custom per-op metrics shared by every
// benchmark in this file. cpuTotal, wireBytes, and appBytes are totals for the
// whole measured loop; a zero total disables that metric, which is what the
// recovery benchmarks want because their dominant cost is QUIC backoff, not
// throughput.
func reportPerfBenchmarkMetrics(b *testing.B, latencies []time.Duration, cpuTotal time.Duration, wireBytes, appBytes uint64) {
	b.Helper()
	latency := perfLatencyFor(latencies)
	b.ReportMetric(float64(latency.P50.Nanoseconds()), "p50-ns/op")
	b.ReportMetric(float64(latency.P95.Nanoseconds()), "p95-ns/op")
	b.ReportMetric(float64(latency.P99.Nanoseconds()), "p99-ns/op")
	b.ReportMetric(float64(latency.Max.Nanoseconds()), "max-ns/op")

	operations := float64(len(latencies))
	if operations == 0 {
		return
	}
	if cpuTotal > 0 {
		b.ReportMetric(float64(cpuTotal.Nanoseconds())/operations, "cpu-ns/op")
	}
	if wireBytes > 0 {
		b.ReportMetric(float64(wireBytes)/operations, "wire-bytes/op")
	}
	if appBytes > 0 {
		b.ReportMetric(float64(appBytes)/operations, "app-bytes/op")
	}
}
