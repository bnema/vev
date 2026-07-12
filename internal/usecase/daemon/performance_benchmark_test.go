package daemon

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestCountingSnapshotStoreWriteCountsAndRetainsOpaquePayload(t *testing.T) {
	store := &countingSnapshotStore{}
	payload := []byte{0xff, 0x00, 0xfe}

	require.NoError(t, store.Write("session", payload))
	require.Equal(t, countingSnapshotMetrics{writes: 1, bytes: uint64(len(payload))}, store.metrics())
	require.Equal(t, payload, store.lastPayload())

	payload[0] = 0x01
	require.Equal(t, payload, store.lastPayload(), "the counting sink must retain the payload slice header, not copy it")
}

func TestCountingOutputTransportCountsOpaquePayloadAndRejectsShortPayload(t *testing.T) {
	transport := &countingOutputTransport{}

	err := transport.Send(ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 23)})
	require.Error(t, err)
	require.Equal(t, countingOutputMetrics{}, transport.metrics())

	payload := append(make([]byte, 24), 0xff, 0x00, 0xfe)
	require.NoError(t, transport.Send(ports.Frame{Type: ports.MsgOutput, Payload: payload}))
	require.Equal(t, countingOutputMetrics{frames: 1, bytes: 3, payloadBytes: uint64(len(payload))}, transport.metrics())
	require.Equal(t, payload, transport.lastPayload())

	payload[24] = 0x01
	require.Equal(t, payload, transport.lastPayload(), "the counting transport must retain the payload slice header, not copy it")
}

func TestPerformanceFixtureCounters(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{})

	fixture.paintLive()
	fixture.paintLive()
	metrics := fixture.metrics()
	require.Equal(t, uint64(2), metrics.outputFrames)
	require.Equal(t, uint64(2), metrics.renderCaptures, "capture is one immutable state snapshot per live render request")
	require.Equal(t, uint64(2), metrics.renderCompositions, "composition is one frame built from each captured state")
	require.Equal(t, uint64(2), metrics.renderEmissions, "emission is one accepted output frame")
	require.Positive(t, metrics.outputBytes)
	require.Positive(t, metrics.outputPayloadBytes)
	require.Equal(t, uint64(2), metrics.coordinatorInvalidations)
	require.Equal(t, uint64(2), metrics.coordinatorWakes)
	require.Equal(t, uint64(2), metrics.coordinatorCoalesced)
	require.Positive(t, metrics.coordinatorWakes, "coordinator metric ratios require a nonzero denominator")

	fixture.captureSnapshot()
	metrics = fixture.metrics()
	require.Equal(t, uint64(1), metrics.snapshotWrites)
	require.Positive(t, metrics.snapshotBytes)

	fixture.enterCopySearch()
	require.Positive(t, fixture.searchMatches())

	fixture.resize()
	require.True(t, fixture.resized())
}

func TestRenderStageHooksCountProductionBoundariesOnFailedSend(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	d.paint(sess, ac, true)
	<-sends
	var captures, compositions, emissions int
	ac.renderStages = renderStageHooks{
		capture: func() { captures++ },
		compose: func() { compositions++ },
		emit:    func() { emissions++ },
	}
	ac.replaceTransport(cacheFailTransport{})
	p := sess.tabs[0].focusedPane()
	p.mu.Lock()
	p.screen.Write([]byte("hook"))
	p.mu.Unlock()
	d.paint(sess, ac, false)
	require.Equal(t, 1, captures, "a completed capture counts even when its emission fails")
	require.Equal(t, 1, compositions, "a completed composition counts even when its emission fails")
	require.Zero(t, emissions, "emit counts only prepare and transport success")
}

func TestPerformanceFixtureResize(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}})
	sequence := []domain.Size{{Cols: 100, Rows: 30}, {Cols: 120, Rows: 40}, {Cols: 160, Rows: 50}, {Cols: 80, Rows: 24}}

	for _, want := range sequence {
		fixture.resizeTo(want)
		require.Equal(t, want, fixture.ac.size)
		require.True(t, fixture.resized())
	}
	metrics := fixture.metrics()
	require.Equal(t, uint64(len(sequence)), metrics.resizeRequests)
	require.Equal(t, uint64(len(sequence)), metrics.resizeCommits)
	require.Equal(t, uint64(len(sequence)), metrics.outputFrames, "one full frame per accepted epoch")
	require.Zero(t, metrics.skippedEpochs)
	require.Zero(t, metrics.ptyFailures)
	require.Zero(t, metrics.frameGapEpochs)
	require.NotZero(t, metrics.resizeCommitNanos)
	require.Equal(t, []domain.Size{{Cols: 50, Rows: 28}, {Cols: 60, Rows: 38}, {Cols: 80, Rows: 48}, {Cols: 40, Rows: 22}}, fixture.pty.requested())
}

func TestTransactionalResizeMetricDefinitionsAreZeroSafe(t *testing.T) {
	metrics := performanceMetrics{}
	require.Zero(t, coordinatorCoalescingRatio(metrics))
	require.Zero(t, metrics.skippedEpochs, "no accepted request means no skipped epoch")
	require.Zero(t, metrics.frameGapEpochs, "no committed epoch means no frame gap")
}

func TestTransactionalResizeMetrics(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, ptyErrors: []error{fmt.Errorf("scripted failure"), nil}})
	fixture.resizeTo(domain.Size{Cols: 100, Rows: 30})
	// The fixture deliberately invokes the coordinator's committed retry path:
	// a retry never republishes geometry and its reset produces one extra frame.
	fixture.retryLatest()

	metrics := fixture.metrics()
	require.Equal(t, uint64(1), metrics.resizeRequests)
	require.Equal(t, uint64(1), metrics.resizeCommits)
	require.Equal(t, uint64(1), metrics.ptyFailures)
	require.Equal(t, uint64(1), metrics.ptyRetries)
	require.Equal(t, uint64(2), metrics.outputFrames, "commit plus successful full-reset retry")
	require.Zero(t, metrics.frameGapEpochs, "every committed epoch emitted its required frame")
	require.Equal(t, []domain.Size{{Cols: 50, Rows: 28}, {Cols: 50, Rows: 28}}, fixture.pty.requested())
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, fixture.ac.size)
}

func TestPerformanceFixtureLargeHistoryTopology(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, tabs: 4, panes: 4, historyRows: 10_000})

	require.Len(t, fixture.sess.tabs, 4)
	for _, tb := range fixture.sess.tabs {
		require.Len(t, tb.panes, 4)
		for _, p := range tb.panes {
			require.Equal(t, 10_000, p.history.Len())
		}
	}
}

func TestPerformanceFixtureSnapshotCaptureRetainsSealedHistory(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, historyRows: 10_000})

	first, ok := fixture.d.captureSnapshotState(fixture.sess, 1)
	require.True(t, ok)
	require.Len(t, first.tabs, 1)
	require.Len(t, first.tabs[0].panes, 2)
	require.Equal(t, 10_000, first.tabs[0].panes[0].history.Len())
	require.Equal(t, 40, first.tabs[0].panes[0].history.ChunkCount())

	second, ok := fixture.d.captureSnapshotState(fixture.sess, 2)
	require.True(t, ok)
	secondPane := capturePaneByID(second, first.tabs[0].panes[0].id)
	if secondPane == nil || first.tabs[0].panes[0].history.Chunk(0) != secondPane.history.Chunk(0) {
		t.Fatal("unchanged sealed history chunk was not reused across captures")
	}
}

func TestPerformanceFixtureCoordinatorMetricsForCanonicalTopologies(t *testing.T) {
	for _, topology := range daemonHistoryTopologies {
		t.Run(topology.name, func(t *testing.T) {
			fixture := newPerformanceFixture(t, performanceConfig{
				size: domain.Size{Cols: 120, Rows: 40}, tabs: topology.tabs, panes: topology.panes, historyRows: 10_000,
			})
			fixture.paintLive()
			metrics := fixture.metrics()
			require.Equal(t, uint64(1), metrics.coordinatorInvalidations)
			require.Equal(t, uint64(1), metrics.coordinatorWakes)
			require.Equal(t, uint64(1), metrics.coordinatorCoalesced)
			require.Positive(t, metrics.coordinatorWakes, "coalescing ratio denominator")
		})
	}
}

type daemonHistoryTopology struct {
	name        string
	tabs, panes int
}

var daemonHistoryTopologies = []daemonHistoryTopology{
	{name: "1tab-1pane-control", tabs: 1, panes: 1},
	{name: "1tab-4panes", tabs: 1, panes: 4},
	{name: "4tabs-1pane", tabs: 4, panes: 1},
	{name: "4tabs-4panes", tabs: 4, panes: 4},
	{name: "8tabs-1pane", tabs: 8, panes: 1},
}

// v3 bounds a decoded snapshot to 256 MiB. One 120-column 10k-row tab is
// representative and valid; multi-tab fixtures exceed that durable budget, so
// snapshot capture/encode benchmark only the valid one-tab topologies.
var daemonSnapshotTopologies = []daemonHistoryTopology{
	{name: "1tab-1pane-control", tabs: 1, panes: 1},
	{name: "1tab-4panes", tabs: 1, panes: 4},
}

func BenchmarkDaemonHistoryLivePaint(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "live-paint", func(f *performanceFixture) { f.paintLive() })
}

// BenchmarkDaemonHistorySnapshotCapture measures the synchronous immutable
// capture boundary only. Encoding and the bounded worker queue are deliberately
// excluded so scheduler latency cannot affect this benchmark.
func BenchmarkDaemonHistorySnapshotCapture(b *testing.B) {
	benchmarkDaemonSnapshotCapture(b)
}

// BenchmarkDaemonHistorySnapshotEncode measures v3 encoding from an already
// immutable capture, without queueing or persistence.
func BenchmarkDaemonHistorySnapshotEncode(b *testing.B) {
	benchmarkDaemonSnapshotEncode(b)
}

// BenchmarkDaemonHistorySnapshotRestore measures terminal-state restoration
// from a validated v3 pane manifest without spawning PTY or session workers.
func BenchmarkDaemonHistorySnapshotRestore(b *testing.B) {
	benchmarkDaemonSnapshotRestore(b)
}

func BenchmarkDaemonHistoryCopyEnter(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "copy-enter", func(f *performanceFixture) { f.d.enterCopyMode(f.sess, f.ac) })
}
func BenchmarkDaemonHistoryCopySearch(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "copy-search", func(f *performanceFixture) { f.d.handleInput(f.sess, f.ac, []byte("/needle\r")) })
}

// BenchmarkDaemonHistoryResizeSweep alternates deterministic grow/shrink
// geometry. Metrics are fixture counters; no benchmark result depends on wall
// time or scheduler delivery.
func BenchmarkDaemonHistoryResizeSweep(b *testing.B) {
	sequence := []domain.Size{{Cols: 80, Rows: 24}, {Cols: 100, Rows: 30}, {Cols: 120, Rows: 40}, {Cols: 160, Rows: 50}, {Cols: 120, Rows: 40}, {Cols: 100, Rows: 30}}
	benchmarkDaemonLargeHistory(b, "resize-sweep", func(f *performanceFixture) {
		f.resizeTo(sequence[f.resizes%len(sequence)])
	})
}

// BenchmarkDaemonHistoryResizeRetry measures one deterministic failed apply
// followed by its successful full-reset retry for each operation.
func BenchmarkDaemonHistoryResizeRetry(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "resize-retry", func(f *performanceFixture) {
		f.pty.setErrors([]error{fmt.Errorf("scripted resize failure"), nil})
		size := domain.Size{Cols: 100, Rows: 30}
		if f.resizes%2 != 0 {
			size = domain.Size{Cols: 120, Rows: 40}
		}
		f.resizeTo(size)
		f.retryLatest()
	})
}

func benchmarkDaemonLargeHistory(b *testing.B, workload string, run func(*performanceFixture)) {
	b.Helper()
	for _, topology := range daemonHistoryTopologies {
		b.Run(topology.name, func(b *testing.B) {
			fixture := newPerformanceFixture(b, performanceConfig{
				size: domain.Size{Cols: 120, Rows: 40}, tabs: topology.tabs, panes: topology.panes, historyRows: 10_000,
			})
			if !fixture.hasHistoryTopology(topology.tabs, topology.panes, 10_000) {
				b.Fatal("invalid daemon history fixture")
			}
			if workload == "copy-enter" || workload == "copy-search" {
				benchmarkDaemonCopyOperation(b, fixture, workload, run)
				return
			}
			b.ReportAllocs()
			fixture.resetMetrics()
			for b.Loop() {
				run(fixture)
				fixture.ac.ackOutputState(fixture.ac.output.next)
			}
			metrics := fixture.metrics()
			if payload := fixture.output.lastPayload(); payload != nil {
				if _, err := ports.UnmarshalOutput(payload); err != nil {
					b.Fatalf("decode last output: %v", err)
				}
			}
			if workload == "live-paint" && metrics.outputFrames != uint64(b.N) {
				b.Fatalf("live paint emitted %d frames for %d operations", metrics.outputFrames, b.N)
			}
			benchmarkReportMetrics(b, metrics, b.N, 10_000)
		})
	}
}

func benchmarkDaemonCopyOperation(b *testing.B, fixture *performanceFixture, workload string, run func(*performanceFixture)) {
	b.Helper()
	b.ReportAllocs()
	b.ResetTimer()
	operations := 0
	for range b.N {
		b.StopTimer()
		fixture.d.exitCopyMode(fixture.ac)
		if workload == "copy-search" {
			fixture.d.enterCopyMode(fixture.sess, fixture.ac)
		}
		fixture.ac.ackOutputState(fixture.ac.output.next)
		b.StartTimer()
		run(fixture)
		b.StopTimer()
		if workload == "copy-enter" && !fixture.copyModeActive() {
			b.Fatal("copy enter did not install a copy mode")
		}
		if workload == "copy-search" && fixture.searchMatches() == 0 {
			b.Fatal("copy search produced no deterministic matches")
		}
		operations++
	}
	if operations != b.N {
		b.Fatalf("copy workload ran %d operations for %d iterations", operations, b.N)
	}
	b.ReportMetric(float64(operations)/float64(b.N), "copyoperations/op")
}

func benchmarkDaemonSnapshotCapture(b *testing.B) {
	for _, topology := range daemonSnapshotTopologies {
		b.Run(topology.name, func(b *testing.B) {
			fixture := newPerformanceFixture(b, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, tabs: topology.tabs, panes: topology.panes, historyRows: 10_000})
			if !fixture.hasHistoryTopology(topology.tabs, topology.panes, 10_000) {
				b.Fatal("invalid daemon snapshot capture fixture")
			}
			first, ok := fixture.d.captureSnapshotState(fixture.sess, 1)
			if !ok || !validBenchmarkCapture(first) {
				b.Fatal("invalid snapshot capture fixture")
			}
			firstPane := first.tabs[0].panes[0]
			firstChunk := firstPane.history.Chunk(0)
			captures := 0
			var last *snapshotCapture
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				last, ok = fixture.d.captureSnapshotState(fixture.sess, uint64(captures+2))
				if !ok {
					b.Fatal("snapshot capture unexpectedly rejected fixture")
				}
				captures++
			}
			b.StopTimer()
			lastPane := capturePaneByID(last, firstPane.id)
			if captures != b.N || !validBenchmarkCapture(last) || lastPane == nil || lastPane.history.Chunk(0) != firstChunk {
				b.Fatal("snapshot capture did not preserve immutable sealed history")
			}
			b.ReportMetric(float64(capturePaneCount(last)), "capturepanes/op")
		})
	}
}

func benchmarkDaemonSnapshotEncode(b *testing.B) {
	for _, topology := range daemonSnapshotTopologies {
		b.Run(topology.name, func(b *testing.B) {
			fixture := newPerformanceFixture(b, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, tabs: topology.tabs, panes: topology.panes, historyRows: 10_000})
			if !fixture.hasHistoryTopology(topology.tabs, topology.panes, 10_000) {
				b.Fatal("invalid daemon snapshot encode fixture")
			}
			capture, ok := fixture.d.captureSnapshotState(fixture.sess, 1)
			if !ok || !validBenchmarkCapture(capture) {
				b.Fatal("invalid snapshot encode fixture")
			}
			encoded, err := fixture.d.encodeSnapshotCapture(capture)
			if err != nil {
				b.Fatal(err)
			}
			if _, err := snapcodec.Unmarshal(encoded); err != nil {
				b.Fatalf("invalid v3 snapshot encode fixture: %v", err)
			}
			var last []byte
			b.ReportAllocs()
			b.SetBytes(int64(len(encoded)))
			b.ResetTimer()
			for range b.N {
				last, err = fixture.d.encodeSnapshotCapture(capture)
				if err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if _, err := snapcodec.Unmarshal(last); err != nil {
				b.Fatalf("snapshot encoder produced invalid v3 data: %v", err)
			}
			b.ReportMetric(float64(len(last)), "snapshotbytes/op")
		})
	}
}

func benchmarkDaemonSnapshotRestore(b *testing.B) {
	fixture := newPerformanceFixture(b, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, panes: 1, historyRows: 10_000})
	if !fixture.hasHistoryTopology(1, 1, 10_000) {
		b.Fatal("invalid daemon snapshot restore fixture")
	}
	capture, ok := fixture.d.captureSnapshotState(fixture.sess, 1)
	if !ok || !validBenchmarkCapture(capture) {
		b.Fatal("invalid snapshot restore fixture")
	}
	encoded, err := fixture.d.encodeSnapshotCapture(capture)
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := snapcodec.Unmarshal(encoded)
	if err != nil || len(snapshot.Tabs) != 1 || len(snapshot.Tabs[0].Panes) == 0 {
		b.Fatalf("invalid v3 snapshot restore fixture: %v", err)
	}
	paneSnapshot := snapshot.Tabs[0].Panes[0]
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for range b.N {
		if err := restorePaneTerminal(fixture.activePane, paneSnapshot); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	fixture.activePane.mu.Lock()
	restoredRows := fixture.activePane.history.Len()
	fixture.activePane.mu.Unlock()
	if restoredRows != 10_000 {
		b.Fatalf("snapshot restore retained %d history rows, want 10000", restoredRows)
	}
	b.ReportMetric(float64(restoredRows), "historyrows/restore")
}

func validBenchmarkCapture(capture *snapshotCapture) bool {
	return capture != nil && len(capture.tabs) > 0 && len(capture.tabs[0].panes) > 0 && capture.tabs[0].panes[0].history.Len() == 10_000 && capture.tabs[0].panes[0].history.ChunkCount() > 0
}

func capturePaneCount(capture *snapshotCapture) int {
	count := 0
	for _, tab := range capture.tabs {
		count += len(tab.panes)
	}
	return count
}

func capturePaneByID(capture *snapshotCapture, id layout.PaneID) *snapshotCapturePane {
	if capture == nil {
		return nil
	}
	for tabIndex := range capture.tabs {
		for paneIndex := range capture.tabs[tabIndex].panes {
			pane := &capture.tabs[tabIndex].panes[paneIndex]
			if pane.id == id {
				return pane
			}
		}
	}
	return nil
}

func TestCoordinatorCoalescingRatioReportsInvalidationsPerWake(t *testing.T) {
	require.Equal(t, float64(3), coordinatorCoalescingRatio(performanceMetrics{
		coordinatorWakes:     1,
		coordinatorCoalesced: 3,
	}))
	require.Zero(t, coordinatorCoalescingRatio(performanceMetrics{}))
}

func coordinatorCoalescingRatio(metrics performanceMetrics) float64 {
	if metrics.coordinatorWakes == 0 {
		return 0
	}
	return float64(metrics.coordinatorCoalesced) / float64(metrics.coordinatorWakes)
}

func benchmarkReportMetrics(b *testing.B, metrics performanceMetrics, operations, historyRows int) {
	b.Helper()
	if operations == 0 {
		return
	}
	perOperation := float64(operations)
	b.ReportMetric(float64(metrics.outputFrames)/perOperation, "outputframes/op")
	b.ReportMetric(float64(metrics.outputBytes)/perOperation, "outputbytes/op")
	b.ReportMetric(float64(metrics.outputPayloadBytes)/perOperation, "framepayloadbytes/op")
	b.ReportMetric(float64(metrics.snapshotWrites)/perOperation, "snapshotwrites/op")
	b.ReportMetric(float64(metrics.snapshotBytes)/perOperation, "snapshotbytes/op")
	b.ReportMetric(float64(metrics.coordinatorInvalidations)/perOperation, "coordinatorinvalidations/op")
	b.ReportMetric(float64(metrics.coordinatorWakes)/perOperation, "coordinatorwakes/op")
	b.ReportMetric(float64(metrics.coordinatorCoalesced)/perOperation, "coordinatorcoalesced/op")
	// Split counters have precise fixture definitions: capture is an immutable
	// state snapshot requested for a live paint; composition builds the frame
	// from that capture; emission accepts its output frame. Keep bytes/frames
	// above as transport observables rather than folding them into these rates.
	b.ReportMetric(float64(metrics.renderCaptures)/perOperation, "rendercaptures/op")
	b.ReportMetric(float64(metrics.renderCompositions)/perOperation, "rendercompositions/op")
	b.ReportMetric(float64(metrics.renderEmissions)/perOperation, "renderemissions/op")
	// resizecommitns is the injected-clock elapsed time from accepted request
	// to committed epoch. The remaining values are counter deltas: skipped is
	// accepted minus committed (never negative), framegap is committed epochs
	// lacking their required output frame (never negative), and retries/failures
	// are PTY Resize outcomes. All divisions are guarded above.
	b.ReportMetric(float64(metrics.resizeCommitNanos)/perOperation, "resizecommitns/op")
	b.ReportMetric(float64(metrics.skippedEpochs)/perOperation, "skippedepochs/op")
	b.ReportMetric(float64(metrics.ptyFailures)/perOperation, "ptyfailures/op")
	b.ReportMetric(float64(metrics.ptyRetries)/perOperation, "ptyretries/op")
	b.ReportMetric(float64(metrics.frameGapEpochs)/perOperation, "framegapepochs/op")
	b.ReportMetric(coordinatorCoalescingRatio(metrics), "coordinatorcoalescingratio")
	b.ReportMetric(float64(historyRows), "historyrows/pane")
}

// Root-cause hypothesis (verified by this guard before the fix): S2 allocates
// one visible VT-frame copy during capture plus a new composed frame and its
// base-frame clone for every live paint. Those client-sized copies, rather
// than history, account for the ~1 MiB/op regression in the pinned benchmark.
func TestLivePaintAllocationBudget(t *testing.T) {
	for _, tt := range []struct {
		name  string
		panes int
	}{
		{name: "1tab-1pane", panes: 1},
		{name: "1tab-4panes", panes: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, panes: tt.panes, historyRows: 10_000})
			allocs := testing.AllocsPerRun(20, func() {
				fixture.paintLive()
				fixture.ac.ackOutputState(fixture.ac.output.next)
			})
			require.LessOrEqual(t, allocs, float64(38), "live paint must reuse attachment-owned render scratch across warm paints")
		})
	}
}

func TestPerformanceFixturePaintLiveUsesPrecomputedAlternatingWrites(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{})

	require.NotNil(t, fixture.activePane)
	require.Equal(t, [][]byte{
		[]byte("\x1b[1;1HA\x1b[2;2HA"),
		[]byte("\x1b[1;1HB\x1b[2;2HB"),
	}, fixture.liveWrites)

	for range 4 {
		fixture.paintLive()
	}
	require.Equal(t, uint64(4), fixture.metrics().outputFrames)
}

// performanceConfig intentionally describes only an in-process daemon shape:
// no PTY readers, schedulers, clocks, or transports are started by this fixture.
type performanceConfig struct {
	size        domain.Size
	tabs        int
	panes       int
	historyRows int
	ptyErrors   []error
}

type performanceMetrics struct {
	outputFrames             uint64
	outputBytes              uint64
	outputPayloadBytes       uint64
	snapshotWrites           uint64
	snapshotBytes            uint64
	coordinatorInvalidations uint64
	coordinatorWakes         uint64
	coordinatorCoalesced     uint64
	renderCaptures           uint64
	renderCompositions       uint64
	renderEmissions          uint64
	resizeRequests           uint64
	resizeCommits            uint64
	resizeCommitNanos        uint64
	skippedEpochs            uint64
	ptyFailures              uint64
	ptyRetries               uint64
	frameGapEpochs           uint64
}

// performanceClock makes transition latency a reproducible counter rather
// than a wall-clock observation. A nil timer channel deliberately executes
// coordinator work synchronously.
type performanceClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *performanceClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(time.Microsecond)
	return c.now
}
func (*performanceClock) NewTimer(time.Duration) ports.Timer { return performanceTimer{} }

type performanceTimer struct{}

func (performanceTimer) C() <-chan time.Time      { return nil }
func (performanceTimer) Reset(time.Duration) bool { return true }
func (performanceTimer) Stop() bool               { return true }

// scriptedPerformancePTY records every requested terminal size and returns a
// caller-provided error script. It has no reads or scheduling side effects.
type scriptedPerformancePTY struct {
	mu    sync.Mutex
	sizes []domain.Size
	errs  []error
	fails uint64
}

func (p *scriptedPerformancePTY) Resize(size domain.Size) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.sizes = append(p.sizes, size)
	index := len(p.sizes) - 1
	if index < len(p.errs) && p.errs[index] != nil {
		p.fails++
		return p.errs[index]
	}
	return nil
}
func (*scriptedPerformancePTY) Read([]byte) (int, error)     { return 0, io.EOF }
func (*scriptedPerformancePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (*scriptedPerformancePTY) Close() error                 { return nil }
func (*scriptedPerformancePTY) Pid() int                     { return 0 }
func (*scriptedPerformancePTY) ForegroundPgid() (int, error) { return 0, nil }
func (p *scriptedPerformancePTY) requested() []domain.Size {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]domain.Size(nil), p.sizes...)
}
func (p *scriptedPerformancePTY) metrics() (uint64, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.fails, uint64(len(p.sizes))
}
func (p *scriptedPerformancePTY) setErrors(errs []error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.errs = append([]error(nil), errs...)
	p.sizes = nil
	p.fails = 0
}

type performanceFixture struct {
	t                   testing.TB
	d                   *Daemon
	sess                *session
	ac                  *attachedClient
	output              *countingOutputTransport
	snaps               *countingSnapshotStore
	activePane          *pane
	liveWrites          [][]byte
	paints              int
	resizeSizes         [2]domain.Size
	resizes             int
	resizedSize         domain.Size
	pty                 *scriptedPerformancePTY
	clock               *performanceClock
	resizeRequests      uint64
	resizeCommits       uint64
	resizeCommitNanos   uint64
	ptyFailures         uint64
	ptyRetries          uint64
	coordinatorBaseline renderCoordinatorBurstMetricsSnapshot
	renderCaptures      uint64
	renderCompositions  uint64
	renderEmissions     uint64
}

func newPerformanceFixture(t testing.TB, config performanceConfig) *performanceFixture {
	t.Helper()
	if !config.size.Valid() {
		config.size = domain.Size{Cols: 80, Rows: 24}
	}
	if config.tabs == 0 {
		config.tabs = 1
	}
	if config.panes == 0 {
		config.panes = 2
	}
	if config.historyRows == 0 {
		config.historyRows = 1
	}
	pty := &scriptedPerformancePTY{}
	ptys := make([]ports.PTY, config.tabs)
	ptys[0] = pty
	d, sess, ac, _ := newManualSessionWithPTYs(t, ptys...)
	clock := &performanceClock{}
	d.clock = clock
	output := &countingOutputTransport{}
	ac.tr = output
	ac.size = config.size
	sess.name = "performance"
	sess.ephemeral = false
	sess.snapEligible.Store(true)

	fixture := &performanceFixture{
		t:           t,
		d:           d,
		sess:        sess,
		ac:          ac,
		output:      output,
		snaps:       &countingSnapshotStore{done: make(chan struct{}, 1)},
		pty:         pty,
		clock:       clock,
		liveWrites:  [][]byte{[]byte("\x1b[1;1HA\x1b[2;2HA"), []byte("\x1b[1;1HB\x1b[2;2HB")},
		resizeSizes: [2]domain.Size{{Cols: 100, Rows: 30}, config.size},
	}
	WithSnapshotStore(fixture.snaps)(d)
	snapshotCtx, cancelSnapshotWorker := context.WithCancel(context.Background())
	d.startSnapshotEncodeWorker(snapshotCtx)
	t.Cleanup(func() {
		cancelSnapshotWorker()
		d.stopSnapshotEncodeWorker()
	})
	for tabIndex, tb := range sess.tabs {
		fixture.configureTab(tb, tabIndex, config.panes, config.historyRows, tabSize(config.size))
	}
	fixture.activePane = fixture.findActivePane()
	// Fixture construction may solve the initial layout; benchmark scripts begin
	// after that setup work and therefore have an empty call history.
	pty.setErrors(config.ptyErrors)

	// The fixture uses the production coordinator. performanceClock's nil timer
	// channel completes the coordinator path synchronously and deterministically.
	ac.renderStages = renderStageHooks{
		capture: func() { fixture.renderCaptures++ },
		compose: func() { fixture.renderCompositions++ },
		emit:    func() { fixture.renderEmissions++ },
	}
	d.attachCoordinator(sess, nil, ac, true)

	// Prime the real renderer shadow before measurements. Subsequent paints use
	// actual production diffs rather than the initial full frame.
	d.paint(sess, ac, true)
	fixture.resetMetrics()
	return fixture
}

func (f *performanceFixture) configureTab(tb *tab, tabIndex, paneCount, historyRows int, size domain.Size) {
	f.t.Helper()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	tb.size = size
	left := tb.focusedPane()
	for paneIndex := 1; paneIndex < paneCount; paneIndex++ {
		id := layout.PaneID(fmt.Sprintf("pane-%d-%d", tabIndex+1, paneIndex+1))
		tb.panes[id] = newPane(id, nil, size)
		require.NoError(f.t, tb.tree.Split(left.id, layout.Right, true, id, domain.Rect{Width: size.Cols, Height: size.Rows}))
	}
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: size.Cols, Height: size.Rows})
	require.True(f.t, ok, "performance fixture layout must be solvable")
	require.Len(f.t, placements, paneCount)
	f.d.applyLayoutLocked(tb)
	for _, p := range tb.panes {
		p.mu.Lock()
		width := p.screen.Frame.Width
		for row := 0; p.history.Len() < historyRows; row++ {
			p.screen.Write([]byte(performanceFullWidthRow(width, tabIndex, row) + "\r\n"))
		}
		p.mu.Unlock()
	}
}

// performanceFullWidthRow produces stable, full-width visible content without
// relying on process output or timing.
func performanceFullWidthRow(width, tabIndex, row int) string {
	prefix := fmt.Sprintf("needle-%d-%02d ", tabIndex, row)
	out := make([]byte, width)
	for i := range out {
		out[i] = byte('a' + (tabIndex+row+i)%26)
	}
	copy(out, prefix)
	return string(out)
}

func (f *performanceFixture) findActivePane() *pane {
	f.t.Helper()
	tb := f.sess.activeTab()
	require.NotNil(f.t, tb)
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.focusedPane()
}

func (f *performanceFixture) paintLive() {
	p := f.activePane
	p.mu.Lock()
	p.screen.Write(f.liveWrites[f.paints%len(f.liveWrites)])
	p.mu.Unlock()
	if rc := f.sess.renderCoordinator(); rc != nil {
		rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "performance_benchmark_test.go"})
	} else {
		f.d.paint(f.sess, f.ac, false)
	}
	f.paints++
}

func (f *performanceFixture) captureSnapshot() {
	f.t.Helper()
	require.True(f.t, f.d.captureSession(f.sess))
	<-f.snaps.done
	for range 4096 {
		f.sess.snapshotMu.Lock()
		pending := f.sess.snapshotPending
		f.sess.snapshotMu.Unlock()
		if !pending {
			return
		}
		runtime.Gosched()
	}
	f.t.Fatal("snapshot worker did not finish performance capture")
}

func (f *performanceFixture) enterCopySearch() {
	f.t.Helper()
	f.d.enterCopyMode(f.sess, f.ac)
	f.d.handleInput(f.sess, f.ac, []byte("/needle\r"))
}

func (f *performanceFixture) hasHistoryTopology(tabs, panes, historyRows int) bool {
	if len(f.sess.tabs) != tabs {
		return false
	}
	for _, tb := range f.sess.tabs {
		tb.mu.Lock()
		if len(tb.panes) != panes {
			tb.mu.Unlock()
			return false
		}
		for _, p := range tb.panes {
			p.mu.Lock()
			rows := p.history.Len()
			p.mu.Unlock()
			if rows != historyRows {
				tb.mu.Unlock()
				return false
			}
		}
		tb.mu.Unlock()
	}
	return true
}

func (f *performanceFixture) copyModeActive() bool {
	f.ac.overlays.copyMu.Lock()
	defer f.ac.overlays.copyMu.Unlock()
	return f.ac.overlays.copyMode != nil
}

func (f *performanceFixture) searchMatches() int {
	f.ac.overlays.copyMu.Lock()
	defer f.ac.overlays.copyMu.Unlock()
	if f.ac.overlays.copyMode == nil {
		return 0
	}
	return len(f.ac.overlays.copyMode.Searches)
}

func (f *performanceFixture) resize() {
	f.resizeTo(f.resizeSizes[f.resizes%len(f.resizeSizes)])
}

func (f *performanceFixture) resizeTo(size domain.Size) {
	f.resizedSize = size
	f.resizes++
	before := f.sess.renderCoordinator().resizeSnapshot()
	start := f.clock.Now()
	failuresBefore, callsBefore := f.pty.metrics()
	f.d.resize(f.sess, f.ac, size)
	after := f.sess.renderCoordinator().resizeSnapshot()
	if after.epoch > before.epoch {
		f.resizeRequests += after.epoch - before.epoch
	}
	if after.committed > before.committed {
		f.resizeCommits += after.committed - before.committed
		f.resizeCommitNanos += uint64(f.clock.Now().Sub(start).Nanoseconds())
	}
	failuresAfter, callsAfter := f.pty.metrics()
	f.ptyFailures += failuresAfter - failuresBefore
	if callsAfter > callsBefore+1 {
		f.ptyRetries += callsAfter - callsBefore - 1
	}
}

func (f *performanceFixture) retryLatest() {
	epoch := f.sess.renderCoordinator().resizeSnapshot().committed
	plan := f.d.prepareResize(f.sess, f.ac.size)
	failuresBefore, callsBefore := f.pty.metrics()
	f.d.retryResizeMembers(f.sess, f.ac, epoch, plan.members)
	failuresAfter, callsAfter := f.pty.metrics()
	f.ptyFailures += failuresAfter - failuresBefore
	if callsAfter > callsBefore {
		f.ptyRetries += callsAfter - callsBefore
	}
}

func (f *performanceFixture) resized() bool {
	if f.ac.size != f.resizedSize {
		return false
	}
	for _, tb := range f.sess.tabs {
		tb.mu.Lock()
		ok := tb.size == tabSize(f.resizedSize)
		tb.mu.Unlock()
		if !ok {
			return false
		}
	}
	return true
}

func (f *performanceFixture) resetMetrics() {
	f.output.reset()
	f.snaps.reset()
	f.renderCaptures = 0
	f.renderCompositions = 0
	f.renderEmissions = 0
	f.resizeRequests = 0
	f.resizeCommits = 0
	f.resizeCommitNanos = 0
	f.ptyFailures = 0
	f.ptyRetries = 0
	if rc := f.sess.renderCoordinator(); rc != nil {
		f.coordinatorBaseline = rc.burstMetricsSnapshot()
	}
}

func (f *performanceFixture) metrics() performanceMetrics {
	output := f.output.metrics()
	snapshots := f.snaps.metrics()
	metrics := performanceMetrics{
		outputFrames: output.frames, outputBytes: output.bytes, outputPayloadBytes: output.payloadBytes,
		snapshotWrites: snapshots.writes, snapshotBytes: snapshots.bytes,
		renderCaptures: f.renderCaptures, renderCompositions: f.renderCompositions, renderEmissions: f.renderEmissions,
		resizeRequests: f.resizeRequests, resizeCommits: f.resizeCommits, resizeCommitNanos: f.resizeCommitNanos,
		ptyFailures: f.ptyFailures, ptyRetries: f.ptyRetries,
	}
	if metrics.resizeRequests > metrics.resizeCommits {
		metrics.skippedEpochs = metrics.resizeRequests - metrics.resizeCommits
	}
	if metrics.resizeCommits > metrics.outputFrames {
		metrics.frameGapEpochs = metrics.resizeCommits - metrics.outputFrames
	}
	if rc := f.sess.renderCoordinator(); rc != nil {
		coordinator := rc.burstMetricsSnapshot()
		metrics.coordinatorInvalidations = coordinator.invalidations - f.coordinatorBaseline.invalidations
		metrics.coordinatorWakes = coordinator.wakes - f.coordinatorBaseline.wakes
		metrics.coordinatorCoalesced = coordinator.coalesced - f.coordinatorBaseline.coalesced
	}
	return metrics
}

const outputPayloadHeaderBytes = 24

type countingOutputMetrics struct{ frames, bytes, payloadBytes uint64 }

type countingOutputTransport struct {
	mu sync.Mutex
	countingOutputMetrics
	last []byte
}

func (t *countingOutputTransport) Send(frame ports.Frame) error {
	if frame.Type != ports.MsgOutput {
		return nil
	}
	if len(frame.Payload) < outputPayloadHeaderBytes {
		return fmt.Errorf("output payload is %d bytes, want at least %d", len(frame.Payload), outputPayloadHeaderBytes)
	}
	t.mu.Lock()
	t.frames++
	t.bytes += uint64(len(frame.Payload) - outputPayloadHeaderBytes)
	t.payloadBytes += uint64(len(frame.Payload))
	t.last = frame.Payload
	t.mu.Unlock()
	return nil
}
func (*countingOutputTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*countingOutputTransport) Close() error               { return nil }
func (t *countingOutputTransport) reset() {
	t.mu.Lock()
	t.countingOutputMetrics = countingOutputMetrics{}
	t.last = nil
	t.mu.Unlock()
}
func (t *countingOutputTransport) metrics() countingOutputMetrics {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.countingOutputMetrics
}
func (t *countingOutputTransport) lastPayload() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.last
}

type countingSnapshotMetrics struct{ writes, bytes uint64 }
type countingSnapshotStore struct {
	mu sync.Mutex
	countingSnapshotMetrics
	last []byte
	done chan struct{}
}

func (s *countingSnapshotStore) Write(_ string, data []byte) error {
	s.mu.Lock()
	s.writes++
	s.bytes += uint64(len(data))
	s.last = data
	s.mu.Unlock()
	if s.done != nil {
		s.done <- struct{}{}
	}
	return nil
}
func (*countingSnapshotStore) Load() ([]ports.SnapshotBlob, error) { return nil, nil }
func (*countingSnapshotStore) Delete(string) error                 { return nil }
func (s *countingSnapshotStore) reset() {
	s.mu.Lock()
	s.countingSnapshotMetrics = countingSnapshotMetrics{}
	s.last = nil
	s.mu.Unlock()
}
func (s *countingSnapshotStore) metrics() countingSnapshotMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.countingSnapshotMetrics
}
func (s *countingSnapshotStore) lastPayload() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}
