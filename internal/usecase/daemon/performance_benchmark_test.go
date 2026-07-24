package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
)

func TestCountingSnapshotRepositoryPublishCountsAndRetainsManifest(t *testing.T) {
	repository := &countingSnapshotRepository{}
	payload := []byte{0xff, 0x00, 0xfe}
	object := []byte{0x01, 0x02}

	require.NoError(t, repository.Publish(context.Background(), ports.SnapshotPublication{Name: "session", Manifest: payload, Objects: []ports.SnapshotObject{{Data: object}}}))
	require.Equal(t, countingSnapshotMetrics{writes: 1, manifestBytes: uint64(len(payload)), objectBytes: uint64(len(object)), suppliedObjectBytes: uint64(len(object)), headBytes: snapshotHeadBytes}, repository.metrics())
	require.Equal(t, payload, repository.lastPayload())

	payload[0] = 0x01
	require.Equal(t, payload, repository.lastPayload(), "the counting sink must retain the payload slice header, not copy it")
}

func TestCountingSnapshotRepositoryDoesNotRewriteKnownObjects(t *testing.T) {
	repository := &countingSnapshotRepository{}
	object, err := snapcodec.MarshalObject(snapcodec.HistoryChunk, []byte{0x01})
	require.NoError(t, err)
	publication := ports.SnapshotPublication{Name: "session", Manifest: []byte("manifest"), Objects: []ports.SnapshotObject{object}}

	require.NoError(t, repository.Publish(context.Background(), publication))
	repository.reset()
	require.NoError(t, repository.Publish(context.Background(), publication))

	require.Zero(t, repository.metrics().objectBytes, "filesystem repositories retain immutable content-addressed objects")
}

func TestIncrementalSnapshotMetricsWriteNoUnchangedHistoryBlobsAndBoundCache(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, panes: 1, historyRows: 10_000})

	publishBenchmarkSnapshot(t, fixture, 1)
	fixture.snapshots.reset()
	publishBenchmarkSnapshot(t, fixture, 2)

	metrics := fixture.snapshots.metrics()
	require.Zero(t, metrics.historyBlobBytes)
	require.Zero(t, metrics.suppliedHistoryBytes)
	require.Zero(t, metrics.objectBytes)
	require.Positive(t, metrics.manifestBytes)
	require.Equal(t, uint64(snapshotHeadBytes), metrics.headBytes)
	require.LessOrEqual(t, benchmarkSnapshotCacheBytes(fixture.sess), snapshotChunkCacheLimit)
}

func TestDaemonSnapshotDoesNotResupplyUnchangedTenThousandChunkHistory(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, panes: 1, historyRows: 10_000})

	markSnapshotDirty(fixture.sess)
	require.True(t, fixture.d.scheduleSnapshot(fixture.sess))
	awaitSnapshotClean(t, fixture.sess)

	var copies atomic.Uint64
	fixture.sess.snapshotMu.Lock()
	fixture.sess.snapshotChunkCache.copyObject = func(object ports.SnapshotObject) ports.SnapshotObject {
		copies.Add(1)
		return copySnapshotObject(object)
	}
	fixture.sess.snapshotMu.Unlock()
	fixture.snapshots.reset()
	fixture.activePane.mu.Lock()
	fixture.activePane.screen.Write([]byte("\x1b[1;1Hchanged"))
	frame := fixture.activePane.screen.PrimaryVisibleFrame()
	fixture.activePane.mu.Unlock()
	for i, r := range "changed" {
		require.Equalf(t, r, frame.At(i, 0).Rune, "fixture must overwrite at column %d", i)
	}
	markSnapshotDirty(fixture.sess)
	require.True(t, fixture.d.scheduleFinalSnapshot(fixture.sess))
	awaitSnapshotClean(t, fixture.sess)

	metrics := fixture.snapshots.metrics()
	require.Equal(t, uint64(1), metrics.writes)
	require.Zero(t, metrics.suppliedHistoryBytes, "retained sealed history must not be resupplied")
	require.Zero(t, copies.Load(), "retained sealed history must not be deep-copied")
	require.Positive(t, metrics.suppliedObjectBytes, "changed tail and visible state must still be supplied")
	requireCompleteSnapshotManifest(t, fixture.snapshots, fixture.sess.name)
}

func requireCompleteSnapshotManifest(t *testing.T, repository *countingSnapshotRepository, name string) {
	t.Helper()
	repository.mu.Lock()
	manifestBytes := append([]byte(nil), repository.last...)
	objects := repository.objects[name]
	repository.mu.Unlock()
	manifest, err := snapcodec.UnmarshalManifest(manifestBytes)
	require.NoError(t, err)
	for _, tab := range manifest.Tabs {
		for _, pane := range tab.Panes {
			for _, ref := range append(append([]snapcodec.ObjectRef(nil), pane.Sealed...), pane.Tail, pane.Visible) {
				_, ok := objects[ref.Digest]
				require.True(t, ok, "manifest reference %x is not retained", ref.Digest)
			}
		}
	}
}

func TestIncrementalSnapshotMetricScenarios(t *testing.T) {
	for _, tt := range []struct {
		name        string
		historyRows int
		mutate      func(*performanceFixture, int)
		wantHistory bool
	}{
		{name: "tail-only", historyRows: 9_999, mutate: mutateBenchmarkTail},
		{name: "new-sealed-chunk", historyRows: 10_000, mutate: mutateBenchmarkSealedChunk, wantHistory: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, panes: 1, historyRows: tt.historyRows})
			publishBenchmarkSnapshot(t, fixture, 1)
			fixture.snapshots.reset()
			tt.mutate(fixture, 0)
			publishBenchmarkSnapshot(t, fixture, 2)

			metrics := fixture.snapshots.metrics()
			if tt.wantHistory {
				require.Positive(t, metrics.historyBlobBytes)
			} else {
				require.Zero(t, metrics.historyBlobBytes)
			}
			require.LessOrEqual(t, benchmarkSnapshotCacheBytes(fixture.sess), snapshotChunkCacheLimit)
		})
	}
}

func TestPerformanceFixtureCellLimitedHistory(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 293, Rows: 40}, panes: 1, historyRows: 10_000})
	fixture.activePane.mu.Lock()
	rows, cells, capCells := fixture.activePane.history.Len(), fixture.activePane.history.Cells(), fixture.activePane.history.CellCap()
	fixture.activePane.mu.Unlock()
	require.Less(t, rows, 10_000)
	require.LessOrEqual(t, cells, capCells)
}

func TestPerformanceFixtureExplicitCloseReleasesIterationState(t *testing.T) {
	fixture := newPerformanceFixtureWithCleanup(t, performanceConfig{}, false)
	d, sess := fixture.d, fixture.sess
	serveCtx, hardCtx := d.serveCtx, d.hardCtx
	killErr := errors.New("scripted kill failure")
	fixture.killSession = func(sess *session, reason uint8, purge bool) error {
		require.NoError(t, d.killSession(sess, reason, purge))
		return killErr
	}

	err := fixture.close()

	require.ErrorIs(t, err, killErr)
	require.Nil(t, fixture.d)
	require.Nil(t, fixture.sess)
	require.Nil(t, fixture.activePane)
	require.Error(t, sess.ctx.Err())
	require.Error(t, serveCtx.Err())
	require.Error(t, hardCtx.Err())
	d.mu.Lock()
	require.NotContains(t, d.sessions, sess.id)
	d.mu.Unlock()
	d.snapshotWorkerMu.Lock()
	require.Nil(t, d.snapshotWorkerCancel)
	d.snapshotWorkerMu.Unlock()
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
	require.Positive(t, metrics.snapshotManifestBytes)
	require.Positive(t, metrics.snapshotObjectBytes)

	fixture.enterCopySearch()
	require.Positive(t, fixture.searchMatches())

	fixture.resize()
	require.True(t, fixture.resized())
}

func TestRenderStageHooksCountProductionBoundariesOnFailedSend(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	d.paint(sess, ac, true, nil)
	_ = awaitFrame(t, sends, ports.MsgOutput)
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
	d.paint(sess, ac, false, nil)
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
	require.Equal(t, uint64(3), metrics.outputFrames, "commit, resize-failure notice toast, and successful full-reset retry")
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
	require.Equal(t, 10_000, first.tabs[0].panes[0].sealed.Len())
	require.Positive(t, first.tabs[0].panes[0].sealed.ChunkCount())

	second, ok := fixture.d.captureSnapshotState(fixture.sess, 2)
	require.True(t, ok)
	secondPane := capturePaneByID(second, first.tabs[0].panes[0].id)
	if secondPane == nil || first.tabs[0].panes[0].sealed.Chunk(0) != secondPane.sealed.Chunk(0) {
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

// BenchmarkDaemonIncrementalSnapshotRepository measures the complete
// content-addressed publication path against a filesystem-equivalent byte
// counter. Each scenario reports persisted VEVO, VEVM, and VEVH bytes, plus
// allocation/time and the per-session encoded-history cache high-water mark.
func BenchmarkDaemonIncrementalSnapshotRepository(b *testing.B) {
	for _, scenario := range snapshotBenchmarkScenarios {
		b.Run(scenario.name, func(b *testing.B) {
			benchmarkIncrementalSnapshotRepository(b, scenario)
		})
	}
}

// BenchmarkDaemonHistorySnapshotRestore measures terminal-state restoration
// from a validated incremental manifest without spawning PTY or session workers.
func BenchmarkDaemonHistorySnapshotRestore(b *testing.B) {
	benchmarkDaemonSnapshotRestore(b)
}

// BenchmarkDaemonHistoryCopyEnter uses the parent benchmark's repeated-entry
// semantics: each operation replaces the active copy document. Exit cleanup is
// intentionally outside this benchmark, so its allocations cannot be reported
// as copy-entry work.
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
			if workload == "copy-search" {
				benchmarkDaemonCopySearch(b, fixture, run)
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

func benchmarkDaemonCopySearch(b *testing.B, fixture *performanceFixture, run func(*performanceFixture)) {
	b.Helper()
	fixture.d.enterCopyMode(fixture.sess, fixture.ac)
	fixture.ac.ackOutputState(fixture.ac.output.next)
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		run(fixture)
		fixture.ac.ackOutputState(fixture.ac.output.next)
	}
	b.StopTimer()
	if fixture.searchMatches() == 0 {
		b.Fatal("copy search produced no deterministic matches")
	}
	b.ReportMetric(1, "copyoperations/op")
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
			firstChunk := firstPane.sealed.Chunk(0)
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
			if captures != b.N || !validBenchmarkCapture(last) || lastPane == nil || lastPane.sealed.Chunk(0) != firstChunk {
				b.Fatal("snapshot capture did not preserve immutable sealed history")
			}
			b.ReportMetric(float64(capturePaneCount(last)), "capturepanes/op")
		})
	}
}

type snapshotBenchmarkScenario struct {
	name        string
	size        domain.Size
	historyRows int
	baseline    bool
	mutate      func(*performanceFixture, int)
	unchanged   bool
	cellLimited bool
}

var snapshotBenchmarkScenarios = []snapshotBenchmarkScenario{
	{name: "initial-10k-x-120", size: domain.Size{Cols: 120, Rows: 40}, historyRows: 10_000},
	{name: "unchanged", size: domain.Size{Cols: 120, Rows: 40}, historyRows: 10_000, baseline: true, unchanged: true},
	{name: "visible-only", size: domain.Size{Cols: 120, Rows: 40}, historyRows: 10_000, baseline: true, mutate: mutateBenchmarkVisible},
	// Leave one row below the retention cap so the mutation changes only the
	// mutable tail and cannot evict a sealed chunk.
	{name: "tail-only", size: domain.Size{Cols: 120, Rows: 40}, historyRows: 9_999, baseline: true, mutate: mutateBenchmarkTail},
	{name: "new-sealed-chunk", size: domain.Size{Cols: 120, Rows: 40}, historyRows: 10_000, baseline: true, mutate: mutateBenchmarkSealedChunk},
	{name: "cell-limited-293-columns", size: domain.Size{Cols: 293, Rows: 40}, historyRows: 10_000, cellLimited: true},
}

func benchmarkIncrementalSnapshotRepository(b *testing.B, scenario snapshotBenchmarkScenario) {
	var (
		metrics        countingSnapshotMetrics
		peakCacheBytes int
	)
	b.ReportAllocs()
	b.ResetTimer()
	for operation := 0; b.Loop(); operation++ {
		func() {
			// A new fixture is deliberately outside the timed region. It gives every
			// iteration the same filesystem state: initial has no generation,
			// unchanged/visible/tail/sealed begin after one persisted generation.
			// Reusing one fixture would eventually turn a tail into a sealed chunk
			// and dilute initial-write bytes over later idempotent publications.
			b.StopTimer()
			// b.Cleanup retains callbacks until the sub-benchmark ends; deferring an
			// explicit close avoids retaining every 10k-row fixture and OOM.
			fixture := newPerformanceFixtureWithCleanup(b, performanceConfig{size: scenario.size, panes: 1, historyRows: scenario.historyRows}, false)
			defer func() {
				b.StopTimer()
				if err := fixture.close(); err != nil {
					b.Fatalf("close performance fixture: %v", err)
				}
			}()
			validateSnapshotBenchmarkFixture(b, fixture, scenario)
			generation := uint64(1)
			if scenario.baseline {
				publishBenchmarkSnapshot(b, fixture, generation)
				generation++
				fixture.snapshots.reset()
			}
			b.StartTimer()
			if scenario.mutate != nil {
				scenario.mutate(fixture, operation)
			}
			publishBenchmarkSnapshot(b, fixture, generation)
			b.StopTimer()

			metrics = addCountingSnapshotMetrics(metrics, fixture.snapshots.metrics())
			peakCacheBytes = max(peakCacheBytes, benchmarkSnapshotCacheBytes(fixture.sess))
		}()
		// B.Loop requires the timer to be running at the next iteration.
		b.StartTimer()
	}
	b.StopTimer()
	if scenario.unchanged && metrics.historyBlobBytes != 0 {
		b.Fatalf("unchanged snapshot wrote %d history blob bytes", metrics.historyBlobBytes)
	}
	if peakCacheBytes > snapshotChunkCacheLimit {
		b.Fatalf("snapshot cache peak = %d, limit = %d", peakCacheBytes, snapshotChunkCacheLimit)
	}
	benchmarkReportSnapshotRepositoryMetrics(b, metrics, b.N, peakCacheBytes)
}

func validateSnapshotBenchmarkFixture(b *testing.B, fixture *performanceFixture, scenario snapshotBenchmarkScenario) {
	b.Helper()
	if !fixture.hasHistoryTopology(1, 1, scenario.historyRows) && !scenario.cellLimited {
		b.Fatal("invalid incremental snapshot fixture")
	}
	if !scenario.cellLimited {
		return
	}
	fixture.activePane.mu.Lock()
	rows, cells, capCells := fixture.activePane.history.Len(), fixture.activePane.history.Cells(), fixture.activePane.history.CellCap()
	fixture.activePane.mu.Unlock()
	if rows >= scenario.historyRows || cells > capCells {
		b.Fatalf("293-column fixture did not enforce cell limit: rows=%d cells=%d cap=%d", rows, cells, capCells)
	}
}

func addCountingSnapshotMetrics(a, b countingSnapshotMetrics) countingSnapshotMetrics {
	return countingSnapshotMetrics{
		writes:               a.writes + b.writes,
		objectBytes:          a.objectBytes + b.objectBytes,
		historyBlobBytes:     a.historyBlobBytes + b.historyBlobBytes,
		suppliedObjectBytes:  a.suppliedObjectBytes + b.suppliedObjectBytes,
		suppliedHistoryBytes: a.suppliedHistoryBytes + b.suppliedHistoryBytes,
		manifestBytes:        a.manifestBytes + b.manifestBytes,
		headBytes:            a.headBytes + b.headBytes,
	}
}

func publishBenchmarkSnapshot(b testing.TB, fixture *performanceFixture, generation uint64) {
	b.Helper()
	capture, ok := fixture.d.captureSnapshotState(fixture.sess, generation)
	if !ok {
		b.Fatal("snapshot capture unexpectedly rejected benchmark fixture")
	}
	publication, err := fixture.d.incrementalPublication(capture)
	if err != nil {
		b.Fatal(err)
	}
	if err := fixture.snapshots.Publish(context.Background(), publication); err != nil {
		b.Fatal(err)
	}
	markSnapshotCaptureObjectsPublished(capture)
}

func mutateBenchmarkVisible(fixture *performanceFixture, operation int) {
	fixture.activePane.mu.Lock()
	fixture.activePane.screen.Write([]byte(fmt.Sprintf("\x1b[1;1Hvisible-%08d", operation)))
	fixture.activePane.mu.Unlock()
}

func mutateBenchmarkTail(fixture *performanceFixture, operation int) {
	fixture.activePane.mu.Lock()
	defer fixture.activePane.mu.Unlock()
	row := make([]renderer.Cell, fixture.activePane.screen.Frame.Width)
	for column := range row {
		row[column] = renderer.Cell{Rune: rune('a' + (operation+column)%26)}
	}
	if err := fixture.activePane.history.Append(row); err != nil {
		fixture.t.Fatal(err)
	}
}

func mutateBenchmarkSealedChunk(fixture *performanceFixture, operation int) {
	for row := 0; row < 256; row++ {
		mutateBenchmarkTail(fixture, operation*256+row)
	}
}

func benchmarkSnapshotCacheBytes(sess *session) int {
	sess.snapshotMu.Lock()
	defer sess.snapshotMu.Unlock()
	if sess.snapshotChunkCache == nil {
		return 0
	}
	return sess.snapshotChunkCache.used
}

func benchmarkReportSnapshotRepositoryMetrics(b *testing.B, metrics countingSnapshotMetrics, operations int, peakCacheBytes int) {
	b.Helper()
	if operations == 0 {
		return
	}
	perOperation := float64(operations)
	b.ReportMetric(float64(metrics.objectBytes)/perOperation, "objectbytes/op")
	b.ReportMetric(float64(metrics.historyBlobBytes)/perOperation, "historyblobbytes/op")
	b.ReportMetric(float64(metrics.manifestBytes)/perOperation, "manifestbytes/op")
	b.ReportMetric(float64(metrics.headBytes)/perOperation, "headbytes/op")
	b.ReportMetric(float64(peakCacheBytes), "peakcachebytes")
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
	publication, err := fixture.d.incrementalPublication(capture)
	if err != nil {
		b.Fatal(err)
	}
	snapshot, err := sessionFromGeneration(snapshotGeneration(publication))
	if err != nil || len(snapshot.Tabs) != 1 || len(snapshot.Tabs[0].Panes) == 0 {
		b.Fatalf("invalid incremental snapshot restore fixture: %v", err)
	}
	paneSnapshot := snapshot.Tabs[0].Panes[0]
	b.ReportAllocs()
	b.SetBytes(int64(snapshotPublicationBytes(publication)))
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
	return capture != nil && len(capture.tabs) > 0 && len(capture.tabs[0].panes) > 0 && capture.tabs[0].panes[0].sealed.Len() == 10_000 && capture.tabs[0].panes[0].sealed.ChunkCount() > 0
}

func snapshotGeneration(publication ports.SnapshotPublication) ports.SnapshotGeneration {
	objects := make(map[ports.SnapshotDigest][]byte, len(publication.Objects))
	for _, object := range publication.Objects {
		objects[object.Digest] = object.Data
	}
	return ports.SnapshotGeneration{Name: publication.Name, Generation: publication.Generation, Manifest: publication.Manifest, Objects: objects}
}

func snapshotObjectBytes(publication ports.SnapshotPublication) uint64 {
	var bytes uint64
	for _, object := range publication.Objects {
		bytes += uint64(len(object.Data))
	}
	return bytes
}

func snapshotPublicationBytes(publication ports.SnapshotPublication) uint64 {
	return uint64(len(publication.Manifest)) + snapshotObjectBytes(publication)
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
	b.ReportMetric(float64(metrics.snapshotManifestBytes)/perOperation, "snapshotmanifestbytes/op")
	b.ReportMetric(float64(metrics.snapshotObjectBytes)/perOperation, "snapshotobjectbytes/op")
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

// TestCopyEnterAllocationBudget protects the parent baseline plus 10%. Copy
// rendering borrows sealed VT history rows; allocating a copy per viewport row
// is a production regression even though the capture itself remains immutable.
func TestCopyEnterAllocationBudget(t *testing.T) {
	for _, tt := range []struct {
		name             string
		tabs, panes, max int
	}{
		{name: "1tab-1pane", tabs: 1, panes: 1, max: 38},
		{name: "1tab-4panes", tabs: 1, panes: 4, max: 43},
		{name: "4tabs-1pane", tabs: 4, panes: 1, max: 42},
		{name: "4tabs-4panes", tabs: 4, panes: 4, max: 48},
		{name: "8tabs-1pane", tabs: 8, panes: 1, max: 50},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, tabs: tt.tabs, panes: tt.panes, historyRows: 10_000})
			run := func() {
				fixture.d.enterCopyMode(fixture.sess, fixture.ac)
				fixture.ac.ackOutputState(fixture.ac.output.next)
			}

			// Always exercise and validate copy entry. The race detector adds
			// instrumentation allocations which are not reported by the parent
			// benchmark, so its allocation count cannot represent this budget.
			run()
			require.True(t, fixture.copyModeActive(), "copy entry must install a history-backed mode")
			if !copyEnterAllocationBudgetEnabled {
				return
			}

			allocs := testing.AllocsPerRun(20, run)
			require.LessOrEqual(t, allocs, float64(tt.max), "copy enter must stay within 10%% of the parent allocation baseline")
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
	snapshotManifestBytes    uint64
	snapshotObjectBytes      uint64
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
	snapshots           *countingSnapshotRepository
	activePane          *pane
	liveWrites          [][]byte
	paints              int
	resizeSizes         [2]domain.Size
	resizes             int
	resizedSize         domain.Size
	pty                 *scriptedPerformancePTY
	clock               *performanceClock
	killSession         func(*session, uint8, bool) error
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
	return newPerformanceFixtureWithCleanup(t, config, true)
}

func newPerformanceFixtureWithCleanup(t testing.TB, config performanceConfig, registerCleanup bool) *performanceFixture {
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
	d, sess, ac, _ := newManualSessionWithPTYsCleanup(t, registerCleanup, ptys...)
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
		snapshots:   &countingSnapshotRepository{},
		pty:         pty,
		clock:       clock,
		killSession: d.killSession,
		liveWrites:  [][]byte{[]byte("\x1b[1;1HA\x1b[2;2HA"), []byte("\x1b[1;1HB\x1b[2;2HB")},
		resizeSizes: [2]domain.Size{{Cols: 100, Rows: 30}, config.size},
	}
	WithSnapshotRepository(fixture.snapshots, nil)(d)
	d.startSnapshotEncodeWorker()
	if registerCleanup {
		t.Cleanup(d.stopSnapshotEncodeWorker)
	}
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
	d.paint(sess, ac, true, nil)
	fixture.resetMetrics()
	return fixture
}

func (f *performanceFixture) close() error {
	if f.d == nil {
		return nil
	}
	d, sess := f.d, f.sess
	sess.mu.Lock()
	sess.ephemeral = true
	sess.mu.Unlock()
	killErr := f.killSession(sess, ports.ReasonServerShutdown, false)
	d.stopSnapshotEncodeWorker()
	d.serveCancel()
	d.hardCancel()

	f.d = nil
	f.sess = nil
	f.ac = nil
	f.output = nil
	f.snapshots = nil
	f.activePane = nil
	f.pty = nil
	f.clock = nil
	f.killSession = nil
	return killErr
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
		for row := 0; p.history.Len() < historyRows && row < historyRows+p.screen.Frame.Height; row++ {
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
		f.d.paint(f.sess, f.ac, false, nil)
	}
	f.paints++
}

func (f *performanceFixture) captureSnapshot() {
	f.t.Helper()
	require.True(f.t, f.d.captureSession(f.sess))
	// Worker finalization clears snapshotPending and signals snapshotChanged on
	// every success and failure path after persistence has returned.
	awaitSnapshotIdle(f.t, f.sess)
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

func (f *performanceFixture) searchMatches() int {
	f.ac.overlays.copyMu.Lock()
	defer f.ac.overlays.copyMu.Unlock()
	if f.ac.overlays.copyMode == nil {
		return 0
	}
	return len(f.ac.overlays.copyMode.Searches)
}

func (f *performanceFixture) copyModeActive() bool {
	f.ac.overlays.copyMu.Lock()
	defer f.ac.overlays.copyMu.Unlock()
	return f.ac.overlays.copyMode != nil && f.ac.overlays.copyDocument != nil && f.ac.overlays.copyDocument.Len() >= 10_000
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
	f.d.retryResizeMembers(f.sess, f.ac, f.sess.renderCoordinator().attachmentLease(f.ac), epoch, plan.members)
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
	f.snapshots.reset()
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
	snapshots := f.snapshots.metrics()
	metrics := performanceMetrics{
		outputFrames: output.frames, outputBytes: output.bytes, outputPayloadBytes: output.payloadBytes,
		snapshotWrites: snapshots.writes, snapshotManifestBytes: snapshots.manifestBytes, snapshotObjectBytes: snapshots.objectBytes,
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

// snapshotHeadBytes is the on-disk VEVH header (magic, generation, and
// manifest digest) written by the filesystem repository for every generation.
const snapshotHeadBytes = 4 + 8 + 32

type countingSnapshotMetrics struct {
	writes, objectBytes, historyBlobBytes, suppliedObjectBytes, suppliedHistoryBytes, manifestBytes, headBytes uint64
}
type countingSnapshotRepository struct {
	mu sync.Mutex
	countingSnapshotMetrics
	objects map[string]map[ports.SnapshotDigest]struct{}
	last    []byte
}

// Publish models the bytes written by the filesystem repository: immutable
// VEVO objects are written once per session and each generation writes a VEVM
// manifest plus the mutable VEVH pointer. It intentionally does not measure
// directory metadata, temporary files, or fsyncs.
func (s *countingSnapshotRepository) Publish(_ context.Context, publication ports.SnapshotPublication) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.objects == nil {
		s.objects = make(map[string]map[ports.SnapshotDigest]struct{})
	}
	objects := s.objects[publication.Name]
	if objects == nil {
		objects = make(map[ports.SnapshotDigest]struct{})
		s.objects[publication.Name] = objects
	}
	s.writes++
	s.manifestBytes += uint64(len(publication.Manifest))
	s.headBytes += snapshotHeadBytes
	for _, object := range publication.Objects {
		s.suppliedObjectBytes += uint64(len(object.Data))
		kind, _, err := snapcodec.PreflightObject(object.Data)
		if err == nil && kind == snapcodec.HistoryChunk {
			s.suppliedHistoryBytes += uint64(len(object.Data))
		}
		if _, exists := objects[object.Digest]; exists {
			continue
		}
		objects[object.Digest] = struct{}{}
		s.objectBytes += uint64(len(object.Data))
		if err == nil && kind == snapcodec.HistoryChunk {
			s.historyBlobBytes += uint64(len(object.Data))
		}
	}
	s.last = publication.Manifest
	return nil
}
func (*countingSnapshotRepository) List(context.Context) ([]string, error) { return nil, nil }
func (*countingSnapshotRepository) Load(context.Context, string) (ports.SnapshotGeneration, error) {
	return ports.SnapshotGeneration{}, nil
}
func (*countingSnapshotRepository) Delete(context.Context, string) error          { return nil }
func (*countingSnapshotRepository) Tombstone(context.Context, string) error       { return nil }
func (*countingSnapshotRepository) DeleteTombstone(context.Context, string) error { return nil }
func (*countingSnapshotRepository) Maintain(context.Context) error                { return nil }
func (s *countingSnapshotRepository) reset() {
	s.mu.Lock()
	s.countingSnapshotMetrics = countingSnapshotMetrics{}
	s.last = nil
	s.mu.Unlock()
}
func (s *countingSnapshotRepository) metrics() countingSnapshotMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.countingSnapshotMetrics
}
func (s *countingSnapshotRepository) lastPayload() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.last
}
