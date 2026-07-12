package daemon

import (
	"fmt"
	"io"
	"sync"
	"testing"

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

func TestPerformanceFixtureResizeAlternatesRealDimensions(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}})

	for _, want := range []domain.Size{{Cols: 100, Rows: 30}, {Cols: 120, Rows: 40}} {
		fixture.resize()
		require.Equal(t, want, fixture.ac.size)
		require.True(t, fixture.resized())
	}
}

func TestPerformanceFixtureLargeHistoryTopology(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, tabs: 4, panes: 4, historyRows: 10_000})

	require.Len(t, fixture.sess.tabs, 4)
	for _, tb := range fixture.sess.tabs {
		require.Len(t, tb.panes, 4)
		for _, p := range tb.panes {
			require.Equal(t, 10_000, p.scrollback.Len())
		}
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

func BenchmarkDaemonHistoryLivePaint(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "live-paint", func(f *performanceFixture) { f.paintLive() })
}
func BenchmarkDaemonHistorySnapshotCapture(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "snapshot-capture", func(f *performanceFixture) { f.captureSnapshot() })
}
func BenchmarkDaemonHistoryCopyEnter(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "copy-enter", func(f *performanceFixture) { f.d.enterCopyMode(f.sess, f.ac) })
}
func BenchmarkDaemonHistoryCopySearch(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "copy-search", func(f *performanceFixture) { f.d.handleInput(f.sess, f.ac, []byte("/needle\r")) })
}
func BenchmarkDaemonHistoryResize(b *testing.B) {
	benchmarkDaemonLargeHistory(b, "resize", func(f *performanceFixture) { f.resize() })
}

func benchmarkDaemonLargeHistory(b *testing.B, workload string, run func(*performanceFixture)) {
	b.Helper()
	for _, topology := range daemonHistoryTopologies {
		b.Run(topology.name, func(b *testing.B) {
			fixture := newPerformanceFixture(b, performanceConfig{
				size: domain.Size{Cols: 120, Rows: 40}, tabs: topology.tabs, panes: topology.panes, historyRows: 10_000,
			})
			if workload == "copy-search" {
				fixture.d.enterCopyMode(fixture.sess, fixture.ac)
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
			if workload == "snapshot-capture" {
				payload := fixture.snaps.lastPayload()
				if payload == nil {
					b.Fatal("snapshot capture retained no snapshot payload")
				}
				if _, err := snapcodec.Unmarshal(payload); err != nil {
					b.Fatalf("decode last snapshot: %v", err)
				}
			}
			if workload == "live-paint" && metrics.outputFrames != uint64(b.N) {
				b.Fatalf("live paint emitted %d frames for %d operations", metrics.outputFrames, b.N)
			}
			if workload == "snapshot-capture" && metrics.snapshotWrites != uint64(b.N) {
				b.Fatalf("snapshot capture wrote %d snapshots for %d operations", metrics.snapshotWrites, b.N)
			}
			benchmarkReportMetrics(b, metrics, b.N, 10_000)
		})
	}
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
	ptys := make([]ports.PTY, config.tabs)
	d, sess, ac, _ := newManualSessionWithPTYs(t, ptys...)
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
		snaps:       &countingSnapshotStore{},
		liveWrites:  [][]byte{[]byte("\x1b[1;1HA\x1b[2;2HA"), []byte("\x1b[1;1HB\x1b[2;2HB")},
		resizeSizes: [2]domain.Size{{Cols: 100, Rows: 30}, config.size},
	}
	WithSnapshotStore(fixture.snaps)(d)
	for tabIndex, tb := range sess.tabs {
		fixture.configureTab(tb, tabIndex, config.panes, config.historyRows, tabSize(config.size))
	}
	fixture.activePane = fixture.findActivePane()

	// The fixture uses the production coordinator. stubClock's inert timer is
	// completed synchronously by the coordinator, keeping benchmark operations
	// deterministic while retaining their real invalidation path.
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
		for row := 0; p.scrollback.Len() < historyRows; row++ {
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
	// The production coordinator is synchronous under the fixture's inert
	// clock, so this request completes all three single-owner pipeline stages.
	f.renderCaptures++
	f.renderCompositions++
	f.renderEmissions++
}

func (f *performanceFixture) captureSnapshot() {
	f.t.Helper()
	require.True(f.t, f.d.captureSession(f.sess))
}

func (f *performanceFixture) enterCopySearch() {
	f.t.Helper()
	f.d.enterCopyMode(f.sess, f.ac)
	f.d.handleInput(f.sess, f.ac, []byte("/needle\r"))
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
	f.resizedSize = f.resizeSizes[f.resizes%len(f.resizeSizes)]
	f.resizes++
	f.d.resize(f.sess, f.ac, f.resizedSize)
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
}

func (s *countingSnapshotStore) Write(_ string, data []byte) error {
	s.mu.Lock()
	s.writes++
	s.bytes += uint64(len(data))
	s.last = data
	s.mu.Unlock()
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
