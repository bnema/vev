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

func TestPerformanceFixtureCounters(t *testing.T) {
	fixture := newPerformanceFixture(t, performanceConfig{})

	fixture.paintLive()
	fixture.paintLive()
	metrics := fixture.metrics()
	require.Equal(t, uint64(2), metrics.outputFrames)
	require.Positive(t, metrics.outputBytes)
	require.Positive(t, metrics.outputPayloadBytes)

	fixture.captureSnapshot()
	metrics = fixture.metrics()
	require.Equal(t, uint64(1), metrics.snapshotWrites)
	require.Positive(t, metrics.snapshotBytes)

	fixture.enterCopySearch()
	require.Positive(t, fixture.searchMatches())

	fixture.resize()
	require.True(t, fixture.resized())
}

// performanceConfig intentionally describes only an in-process daemon shape:
// no PTY readers, schedulers, clocks, or transports are started by this fixture.
type performanceConfig struct {
	size domain.Size
}

type performanceMetrics struct {
	outputFrames       uint64
	outputBytes        uint64
	outputPayloadBytes uint64
	snapshotWrites     uint64
	snapshotBytes      uint64
}

type performanceFixture struct {
	t           testing.TB
	d           *Daemon
	sess        *session
	ac          *attachedClient
	output      *countingOutputTransport
	snaps       *countingSnapshotStore
	paints      int
	resizedSize domain.Size
}

func newPerformanceFixture(t testing.TB, config performanceConfig) *performanceFixture {
	t.Helper()
	if !config.size.Valid() {
		config.size = domain.Size{Cols: 80, Rows: 24}
	}
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil, nil)
	output := &countingOutputTransport{}
	ac.tr = output
	ac.size = config.size
	sess.name = "performance"
	sess.ephemeral = false
	sess.snapEligible.Store(true)

	fixture := &performanceFixture{t: t, d: d, sess: sess, ac: ac, output: output, snaps: &countingSnapshotStore{}}
	WithSnapshotStore(fixture.snaps)(d)
	for tabIndex, tb := range sess.tabs {
		fixture.configureTab(tb, tabIndex, tabSize(config.size))
	}

	// Prime the real renderer shadow before measurements. Subsequent paints use
	// actual production diffs rather than the initial full frame.
	d.paint(sess, ac, true)
	fixture.resetMetrics()
	return fixture
}

func (f *performanceFixture) configureTab(tb *tab, tabIndex int, size domain.Size) {
	f.t.Helper()
	tb.mu.Lock()
	tb.size = size
	left := tb.focusedPane()
	rightID := layout.PaneID(fmt.Sprintf("pane-%d-right", tabIndex+1))
	right := newPane(rightID, nil, size)
	tb.panes[rightID] = right
	require.NoError(f.t, tb.tree.Split(left.id, layout.Right, true, rightID, domain.Rect{Width: size.Cols, Height: size.Rows}))
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: size.Cols, Height: size.Rows})
	require.True(f.t, ok, "performance fixture layout must be solvable")
	require.Len(f.t, placements, 2)
	f.d.applyLayoutLocked(tb)
	for _, p := range tb.panes {
		p.mu.Lock()
		width, height := p.screen.Frame.Width, p.screen.Frame.Height
		for row := range max(height-1, 1) {
			p.screen.Write([]byte(performanceFullWidthRow(width, tabIndex, row) + "\r\n"))
		}
		p.mu.Unlock()
	}
	tb.mu.Unlock()
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

func (f *performanceFixture) activePane() *pane {
	f.t.Helper()
	tb := f.sess.activeTab()
	require.NotNil(f.t, tb)
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return tb.focusedPane()
}

func (f *performanceFixture) paintLive() {
	f.t.Helper()
	p := f.activePane()
	x, y := 1, 1
	if f.paints%2 == 1 {
		x, y = 2, 2
	}
	p.mu.Lock()
	p.screen.Write([]byte(fmt.Sprintf("\x1b[%d;%dH%c", y, x, 'A'+rune(f.paints))))
	p.mu.Unlock()
	f.d.paint(f.sess, f.ac, false)
	f.paints++
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
	f.resizedSize = domain.Size{Cols: 100, Rows: 30}
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
}

func (f *performanceFixture) metrics() performanceMetrics {
	output := f.output.metrics()
	snapshots := f.snaps.metrics()
	return performanceMetrics{
		outputFrames: output.frames, outputBytes: output.bytes, outputPayloadBytes: output.payloadBytes,
		snapshotWrites: snapshots.writes, snapshotBytes: snapshots.bytes,
	}
}

type countingOutputMetrics struct{ frames, bytes, payloadBytes uint64 }

type countingOutputTransport struct {
	mu sync.Mutex
	countingOutputMetrics
}

func (t *countingOutputTransport) Send(frame ports.Frame) error {
	if frame.Type != ports.MsgOutput {
		return nil
	}
	output, err := ports.UnmarshalOutput(frame.Payload)
	if err != nil {
		return err
	}
	t.mu.Lock()
	t.frames++
	t.bytes += uint64(len(output.Data))
	t.payloadBytes += uint64(len(frame.Payload))
	t.mu.Unlock()
	return nil
}
func (*countingOutputTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*countingOutputTransport) Close() error               { return nil }
func (t *countingOutputTransport) reset() {
	t.mu.Lock()
	t.countingOutputMetrics = countingOutputMetrics{}
	t.mu.Unlock()
}
func (t *countingOutputTransport) metrics() countingOutputMetrics {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.countingOutputMetrics
}

type countingSnapshotMetrics struct{ writes, bytes uint64 }
type countingSnapshotStore struct {
	mu sync.Mutex
	countingSnapshotMetrics
}

func (s *countingSnapshotStore) Write(_ string, data []byte) error {
	if _, err := snapcodec.Unmarshal(data); err != nil {
		return err
	}
	s.mu.Lock()
	s.writes++
	s.bytes += uint64(len(data))
	s.mu.Unlock()
	return nil
}
func (*countingSnapshotStore) Load() ([]ports.SnapshotBlob, error) { return nil, nil }
func (*countingSnapshotStore) Delete(string) error                 { return nil }
func (s *countingSnapshotStore) reset() {
	s.mu.Lock()
	s.countingSnapshotMetrics = countingSnapshotMetrics{}
	s.mu.Unlock()
}
func (s *countingSnapshotStore) metrics() countingSnapshotMetrics {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.countingSnapshotMetrics
}
