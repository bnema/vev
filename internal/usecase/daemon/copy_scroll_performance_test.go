package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/stretchr/testify/require"
)

func TestCopyCompositionDoesNotAllocateIntermediateFrame(t *testing.T) {
	rows := make([][]renderer.Cell, 53)
	for y := range rows {
		rows[y] = testRow("scrollback")
	}
	mode := scopy.NewMode(scopy.NewDocument(scopy.NewSnapshotFromRows(rows, 182, 53), ""))
	frame := renderer.NewFrame(182, 55)
	target := domain.Rect{Y: 1, Width: 182, Height: 53}
	styles := resolveStyles(nil)
	result := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			composeCopyClientFrame(mode, target, frame, styles)
		}
	})
	// A reusable semantic row and status text fit in 32 KiB. A second compact
	// viewport alone needs over 150 KiB and must not be built just to copy it.
	require.LessOrEqual(t, result.AllocedBytesPerOp(), int64(32<<10))
}

// Each operation renders two wheel movements, up three rows then down three.
// Keep history setup and copy entry outside the measured steady-state scroll.
func BenchmarkDaemonHistoryCopyScroll(b *testing.B) {
	for _, size := range []struct {
		name string
		size domain.Size
	}{
		{"120x40", domain.Size{Cols: 120, Rows: 40}},
		{"182x53", domain.Size{Cols: 182, Rows: 53}},
		{"240x70", domain.Size{Cols: 240, Rows: 70}},
	} {
		b.Run(size.name, func(b *testing.B) {
			f := newPerformanceFixture(b, performanceConfig{size: size.size, tabs: 1, panes: 1, historyRows: 10000})
			require.True(b, f.hasHistoryTopology(1, 1, 10000))
			f.d.enterCopyMode(f.sess, f.ac)
			f.d.copyWheel(f.sess, f.ac, -5000)
			f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
			b.ReportAllocs()
			for b.Loop() {
				f.d.copyWheel(f.sess, f.ac, -3)
				f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
				f.d.copyWheel(f.sess, f.ac, 3)
				f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
			}
		})
	}
}
