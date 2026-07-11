package copy

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestNewSnapshotFreezesHistoryAndVisibleScreen(t *testing.T) {
	sb := NewScrollback(1)
	sb.Append(row("old"))
	screen := renderer.NewFrame(3, 1)
	copy(screen.Row(0), row("one"))

	snapshot := NewSnapshot(sb, screen)
	if &snapshot.Row(0)[0] != &sb.View().Row(0)[0] {
		t.Fatal("NewSnapshot() deep-copied immutable history")
	}
	sb.Append(row("new"))
	screen.Set(0, 0, renderer.Cell{Rune: 'x'})

	if got := snapshot.Len(); got != 2 {
		t.Fatalf("Len() = %d, want 2", got)
	}
	for i, want := range []string{"old", "one"} {
		if got := rowText(snapshot.Row(i)); got != want {
			t.Errorf("Row(%d) = %q, want %q", i, got, want)
		}
	}
	for _, i := range []int{-1, 2} {
		if got := snapshot.Row(i); got != nil {
			t.Errorf("Row(%d) = %v, want nil", i, got)
		}
	}
}

func TestNewSnapshotFromRowsOwnsCallerRows(t *testing.T) {
	rows := [][]renderer.Cell{row("one")}
	snapshot := NewSnapshotFromRows(rows, 3, 1)
	rows[0][0].Rune = 'x'
	if got := rowText(snapshot.Row(0)); got != "one" {
		t.Fatalf("Row(0) = %q, want deep-owned caller row", got)
	}
}

// This scaling gate deliberately compares equal row counts rather than an
// absolute budget, which varies between Go releases. A full history-row clone
// grows with row width; the immutable HistoryView allocation does not.
func TestNewSnapshotAllocationIsWidthIndependent(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping benchmark-driven allocation gate in short mode")
	}
	narrow := benchmarkNewSnapshotBytes(16)
	wide := benchmarkNewSnapshotBytes(512)
	assertWidthIndependentAllocations(t, "NewSnapshot", narrow, wide)
}

func benchmarkNewSnapshotBytes(width int) int64 {
	const historyRows = 256

	scrollback := NewScrollback(historyRows)
	for range historyRows {
		scrollback.Append(make([]renderer.Cell, width))
	}
	// Keep visible-frame work constant so the comparison isolates history rows.
	screen := renderer.NewFrame(8, 4)
	result := testing.Benchmark(func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for b.Loop() {
			benchmarkSnapshotSink = NewSnapshot(scrollback, screen)
		}
	})
	return result.AllocedBytesPerOp()
}

func assertWidthIndependentAllocations(t *testing.T, operation string, narrow, wide int64) {
	t.Helper()
	// Two times permits allocator noise while decisively rejecting a cell-row
	// copy, whose allocation grows by 32x for the widths above.
	const conservativeWidthTolerance = 2
	if wide > narrow*conservativeWidthTolerance {
		t.Fatalf("%s B/op scaled with row width: narrow=%d wide=%d (limit=%d)", operation, narrow, wide, narrow*conservativeWidthTolerance)
	}
}
