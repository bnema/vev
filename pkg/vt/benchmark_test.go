package vt

import (
	"bytes"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

var (
	benchmarkHistoryViewSink     HistoryView
	benchmarkHistorySnapshotSink HistorySnapshotView
	benchmarkHistoryBlobSink     []byte
	benchmarkHistoryBlobsSink    [][]byte
)

// TestHistoryViewPartialTailAllocationBounded guards the capture property used
// by persistence: retained sealed chunks are shared, while only the small
// mutable tail is copied into a view.
func TestHistoryViewPartialTailAllocationBounded(t *testing.T) {
	const (
		width     = 120
		chunkRows = 64
		tailRows  = 7
	)
	sealed := benchmarkHistory(t, chunkRows*4, width, chunkRows)
	sealed.SealAndView()
	partial := benchmarkHistory(t, chunkRows*4+tailRows, width, chunkRows)
	partialBefore := partial.View()

	sealedAllocs := testing.AllocsPerRun(20, func() { benchmarkHistoryViewSink = sealed.View() })
	partialAllocs := testing.AllocsPerRun(20, func() { benchmarkHistoryViewSink = partial.View() })
	if partialAllocs > sealedAllocs+tailRows+3 {
		t.Fatalf("partial-tail View allocations = %v, sealed=%v, tail rows=%d; sealed history was copied", partialAllocs, sealedAllocs, tailRows)
	}
	if partial.View().Chunk(0) != partialBefore.Chunk(0) {
		t.Fatal("unchanged sealed chunks must retain identity across partial-tail views")
	}
}

func BenchmarkHistoryView10KRowsSealedReuse(b *testing.B) {
	history := benchmarkHistory(b, 10_000, 120, 256)
	sealed := history.SealAndView()
	if sealed.Len() != 10_000 || sealed.ChunkCount() != 40 || sealed.Chunk(0) == nil {
		b.Fatalf("invalid sealed history fixture: rows=%d chunks=%d", sealed.Len(), sealed.ChunkCount())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkHistoryViewSink = history.View()
	}
	b.StopTimer()
	if got := benchmarkHistoryViewSink; got.Len() != sealed.Len() || got.Chunk(0) != sealed.Chunk(0) {
		b.Fatal("unchanged sealed history chunks were not reused")
	}
	b.ReportMetric(float64(sealed.ChunkCount()), "sealedchunks/view")
}

func BenchmarkHistoryView10KRowsPartialTail(b *testing.B) {
	history := benchmarkHistory(b, 10_000, 120, 256)
	before := history.View()
	if before.Len() != 10_000 || before.ChunkCount() != 40 || before.Chunk(0) == nil {
		b.Fatalf("invalid partial-tail history fixture: rows=%d chunks=%d", before.Len(), before.ChunkCount())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkHistoryViewSink = history.View()
	}
	b.StopTimer()
	if got := benchmarkHistoryViewSink; got.Len() != before.Len() || got.Chunk(0) != before.Chunk(0) {
		b.Fatal("partial-tail capture did not reuse its sealed chunks")
	}
	b.ReportMetric(float64(10_000%256), "tailrows/view")
}

func BenchmarkHistorySnapshotView10KRowsPartialTail(b *testing.B) {
	history := benchmarkHistory(b, 10_000, 120, 256)
	if got, want := len(history.tail), 16; got != want {
		b.Fatalf("partial tail rows = %d, want %d", got, want)
	}
	sealed := history.View().Chunk(0)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		benchmarkHistorySnapshotSink = history.SnapshotView()
	}
	b.StopTimer()
	if got := benchmarkHistorySnapshotSink; got.Chunk(0) != sealed || got.Tail().Len() != 16 {
		b.Fatal("snapshot did not preserve chunk identity and tail rows")
	}
	b.ReportMetric(float64(benchmarkHistorySnapshotSink.ChunkCount()), "sealedchunks/view")
}

func BenchmarkMarshalHistoryChunk256x120(b *testing.B) {
	history := benchmarkHistory(b, 256, 120, 256)
	chunk := history.SealAndView().Chunk(0)
	if chunk == nil {
		b.Fatal("invalid history chunk fixture")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkHistoryBlobSink, err = MarshalHistoryChunk(chunk)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if _, err := PreflightHistoryBlob(benchmarkHistoryBlobSink); err != nil {
		b.Fatalf("encoded history chunk is invalid: %v", err)
	}
	b.SetBytes(int64(len(benchmarkHistoryBlobSink)))
}

func BenchmarkUnmarshalHistoryChunk256x120(b *testing.B) {
	history := benchmarkHistory(b, 256, 120, 256)
	blob, err := MarshalHistoryChunk(history.SealAndView().Chunk(0))
	if err != nil {
		b.Fatal(err)
	}
	if stats, err := PreflightHistoryBlob(blob); err != nil || stats.Rows != 256 || stats.Cells != 256*120 {
		b.Fatalf("invalid encoded history chunk fixture: stats=%+v err=%v", stats, err)
	}

	b.ReportAllocs()
	b.SetBytes(int64(len(blob)))
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkHistoryViewSink, err = UnmarshalHistory(blob)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if benchmarkHistoryViewSink.Len() != 256 || benchmarkHistoryViewSink.ChunkCount() != 1 {
		b.Fatal("decoded history chunk lost rows")
	}
}

func BenchmarkMarshalSealedHistory10KRows(b *testing.B) {
	history := benchmarkHistory(b, 10_000, 120, 256)
	view := history.SealAndView()
	if view.Len() != 10_000 || view.ChunkCount() != 40 {
		b.Fatalf("invalid sealed history fixture: rows=%d chunks=%d", view.Len(), view.ChunkCount())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkHistoryBlobsSink, benchmarkHistoryBlobSink, err = MarshalSealedHistory(view)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if len(benchmarkHistoryBlobsSink) != view.ChunkCount() || len(benchmarkHistoryBlobSink) == 0 {
		b.Fatal("sealed-history encoder produced an incomplete capture")
	}
	b.ReportMetric(float64(len(benchmarkHistoryBlobsSink)), "sealedchunks/op")
}

func benchmarkHistory(t testing.TB, rows, width, chunkRows int) *History {
	t.Helper()
	history := NewHistory(HistoryConfig{MaxRows: rows, ChunkRows: chunkRows})
	for row := range rows {
		cells := make([]renderer.Cell, width)
		for col := range cells {
			cells[col] = renderer.Cell{Rune: rune('a' + (row+col)%26)}
		}
		requireHistoryAppend(t, history, cells)
	}
	return history
}

// BenchmarkScreenResizeReflowViewport verifies resize only visits the live
// viewport. The 10K-history variant must track the control rather than history
// length; history is deliberately populated without changing the viewport.
func BenchmarkScreenResizeReflowViewport(b *testing.B) {
	b.Run("control", func(b *testing.B) { benchmarkScreenResizeReflow(b, 0) })
	b.Run("10k-history", func(b *testing.B) { benchmarkScreenResizeReflow(b, 10_000) })
}

func benchmarkScreenResizeReflow(b *testing.B, historyRows int) {
	b.Helper()
	s := NewScreenWithHistory(120, 40, HistoryConfig{MaxRows: historyRows + 500})
	if historyRows > 0 {
		for range historyRows {
			requireHistoryAppend(b, s.history, make([]renderer.Cell, 120))
		}
	}
	s.Write(bytes.Repeat([]byte("x"), 120*39))
	s.ClearDamage()
	sizes := [2][2]int{{80, 40}, {120, 40}}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		s.Resize(sizes[i%len(sizes)][0], sizes[i%len(sizes)][1])
		s.ClearDamage()
	}
	b.StopTimer()
	if got := s.History(); historyRows > 0 && got.Len() < historyRows {
		b.Fatalf("resize discarded retained history rows: got %d want at least %d", got.Len(), historyRows)
	}
}

func BenchmarkScreenShellRedrawBurst(b *testing.B) {
	chunk := []byte("\r\x1b[K❯ abc\x1b[90m autosuggestion\x1b[39m\r\x1b[5C")
	b.ReportAllocs()
	for b.Loop() {
		s := NewScreen(120, 40)
		for range 200 {
			s.Write(chunk)
			s.ClearDamage()
		}
	}
}

func BenchmarkScreenFullscreenScrollRegion(b *testing.B) {
	chunk := []byte("\x1b[2;39r\x1b[39;1Hline\n")
	b.ReportAllocs()
	for b.Loop() {
		s := NewScreen(120, 40)
		for range 500 {
			s.Write(chunk)
			s.ClearDamage()
		}
	}
}
