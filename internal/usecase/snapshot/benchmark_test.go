package snapshot

import (
	"testing"

	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

var (
	benchmarkV3BytesSink   []byte
	benchmarkV3SessionSink Session
)

func BenchmarkSnapshotV3Marshal10KRows(b *testing.B) {
	session, encoded := benchmarkV3Session(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkV3BytesSink, err = Marshal(session)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if _, err := Unmarshal(benchmarkV3BytesSink); err != nil {
		b.Fatalf("marshal produced an invalid v3 snapshot: %v", err)
	}
}

func BenchmarkSnapshotV3Unmarshal10KRows(b *testing.B) {
	_, encoded := benchmarkV3Session(b)
	b.ReportAllocs()
	b.SetBytes(int64(len(encoded)))
	b.ResetTimer()
	for b.Loop() {
		var err error
		benchmarkV3SessionSink, err = Unmarshal(encoded)
		if err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	if len(benchmarkV3SessionSink.Tabs) != 1 || len(benchmarkV3SessionSink.Tabs[0].Panes) != 1 || len(benchmarkV3SessionSink.Tabs[0].Panes[0].SealedChunks) != 40 {
		b.Fatal("unmarshal lost canonical 10k-row snapshot data")
	}
}

func benchmarkV3Session(t testing.TB) (Session, []byte) {
	t.Helper()
	const (
		cols        = 120
		rows        = 40
		historyRows = 10_000
		chunkRows   = 256
	)
	history := vt.NewHistory(vt.HistoryConfig{MaxRows: historyRows, ChunkRows: chunkRows})
	for row := range historyRows {
		cells := make([]renderer.Cell, cols)
		for col := range cells {
			cells[col] = renderer.Cell{Rune: rune('a' + (row+col)%26)}
		}
		history.Append(cells)
	}
	sealed, tail, err := vt.MarshalSealedHistory(history.SealAndView())
	if err != nil {
		t.Fatal(err)
	}
	visibleFrame := renderer.NewFrame(cols, rows)
	for y := range rows {
		for x := range cols {
			visibleFrame.Set(x, y, renderer.Cell{Rune: rune('A' + (x+y)%26)})
		}
	}
	visible, err := vt.MarshalVisible(visibleFrame)
	if err != nil {
		t.Fatal(err)
	}
	session := Session{
		Name:      "benchmark",
		CreatedAt: 1,
		Tabs: []Tab{{
			StableID:   "tab-1",
			Cols:       cols,
			Rows:       rows,
			NextPaneID: 2,
			Focus:      "pane-1",
			Tree:       layout.NewTree("pane-1"),
			Panes: []Pane{{
				ID:           "pane-1",
				StableID:     "pane-1",
				Cwd:          "/benchmark",
				SealedChunks: sealed,
				Tail:         tail,
				Visible:      visible,
			}},
		}},
	}
	encoded, err := Marshal(session)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := Unmarshal(encoded)
	if err != nil {
		t.Fatal(err)
	}
	paneCount := 0
	decodedChunks := 0
	if len(decoded.Tabs) == 1 {
		paneCount = len(decoded.Tabs[0].Panes)
		if paneCount == 1 {
			decodedChunks = len(decoded.Tabs[0].Panes[0].SealedChunks)
		}
	}
	if len(sealed) != (historyRows+chunkRows-1)/chunkRows || paneCount != 1 || decodedChunks != len(sealed) {
		t.Fatalf("invalid v3 benchmark fixture: chunks=%d tabs=%d panes=%d decoded chunks=%d", len(sealed), len(decoded.Tabs), paneCount, decodedChunks)
	}
	return session, encoded
}
