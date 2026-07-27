package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

var (
	recoveryTranscriptSnapshotSink RecoveryTranscriptSnapshot
	recoveryTranscriptScreenSink   *Screen
)

func TestRecoveryTranscriptSnapshotOwnsPrimaryCellsAndBounds(t *testing.T) {
	s := NewScreen(4, 2)
	s.Write([]byte("keep"))

	snapshot := s.RecoveryTranscriptSnapshot()
	s.Frame.Set(0, 0, renderer.BlankCell())
	s.buffer.boundaries[0] = LineBound{}

	view := decodeRecoveryTranscript(t, snapshot)
	require.Equal(t, []string{"keep"}, historyViewTexts(view))
	require.Equal(t, LineBound{End: 4}, view.Bound(0))
}

func TestRecoveryTranscriptSnapshotRetainsReflowBounds(t *testing.T) {
	s := NewScreen(5, 3)
	s.Write([]byte("abcdefgh"))

	view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

	require.Equal(t, []string{"abcde", "fgh  "}, historyViewTexts(view))
	require.Equal(t, LineBound{End: 5, Soft: true}, view.Bound(0))
	require.Equal(t, LineBound{End: 3}, view.Bound(1))
}

func TestRecoveryTranscriptSnapshotAllocationsDoNotScaleWithRetainedRows(t *testing.T) {
	const (
		shortHeight = 8
		tallHeight  = 512
	)
	newRetainedScreen := func(height int) *Screen {
		screen := NewScreen(4, height)
		for y := range height {
			screen.Frame.Set(0, y, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
			screen.buffer.boundaries[y] = LineBound{End: 1}
		}
		return screen
	}
	short, tall := newRetainedScreen(shortHeight), newRetainedScreen(tallHeight)

	shortAllocs := testing.AllocsPerRun(20, func() {
		recoveryTranscriptSnapshotSink = short.RecoveryTranscriptSnapshot()
	})
	tallAllocs := testing.AllocsPerRun(20, func() {
		recoveryTranscriptSnapshotSink = tall.RecoveryTranscriptSnapshot()
	})

	// Allow a small fixed variance while rejecting the former allocation per row.
	if tallAllocs > shortAllocs+4 {
		t.Fatalf("snapshot allocations scale with retained rows: height %d = %.0f, height %d = %.0f", shortHeight, shortAllocs, tallHeight, tallAllocs)
	}
}

func TestRecoveryTranscriptSnapshotOrdersPrimaryThenActiveAlternateAndHardensSeams(t *testing.T) {
	s := NewScreen(4, 3)
	s.Write([]byte("prim"))
	s.buffer.boundaries[0].Soft = true
	s.Write([]byte("\x1b[?1049h"))
	s.Write([]byte("alt"))
	s.buffer.boundaries[0].Soft = true

	snapshot := s.RecoveryTranscriptSnapshot()
	s.Frame.Set(0, 0, renderer.BlankCell())
	s.buffer.boundaries[0] = LineBound{}
	s.alternate.buffer.frame.Set(0, 0, renderer.BlankCell())
	s.alternate.buffer.boundaries[0] = LineBound{}

	view := decodeRecoveryTranscript(t, snapshot)

	require.Equal(t, []string{"prim", "alt "}, historyViewTexts(view))
	require.Equal(t, LineBound{End: 4}, view.Bound(0), "saved-primary seam must be hard")
	require.Equal(t, LineBound{End: 3}, view.Bound(1), "final alternate seam must be hard")
}

func TestRecoveryTranscriptSnapshotTrimsEachViewportIndependently(t *testing.T) {
	s := NewScreen(4, 3)
	s.Write([]byte("main"))
	s.Write([]byte("\x1b[?1049h"))
	s.Write([]byte("alt"))

	view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

	require.Equal(t, []string{"main", "alt "}, historyViewTexts(view))
}

func TestRecoveryTranscriptSnapshotExcludesExistingBoundedHistory(t *testing.T) {
	s := NewScreenWithHistory(4, 2, HistoryConfig{MaxRows: 2})
	require.NoError(t, s.History().Append(historyRow("old"), LineBound{End: 3}))
	s.Write([]byte("live"))

	view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

	require.Equal(t, []string{"live"}, historyViewTexts(view))
}

func TestRecoveryTranscriptSnapshotTrimsOnlyTrailingUntouchedRows(t *testing.T) {
	styledBlank := renderer.BlankCell()
	styledBlank.Style.Bold = true

	tests := []struct {
		name           string
		candidateCell  renderer.Cell
		candidateBound LineBound
		wantRows       int
		wantCell       renderer.Cell
	}{
		{
			name:          "untouched default blank with zero hard bound",
			candidateCell: renderer.BlankCell(),
			wantRows:      1,
		},
		{
			name:           "written default space",
			candidateCell:  renderer.BlankCell(),
			candidateBound: LineBound{End: 1},
			wantRows:       2,
			wantCell:       renderer.BlankCell(),
		},
		{
			name:          "styled blank",
			candidateCell: styledBlank,
			wantRows:      2,
			wantCell:      styledBlank,
		},
		{
			name:           "soft blank",
			candidateCell:  renderer.BlankCell(),
			candidateBound: LineBound{Soft: true},
			wantRows:       2,
			wantCell:       renderer.BlankCell(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewScreen(3, 3)
			s.Frame.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
			s.buffer.boundaries[0] = LineBound{End: 1}
			s.Frame.Set(0, 1, tt.candidateCell)
			s.buffer.boundaries[1] = tt.candidateBound

			view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

			require.Equal(t, tt.wantRows, view.Len())
			if tt.wantRows == 2 {
				require.True(t, view.Row(1)[0].Equal(tt.wantCell))
				require.False(t, view.Bound(1).Soft, "the retained segment's final row must be hardened")
			}
		})
	}
}

func TestRecoveryTranscriptSnapshotPreservesLeadingAndInternalBlankRows(t *testing.T) {
	s := NewScreen(3, 4)
	s.Frame.Set(0, 2, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	s.buffer.boundaries[2] = LineBound{End: 1}

	view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

	require.Equal(t, []string{"   ", "   ", "x  "}, historyViewTexts(view))
	require.Equal(t, []LineBound{{}, {}, {End: 1}}, []LineBound{view.Bound(0), view.Bound(1), view.Bound(2)})
}

func TestRecoveryTranscriptSnapshotPreservesBoundsAndHardensOnlyTheFinalRow(t *testing.T) {
	s := NewScreen(4, 3)
	s.buffer.boundaries = []LineBound{{End: 4, Soft: true}, {End: 2}, {End: 3, Soft: true}}

	view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

	require.Equal(t, []LineBound{{End: 4, Soft: true}, {End: 2}, {End: 3}}, []LineBound{view.Bound(0), view.Bound(1), view.Bound(2)})
}

func TestRecoveryTranscriptSnapshotMarshalsCanonicalEmptyHistory(t *testing.T) {
	s := NewScreen(4, 2)

	got, err := s.RecoveryTranscriptSnapshot().Marshal()
	require.NoError(t, err)
	want, err := MarshalHistory(HistoryView{})
	require.NoError(t, err)

	require.Equal(t, want, got)
	view, err := UnmarshalHistory(got)
	require.NoError(t, err)
	require.Zero(t, view.Len())
	require.Zero(t, view.ChunkCount())
}

func TestRecoveryTranscriptSnapshotChunksMoreThan256RowsCanonically(t *testing.T) {
	s := NewScreen(1, 257)
	for y := range s.Frame.Height {
		s.Frame.Set(0, y, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
		s.buffer.boundaries[y] = LineBound{End: 1, Soft: true}
	}

	view := decodeRecoveryTranscript(t, s.RecoveryTranscriptSnapshot())

	require.Equal(t, 257, view.Len())
	require.Equal(t, 2, view.ChunkCount())
	require.Len(t, view.Chunk(0).rows, 256)
	require.Len(t, view.Chunk(1).rows, 1)
	require.True(t, view.Bound(255).Soft, "a chunk boundary is not a transcript seam")
	require.False(t, view.Bound(256).Soft, "the transcript's final seam must be hard")
}

func TestRecoveryTranscriptSnapshotPreservesWideCells(t *testing.T) {
	s := NewScreen(4, 2)
	s.Write([]byte("界"))
	want := append([]renderer.Cell(nil), s.Frame.Row(0)...)

	snapshot := s.RecoveryTranscriptSnapshot()
	for x := range s.Frame.Width {
		s.Frame.Set(x, 0, renderer.BlankCell())
	}

	view := decodeRecoveryTranscript(t, snapshot)
	require.Equal(t, want, view.Row(0))
	require.Equal(t, LineBound{End: 2}, view.Bound(0))
	require.True(t, view.Row(0)[1].Continuation)
}

func TestNewScreenWithRecoveryTranscriptRestoresHistoryThenTranscriptAndStartsBlank(t *testing.T) {
	sealed, tail := recoveryHistoryBlobs(t, []string{"sealed-a", "sealed-b", "tail"})
	transcript := recoveryTranscriptBlob(t,
		[]string{"primary", "alternate"},
		[]LineBound{{End: 7, Soft: true}, {End: 9, Soft: true}},
	)

	screen, err := NewScreenWithRecoveryTranscript(5, 2, HistoryConfig{MaxRows: 8, MaxCells: 128, ChunkRows: 2}, sealed, tail, transcript)
	require.NoError(t, err)

	view := screen.History().View()
	require.Equal(t, []string{"sealed-a", "sealed-b", "tail", "primary", "alternate"}, historyViewTexts(view))
	require.Equal(t, LineBound{End: 7, Soft: true}, view.Bound(3))
	require.Equal(t, LineBound{End: 9}, view.Bound(4), "the restored transcript must end at a hard seam")
	for y := range screen.Frame.Height {
		for x := range screen.Frame.Width {
			require.True(t, screen.Frame.At(x, y).Equal(renderer.BlankCell()), "cell (%d,%d) must start blank", x, y)
		}
	}
	require.Equal(t, []LineBound{{}, {}}, screen.LineBounds())
}

func TestNewScreenWithRecoveryTranscriptEvictsOldestRowsWithinBounds(t *testing.T) {
	tests := []struct {
		name       string
		config     HistoryConfig
		history    []string
		transcript []string
		want       []string
	}{
		{
			name:       "row budget",
			config:     HistoryConfig{MaxRows: 3, MaxCells: 128, ChunkRows: 2},
			history:    []string{"old-1", "old-2", "old-3"},
			transcript: []string{"new-1", "new-2"},
			want:       []string{"old-3", "new-1", "new-2"},
		},
		{
			name:       "cell budget",
			config:     HistoryConfig{MaxRows: 10, MaxCells: 4, ChunkRows: 4},
			history:    []string{"aa", "b", "ccc"},
			transcript: []string{"d", "ee"},
			want:       []string{"d", "ee"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sealed, tail := recoveryHistoryBlobs(t, tt.history)
			transcript := recoveryTranscriptBlob(t, tt.transcript, nil)

			screen, err := NewScreenWithRecoveryTranscript(4, 2, tt.config, sealed, tail, transcript)
			require.NoError(t, err)
			require.Equal(t, tt.want, historyViewTexts(screen.History().View()))
		})
	}
}

func TestNewScreenWithRecoveryTranscriptRejectsOversizedTranscriptRow(t *testing.T) {
	sealed, tail := recoveryHistoryBlobs(t, []string{"ok"})
	transcript := recoveryTranscriptBlob(t, []string{"wide"}, nil)

	screen, err := NewScreenWithRecoveryTranscript(4, 2, HistoryConfig{MaxRows: 4, MaxCells: 3}, sealed, tail, transcript)

	require.ErrorIs(t, err, ErrHistoryRowTooWide)
	require.Nil(t, screen)
}

func BenchmarkRecoveryTranscriptSnapshot(b *testing.B) {
	for _, tt := range []struct {
		name      string
		alternate bool
		wantRows  int
	}{
		{name: "primary", wantRows: 40},
		{name: "primary-and-active-alternate", alternate: true, wantRows: 80},
	} {
		b.Run(tt.name, func(b *testing.B) {
			screen := NewScreen(120, 40)
			fillRecoveryTranscriptBenchmarkFrame(screen)
			if tt.alternate {
				screen.Write([]byte("\x1b[?1049h"))
				fillRecoveryTranscriptBenchmarkFrame(screen)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(tt.wantRows), "rows/snapshot")
			b.ResetTimer()
			for b.Loop() {
				recoveryTranscriptSnapshotSink = screen.RecoveryTranscriptSnapshot()
			}
			b.StopTimer()
			if got := decodeRecoveryTranscript(b, recoveryTranscriptSnapshotSink).Len(); got != tt.wantRows {
				b.Fatalf("captured rows = %d, want %d", got, tt.wantRows)
			}
		})
	}
}

func fillRecoveryTranscriptBenchmarkFrame(screen *Screen) {
	for y := range screen.Frame.Height {
		for x := range screen.Frame.Width {
			screen.Frame.Set(x, y, renderer.Cell{Rune: rune('a' + (x+y)%26), Style: renderer.DefaultStyle()})
		}
		screen.buffer.boundaries[y] = LineBound{End: screen.Frame.Width}
	}
}

func BenchmarkNewScreenWithRecoveryTranscriptManyChunks(b *testing.B) {
	const rows = maxHistoryRows
	source := NewScreen(1, rows)
	for y := range rows {
		source.Frame.Set(0, y, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
		source.buffer.boundaries[y] = LineBound{End: 1}
	}
	transcript, err := source.RecoveryTranscriptSnapshot().Marshal()
	require.NoError(b, err)
	tail, err := MarshalEmptyHistoryTail()
	require.NoError(b, err)
	config := HistoryConfig{MaxRows: rows, MaxCells: rows, ChunkRows: maxHistoryChunkRows}

	b.ReportAllocs()
	b.ReportMetric(float64(rows), "rows/restore")
	b.ReportMetric(float64((rows+maxHistoryChunkRows-1)/maxHistoryChunkRows), "chunks/restore")
	b.ResetTimer()
	for b.Loop() {
		recoveryTranscriptScreenSink, err = NewScreenWithRecoveryTranscript(1, 1, config, nil, tail, transcript)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func recoveryHistoryBlobs(t testing.TB, texts []string) ([][]byte, []byte) {
	t.Helper()
	history := NewHistory(HistoryConfig{MaxRows: len(texts) + 1, MaxCells: 1024, ChunkRows: 2})
	for _, text := range texts {
		require.NoError(t, history.Append(historyRow(text), LineBound{End: len(text)}))
	}
	view := history.SnapshotView()
	sealed := make([][]byte, view.ChunkCount())
	for i := range sealed {
		var err error
		sealed[i], err = MarshalHistoryChunk(view.Chunk(i))
		require.NoError(t, err)
	}
	tail, err := MarshalHistoryTail(view)
	require.NoError(t, err)
	return sealed, tail
}

func recoveryTranscriptBlob(t testing.TB, texts []string, bounds []LineBound) []byte {
	t.Helper()
	rows := make([][]renderer.Cell, len(texts))
	cells := 0
	for i, text := range texts {
		rows[i] = historyRow(text)
		cells += len(rows[i])
	}
	if bounds == nil {
		bounds = make([]LineBound, len(rows))
		for i, row := range rows {
			bounds[i].End = len(row)
		}
	}
	view := HistoryView{rows: len(rows), cells: cells}
	if len(rows) > 0 {
		view.chunks = []*HistoryChunk{{rows: rows, bounds: bounds}}
	}
	blob, err := MarshalHistory(view)
	require.NoError(t, err)
	return blob
}

func decodeRecoveryTranscript(t testing.TB, snapshot RecoveryTranscriptSnapshot) HistoryView {
	t.Helper()
	blob, err := snapshot.Marshal()
	require.NoError(t, err)
	view, err := UnmarshalHistory(blob)
	require.NoError(t, err)
	return view
}
