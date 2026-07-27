package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
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

func decodeRecoveryTranscript(t testing.TB, snapshot RecoveryTranscriptSnapshot) HistoryView {
	t.Helper()
	blob, err := snapshot.Marshal()
	require.NoError(t, err)
	view, err := UnmarshalHistory(blob)
	require.NoError(t, err)
	return view
}
