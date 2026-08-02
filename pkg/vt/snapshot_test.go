package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestHistorySnapshotViewDoesNotSealTailAndCopiesIt(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 8, ChunkRows: 2})
	requireHistoryAppend(t, history, historyRow("AAAA"))
	requireHistoryAppend(t, history, historyRow("BBBB"))
	requireHistoryAppend(t, history, historyRow("CCCC"))
	sealed := history.View().Chunk(0)

	view := history.SnapshotView()
	if got, want := view.ChunkCount(), 1; got != want {
		t.Fatalf("sealed chunks = %d, want %d", got, want)
	}
	if view.Chunk(0) != sealed {
		t.Fatal("snapshot copied an immutable sealed chunk")
	}
	if got, want := len(history.tail), 1; got != want {
		t.Fatalf("live tail rows = %d, want %d; snapshot sealed it", got, want)
	}
	history.tail[0][0].Rune = 'X'
	if got, want := rowText(view.Tail().Row(0)), "CCCC"; got != want {
		t.Fatalf("snapshot tail = %q, want %q", got, want)
	}
	if got, want := view.Len(), 3; got != want {
		t.Fatalf("snapshot rows = %d, want %d", got, want)
	}
	if got, want := view.Cells(), 12; got != want {
		t.Fatalf("snapshot cells = %d, want %d", got, want)
	}

	tail, err := MarshalHistoryTail(view)
	require.NoError(t, err)
	decoded, err := UnmarshalHistory(tail)
	require.NoError(t, err)
	require.Equal(t, []string{"CCCC"}, historyViewTexts(decoded))
}

func TestHistoryFromBlobsNormalizesFullTailBeforeAppend(t *testing.T) {
	fullTail, err := MarshalHistory(HistoryView{
		chunks:    []*HistoryChunk{{rows: [][]renderer.Cell{historyRow("AAAA"), historyRow("BBBB")}, rowIDs: []RowID{1, 2}}},
		rows:      2,
		nextRowID: 3,
	})
	require.NoError(t, err)

	restored, err := HistoryFromBlobs(HistoryConfig{MaxRows: 4, ChunkRows: 2}, nil, fullTail)
	require.NoError(t, err)
	requireHistoryAppend(t, restored, historyRow("CCCC"))

	require.Equal(t, []string{"AAAA", "BBBB", "CCCC"}, historyViewTexts(restored.View()))
	require.Len(t, restored.chunks, 1, "a full restored tail must become sealed")
	require.Len(t, restored.tail, 1, "the appended row must be the mutable tail")
}

func TestHistoryFromBlobsRestoresCellAccountingAndEvictsToBothBudgets(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 4, MaxCells: 10, ChunkRows: 2})
	for _, text := range []string{"a", "b", "cc"} {
		requireHistoryAppend(t, history, historyRow(text))
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

	restored, err := HistoryFromBlobs(HistoryConfig{MaxRows: 4, MaxCells: 2, ChunkRows: 2}, sealed, tail)
	require.NoError(t, err)
	require.Equal(t, []string{"cc"}, historyViewTexts(restored.View()))
	require.Equal(t, 2, restored.Cells())
}
