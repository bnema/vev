package vt

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestHistoryAcceptanceKeepsBookmarksAcrossLiveScrollEvictionAndRestore(t *testing.T) {
	history := NewHistory(HistoryConfig{MaxRows: 3, MaxCells: 32, ChunkRows: 2})
	for _, text := range []string{"live-1", "live-2", "live-3"} {
		require.NoError(t, history.Append(historyAcceptanceRow(text), LineBound{End: len(text)}))
	}

	beforeScroll := history.View()
	bookmark := beforeScroll.RowID(1)
	immutable := beforeScroll.Row(1)
	immutable[0].Rune = 'X'
	require.Equal(t, 'l', beforeScroll.Row(1)[0].Rune, "immutable history view exposed mutable cells")

	require.NoError(t, history.Append(historyAcceptanceRow("live-4"), LineBound{End: 6}))
	afterScroll := history.View()
	require.Equal(t, 3, afterScroll.Len())
	require.Equal(t, 0, afterScroll.FindRowID(bookmark), "bookmark row was not retained after one live scroll")
	require.Equal(t, "live-2", historyAcceptanceText(afterScroll.Row(0)))
	require.Equal(t, "live-1", historyAcceptanceText(beforeScroll.Row(0)), "retained view changed after eviction")

	sealed, tail, err := MarshalSealedHistory(history.SealAndView())
	require.NoError(t, err)
	restored, err := HistoryFromBlobs(HistoryConfig{MaxRows: 3, MaxCells: 32, ChunkRows: 2}, sealed, tail)
	require.NoError(t, err)
	restoredView := restored.View()
	require.Equal(t, afterScroll.Len(), restoredView.Len())
	require.Equal(t, bookmark, restoredView.RowID(0), "bookmark row identity changed during restore")
	require.Equal(t, "live-2", historyAcceptanceText(restoredView.Row(0)))

	next := restoredView.NextRowID()
	require.NoError(t, restored.Append(historyAcceptanceRow("live-5"), LineBound{End: 6}))
	require.Greater(t, restored.View().NextRowID(), next)
}

func historyAcceptanceRow(text string) []renderer.Cell {
	row := make([]renderer.Cell, len(text))
	for i, r := range text {
		row[i] = renderer.Cell{Rune: r}
	}
	return row
}

func historyAcceptanceText(row []renderer.Cell) string {
	text := make([]rune, len(row))
	for i, cell := range row {
		text[i] = cell.Rune
	}
	return string(text)
}
