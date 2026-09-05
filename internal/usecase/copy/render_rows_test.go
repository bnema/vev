package copy

import (
	"strings"
	"testing"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/stretchr/testify/require"
)

func TestRenderRowsFixedSemanticCells(t *testing.T) {
	base := renderer.DefaultStyle()
	base.Bold = true
	selected := renderer.DefaultStyle()
	selected.Inverse = true
	status := renderer.DefaultStyle()
	status.Inverse = true
	input := []renderer.Cell{
		{Rune: 'A', Style: base},
		{Rune: '界', Style: base},
		{Continuation: true, Style: base},
		{Rune: 'Z', Style: base},
	}
	m := NewMode(NewDocument(NewSnapshotFromRows([][]renderer.Cell{input}, 32, 3), ""))
	require.True(t, m.Search("A"))
	require.True(t, m.StartCharacterSelection(Pos{Col: 1}))
	require.True(t, m.ExtendCharacterSelection(Pos{Col: 2}))

	want := make([][]renderer.Cell, 4)
	for y := range want {
		want[y] = make([]renderer.Cell, 32)
		for x := range want[y] {
			want[y][x] = renderer.BlankCell()
		}
	}
	copy(want[0], input)
	// Search highlights A; selection covers both cells of the wide glyph.
	// Inverse highlighting preserves the source's bold style, including the
	// cursor already covered by selection (it must not toggle inverse twice).
	for x := 0; x < 3; x++ {
		want[0][x].Style.Inverse = true
	}
	text := " [SELECT] 1/1 1/1 /A "
	for x, r := range text + strings.Repeat(" ", 32-len(text)) {
		want[3][x] = renderer.Cell{Rune: r, Style: status}
	}

	var got [][]renderer.Cell
	m.RenderRows(func(y int, cells []renderer.Cell) {
		require.Equal(t, len(got), y, "body rows precede the final status row")
		// Own the callback data; later callbacks reuse the borrowed row.
		got = append(got, append([]renderer.Cell(nil), cells...))
	}, status, selected)
	require.Equal(t, want, got)
	require.Equal(t, input, m.Document().Row(0), "rendering must not style the immutable document")
}
