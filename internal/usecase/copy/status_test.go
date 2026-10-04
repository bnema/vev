package copy

import (
	"testing"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/stretchr/testify/require"
)

func TestDrawCopyStatus(t *testing.T) {
	tests := []struct {
		name  string
		width int
		lines []string
		query string
		want  string
	}{
		{name: "empty document", width: 12, want: " [SCROLL] 0/"},
		{name: "position", width: 16, lines: []string{"a", "b"}, want: " [SCROLL] 2/2   "},
		{name: "truncated", width: 5, lines: []string{"a"}, want: " [SCR"},
		{name: "zero width", width: 0, lines: []string{"a"}, want: ""},
		{name: "multibyte query is contiguous", width: 22, lines: []string{"éa"}, query: "é", want: " [SCROLL] 1/1 1/1 /é "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := modeFor(tt.lines, 1)
			if tt.query != "" {
				m.Search(tt.query)
			}
			row := make([]renderer.Cell, tt.width)
			drawCopyStatus(row, m, m.document.Len(), renderer.Style{})
			var got []rune
			for _, cell := range row {
				got = append(got, cell.Rune)
			}
			require.Equal(t, tt.want, string(got)[:len(tt.want)])
			require.Len(t, row, tt.width)
		})
	}
}

// TestRenderRowsRangeReusesRowsAcrossWidths guards the pooled row buffer:
// a narrow render between two wide ones must not leak cells.
func TestRenderRowsRangeReusesRowsAcrossWidths(t *testing.T) {
	wide := NewMode(NewDocument(NewSnapshotFromRows([][]renderer.Cell{row("abcdefgh")}, 8, 1), ""))
	narrow := NewMode(NewDocument(NewSnapshotFromRows([][]renderer.Cell{row("xy")}, 2, 1), ""))
	for _, m := range []*Mode{wide, narrow, wide} {
		full := m.Render()
		m.RenderRowsRange(0, 1, func(y int, got []renderer.Cell) {
			require.Equal(t, full.Row(y), got)
		})
	}
}
