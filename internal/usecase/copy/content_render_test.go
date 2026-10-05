package copy

import (
	"strings"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestRenderRowsSelectionHighlightsContentOnly(t *testing.T) {
	wide := padRow("", 8)
	wide[0] = vt.Cell{Rune: 'a'}
	wide[1] = vt.Cell{Rune: '界'}
	wide[2] = vt.Cell{Continuation: true}
	tests := []struct {
		name        string
		rows        [][]vt.Cell
		bounds      []vt.LineBound
		end         Pos
		granularity Granularity
		want        [][]int
	}{
		{"hard rows", [][]vt.Cell{padRow("ab", 8), padRow("cd", 8)}, nil, Pos{1, 1}, Character, [][]int{{0, 1, 2}, {0, 1}}},
		{"word rows", [][]vt.Cell{padRow("ab", 8), padRow("cd", 8)}, nil, Pos{1, 1}, Word, [][]int{{0, 1, 2}, {0, 1}}},
		{"line mode", [][]vt.Cell{padRow("ab", 8), padRow("", 8), padRow("cd", 8)}, nil, Pos{2, 0}, Line, [][]int{{0, 1, 2}, {}, {0, 1, 2}}},
		{"soft spaces", [][]vt.Cell{padRow("abc     ", 8), padRow("de", 8)}, []vt.LineBound{{End: 8, Soft: true}, {End: 2}}, Pos{1, 1}, Character, [][]int{{0, 1, 2, 3, 4, 5, 6, 7}, {0, 1}}},
		{"soft partial", [][]vt.Cell{padRow("abc", 8), padRow("de", 8)}, []vt.LineBound{{End: 5, Soft: true}, {End: 2}}, Pos{1, 1}, Character, [][]int{{0, 1, 2, 3, 4}, {0, 1}}},
		{"full hard row", [][]vt.Cell{padRow("abcdefgh", 8), padRow("ij", 8)}, nil, Pos{1, 0}, Character, [][]int{{0, 1, 2, 3, 4, 5, 6, 7}, {0}}},
		{"wide glyph", [][]vt.Cell{wide}, nil, Pos{0, 0}, Line, [][]int{{0, 1, 2, 3}}},
		{"end in padding", [][]vt.Cell{padRow("ab", 8)}, nil, Pos{0, 6}, Character, [][]int{{0, 1}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			doc := NewDocument(NewSnapshotFromLines(tt.rows, tt.bounds, 8, len(tt.rows)), "")
			m := NewMode(doc)
			m.selection = Selection{Anchor: Pos{0, 0}, Active: tt.end, Granularity: tt.granularity, Enabled: true}
			m.navigator.Pos = Pos{0, 0}
			f := m.Render(vt.DefaultStyle(), vt.Style{HasBackgroundRGB: true, BackgroundRGB: vt.RGB{R: 1}})
			var copied strings.Builder
			bounds, _ := m.selection.bounds(doc)
			for y, source := range tt.rows {
				cols := []int{}
				marker, hasMarker := bounds.newlineMarker(doc, y)
				for x, cell := range source {
					if f.At(x, y).Style.Inverse == cell.Style.Inverse {
						continue
					}
					cols = append(cols, x)
					if (hasMarker && x == marker) || cell.Continuation {
						continue
					}
					if cell.Rune == 0 {
						copied.WriteByte(' ')
					} else {
						copied.WriteRune(cell.Rune)
					}
				}
				require.Equal(t, tt.want[y], cols)
				if y < len(tt.rows)-1 && !doc.snapshot.Bound(y).Soft {
					copied.WriteByte('\n')
				}
			}
			require.Equal(t, copied.String(), m.SelectedText(), "highlighted glyphs match copied text")
			for y, source := range tt.rows {
				require.Equal(t, source, doc.Row(y))
			}
		})
	}
}

func TestRenderRowsSelectionAfterHistoryResize(t *testing.T) {
	screen := vt.NewScreenWithHistory(12, 2, vt.HistoryConfig{MaxRows: 64})
	screen.Write([]byte("abc     xyz\r\nq\r\nr\r\ns"))
	screen.Resize(8, 2)
	doc := NewDocument(NewSnapshot(screen.History(), screen, screen.LineBounds(), nil), "")
	m := NewMode(doc)
	m.navigator.Pos = Pos{0, 0}
	m.ViewportTop = 0
	m.selection = Selection{Anchor: Pos{0, 0}, Active: Pos{1, 0}, Granularity: Character, Enabled: true}
	f := m.Render(vt.DefaultStyle(), vt.Style{HasBackgroundRGB: true})
	require.Equal(t, "abc     xyz\nq", m.SelectedText())
	for x := 0; x < 8; x++ {
		require.True(t, f.At(x, 0).Style.Inverse, "visible content column %d", x)
	}
}

func TestRenderRowsSelectionPreservesCellColors(t *testing.T) {
	styles := []vt.Style{
		{HasForegroundRGB: true, ForegroundRGB: vt.RGB{G: 255, B: 255}, Bold: true},
		{Inverse: true, HasBackgroundRGB: true, BackgroundRGB: vt.RGB{R: 20}, Italic: true},
	}
	cells := []vt.Cell{{Rune: 'a', Style: styles[0]}, {Rune: 'b', Style: styles[1]}}
	m := NewMode(NewDocument(NewSnapshotFromRows([][]vt.Cell{cells}, 2, 1), ""))
	require.True(t, m.StartCharacterSelection(Pos{0, 0}))
	require.True(t, m.ExtendCharacterSelection(Pos{0, 1}))
	f := m.Render(vt.DefaultStyle(), vt.Style{HasBackgroundRGB: true, BackgroundRGB: vt.RGB{R: 1}})
	for x, style := range styles {
		style.Inverse = !style.Inverse
		require.Equal(t, style, f.At(x, 0).Style)
	}
	require.Equal(t, cells, m.Document().Row(0))
}
