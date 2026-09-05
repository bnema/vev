package copy

import (
	"fmt"
	"testing"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/stretchr/testify/require"
)

func TestRenderRowsRangeMatchesFullRender(t *testing.T) {
	rows := [][]renderer.Cell{
		{{Rune: 'a'}, {Rune: 'b'}, {Rune: 'c'}},
		{{Rune: 'd'}, {Rune: 'e'}},
		{{Rune: 'f'}},
	}
	mode := NewMode(NewDocument(NewSnapshotFromRows(rows, 3, 5), ""))
	mode.Search("a")
	mode.StartCharacterSelection(Pos{})
	mode.ExtendCharacterSelection(Pos{Row: 1, Col: 1})
	full := mode.Render()
	for _, bounds := range [][2]int{{0, 5}, {1, 2}, {-3, 99}, {4, 2}, {9, 10}} {
		t.Run(fmt.Sprint(bounds), func(t *testing.T) {
			var painted []int
			mode.RenderRowsRange(bounds[0], bounds[1], func(y int, row []renderer.Cell) {
				painted = append(painted, y)
				require.Equal(t, full.Row(y), row)
			})
			var want []int
			for y := max(bounds[0], 0); y < min(bounds[1], 5); y++ {
				want = append(want, y)
			}
			want = append(want, 5)
			require.Equal(t, want, painted)
		})
	}
}
