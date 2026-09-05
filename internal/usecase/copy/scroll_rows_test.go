package copy

import (
	"math"
	"testing"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/stretchr/testify/require"
)

func TestScrollRowsMovesViewportImmediately(t *testing.T) {
	rows := make([][]renderer.Cell, 100)
	for i := range rows {
		rows[i] = []renderer.Cell{{Rune: 'x', Style: renderer.DefaultStyle()}}
	}
	for _, tc := range []struct {
		name   string
		deltas []int
		want   []int
	}{
		{"reverse", []int{-3, 3, -3, 3}, []int{87, 90, 87, 90}},
		{"clamp", []int{math.MinInt, -1, math.MaxInt}, []int{0, 0, 90}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mode := NewMode(NewDocument(NewSnapshotFromRows(rows, 1, 10), ""))
			for i, delta := range tc.deltas {
				mode.ScrollRows(delta)
				require.Equal(t, tc.want[i], mode.ViewportTop)
				require.GreaterOrEqual(t, mode.Cursor().Row, mode.ViewportTop)
				require.Less(t, mode.Cursor().Row, mode.ViewportTop+10)
			}
			require.True(t, mode.AtBottom())
		})
	}
}
