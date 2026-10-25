package protocol

import (
	"fmt"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestCapRemotePreviewStyles(t *testing.T) {
	for _, n := range []int{1, 1024, 1025, 32768} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			cells := make([]renderer.Cell, n)
			for i := range cells {
				s := renderer.DefaultStyle()
				s.HasForegroundRGB = true
				s.ForegroundRGB = renderer.RGB{R: uint8(i), G: uint8(i >> 8)}
				cells[i] = renderer.Cell{Rune: 'x', Style: s}
			}
			before := append([]renderer.Cell(nil), cells...)
			CapRemotePreviewStyles(cells)
			styles := map[renderer.Style]bool{}
			for i, c := range cells {
				require.Equal(t, before[i].Rune, c.Rune)
				styles[c.Style] = true
			}
			require.LessOrEqual(t, len(styles), RemotePreviewMaxStyles)
			if n <= 1024 {
				require.Equal(t, before, cells)
			} else {
				require.Equal(t, renderer.DefaultStyle(), cells[n-1].Style)
			}
		})
	}
}
