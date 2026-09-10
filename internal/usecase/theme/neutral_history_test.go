package theme

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestNeutralHistoryGradient(t *testing.T) {
	theme := Theme{Foreground: renderer.RGB{R: 216, G: 216, B: 216}, Background: renderer.RGB{R: 16, G: 16, B: 16}, HasFG: true, HasBG: true, Known: true, TrueColor: true}
	styles := NewStyles(theme)
	require.Equal(t, theme.Foreground, styles.TabActive.BackgroundRGB)
	for count := 1; count <= 9; count++ {
		previous := styles.TabActive.BackgroundRGB.R
		for index := 0; index < count; index++ {
			style := styles.MRUStyle(index, count)
			require.Less(t, style.BackgroundRGB.R, previous)
			previous = style.BackgroundRGB.R
		}
		require.Equal(t, styles.SurfaceBar.BackgroundRGB, styles.MRUStyle(count-1, count).BackgroundRGB)
	}
}
