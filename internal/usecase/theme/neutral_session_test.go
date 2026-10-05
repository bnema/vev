package theme

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestNeutralSessionSurfaceHierarchy(t *testing.T) {
	black := renderer.RGB{}
	white := renderer.RGB{R: 255, G: 255, B: 255}
	for _, tt := range []struct {
		name                   string
		foreground, background renderer.RGB
	}{
		{name: "dark", foreground: white, background: black},
		{name: "light", foreground: black, background: white},
	} {
		t.Run(tt.name, func(t *testing.T) {
			theme := Theme{Known: true, Foreground: tt.foreground, Background: tt.background, HasFG: true, HasBG: true}
			styles := neutralStyles(theme)
			require.Equal(t, tt.foreground, styles.SurfaceActive.BackgroundRGB)
			require.Equal(t, tt.background, styles.SurfaceActive.ForegroundRGB)
			require.Equal(t, styles.TabActive.BackgroundRGB, styles.SurfaceActive.BackgroundRGB)
			for count := 1; count <= 9; count++ {
				for index := range count {
					want := Blend(styles.SurfaceActive.BackgroundRGB, styles.SurfaceBar.BackgroundRGB, 1-mruWeight(index, count))
					require.Equal(t, want, styles.MRUStyle(index, count).BackgroundRGB)
				}
			}
		})
	}
}
