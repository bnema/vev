package theme

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

// TestIndexedFallbackKeepsActiveTitleReadable covers a terminal that reports
// its default colors but no palette: the scheme blue fallback must not recolor
// the title drawn on the full-strength active surface.
func TestIndexedFallbackKeepsActiveTitleReadable(t *testing.T) {
	black := renderer.RGB{}
	white := renderer.RGB{R: 255, G: 255, B: 255}
	for _, tt := range []struct {
		name                   string
		foreground, background renderer.RGB
		light                  bool
	}{
		{name: "dark", foreground: white, background: black},
		{name: "light", foreground: black, background: white, light: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			theme := Theme{
				Known: true, SchemeKnown: true, Light: tt.light, UsePalette: true,
				Foreground: tt.foreground, Background: tt.background, HasFG: true, HasBG: true,
			}
			resolved := Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})
			require.True(t, resolved.Accent.IndexedOnly)

			want := MutedVariantStyle(resolved.Styles.SurfaceActive, theme)
			require.Equal(t, want, resolved.Styles.TabActiveTitle)
			require.Equal(t, want, resolved.Styles.TabTitleActive)
			require.Equal(t, int(resolved.Accent.Slot), resolved.Styles.TabInactiveTitle.Foreground)
		})
	}
}

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
			require.Equal(t, styles.SurfaceActive.BackgroundRGB, styles.TabActiveTitle.BackgroundRGB)
			soft := Blend(tt.background, tt.foreground, neutralAccentBlend)
			for _, selection := range []renderer.Style{styles.PickerSelection, styles.Selection, styles.SearchSelection} {
				require.Equal(t, soft, selection.BackgroundRGB)
			}
			require.Equal(t, Blend(styles.SurfaceBar.BackgroundRGB, tt.foreground, 0.5), styles.MRUStyle(0, 3).BackgroundRGB)
			for count := 1; count <= 9; count++ {
				for index := range count {
					want := Blend(styles.SurfaceActive.BackgroundRGB, styles.SurfaceBar.BackgroundRGB, 1-mruWeight(index, count))
					require.Equal(t, want, styles.MRUStyle(index, count).BackgroundRGB)
				}
			}
		})
	}
}
