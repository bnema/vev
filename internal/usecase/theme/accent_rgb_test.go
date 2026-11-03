package theme

import (
	"testing"

	renderer "bnema.dev/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/stretchr/testify/require"
)

func TestExplicitRGBAccent(t *testing.T) {
	color := renderer.RGB{R: 0x74, G: 0xe7, B: 0xd4}
	policy := domain.ThemeAccent{Mode: domain.ThemeAccentRGB, RGB: [3]uint8{color.R, color.G, color.B}}
	for _, palette := range []bool{false, true} {
		t.Run(map[bool]string{false: "without palette", true: "with palette"}[palette], func(t *testing.T) {
			theme := BuiltinDark
			theme.UsePalette = palette
			theme.PaletteKnown = 1 << 6
			theme.Palette[6] = color
			accent := ResolveAccent(theme, policy)
			require.True(t, accent.Known)
			require.False(t, accent.IndexedOnly)
			require.Equal(t, color, accent.RGB)
			require.Equal(t, uint8(6), accent.Slot)
			resolved := Resolve(theme, policy)
			require.True(t, resolved.Ramp.rgb)
			require.GreaterOrEqual(t, ContrastRatio(resolved.Ramp.SurfaceActive.ForegroundRGB, resolved.Ramp.SurfaceActive.BackgroundRGB), 4.5)
			reduced := ResolveForColor(theme, policy, terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16})
			require.Equal(t, 6, reduced.Styles.BorderActive.Foreground)
			mono := ResolveForColor(theme, policy, terminalcap.ColorCapabilities{Mode: terminalcap.Monochrome})
			require.Equal(t, -1, mono.Styles.BorderActive.Foreground)
			theme.HasBG = false
			require.True(t, ResolveAccent(theme, policy).IndexedOnly)
		})
	}
	t.Run("no known palette uses blue fallback", func(t *testing.T) {
		require.Equal(t, uint8(4), ResolveAccent(BuiltinDark, policy).Slot)
	})
	t.Run("explicit neutral is not filtered by auto eligibility", func(t *testing.T) {
		require.True(t, ResolveAccent(BuiltinDark, domain.ThemeAccent{Mode: domain.ThemeAccentRGB}).Known)
	})
}
