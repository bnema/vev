package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

func TestBarBackgroundConfigResolvesBarSurface(t *testing.T) {
	paletteColors := [16]renderer.RGB{}
	paletteColors[4] = renderer.RGB{R: 40, G: 90, B: 220}
	raw := themeui.Theme{
		Foreground: renderer.RGB{R: 230, G: 230, B: 230}, Background: renderer.RGB{R: 8, G: 9, B: 10},
		HasFG: true, HasBG: true, Known: true, UsePalette: true,
		Palette: paletteColors, PaletteKnown: 1 << 4,
	}

	tests := []struct {
		name        string
		transparent bool
		color       terminalcap.ColorCapabilities
	}{
		{name: "truecolor themed", color: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor}},
		{name: "truecolor transparent", transparent: true, color: terminalcap.ColorCapabilities{Mode: terminalcap.TrueColor}},
		{name: "ansi16 transparent", transparent: true, color: terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := domain.Defaults()
			cfg.Bar.Transparent = tt.transparent
			d := newTestDaemon(t, nil, stubClock{})
			d.ApplyConfig(cfg)

			themed := themeui.ResolveForColor(raw, cfg.ThemeAccent, tt.color).Styles
			got := d.resolveAppliedTheme(raw, tt.color).Resolved.Styles

			if tt.transparent {
				require.True(t, got.SurfaceBar.Equal(renderer.DefaultStyle()), "bar fill uses the terminal background")
			} else {
				require.True(t, got.SurfaceBar.Equal(themed.SurfaceBar))
			}
			// Every other role keeps its theme colors.
			require.True(t, got.SurfaceInactive.Equal(themed.SurfaceInactive))
			require.True(t, got.TabActive.Equal(themed.TabActive))
			require.True(t, got.MRUStyle(0, 2).Equal(themed.MRUStyle(0, 2)))
			require.True(t, got.CopyStatus.Equal(themed.CopyStatus))
			require.True(t, got.StatusBar.Equal(themed.StatusBar))
		})
	}
}
