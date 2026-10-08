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
		mode        domain.ThemeMode
		paletteOff  bool
		transparent bool
	}{
		{name: "accent themed"},
		{name: "accent transparent", transparent: true},
		{name: "palette off transparent", paletteOff: true, transparent: true},
		{name: "forced dark transparent", mode: domain.ThemeDark, transparent: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := domain.Defaults()
			cfg.Theme = tt.mode
			cfg.ThemePalette = !tt.paletteOff
			cfg.Bar.Transparent = tt.transparent
			d := newTestDaemon(t, nil, stubClock{})
			d.ApplyConfig(cfg)

			themedCfg := themeConfigSnapshot{mode: cfg.Theme, paletteOff: tt.paletteOff, accent: cfg.ThemeAccent}
			themed := themeui.ResolveForColor(effectiveThemeForConfig(raw, themedCfg), cfg.ThemeAccent, terminalcap.ColorCapabilities{}).Styles
			require.False(t, themed.SurfaceBar.Equal(renderer.DefaultStyle()), "fixture must produce a tinted bar")

			got := d.resolveAppliedTheme(raw, terminalcap.ColorCapabilities{}).Resolved.Styles

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

func TestApplyConfigBarBackgroundRepaintsBars(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark})

	win := testAttachmentTab(sess)
	win.mu.Lock()
	win.size = domain.Size{Cols: 40, Rows: 4}
	win.mu.Unlock()
	pane := win.focusedPane()
	pane.mu.Lock()
	pane.screen.Resize(40, 4)
	pane.mu.Unlock()

	d.paint(sess, ac, true, nil)
	ac.sendMu.Lock()
	require.True(t, ac.render.cache.valid)
	themedFill := ac.render.cache.frame.At(39, 0).Style
	themedTab := ac.render.cache.frame.At(1, 0).Style
	ac.sendMu.Unlock()
	require.False(t, themedFill.Equal(renderer.DefaultStyle()), "themed bar fill is tinted")

	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark, Bar: domain.BarConfig{Transparent: true}})

	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	require.True(t, ac.render.cache.valid)
	require.True(t, ac.render.cache.frame.At(39, 0).Style.Equal(renderer.DefaultStyle()), "reload clears the bar fill")
	require.True(t, ac.render.cache.frame.At(1, 0).Style.Equal(themedTab), "tab keeps its theme style")
}
