package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	themeui "github.com/bnema/vev/internal/usecase/theme"
)

func lifecycleTheme(foreground, background renderer.RGB, palette map[uint8]renderer.RGB) protocol.Theme {
	out := protocol.Theme{
		Foreground: foreground, Background: background,
		HasForeground: true, HasBackground: true, TrueColor: true,
		SchemeKnown: true,
	}
	for slot, color := range palette {
		out.Palette[slot] = color
		out.PaletteKnown |= 1 << slot
	}
	return out
}

func chromeStyleSnapshot(t *testing.T, styles themeui.Styles) []byte {
	t.Helper()
	frame := renderer.NewFrame(6, 1)
	for x, style := range []renderer.Style{
		styles.SurfaceBar, styles.SurfaceInactive, styles.SurfaceRecent,
		styles.SurfaceActive, styles.BorderMuted, styles.BorderActive,
	} {
		frame.Set(x, 0, renderer.Cell{Rune: rune('A' + x), Style: style})
	}
	out, err := renderer.New(renderer.Capabilities{}).Draw(frame, []renderer.Damage{renderer.FullRedraw()})
	require.NoError(t, err)
	return out
}

func TestAccentLifecycleGenerationDaemonResolutionAndReload(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)

	oldForeground, oldBackground := renderer.RGB{R: 230, G: 230, B: 230}, renderer.RGB{R: 8, G: 9, B: 10}
	old := lifecycleTheme(oldForeground, oldBackground, map[uint8]renderer.RGB{
		4: {R: 108, G: 155, B: 217}, 12: {R: 108, G: 155, B: 217},
	})
	d.applyTheme(sess, ac, old)
	beforeClear := ac.getAppliedTheme()
	require.True(t, beforeClear.Resolved.Accent.Known)
	require.Equal(t, uint8(4), beforeClear.Resolved.Accent.Slot)

	// A replacement generation's first publication retains pane defaults but
	// clears every palette bit before daemon style resolution.
	cleared := old
	cleared.PaletteKnown = 0
	cleared.Palette = [16]renderer.RGB{}
	cleared.Light = true
	d.applyTheme(sess, ac, cleared)
	clearedApplied := ac.getAppliedTheme()
	require.Equal(t, oldForeground, clearedApplied.Raw.Foreground)
	require.Equal(t, oldBackground, clearedApplied.Raw.Background)
	require.True(t, clearedApplied.Raw.Light)
	require.Zero(t, clearedApplied.Raw.PaletteKnown)
	require.False(t, clearedApplied.Resolved.Accent.Known)
	require.Equal(t, beforeClear.Generation+1, clearedApplied.Generation)
	assertSessionDefaultColors(t, sess, oldForeground, oldBackground)

	newForeground, newBackground := renderer.RGB{R: 240, G: 240, B: 240}, renderer.RGB{R: 12, G: 13, B: 14}
	teal := renderer.RGB{R: 125, G: 181, B: 181}
	definitive := lifecycleTheme(newForeground, newBackground, map[uint8]renderer.RGB{
		2: teal, 10: teal, 14: teal,
		4: {R: 108, G: 155, B: 217}, 12: {R: 108, G: 155, B: 217},
	})
	d.applyTheme(sess, ac, definitive)
	resolved := ac.getAppliedTheme()
	require.Equal(t, clearedApplied.Generation+1, resolved.Generation)
	require.True(t, resolved.Resolved.Accent.Known)
	require.Equal(t, uint8(2), resolved.Resolved.Accent.Slot)
	require.Equal(t, teal, resolved.Resolved.Accent.RGB)
	assertSessionDefaultColors(t, sess, newForeground, newBackground)

	// Policy-only reloads resolve the retained definitive snapshot: no client
	// query or intermediate Theme publication is involved.
	explicit := domain.Defaults()
	explicit.ThemeAccent = domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 4}
	d.ApplyConfig(explicit)
	reloaded := ac.getAppliedTheme()
	require.Equal(t, resolved.Raw, reloaded.Raw)
	require.Greater(t, reloaded.Generation, resolved.Generation)
	require.Equal(t, uint8(4), reloaded.Resolved.Accent.Slot)
	require.Equal(t, definitive.Palette, ac.getClientTheme().Palette)

	unknown := explicit
	unknown.ThemeAccent = domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 6}
	d.ApplyConfig(unknown)
	require.False(t, ac.getAppliedTheme().Resolved.Accent.Known, "an explicit unknown slot must not substitute another slot")

	paletteOff := explicit
	paletteOff.ThemePalette = false
	d.ApplyConfig(paletteOff)
	off := ac.getAppliedTheme()
	require.False(t, off.Raw.UsePalette)
	require.False(t, off.Resolved.Accent.Known)
	assertSessionDefaultColors(t, sess, newForeground, newBackground)

	for _, tc := range []struct {
		name string
		mode domain.ThemeMode
		want themeui.Theme
	}{
		{name: "forced dark", mode: domain.ThemeDark, want: themeui.BuiltinDark},
		{name: "forced light", mode: domain.ThemeLight, want: themeui.BuiltinLight},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := domain.Defaults()
			cfg.Theme = tc.mode
			d.ApplyConfig(cfg)
			applied := ac.getAppliedTheme()
			require.Equal(t, tc.want, applied.Raw)
			require.False(t, applied.Resolved.Accent.Known)
			assertSessionDefaultColors(t, sess, tc.want.Foreground, tc.want.Background)
		})
	}

	// A timeout-finalized partial report is still one definitive daemon apply;
	// an unavailable explicit slot remains neutral.
	partialCfg := domain.Defaults()
	partialCfg.ThemeAccent = domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 6}
	d.ApplyConfig(partialCfg)
	beforePartial := ac.getAppliedTheme()
	partial := lifecycleTheme(newForeground, newBackground, map[uint8]renderer.RGB{2: teal})
	d.applyTheme(sess, ac, partial)
	partialApplied := ac.getAppliedTheme()
	require.Equal(t, beforePartial.Generation+1, partialApplied.Generation)
	require.Equal(t, uint16(1<<2), ac.getClientTheme().PaletteKnown)
	require.False(t, partialApplied.Resolved.Accent.Known)
}

func TestAccentLifecycleNeutralChromeByteSnapshots(t *testing.T) {
	raw := lifecycleTheme(
		renderer.RGB{R: 230, G: 230, B: 230}, renderer.RGB{R: 8, G: 9, B: 10},
		map[uint8]renderer.RGB{2: {R: 125, G: 181, B: 181}, 10: {R: 125, G: 181, B: 181}},
	)
	tests := []struct {
		name   string
		config domain.Config
		want   string
	}{
		{
			name: "palette off",
			config: func() domain.Config {
				cfg := domain.Defaults()
				cfg.ThemePalette = false
				return cfg
			}(),
			want: "\x1b[1;1HABC\x1b[0;7mD\x1b[0mEF\x1b[0m",
		},
		{
			name:   "forced dark",
			config: domain.Config{Theme: domain.ThemeDark},
			want:   "\x1b[1;1H\x1b[0;38;2;216;216;216;48;2;47;47;47mABC\x1b[0;38;2;216;216;216;48;2;72;72;72mD\x1b[0;38;2;139;139;139mEF\x1b[0m",
		},
		{
			name:   "forced light",
			config: domain.Config{Theme: domain.ThemeLight},
			want:   "\x1b[1;1H\x1b[0;38;2;32;32;32;48;2;222;222;222mABC\x1b[0;38;2;32;32;32;48;2;194;194;194mD\x1b[0;38;2;118;118;118mEF\x1b[0m",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			d.ApplyConfig(tt.config)
			got := chromeStyleSnapshot(t, d.resolveAppliedTheme(themeui.Theme{
				Foreground: raw.Foreground, Background: raw.Background, Palette: raw.Palette, PaletteKnown: raw.PaletteKnown,
				HasFG: true, HasBG: true, TrueColor: true,
			}).Resolved.Styles)
			require.Equal(t, []byte(tt.want), got)
		})
	}
}
