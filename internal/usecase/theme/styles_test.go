package theme

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestRelativeLuminance(t *testing.T) {
	tests := []struct {
		name  string
		color renderer.RGB
		want  float64
	}{
		{name: "black", color: renderer.RGB{}, want: 0},
		{name: "white", color: renderer.RGB{R: 255, G: 255, B: 255}, want: 1},
		{name: "sRGB linearization breakpoint", color: renderer.RGB{R: 10}, want: 0.2126 * (10.0 / 255.0 / 12.92)},
		{name: "nonlinear sRGB", color: renderer.RGB{R: 128, G: 128, B: 128}, want: 0.21586050011389923},
		{name: "red", color: renderer.RGB{R: 255}, want: 0.2126},
		{name: "green", color: renderer.RGB{G: 255}, want: 0.7152},
		{name: "blue", color: renderer.RGB{B: 255}, want: 0.0722},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RelativeLuminance(tt.color)
			require.InDelta(t, tt.want, got, 1e-12)
			require.Equal(t, got, RelativeLuminance(tt.color))
		})
	}
}

func TestContrastRatio(t *testing.T) {
	black := renderer.RGB{}
	white := renderer.RGB{R: 255, G: 255, B: 255}
	middle := renderer.RGB{R: 123, G: 45, B: 67}

	require.InDelta(t, 21, ContrastRatio(black, white), 1e-12)
	require.InDelta(t, 1, ContrastRatio(middle, middle), 1e-12)
	require.Equal(t, ContrastRatio(black, middle), ContrastRatio(middle, black))
}

func TestAccentContrastMinimum(t *testing.T) {
	require.Equal(t, 3.0, accentContrastMin)
}

func TestPulseColor(t *testing.T) {
	rgbBase := renderer.Style{
		Italic:               true,
		Foreground:           7,
		Background:           8,
		HasForegroundRGB:     true,
		ForegroundRGB:        renderer.RGB{R: 200, G: 100, B: 50},
		HasBackgroundRGB:     true,
		BackgroundRGB:        renderer.RGB{R: 20, G: 40, B: 60},
		HasUnderlineColor:    true,
		UnderlineColor:       4,
		HasUnderlineColorRGB: false,
	}
	indexedBase := renderer.Style{Foreground: -1, Background: -1, Inverse: true}

	tests := []struct {
		name      string
		base      renderer.Style
		intensity float64
		want      renderer.Style
	}{
		{
			name:      "RGB starts at the background",
			base:      rgbBase,
			intensity: 0,
			want: func() renderer.Style {
				style := rgbBase
				style.Bold = true
				style.ForegroundRGB = renderer.RGB{R: 20, G: 40, B: 60}
				return style
			}(),
		},
		{
			name:      "RGB blends halfway with rounding",
			base:      rgbBase,
			intensity: 0.5,
			want: func() renderer.Style {
				style := rgbBase
				style.Bold = true
				style.ForegroundRGB = renderer.RGB{R: 110, G: 70, B: 55}
				return style
			}(),
		},
		{
			name:      "RGB reaches the foreground",
			base:      rgbBase,
			intensity: 1,
			want: func() renderer.Style {
				style := rgbBase
				style.Bold = true
				return style
			}(),
		},
		{
			name:      "indexed color uses xterm grayscale start",
			base:      indexedBase,
			intensity: 0,
			want:      renderer.Style{Bold: true, Inverse: true, Foreground: 244, Background: -1},
		},
		{
			name:      "indexed color truncates the grayscale ramp",
			base:      indexedBase,
			intensity: 0.5,
			want:      renderer.Style{Bold: true, Inverse: true, Foreground: 249, Background: -1},
		},
		{
			name:      "indexed color reaches xterm grayscale end",
			base:      indexedBase,
			intensity: 1,
			want:      renderer.Style{Bold: true, Inverse: true, Foreground: 255, Background: -1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, PulseColor(tt.base, tt.intensity))
		})
	}
}

func TestMRUFade(t *testing.T) {
	base := renderer.Style{
		Bold:                 true,
		Foreground:           7,
		Background:           8,
		HasForegroundRGB:     true,
		ForegroundRGB:        renderer.RGB{R: 200, G: 100, B: 50},
		HasBackgroundRGB:     true,
		BackgroundRGB:        renderer.RGB{R: 40, G: 60, B: 80},
		HasUnderlineColorRGB: true,
		UnderlineColorRGB:    renderer.RGB{R: 1, G: 2, B: 3},
	}
	terminalTheme := Theme{HasBG: true, Background: renderer.RGB{R: 10, G: 20, B: 30}}

	tests := []struct {
		name  string
		base  renderer.Style
		theme Theme
		i     int
		count int
		want  renderer.Style
	}{
		{name: "single entry is unchanged", base: base, theme: terminalTheme, count: 1, want: base},
		{name: "missing theme background is unchanged", base: base, theme: Theme{}, i: 1, count: 3, want: base},
		{name: "indexed base is unchanged", base: renderer.DefaultStyle(), theme: terminalTheme, i: 1, count: 3, want: renderer.DefaultStyle()},
		{name: "newest entry starts unfaded", base: base, theme: terminalTheme, count: 3, want: base},
		{
			name:  "middle entry fades thirty percent toward terminal background",
			base:  base,
			theme: terminalTheme,
			i:     1,
			count: 3,
			want: func() renderer.Style {
				style := base
				style.ForegroundRGB = renderer.RGB{R: 143, G: 76, B: 44}
				style.BackgroundRGB = renderer.RGB{R: 31, G: 48, B: 65}
				return style
			}(),
		},
		{
			name:  "oldest entry fades sixty percent toward terminal background",
			base:  base,
			theme: terminalTheme,
			i:     2,
			count: 3,
			want: func() renderer.Style {
				style := base
				style.ForegroundRGB = renderer.RGB{R: 86, G: 52, B: 38}
				style.BackgroundRGB = renderer.RGB{R: 22, G: 36, B: 50}
				return style
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, MRUFade(tt.base, tt.theme, tt.i, tt.count))
		})
	}
}

func TestThemePaletteColor(t *testing.T) {
	palette := [16]renderer.RGB{
		3: {R: 0x11, G: 0x22, B: 0x33},
		7: {R: 0xaa, G: 0xbb, B: 0xcc},
	}
	theme := Theme{
		UsePalette:   true,
		Palette:      palette,
		PaletteKnown: 1<<3 | 1<<7,
	}

	tests := []struct {
		name string
		slot int
		want renderer.RGB
		ok   bool
	}{
		{name: "known slot", slot: 3, want: palette[3], ok: true},
		{name: "unknown slot", slot: 4},
		{name: "negative slot", slot: -1},
		{name: "slot above palette", slot: 16},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := theme.PaletteColor(tt.slot)
			require.Equal(t, tt.ok, ok)
			require.Equal(t, tt.want, got)
		})
	}

	theme.UsePalette = false
	got, ok := theme.PaletteColor(3)
	require.False(t, ok)
	require.Equal(t, renderer.RGB{}, got)
}

func TestBuiltinThemesDoNotUsePalette(t *testing.T) {
	for name, theme := range map[string]Theme{
		"dark":  BuiltinDark,
		"light": BuiltinLight,
	} {
		t.Run(name, func(t *testing.T) {
			require.False(t, theme.UsePalette)
			require.Zero(t, theme.PaletteKnown)
			require.Equal(t, [16]renderer.RGB{}, theme.Palette)
		})
	}
}

func TestThemeIsComparable(t *testing.T) {
	themes := map[Theme]bool{BuiltinDark: true}
	require.True(t, themes[BuiltinDark])
}

func TestPaletteAccentBackground(t *testing.T) {
	base := Theme{
		Foreground: renderer.RGB{R: 200, G: 200, B: 200},
		Background: renderer.RGB{R: 100, G: 100, B: 100},
		HasFG:      true,
		HasBG:      true,
		Known:      true,
		TrueColor:  true,
		UsePalette: true,
	}
	neutral := Blend(base.Background, base.Foreground, accentBlend)

	tests := []struct {
		name            string
		palette         [16]renderer.RGB
		known           uint16
		usePalette      bool
		want            renderer.RGB
		wantContrastMin bool
	}{
		{
			name:            "slot 4 supplies a contrasting accent background",
			palette:         [16]renderer.RGB{4: {B: 255}, 12: {R: 200, G: 200, B: 200}},
			known:           1<<4 | 1<<12,
			usePalette:      true,
			want:            Blend(base.Background, renderer.RGB{B: 255}, accentBlend),
			wantContrastMin: true,
		},
		{
			name:            "slot 4 failure escalates to slot 12",
			palette:         [16]renderer.RGB{4: {R: 200, G: 200, B: 200}, 12: {}},
			known:           1<<4 | 1<<12,
			usePalette:      true,
			want:            Blend(base.Background, renderer.RGB{}, accentBlend),
			wantContrastMin: true,
		},
		{
			name:       "both palette blues failing uses exact neutral blend",
			palette:    [16]renderer.RGB{4: {R: 200, G: 200, B: 200}, 12: {R: 200, G: 200, B: 200}},
			known:      1<<4 | 1<<12,
			usePalette: true,
			want:       neutral,
		},
		{
			name:       "missing palette blues uses exact neutral blend",
			usePalette: true,
			want:       neutral,
		},
		{
			name:       "palette disabled preserves neutral accent",
			palette:    [16]renderer.RGB{4: {B: 255}},
			known:      1 << 4,
			usePalette: false,
			want:       neutral,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			theme := base
			theme.Palette = tt.palette
			theme.PaletteKnown = tt.known
			theme.UsePalette = tt.usePalette

			got := AccentStyle(theme)
			require.True(t, got.HasBackgroundRGB)
			require.Equal(t, tt.want, got.BackgroundRGB)
			if tt.wantContrastMin {
				require.GreaterOrEqual(t, ContrastRatio(theme.Foreground, got.BackgroundRGB), accentContrastMin)
			}
		})
	}
}

func TestPaletteTitleHueDerivation(t *testing.T) {
	base := Theme{
		Foreground: renderer.RGB{R: 200, G: 200, B: 200},
		Background: renderer.RGB{R: 100, G: 100, B: 100},
		HasFG:      true,
		HasBG:      true,
		Known:      true,
		TrueColor:  true,
		UsePalette: true,
	}

	tests := []struct {
		name    string
		theme   Theme
		palette [16]renderer.RGB
		known   uint16
	}{
		{
			name: "slot 4 is used when it contrasts for each title",
			theme: Theme{
				Foreground: renderer.RGB{R: 240, G: 240, B: 240},
				Background: renderer.RGB{R: 50, G: 50, B: 50},
				HasFG:      true, HasBG: true, Known: true, TrueColor: true, UsePalette: true,
			},
			palette: [16]renderer.RGB{4: {}, 12: {R: 200, G: 200, B: 200}},

			known: 1<<4 | 1<<12,
		},
		{
			name:    "each title escalates independently from slot 4 to slot 12",
			palette: [16]renderer.RGB{4: {}, 12: {R: 255, G: 255, B: 255}},
			known:   1<<4 | 1<<12,
		},
		{
			name:    "both title hues failing retains neutral muted styles",
			palette: [16]renderer.RGB{4: {R: 200, G: 200, B: 200}, 12: {R: 200, G: 200, B: 200}},
			known:   1<<4 | 1<<12,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			theme := base
			if tt.theme.Known {
				theme = tt.theme
			}
			theme.Palette = tt.palette
			theme.PaletteKnown = tt.known
			styles := NewStyles(theme)
			status := StatusBarStyle(theme)
			accent := AccentStyle(theme)

			if tt.name == "both title hues failing retains neutral muted styles" {
				require.True(t, styles.TabTitle.Equal(MutedVariantStyle(status, theme)))
				require.True(t, styles.TabTitleActive.Equal(MutedVariantStyle(accent, theme)))
				return
			}

			require.True(t, styles.TabTitle.HasForegroundRGB)
			require.True(t, styles.TabTitleActive.HasForegroundRGB)
			require.GreaterOrEqual(t, ContrastRatio(styles.TabTitle.ForegroundRGB, status.BackgroundRGB), accentContrastMin)
			require.GreaterOrEqual(t, ContrastRatio(styles.TabTitleActive.ForegroundRGB, accent.BackgroundRGB), accentContrastMin)

			if tt.name == "slot 4 is used when it contrasts for each title" {
				require.Equal(t, Blend(theme.Foreground, theme.Palette[4], titleHueBlend), styles.TabTitle.ForegroundRGB)
				require.Equal(t, Blend(theme.Foreground, theme.Palette[4], titleHueBlend), styles.TabTitleActive.ForegroundRGB)
			}
			if tt.name == "each title escalates independently from slot 4 to slot 12" {
				require.Equal(t, Blend(theme.Foreground, theme.Palette[12], titleHueBlend), styles.TabTitle.ForegroundRGB)
				require.Equal(t, Blend(theme.Foreground, theme.Palette[4], titleHueBlend), styles.TabTitleActive.ForegroundRGB)
			}
		})
	}
}

func TestPaletteForegroundStylesKeepTheirRenderedBackgrounds(t *testing.T) {
	base := Theme{
		Foreground: renderer.RGB{R: 240, G: 240, B: 240},
		Background: renderer.RGB{R: 50, G: 50, B: 50},
		HasFG:      true, HasBG: true, Known: true, TrueColor: true, UsePalette: true,
	}

	tests := []struct {
		name           string
		theme          Theme
		wantForeground renderer.RGB
		wantIndexed    int
	}{
		{
			name:           "RGB palette foreground",
			theme:          func() Theme { t := base; t.PaletteKnown = 1 << ansiBlue; return t }(),
			wantForeground: Blend(base.Foreground, renderer.RGB{}, titleHueBlend),
			wantIndexed:    -1,
		},
		{
			name:        "indexed foreground fallback",
			theme:       func() Theme { t := base; t.SchemeKnown = true; return t }(),
			wantIndexed: ansiBrightBlue,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			styles := NewStyles(tt.theme)

			for name, style := range map[string]renderer.Style{
				"border":     styles.Border,
				"muted text": styles.PaletteDesc,
			} {
				require.False(t, style.HasBackgroundRGB, "%s must remain foreground-only", name)
				if tt.wantIndexed >= 0 {
					require.False(t, style.HasForegroundRGB, "%s must use an indexed foreground", name)
					require.Equal(t, tt.wantIndexed, style.Foreground, name)
				} else {
					require.True(t, style.HasForegroundRGB, "%s must use the RGB palette foreground", name)
					require.Equal(t, tt.wantForeground, style.ForegroundRGB, name)
				}
			}

			status := StatusBarStyle(tt.theme)
			accent := AccentStyle(tt.theme)
			for name, style := range map[string]renderer.Style{
				"title":        styles.TabTitle,
				"active title": styles.TabTitleActive,
			} {
				wantBackground := status.BackgroundRGB
				if name == "active title" {
					wantBackground = accent.BackgroundRGB
				}
				require.True(t, style.HasBackgroundRGB, "%s must retain its title background", name)
				require.Equal(t, wantBackground, style.BackgroundRGB, name)
				if tt.wantIndexed >= 0 {
					require.False(t, style.HasForegroundRGB, "%s must use an indexed foreground", name)
					require.Equal(t, tt.wantIndexed, style.Foreground, name)
				} else {
					require.True(t, style.HasForegroundRGB, "%s must use the RGB palette foreground", name)
					require.Equal(t, tt.wantForeground, style.ForegroundRGB, name)
				}
			}
		})
	}
}

func TestPaletteIndexedForegroundFallback(t *testing.T) {
	base := Theme{UsePalette: true, SchemeKnown: true, Known: true}

	tests := []struct {
		name  string
		theme Theme
		want  int
		ok    bool
	}{
		{name: "dark scheme uses bright blue", theme: base, want: 12, ok: true},
		{name: "light scheme uses blue", theme: func() Theme { t := base; t.Light = true; return t }(), want: 4, ok: true},
		{name: "unknown scheme remains neutral", theme: Theme{UsePalette: true, Known: true}},
		{name: "known RGB blue that cannot be evaluated remains neutral", theme: Theme{UsePalette: true, SchemeKnown: true, PaletteKnown: 1 << 4}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			styles := NewStyles(tt.theme)
			baselineTheme := tt.theme
			baselineTheme.UsePalette = false
			baseline := NewStyles(baselineTheme)
			for name, style := range map[string]renderer.Style{
				"title":        styles.TabTitle,
				"active title": styles.TabTitleActive,
				"border":       styles.Border,
				"muted text":   styles.PaletteDesc,
			} {
				require.False(t, style.HasBackgroundRGB, "%s must never receive an indexed background", name)
				if tt.ok {
					require.False(t, style.HasForegroundRGB, "%s must use an indexed foreground", name)
					require.Equal(t, tt.want, style.Foreground, name)
				} else {
					want := map[string]renderer.Style{
						"title": baseline.TabTitle, "active title": baseline.TabTitleActive,
						"border": baseline.Border, "muted text": baseline.PaletteDesc,
					}[name]
					require.Equal(t, want, style, name)
				}
			}
		})
	}
}

func TestPaletteMissingRGBUsesOnlyIndexedTitleForeground(t *testing.T) {
	theme := Theme{
		Foreground: renderer.RGB{R: 240, G: 240, B: 240},
		Background: renderer.RGB{R: 30, G: 30, B: 30},
		HasFG:      true, HasBG: true, Known: true, TrueColor: true,
		UsePalette: true, SchemeKnown: true,
	}
	styles := NewStyles(theme)
	for name, style := range map[string]renderer.Style{
		"title": styles.TabTitle, "active title": styles.TabTitleActive,
		"border": styles.Border, "muted text": styles.PaletteDesc,
	} {
		require.False(t, style.HasForegroundRGB, "%s must use an indexed foreground", name)
		require.Equal(t, ansiBrightBlue, style.Foreground, name)
		require.Equal(t, -1, style.Background, "%s must not use an indexed background", name)
	}
}

func TestPaletteDisabledAndForcedModesPreserveExistingStyles(t *testing.T) {
	palette := [16]renderer.RGB{4: {B: 255}, 12: {R: 255, G: 255, B: 255}}
	usableTheme := Theme{Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 100, G: 100, B: 100}, HasFG: true, HasBG: true, Known: true, TrueColor: true}
	withDisabledPalette := usableTheme
	withDisabledPalette.Palette = palette
	withDisabledPalette.PaletteKnown = 1<<4 | 1<<12
	withDisabledPalette.UsePalette = false

	require.Equal(t, NewStyles(usableTheme), NewStyles(withDisabledPalette))

	forced := Theme{Known: true, SchemeKnown: true, UsePalette: false}
	require.Equal(t, NewStyles(Theme{Known: true}), NewStyles(forced))
}

func TestNewStylesComposesSemanticStyles(t *testing.T) {
	theme := Theme{
		Foreground: renderer.RGB{R: 0xc8, G: 0xc8, B: 0xc8},
		Background: renderer.RGB{R: 0x0a, G: 0x14, B: 0x1e},
		HasFG:      true,
		HasBG:      true,
		Known:      true,
		TrueColor:  true,
	}

	styles := NewStyles(theme)
	statusBar := StatusBarStyle(theme)
	accent := AccentStyle(theme)
	selection := SelectionStyle(theme)

	require.True(t, styles.StatusBar.Equal(statusBar))
	require.True(t, styles.Accent.Equal(accent))
	require.True(t, styles.Border.Equal(BorderStyle(theme)))
	require.True(t, styles.Selection.Equal(selection))
	require.True(t, styles.CopyStatus.Equal(selection))
	require.True(t, styles.PaletteDesc.Equal(MutedTextStyle(theme)))
	require.True(t, styles.TabName.Equal(EmphasisStyle(statusBar, theme)))
	require.True(t, styles.TabNameActive.Equal(EmphasisStyle(accent, theme)))
	require.True(t, styles.TabTitle.Equal(MutedVariantStyle(statusBar, theme)))
	require.True(t, styles.TabTitleActive.Equal(MutedVariantStyle(accent, theme)))
	require.True(t, styles.PickerName.Equal(EmphasisStyle(renderer.DefaultStyle(), theme)))
	require.True(t, styles.PickerSelectionName.Equal(EmphasisStyle(selection, theme)))
	require.True(t, styles.PickerSelectionMuted.Equal(MutedVariantStyle(selection, theme)))
	require.True(t, styles.PickerSeparator.Equal(MutedTextStyle(theme)))
}
