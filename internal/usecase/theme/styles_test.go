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
	mixedBase := rgbBase
	mixedBase.HasBackgroundRGB = false

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
			name:      "RGB foreground without RGB background uses indexed grayscale",
			base:      mixedBase,
			intensity: 0.5,
			want: func() renderer.Style {
				style := mixedBase
				style.Bold = true
				style.HasForegroundRGB = false
				style.Foreground = 249
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
