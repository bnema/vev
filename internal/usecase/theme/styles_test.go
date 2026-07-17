package theme

import (
	"testing"

	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

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
