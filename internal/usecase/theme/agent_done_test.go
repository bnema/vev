package theme

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
)

func TestAgentDoneStyle(t *testing.T) {
	green := renderer.RGB{R: 0x50, G: 0xc8, B: 0x78}
	darkGreen := renderer.RGB{R: 0x10, G: 0x20, B: 0x10}
	brightGreen := renderer.RGB{R: 0x7c, G: 0xff, B: 0x9c}
	withPalette := func(known uint16, slot2, slot10 renderer.RGB) Theme {
		t := rampTheme(false)
		t.Palette[2], t.Palette[10] = slot2, slot10
		t.PaletteKnown = known
		return t
	}
	indexed := IndexedAgentDone()
	tests := []struct {
		name    string
		theme   Theme
		color   terminalcap.ColorMode
		wantRGB *renderer.RGB
		wantFG  int
	}{
		{name: "truecolor palette green", theme: withPalette(1<<2|1<<10, green, brightGreen), color: terminalcap.TrueColor, wantRGB: &green},
		{name: "low contrast falls back to bright green", theme: withPalette(1<<2|1<<10, darkGreen, brightGreen), color: terminalcap.TrueColor, wantRGB: &brightGreen},
		{name: "only bright green known", theme: withPalette(1<<10, renderer.RGB{}, brightGreen), color: terminalcap.TrueColor, wantRGB: &brightGreen},
		{name: "low contrast green without bright green keeps green", theme: withPalette(1<<2, darkGreen, renderer.RGB{}), color: terminalcap.TrueColor, wantRGB: &darkGreen},
		{name: "unknown palette is indexed green", theme: withPalette(0, renderer.RGB{}, renderer.RGB{}), color: terminalcap.TrueColor, wantFG: indexed.Foreground},
		{name: "no theme is indexed green", theme: Theme{}, color: terminalcap.TrueColor, wantFG: 2},
		{name: "reduced is ANSI green", theme: withPalette(1<<2, green, renderer.RGB{}), color: terminalcap.ANSI16, wantFG: 2},
		{name: "monochrome has no color", theme: withPalette(1<<2, green, renderer.RGB{}), color: terminalcap.Monochrome, wantFG: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := ResolveForColor(tt.theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, terminalcap.ColorCapabilities{Mode: tt.color, Source: terminalcap.SourceDeclared}).Styles.AgentDone
			if tt.wantRGB != nil {
				require.True(t, got.HasForegroundRGB)
				require.Equal(t, *tt.wantRGB, got.ForegroundRGB)
				return
			}
			require.False(t, got.HasForegroundRGB)
			require.Equal(t, tt.wantFG, got.Foreground)
			require.False(t, got.HasBackgroundRGB, "AgentDone carries only a foreground")
		})
	}
}

func TestPulseDone(t *testing.T) {
	base := rgbSurface(renderer.RGB{R: 200, G: 200, B: 200}, renderer.RGB{R: 20, G: 20, B: 20})
	base.Italic = true
	green := foregroundStyle(renderer.RGB{R: 0, G: 220, B: 100})
	tests := []struct {
		name      string
		base      renderer.Style
		done      renderer.Style
		intensity float64
		wantRGB   *renderer.RGB
		wantFG    int
	}{
		{name: "peak is the done color", base: base, done: green, intensity: 1, wantRGB: &green.ForegroundRGB},
		{name: "faded toward the background", base: base, done: green, intensity: 0.5, wantRGB: ptr(Blend(base.BackgroundRGB, green.ForegroundRGB, 0.5))},
		{name: "no RGB background keeps the done color", base: renderer.DefaultStyle(), done: green, intensity: 0.2, wantRGB: &green.ForegroundRGB},
		{name: "indexed green", base: base, done: IndexedAgentDone(), intensity: 0.4, wantFG: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PulseDone(tt.base, tt.done, tt.intensity)
			require.True(t, got.Bold)
			require.Equal(t, tt.base.Italic, got.Italic)
			require.Equal(t, tt.base.HasBackgroundRGB, got.HasBackgroundRGB)
			require.Equal(t, tt.base.BackgroundRGB, got.BackgroundRGB, "the cell keeps its background")
			if tt.wantRGB != nil {
				require.True(t, got.HasForegroundRGB)
				require.Equal(t, *tt.wantRGB, got.ForegroundRGB)
				return
			}
			require.False(t, got.HasForegroundRGB)
			require.Equal(t, tt.wantFG, got.Foreground)
		})
	}
}

func TestPulseDoneAttribute(t *testing.T) {
	tests := []struct {
		name   string
		done   renderer.Style
		wantFG int
	}{
		{name: "ANSI16 green", done: IndexedAgentDone(), wantFG: 2},
		{name: "monochrome", done: renderer.DefaultStyle(), wantFG: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := PulseDoneAttribute(renderer.DefaultStyle(), tt.done)
			require.True(t, got.Bold)
			require.Equal(t, tt.wantFG, got.Foreground)
		})
	}
}

func ptr[T any](v T) *T { return &v }
