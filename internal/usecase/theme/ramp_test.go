package theme

import (
	"math"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestBuildRampUsesExactSemanticWeights(t *testing.T) {
	theme := rampTheme(false)
	accent := Accent{RGB: renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}, Slot: 2, Known: true}
	ramp := BuildRamp(theme, accent)

	for name, got := range map[string]renderer.Style{
		"bar":      ramp.SurfaceBar,
		"inactive": ramp.SurfaceInactive,
		"recent":   ramp.SurfaceRecent,
	} {
		require.True(t, got.HasBackgroundRGB, name)
	}
	require.Equal(t, okLabLerp(theme.Background, accent.RGB, 0.08), ramp.SurfaceBar.BackgroundRGB)
	require.Equal(t, okLabLerp(theme.Background, accent.RGB, 0.14), ramp.SurfaceInactive.BackgroundRGB)
	require.Equal(t, okLabLerp(theme.Background, accent.RGB, 0.22), ramp.SurfaceRecent.BackgroundRGB)
	require.Equal(t, accent.RGB, ramp.SurfaceActive.BackgroundRGB)
	require.Equal(t, okLabLerp(theme.Background, accent.RGB, 0.60), ramp.BorderMuted.ForegroundRGB)
	require.Equal(t, accent.RGB, ramp.BorderActive.ForegroundRGB)
	require.False(t, ramp.BorderMuted.HasBackgroundRGB)
	require.False(t, ramp.BorderActive.HasBackgroundRGB)
}

func TestBuildRampAdaptsTextAndBordersForDarkAndLightThemes(t *testing.T) {
	for _, light := range []bool{false, true} {
		t.Run(map[bool]string{false: "dark", true: "light"}[light], func(t *testing.T) {
			theme := rampTheme(light)
			accent := Accent{RGB: renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}, Known: true}
			ramp := BuildRamp(theme, accent)
			for name, style := range map[string]renderer.Style{
				"bar": ramp.SurfaceBar, "inactive": ramp.SurfaceInactive,
				"recent": ramp.SurfaceRecent, "active": ramp.SurfaceActive,
			} {
				require.GreaterOrEqual(t, ContrastRatio(style.ForegroundRGB, style.BackgroundRGB), normalTextContrast, name)
			}
			for name, style := range map[string]renderer.Style{"muted": ramp.BorderMuted, "active": ramp.BorderActive} {
				require.GreaterOrEqual(t, ContrastRatio(style.ForegroundRGB, ramp.SurfaceBar.BackgroundRGB), borderContrast, name)
			}
		})
	}
}

func TestBuildRampReducesActiveToHighestSafeWeight(t *testing.T) {
	theme := Theme{
		Foreground: renderer.RGB{R: 0xe0, G: 0xe0, B: 0xe0},
		Background: renderer.RGB{R: 0x59, G: 0x59, B: 0x59},
		HasFG:      true, HasBG: true, Known: true, UsePalette: true,
	}
	accent := Accent{RGB: renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}, Known: true}
	ramp := BuildRamp(theme, accent)

	wantWeight := -1
	for weight := 100; weight >= 0; weight-- {
		surface := okLabLerp(theme.Background, accent.RGB, float64(weight)/100)
		primary, ok := primaryText(theme, surface)
		if !ok {
			continue
		}
		if _, ok := secondaryText(primary, surface); ok {
			wantWeight = weight
			break
		}
	}
	require.NotEqual(t, 100, wantWeight)
	require.Equal(t, okLabLerp(theme.Background, accent.RGB, float64(wantWeight)/100), ramp.SurfaceActive.BackgroundRGB)
}

func TestBuildRampWarnBorderIsAmberAndDistinctAcrossAccentHues(t *testing.T) {
	theme := rampTheme(false)

	for name, accentRGB := range map[string]renderer.RGB{
		"blue accent": {R: 0x6c, G: 0x9b, B: 0xd9},
		"red accent":  {R: 0xcc, G: 0x66, B: 0x66},
	} {
		t.Run(name, func(t *testing.T) {
			accent := Accent{RGB: accentRGB, Known: true}
			ramp := BuildRamp(theme, accent)

			require.True(t, ramp.BorderWarn.HasForegroundRGB)
			require.False(t, ramp.BorderWarn.HasBackgroundRGB)

			accentHue := oklabToOKLCh(rgbToOKLab(accentRGB)).H
			warnHue := oklabToOKLCh(rgbToOKLab(ramp.BorderWarn.ForegroundRGB)).H
			distanceToTarget := func(h float64) float64 {
				d := math.Abs(h - warnHueDegrees)
				if d > 180 {
					d = 360 - d
				}
				return d
			}
			// Warn must land measurably closer to the amber target than the
			// accent itself sits, in both directions (blue and the
			// adversarial case of a red accent, which sits close to zero
			// degrees and could otherwise get confused with an
			// under-rotated "warm" accent border).
			require.Less(t, distanceToTarget(warnHue), distanceToTarget(accentHue))
			require.Less(t, distanceToTarget(warnHue), 5.0)

			// Distinct from the sibling borders by more than a rounding
			// difference: use the same OKLab distance scale accent
			// clustering already treats as "different colors"
			// (accentClusterDistance groups colors within 0.04).
			warnLab := rgbToOKLab(ramp.BorderWarn.ForegroundRGB)
			mutedLab := rgbToOKLab(ramp.BorderMuted.ForegroundRGB)
			activeLab := rgbToOKLab(ramp.BorderActive.ForegroundRGB)
			require.Greater(t, okLabDistance(warnLab, mutedLab), accentClusterDistance)
			require.Greater(t, okLabDistance(warnLab, activeLab), accentClusterDistance)

			require.GreaterOrEqual(t, ContrastRatio(ramp.BorderWarn.ForegroundRGB, ramp.SurfaceBar.BackgroundRGB), borderContrast)
		})
	}
}

func TestMRUWeight(t *testing.T) {
	tests := []struct {
		count int
		want  []float64
	}{
		{count: 1, want: []float64{0.5}},
		{count: 2, want: []float64{0.5, 0.25}},
		{count: 3, want: []float64{0.5, 1.0 / 3, 1.0 / 6}},
		{count: 4, want: []float64{0.5, 0.375, 0.25, 0.125}},
	}
	for _, tt := range tests {
		for index, want := range tt.want {
			require.InDelta(t, want, mruWeight(index, tt.count), 1e-9, "count=%d index=%d", tt.count, index)
		}
	}
}

func TestMRUStylePlacesHistoryBetweenActiveAndBar(t *testing.T) {
	tests := []struct {
		name   string
		theme  Theme
		accent renderer.RGB
	}{
		{name: "dark teal", theme: rampTheme(false), accent: renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}},
		{name: "dark blue", theme: rampTheme(false), accent: renderer.RGB{R: 0x3b, G: 0x82, B: 0xf6}},
		{name: "light teal", theme: rampTheme(true), accent: renderer.RGB{R: 0x2a, G: 0x7a, B: 0x7a}},
		{name: "low contrast grey with blue", theme: Theme{Foreground: renderer.RGB{R: 160, G: 160, B: 160}, Background: renderer.RGB{R: 32, G: 32, B: 32}, HasFG: true, HasBG: true, Known: true, UsePalette: true}, accent: renderer.RGB{R: 0, G: 102, B: 255}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ramp := BuildRamp(tt.theme, Accent{RGB: tt.accent, Known: true})
			require.True(t, ramp.rgb)
			bar, active := ramp.SurfaceBar.BackgroundRGB, ramp.SurfaceActive.BackgroundRGB
			for count := 1; count <= 9; count++ {
				for index := range count {
					weight := mruWeight(index, count)
					style := MRUStyle(ramp, index, count)
					require.Equal(t, okLabLerp(bar, active, weight), style.BackgroundRGB, "count=%d index=%d", count, index)
					foreground, _ := primaryText(tt.theme, style.BackgroundRGB)
					require.Equal(t, foreground, style.ForegroundRGB)
				}
			}
			require.Equal(t, okLabLerp(bar, active, 0.5), MRUStyle(ramp, 0, 1).BackgroundRGB, "the newest entry always sits halfway")
		})
	}
}

func TestMRUStyleWithoutRGBKeepsRecentSurface(t *testing.T) {
	ramp := neutralRamp(rampTheme(false))
	require.Equal(t, ramp.SurfaceRecent, MRUStyle(ramp, 0, 3))
}

func TestResolveBuildsCompleteStylesFromOneAccent(t *testing.T) {
	theme := rampTheme(false)
	theme.Palette[2] = renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	theme.Palette[10] = theme.Palette[2]
	theme.PaletteKnown = 1<<2 | 1<<10
	resolved := Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})

	require.True(t, resolved.Accent.Known)
	require.Equal(t, resolved.Ramp.SurfaceBar, resolved.Styles.SurfaceBar)
	require.Equal(t, resolved.Ramp.SurfaceActive, resolved.Styles.SurfaceActive)
	require.Equal(t, resolved.Ramp.BorderActive, resolved.Styles.BorderActive)
	require.Equal(t, resolved.Ramp.BorderWarn, resolved.Styles.BorderWarn)
	require.Equal(t, foregroundStyle(Blend(theme.Foreground, theme.Background, 0.40)), resolved.Styles.NeutralBorder)
	require.True(t, resolved.Styles.TabActive.Bold)
	require.Equal(t, resolved.Styles.SurfaceRecent, resolved.Styles.MRURecent)
	require.Equal(t, renderer.DefaultStyle(), resolved.Styles.PickerBase)
	require.Equal(t, renderer.DefaultStyle(), resolved.Styles.PromptBase)
	require.False(t, resolved.Styles.PickerDescription.HasBackgroundRGB)
	require.False(t, resolved.Styles.PickerSeparator.HasBackgroundRGB)
	require.Equal(t, resolved.Styles.SurfaceActive.BackgroundRGB, resolved.Styles.PickerSelection.BackgroundRGB)
	require.True(t, resolved.Styles.PickerSelection.Bold)
}

func TestResolveANSI256AndPaletteOffSurfaces(t *testing.T) {
	theme := rampTheme(false)
	theme.Palette[2] = renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	theme.PaletteKnown = 1 << 2
	indexed := Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2})
	require.False(t, indexed.Accent.IndexedOnly)
	for name, style := range map[string]renderer.Style{
		"active tab":       indexed.Styles.TabActive,
		"active title":     indexed.Styles.TabActiveTitle,
		"picker selection": indexed.Styles.PickerSelection,
		"search selection": indexed.Styles.SearchSelection,
	} {
		t.Run(name, func(t *testing.T) {
			require.True(t, style.HasBackgroundRGB)
			require.Equal(t, indexed.Styles.SurfaceActive.BackgroundRGB, style.BackgroundRGB)
		})
	}

	theme.UsePalette = false
	off := Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2})
	require.False(t, off.Accent.Known)
	require.Equal(t, neutralStyles(theme), off.Styles)

	insufficient := rampTheme(false)
	insufficient.Foreground = insufficient.Background
	insufficient.Palette[2] = renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	insufficient.PaletteKnown = 1 << 2
	neutral := Resolve(insufficient, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2})
	require.Equal(t, neutralStyles(insufficient), neutral.Styles)
}

func rampTheme(light bool) Theme {
	if light {
		return Theme{Foreground: renderer.RGB{R: 0x20, G: 0x20, B: 0x20}, Background: renderer.RGB{R: 0xf8, G: 0xf8, B: 0xf8}, HasFG: true, HasBG: true, Known: true, UsePalette: true}
	}
	return Theme{Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8}, Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a}, HasFG: true, HasBG: true, Known: true, UsePalette: true}
}
