package theme

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestResolveAccentIndexedFallbackTruthTable(t *testing.T) {
	teal := renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	explicit := domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 5}
	tests := []struct {
		name   string
		theme  Theme
		policy domain.ThemeAccent
		want   Accent
	}{
		{
			name:   "explicit unknown slot decorates only with its exact index",
			theme:  paletteTheme(map[int]renderer.RGB{2: teal, 10: teal}),
			policy: explicit,
			want:   Accent{Slot: 5, IndexedOnly: true},
		},
		{
			name: "explicit known slot without truecolor is indexed only",
			theme: func() Theme {
				t := paletteTheme(map[int]renderer.RGB{5: teal})
				t.TrueColor = false
				return t
			}(),
			policy: explicit,
			want:   Accent{RGB: teal, Slot: 5, Known: true, IndexedOnly: true},
		},
		{
			name: "automatic absent osc palette keeps dark scheme indexed blue fallback",
			theme: Theme{
				Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8}, Background: renderer.RGB{R: 8, G: 9, B: 10},
				HasFG: true, HasBG: true, TrueColor: true, Known: true, SchemeKnown: true, UsePalette: true,
			},
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			want:   Accent{Slot: 12, IndexedOnly: true},
		},
		{
			name: "automatic absent defaults keeps light scheme indexed blue fallback",
			theme: Theme{
				TrueColor: true, Known: true, SchemeKnown: true, Light: true, UsePalette: true,
			},
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			want:   Accent{Slot: 4, IndexedOnly: true},
		},
		{
			name:   "automatic absent palette and unknown scheme stays neutral",
			theme:  Theme{TrueColor: true, Known: true, UsePalette: true},
			policy: domain.ThemeAccent{Mode: domain.ThemeAccentAuto},
			want:   Accent{},
		},
		{
			name: "palette off suppresses explicit indexed decoration",
			theme: func() Theme {
				t := paletteTheme(map[int]renderer.RGB{5: teal})
				t.UsePalette = false
				return t
			}(),
			policy: explicit,
			want:   Accent{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, ResolveAccent(tt.theme, tt.policy))
		})
	}
}

func TestResolveIndexedAccentNeverFillsAnIndexedBackground(t *testing.T) {
	theme := paletteTheme(map[int]renderer.RGB{2: {R: 0x7d, G: 0xb5, B: 0xb5}})
	resolved := Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 5})
	require.Equal(t, Accent{Slot: 5, IndexedOnly: true}, resolved.Accent)

	for name, style := range legacyAndSemanticStyles(resolved.Styles) {
		require.NotEqualf(t, 5, style.Background, "%s must not use an indexed accent background", name)
	}
	require.Equal(t, 5, resolved.Styles.BorderMuted.Foreground)
	require.Equal(t, 5, resolved.Styles.PickerSeparator.Foreground)
}

func TestPickerSeparatorUsesSecondaryTextContrast(t *testing.T) {
	theme := rampTheme(false)
	theme.Palette[2] = renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	theme.Palette[10] = theme.Palette[2]
	theme.PaletteKnown = 1<<2 | 1<<10
	resolved := Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})

	want, ok := secondaryText(theme.Foreground, theme.Background)
	require.True(t, ok)
	require.Equal(t, want, resolved.Styles.PickerSeparator.ForegroundRGB)
	require.False(t, resolved.Styles.PickerSeparator.HasBackgroundRGB)
	require.Equal(t, want, resolved.Styles.PickerDescription.ForegroundRGB)
	require.False(t, resolved.Styles.PickerDescription.HasBackgroundRGB)
	require.GreaterOrEqual(t, ContrastRatio(resolved.Styles.PickerSeparator.ForegroundRGB, theme.Background), normalTextContrast)
}

func TestLegacyAliasesPaletteOffAndForcedThemesRemainExact(t *testing.T) {
	paletteOff := paletteTheme(map[int]renderer.RGB{2: {R: 0x7d, G: 0xb5, B: 0xb5}})
	paletteOff.UsePalette = false
	tests := []struct {
		name  string
		theme Theme
	}{
		{name: "palette off", theme: paletteOff},
		{name: "forced dark", theme: BuiltinDark},
		{name: "forced light", theme: BuiltinLight},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Resolve(tt.theme, domain.ThemeAccent{Mode: domain.ThemeAccentSlot, Slot: 2}).Styles
			want := prePhase3LegacyAliases(tt.theme)
			// PickerSeparator is now a semantic role with the SurfaceBar
			// background; it was never a compatibility alias.
			delete(want, "PickerSeparator")
			for name, style := range want {
				require.Equalf(t, style, legacyAndSemanticStyles(got)[name], "%s", name)
			}
		})
	}
}

func prePhase3LegacyAliases(t Theme) map[string]renderer.Style {
	status := renderer.DefaultStyle()
	accent := inverseStyle()
	border := renderer.DefaultStyle()
	muted := renderer.DefaultStyle()
	if usable(t) {
		status = rgbSurface(t.Foreground, Blend(t.Background, t.Foreground, 0.12))
		accent = rgbSurface(t.Foreground, Blend(t.Background, t.Foreground, neutralAccentBlend))
		border = foregroundStyle(Blend(t.Foreground, t.Background, 0.40))
		muted = foregroundStyle(Blend(t.Foreground, t.Background, 0.45))
	}
	selection := accent
	return map[string]renderer.Style{
		"StatusBar":            status,
		"Accent":               accent,
		"Border":               border,
		"Selection":            selection,
		"CopyStatus":           selection,
		"PaletteDesc":          muted,
		"TabName":              EmphasisStyle(status, t),
		"TabNameActive":        EmphasisStyle(accent, t),
		"TabTitle":             MutedVariantStyle(status, t),
		"TabTitleActive":       MutedVariantStyle(accent, t),
		"PickerName":           EmphasisStyle(renderer.DefaultStyle(), t),
		"PickerSelectionName":  EmphasisStyle(selection, t),
		"PickerSelectionMuted": MutedVariantStyle(selection, t),
		"PickerSeparator":      muted,
	}
}

func legacyAndSemanticStyles(styles Styles) map[string]renderer.Style {
	return map[string]renderer.Style{
		"SurfaceBar": styles.SurfaceBar, "SurfaceInactive": styles.SurfaceInactive,
		"SurfaceRecent": styles.SurfaceRecent, "SurfaceActive": styles.SurfaceActive,
		"BorderMuted": styles.BorderMuted, "BorderActive": styles.BorderActive,
		"NeutralBorder": styles.NeutralBorder, "PickerSeparator": styles.PickerSeparator, "StatusBar": styles.StatusBar,
		"Accent": styles.Accent, "Border": styles.Border, "Selection": styles.Selection,
		"CopyStatus": styles.CopyStatus, "PaletteDesc": styles.PaletteDesc,
		"TabName": styles.TabName, "TabNameActive": styles.TabNameActive,
		"TabTitle": styles.TabTitle, "TabTitleActive": styles.TabTitleActive,
		"PickerName": styles.PickerName, "PickerSelectionName": styles.PickerSelectionName,
		"PickerSelectionMuted": styles.PickerSelectionMuted,
	}
}
