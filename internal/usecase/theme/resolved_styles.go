package theme

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

const neutralAccentBlend = 0.25

// ResolvedTheme is the immutable, complete chrome style snapshot produced from
// a terminal theme and typed accent policy.
type ResolvedTheme struct {
	Theme  Theme
	Accent Accent
	Ramp   Ramp
	Styles Styles
}

// Resolve is the single entry point for terminal-owned accent inference and
// semantic chrome construction.
func Resolve(t Theme, policy domain.ThemeAccent) ResolvedTheme {
	accent := ResolveAccent(t, policy)
	ramp := BuildRamp(t, accent)
	styles := stylesFromRamp(t, accent, ramp)
	return ResolvedTheme{Theme: t, Accent: accent, Ramp: ramp, Styles: styles}
}

func stylesFromRamp(t Theme, accent Accent, ramp Ramp) Styles {
	if !accent.Known || accent.IndexedOnly || !usable(t) || !ramp.rgb {
		styles := neutralStyles(t)
		if accent.IndexedOnly {
			styles = indexedStyles(styles, accent.Slot)
		}
		return styles
	}

	inactiveTitle := secondarySurface(ramp.SurfaceInactive)
	activeTitle := secondarySurface(ramp.SurfaceActive)
	inactiveDescription := secondarySurface(ramp.SurfaceInactive)
	barDescription := secondarySurface(ramp.SurfaceBar)
	styles := Styles{
		SurfaceBar:      ramp.SurfaceBar,
		SurfaceInactive: ramp.SurfaceInactive,
		SurfaceRecent:   ramp.SurfaceRecent,
		SurfaceActive:   ramp.SurfaceActive,
		BorderMuted:     ramp.BorderMuted,
		BorderActive:    ramp.BorderActive,

		TabInactive:      ramp.SurfaceInactive,
		TabInactiveTitle: inactiveTitle,
		TabActive:        EmphasisStyle(ramp.SurfaceActive, t),
		TabActiveTitle:   activeTitle,
		MRURecent:        ramp.SurfaceRecent,

		PickerBase:        ramp.SurfaceBar,
		PickerSelection:   EmphasisStyle(ramp.SurfaceActive, t),
		PickerDescription: inactiveDescription,
		// Separators are secondary text on the picker base surface, not
		// non-text borders; they therefore require the normal 4.5:1 contrast.
		PickerSeparator: barDescription,
		PromptBase:      ramp.SurfaceBar,
		CopyStatus:      ramp.SurfaceBar,
		SearchSelection: EmphasisStyle(ramp.SurfaceActive, t),
	}
	return withMRUStyles(withLegacyAliases(styles, t), ramp)
}

func secondarySurface(base renderer.Style) renderer.Style {
	if !base.HasForegroundRGB || !base.HasBackgroundRGB {
		return base
	}
	foreground, ok := secondaryText(base.ForegroundRGB, base.BackgroundRGB)
	if !ok {
		return base
	}
	base.ForegroundRGB = foreground
	return base
}

func indexedStyles(styles Styles, slot uint8) Styles {
	styles.BorderMuted = indexedForeground(styles.BorderMuted, slot)
	styles.BorderActive = indexedForeground(styles.BorderActive, slot)
	styles.TabInactiveTitle = indexedForeground(styles.TabInactiveTitle, slot)
	styles.TabActiveTitle = indexedForeground(styles.TabActiveTitle, slot)
	styles.PickerDescription = indexedForeground(styles.PickerDescription, slot)
	styles.PickerSeparator = indexedForeground(styles.PickerSeparator, slot)

	// Compatibility aliases historically received the scheme-aware indexed
	// blue fallback too. Keep the strict explicit index on each foreground
	// decoration without changing any neutral surface background.
	styles.Border = styles.BorderMuted
	styles.PaletteDesc = styles.PickerDescription
	styles.TabTitle = styles.TabInactiveTitle
	styles.TabTitleActive = styles.TabActiveTitle
	styles.PickerSelectionMuted = indexedForeground(styles.PickerSelectionMuted, slot)
	return styles
}

func indexedForeground(style renderer.Style, slot uint8) renderer.Style {
	style.HasForegroundRGB = false
	style.Foreground = int(slot)
	return style
}

// neutralStyles deliberately preserves the pre-accent neutral hierarchy. It
// is used for palette-off, forced themes, incomplete defaults, and RGB-less
// indexed fallbacks; it never performs a palette slot lookup.
func neutralStyles(t Theme) Styles {
	status := renderer.DefaultStyle()
	accent := inverseStyle()
	border := renderer.DefaultStyle()
	muted := renderer.DefaultStyle()
	if usable(t) {
		status = rgbSurface(t.Foreground, Blend(t.Background, t.Foreground, 0.12))
		accent = rgbSurface(t.Foreground, Blend(t.Background, t.Foreground, neutralAccentBlend))
		border = neutralBorderStyle(t)
		muted = foregroundStyle(Blend(t.Foreground, t.Background, 0.45))
	}
	styles := Styles{
		SurfaceBar:      status,
		SurfaceInactive: status,
		SurfaceRecent:   status,
		SurfaceActive:   accent,
		BorderMuted:     border,
		BorderActive:    border,

		TabInactive:      EmphasisStyle(status, t),
		TabInactiveTitle: MutedVariantStyle(status, t),
		TabActive:        EmphasisStyle(accent, t),
		TabActiveTitle:   MutedVariantStyle(accent, t),
		MRURecent:        status,

		PickerBase:        status,
		PickerSelection:   EmphasisStyle(accent, t),
		PickerDescription: muted,
		PickerSeparator:   muted,
		PromptBase:        status,
		CopyStatus:        accent,
		SearchSelection:   accent,
	}
	styles = withLegacyAliases(styles, t)
	// Neutral and indexed fallbacks retain their existing non-RGB hierarchy;
	// MRU entries deliberately share the neutral recent surface.
	return withMRUStyles(styles, Ramp{SurfaceBar: styles.SurfaceBar, SurfaceRecent: styles.SurfaceRecent})
}

func withMRUStyles(styles Styles, ramp Ramp) Styles {
	for count := 1; count <= len(styles.mruStyles); count++ {
		for index := 0; index < count; index++ {
			styles.mruStyles[count-1][index] = MRUStyle(ramp, index, count)
		}
	}
	return styles
}

// NeutralBorderStyle is the existing non-accent structural border input for
// dimmed pane chrome. It intentionally does not consume a resolved accent.
func NeutralBorderStyle(t Theme) renderer.Style {
	if !usable(t) {
		return renderer.DefaultStyle()
	}
	return foregroundStyle(Blend(t.Foreground, t.Background, 0.40))
}

func neutralBorderStyle(t Theme) renderer.Style { return NeutralBorderStyle(t) }

func withLegacyAliases(styles Styles, t Theme) Styles {
	styles.StatusBar = styles.SurfaceBar
	styles.Accent = styles.SurfaceActive
	styles.Border = styles.BorderMuted
	styles.Selection = styles.SurfaceActive
	styles.PaletteDesc = styles.PickerDescription
	styles.TabName = styles.TabInactive
	styles.TabNameActive = styles.TabActive
	styles.TabTitle = styles.TabInactiveTitle
	styles.TabTitleActive = styles.TabActiveTitle
	// These aliases retain their pre-semantic-role contracts for render paths
	// that have not migrated yet.
	styles.PickerName = EmphasisStyle(renderer.DefaultStyle(), t)
	styles.PickerSelectionName = EmphasisStyle(styles.Selection, t)
	styles.PickerSelectionMuted = MutedVariantStyle(styles.Selection, t)
	return styles
}
