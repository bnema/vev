package theme

import (
	"math"

	"github.com/bnema/vev/pkg/renderer"
)

const accentContrastMin = 3.0

// RelativeLuminance returns a color's WCAG 2.x relative luminance.
func RelativeLuminance(color renderer.RGB) float64 {
	linearize := func(channel uint8) float64 {
		srgb := float64(channel) / 255
		if srgb <= 0.04045 {
			return srgb / 12.92
		}
		return math.Pow((srgb+0.055)/1.055, 2.4)
	}

	return 0.2126*linearize(color.R) +
		0.7152*linearize(color.G) +
		0.0722*linearize(color.B)
}

// ContrastRatio returns the WCAG 2.x contrast ratio between two colors.
func ContrastRatio(a, b renderer.RGB) float64 {
	aLuminance := RelativeLuminance(a)
	bLuminance := RelativeLuminance(b)
	lighter := math.Max(aLuminance, bLuminance)
	darker := math.Min(aLuminance, bLuminance)
	return (lighter + 0.05) / (darker + 0.05)
}

// PulseColor derives the attention pulse color at intensity while preserving
// the base style's remaining attributes.
func PulseColor(base renderer.Style, intensity float64) renderer.Style {
	style := base
	style.Bold = true
	if base.HasForegroundRGB && base.HasBackgroundRGB {
		style.ForegroundRGB = Blend(base.BackgroundRGB, base.ForegroundRGB, intensity)
	} else {
		style.Foreground = 244 + int(intensity*11)
	}
	return style
}

// MRUFade fades an MRU style by position, reaching 60 percent of the terminal
// background color at the oldest entry.
func MRUFade(base renderer.Style, t Theme, i, count int) renderer.Style {
	if count <= 1 || !base.HasForegroundRGB || !base.HasBackgroundRGB || !t.HasBG {
		return base
	}
	amount := (float64(i) / float64(count-1)) * 0.6
	base.ForegroundRGB = Blend(base.ForegroundRGB, t.Background, amount)
	base.BackgroundRGB = Blend(base.BackgroundRGB, t.Background, amount)
	return base
}

// Styles is the resolved set of terminal chrome styles for a client theme.
type Styles struct {
	StatusBar   renderer.Style
	Accent      renderer.Style
	Border      renderer.Style
	Selection   renderer.Style
	CopyStatus  renderer.Style
	PaletteDesc renderer.Style

	TabName        renderer.Style
	TabNameActive  renderer.Style
	TabTitle       renderer.Style
	TabTitleActive renderer.Style

	PickerName           renderer.Style
	PickerSelectionName  renderer.Style
	PickerSelectionMuted renderer.Style
	PickerSeparator      renderer.Style
}

// NewStyles composes the terminal chrome styles from the semantic theme styles.
func NewStyles(t Theme) Styles {
	statusBar := StatusBarStyle(t)
	accent := AccentStyle(t)
	selection := SelectionStyle(t)
	return Styles{
		StatusBar:   statusBar,
		Accent:      accent,
		Border:      BorderStyle(t),
		Selection:   selection,
		CopyStatus:  selection,
		PaletteDesc: MutedTextStyle(t),

		TabName:        EmphasisStyle(statusBar, t),
		TabNameActive:  EmphasisStyle(accent, t),
		TabTitle:       PaletteMutedVariantStyle(statusBar, t),
		TabTitleActive: PaletteMutedVariantStyle(accent, t),

		PickerName:           EmphasisStyle(renderer.DefaultStyle(), t),
		PickerSelectionName:  EmphasisStyle(selection, t),
		PickerSelectionMuted: PaletteMutedVariantStyle(selection, t),
		PickerSeparator:      MutedTextStyle(t),
	}
}
