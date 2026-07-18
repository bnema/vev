package theme

import (
	"math"

	"github.com/bnema/vev/internal/domain"
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
		style.HasForegroundRGB = false
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

// Styles is the complete semantic set of terminal chrome styles. The legacy
// aliases remain until daemon renderers consume the semantic fields directly.
type Styles struct {
	// mruStyles are all position/count combinations that status composition
	// can display. They keep OKLab interpolation out of render paths.
	mruStyles [9][9]renderer.Style

	SurfaceBar      renderer.Style
	SurfaceInactive renderer.Style
	SurfaceRecent   renderer.Style
	SurfaceActive   renderer.Style
	BorderMuted     renderer.Style
	BorderActive    renderer.Style
	// NeutralBorder is the non-accent structural border for pane dividers and
	// unfocused pane title bars. It is resolved with the rest of the immutable
	// chrome snapshot so composition never derives theme colors.
	NeutralBorder renderer.Style

	TabInactive      renderer.Style
	TabInactiveTitle renderer.Style
	TabActive        renderer.Style
	TabActiveTitle   renderer.Style
	MRURecent        renderer.Style

	PickerBase        renderer.Style
	PickerSelection   renderer.Style
	PickerDescription renderer.Style
	PickerSeparator   renderer.Style
	PromptBase        renderer.Style
	CopyStatus        renderer.Style
	SearchSelection   renderer.Style

	// Compatibility aliases for pre-cache render paths.
	StatusBar            renderer.Style
	Accent               renderer.Style
	Border               renderer.Style
	Selection            renderer.Style
	PaletteDesc          renderer.Style
	TabName              renderer.Style
	TabNameActive        renderer.Style
	TabTitle             renderer.Style
	TabTitleActive       renderer.Style
	PickerName           renderer.Style
	PickerSelectionName  renderer.Style
	PickerSelectionMuted renderer.Style
}

// NewStyles applies the default automatic policy. New callers that own a
// policy should use Resolve and retain its complete immutable result.
func NewStyles(t Theme) Styles {
	return Resolve(t, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles
}

// MRUStyle returns the position-dependent recent-session surface. Color
// interpolation stays owned by theme; renderers consume only this cached
// semantic style snapshot.
func (s Styles) MRUStyle(index, count int) renderer.Style {
	if count <= 0 || count > len(s.mruStyles) {
		return s.MRURecent
	}
	if index < 0 {
		index = 0
	}
	if index >= count {
		index = count - 1
	}
	return s.mruStyles[count-1][index]
}
