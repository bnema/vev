package theme

import (
	"math"

	renderer "bnema.dev/vev-vt"
	"github.com/bnema/vev/internal/domain"
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

// PulseAttribute is PulseColor for reduced-color attachments: the glyph's
// on/off beat already pulses, so the visible beat is just bold. Grey indexes
// would quantize to arbitrary slots or vanish.
func PulseAttribute(base renderer.Style) renderer.Style {
	base.Bold = true
	return base
}

// PulseDone is PulseColor for the agent Done cell: the glyph fades from the
// cell's background toward the done color instead of the base foreground.
// An indexed done color, or a base without an RGB background, cannot be
// blended, so the glyph keeps the done color and only the beat pulses.
func PulseDone(base, done renderer.Style, intensity float64) renderer.Style {
	style := base
	style.Bold = true
	switch {
	case done.HasForegroundRGB && base.HasBackgroundRGB:
		style.HasForegroundRGB = true
		style.ForegroundRGB = Blend(base.BackgroundRGB, done.ForegroundRGB, intensity)
	case done.HasForegroundRGB:
		style.HasForegroundRGB = true
		style.ForegroundRGB = done.ForegroundRGB
	case done.Foreground >= 0:
		style.HasForegroundRGB = false
		style.Foreground = done.Foreground
	}
	return style
}

// PulseDoneAttribute is PulseDone for reduced-color attachments: the done
// palette slot, if any, in bold on the visible beat.
func PulseDoneAttribute(base, done renderer.Style) renderer.Style {
	base.Bold = true
	if !done.HasForegroundRGB && done.Foreground >= 0 {
		base.HasForegroundRGB = false
		base.Foreground = done.Foreground
	}
	return base
}

// ansiGreen and ansiBrightGreen are the terminal palette slots of the agent
// Done color.
const (
	ansiGreen       = 2
	ansiBrightGreen = 10
)

// IndexedAgentDone is the agent Done color by palette index: ANSI green.
func IndexedAgentDone() renderer.Style {
	style := renderer.DefaultStyle()
	style.Foreground = ansiGreen
	return style
}

// agentDoneStyle resolves the agent Done foreground: the terminal's own ANSI
// green, or bright green when green lacks contrast against the bar, and
// indexed green when the palette or the bar background is unknown.
func agentDoneStyle(t Theme, bar renderer.Style) renderer.Style {
	if !t.UsePalette || !bar.HasBackgroundRGB {
		return IndexedAgentDone()
	}
	known := func(slot uint) bool { return t.PaletteKnown&(uint16(1)<<slot) != 0 }
	background := bar.BackgroundRGB
	switch {
	case known(ansiGreen) && ContrastRatio(t.Palette[ansiGreen], background) >= accentContrastMin:
		return foregroundStyle(t.Palette[ansiGreen])
	case known(ansiBrightGreen) && (!known(ansiGreen) ||
		ContrastRatio(t.Palette[ansiBrightGreen], background) > ContrastRatio(t.Palette[ansiGreen], background)):
		return foregroundStyle(t.Palette[ansiBrightGreen])
	case known(ansiGreen):
		return foregroundStyle(t.Palette[ansiGreen])
	}
	return IndexedAgentDone()
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
	// Reduced marks the ANSI16/monochrome policy: no RGB anywhere, and the
	// attention pulse uses bold instead of a grey ramp (see ResolveForColor).
	Reduced bool

	// mruStyles are all position/count combinations that status composition
	// can display. They keep OKLab interpolation out of render paths.
	mruStyles [9][9]renderer.Style

	SurfaceBar      renderer.Style
	SurfaceInactive renderer.Style
	SurfaceRecent   renderer.Style
	SurfaceActive   renderer.Style
	BorderMuted     renderer.Style
	BorderActive    renderer.Style
	// BorderWarn is the amber-hued warn-severity border. Unlike every other
	// role in this camaïeu palette it does not blend toward the resolved
	// accent color: it keeps the accent's lightness/chroma band but shifts
	// hue to a fixed amber target (see warnHueDegrees), so Warn reads as
	// amber and stays distinct from Error regardless of the accent hue.
	BorderWarn renderer.Style
	// NeutralBorder is the non-accent structural border for pane dividers and
	// unfocused pane title bars. It is resolved with the rest of the immutable
	// chrome snapshot so composition never derives theme colors.
	NeutralBorder renderer.Style

	TabInactive      renderer.Style
	TabInactiveTitle renderer.Style
	TabActive        renderer.Style
	TabActiveTitle   renderer.Style
	MRURecent        renderer.Style

	PickerBase      renderer.Style
	PickerSelection renderer.Style
	// HintKey is the keycap badge style for key hints: an accent background
	// over " key " padding, distinct from PickerSelection's full-line emphasis.
	HintKey           renderer.Style
	PickerDescription renderer.Style
	PickerSeparator   renderer.Style
	PromptBase        renderer.Style
	CopyStatus        renderer.Style
	SearchSelection   renderer.Style
	// AgentDone carries only the foreground of the agent Done cell: an RGB
	// terminal green, or a palette index. Renderers apply it over the cell's
	// own background (see PulseDone).
	AgentDone renderer.Style

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

// WithTransparentBar returns s with the top and bottom bar fill on the
// terminal's default colors. Tabs, MRU entries, and every other role keep
// their theme colors.
func (s Styles) WithTransparentBar() Styles {
	s.SurfaceBar = renderer.DefaultStyle()
	return s
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
