package theme

import "github.com/bnema/vev/pkg/renderer"

const (
	normalTextContrast = 4.5
	borderContrast     = 3.0

	// warnHueDegrees is the fixed OKLCh hue target for the Warn border role.
	// Reference amber hues under this package's own rgbToOKLab: conventional
	// "amber" #FFBF00 sits at ~84 degrees, Tailwind's amber-500 (#F59E0B) at
	// ~70 degrees, CSS "orange" at ~71 degrees. 75 degrees sits centrally in
	// that band, so a warn border reads as amber/gold regardless of which
	// hue the user's single accent happens to occupy (blue ~256, red ~29,
	// green ~145 under the same conversion).
	warnHueDegrees = 75.0
)

// Ramp is the contrast-safe set of semantic terminal chrome colors.
type Ramp struct {
	SurfaceBar      renderer.Style
	SurfaceInactive renderer.Style
	SurfaceRecent   renderer.Style
	SurfaceActive   renderer.Style
	BorderMuted     renderer.Style
	BorderActive    renderer.Style
	BorderWarn      renderer.Style

	background renderer.RGB
	accent     renderer.RGB
	rgb        bool
}

// BuildRamp derives every RGB surface from the terminal background and one
// resolved accent. It deliberately never uses indexed colors for backgrounds.
func BuildRamp(t Theme, accent Accent) Ramp {
	if !accent.Known || accent.IndexedOnly || !usable(t) {
		return neutralRamp(t)
	}

	bar, ok := surfaceAtOrBelow(t, accent.RGB, 8)
	if !ok {
		return neutralRamp(t)
	}
	inactive, ok := surfaceAtOrBelow(t, accent.RGB, 14)
	if !ok {
		return neutralRamp(t)
	}
	recent, ok := surfaceAtOrBelow(t, accent.RGB, 22)
	if !ok {
		return neutralRamp(t)
	}
	active, ok := surfaceAtOrBelow(t, accent.RGB, 100)
	if !ok {
		return neutralRamp(t)
	}

	return Ramp{
		SurfaceBar:      bar,
		SurfaceInactive: inactive,
		SurfaceRecent:   recent,
		SurfaceActive:   active,
		BorderMuted:     mutedBorder(t, accent.RGB, bar.BackgroundRGB),
		BorderActive:    activeBorder(t, accent.RGB, bar.BackgroundRGB),
		BorderWarn:      warnBorder(t, accent.RGB, bar.BackgroundRGB),
		background:      t.Background,
		accent:          accent.RGB,
		rgb:             true,
	}
}

// surfaceAtOrBelow searches from the requested intensity to neutral. The
// integer sequence is intentional: it makes contrast adaptation stable.
func surfaceAtOrBelow(t Theme, accent renderer.RGB, target int) (renderer.Style, bool) {
	for weight := target; weight >= 0; weight-- {
		background := okLabLerp(t.Background, accent, float64(weight)/100)
		foreground, ok := primaryText(t, background)
		if !ok {
			continue
		}
		if _, ok := secondaryText(foreground, background); !ok {
			continue
		}
		return rgbSurface(foreground, background), true
	}
	return renderer.Style{}, false
}

func rgbSurface(foreground, background renderer.RGB) renderer.Style {
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = foreground
	style.HasBackgroundRGB = true
	style.BackgroundRGB = background
	return style
}

// primaryText chooses only the reported default foreground or background.
func primaryText(t Theme, surface renderer.RGB) (renderer.RGB, bool) {
	best := t.Foreground
	bestContrast := ContrastRatio(t.Foreground, surface)
	backgroundContrast := ContrastRatio(t.Background, surface)
	if backgroundContrast > bestContrast {
		best, bestContrast = t.Background, backgroundContrast
	}
	return best, bestContrast >= normalTextContrast
}

// secondaryText mutes toward the actual surface while retaining normal-text
// contrast. Searching down from 40 selects the greatest permitted mute.
func secondaryText(primary, surface renderer.RGB) (renderer.RGB, bool) {
	for weight := 40; weight >= 0; weight-- {
		candidate := okLabLerp(primary, surface, float64(weight)/100)
		if ContrastRatio(candidate, surface) >= normalTextContrast {
			return candidate, true
		}
	}
	return renderer.RGB{}, false
}

// mutedBorder starts at 60 percent and only moves toward the accent, choosing
// the lowest weight that separates from its adjacent surface.
func mutedBorder(t Theme, accent, adjacent renderer.RGB) renderer.Style {
	for weight := 60; weight <= 100; weight++ {
		candidate := okLabLerp(t.Background, accent, float64(weight)/100)
		if ContrastRatio(candidate, adjacent) >= borderContrast {
			return foregroundStyle(candidate)
		}
	}
	return neutralBorder(t)
}

// activeBorder starts at the accent endpoint and reduces only when needed.
func activeBorder(t Theme, accent, adjacent renderer.RGB) renderer.Style {
	for weight := 100; weight >= 0; weight-- {
		candidate := okLabLerp(t.Background, accent, float64(weight)/100)
		if ContrastRatio(candidate, adjacent) >= borderContrast {
			return foregroundStyle(candidate)
		}
	}
	return neutralBorder(t)
}

// warnBorder searches the same accent-derived weight scale activeBorder
// uses, but replaces each candidate's hue with the fixed amber target before
// checking contrast: the result keeps the accent's lightness/chroma band
// (so it belongs to the ramp) while always reading as amber, regardless of
// the user's accent hue.
func warnBorder(t Theme, accent, adjacent renderer.RGB) renderer.Style {
	for weight := 100; weight >= 0; weight-- {
		candidate := okLabLerp(t.Background, accent, float64(weight)/100)
		amber := shiftHue(candidate, warnHueDegrees)
		if ContrastRatio(amber, adjacent) >= borderContrast {
			return foregroundStyle(amber)
		}
	}
	return neutralBorder(t)
}

func foregroundStyle(color renderer.RGB) renderer.Style {
	style := renderer.DefaultStyle()
	style.HasForegroundRGB = true
	style.ForegroundRGB = color
	return style
}

func neutralRamp(t Theme) Ramp {
	styles := neutralStyles(t)
	return Ramp{
		SurfaceBar:      styles.SurfaceBar,
		SurfaceInactive: styles.SurfaceInactive,
		SurfaceRecent:   styles.SurfaceRecent,
		SurfaceActive:   styles.SurfaceActive,
		BorderMuted:     styles.BorderMuted,
		BorderActive:    styles.BorderActive,
		BorderWarn:      styles.BorderWarn,
	}
}

// MRUStyle interpolates the newest entry from SurfaceRecent (22%) to 11% for
// the oldest entry. A singleton is therefore the full recent surface.
func MRUStyle(ramp Ramp, index, count int) renderer.Style {
	if count <= 1 || !ramp.SurfaceRecent.HasBackgroundRGB || !ramp.SurfaceBar.HasBackgroundRGB {
		return ramp.SurfaceRecent
	}
	if index < 0 {
		index = 0
	}
	if index >= count {
		index = count - 1
	}
	if !ramp.rgb {
		return ramp.SurfaceRecent
	}
	weight := 22.0 - 11.0*float64(index)/float64(count-1)
	background := okLabLerp(ramp.background, ramp.accent, weight/100)
	foreground, ok := primaryText(Theme{Foreground: ramp.SurfaceRecent.ForegroundRGB, Background: ramp.background}, background)
	if !ok {
		return ramp.SurfaceRecent
	}
	return rgbSurface(foreground, background)
}
