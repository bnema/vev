package theme

import (
	"math"

	renderer "github.com/bnema/vev-vt"
)

// oklab is the perceptually uniform OKLab representation of an sRGB color.
type oklab struct {
	L float64
	A float64
	B float64
}

// rgbToOKLab converts an 8-bit sRGB color to OKLab via linear sRGB and LMS.
func rgbToOKLab(color renderer.RGB) oklab {
	r := srgbToLinear(color.R)
	g := srgbToLinear(color.G)
	b := srgbToLinear(color.B)

	l := math.Cbrt(0.4122214708*r + 0.5363325363*g + 0.0514459929*b)
	m := math.Cbrt(0.2119034982*r + 0.6806995451*g + 0.1073969566*b)
	s := math.Cbrt(0.0883024619*r + 0.2817188376*g + 0.6299787005*b)

	return oklab{
		L: 0.2104542553*l + 0.7936177850*m - 0.0040720468*s,
		A: 1.9779984951*l - 2.4285922050*m + 0.4505937099*s,
		B: 0.0259040371*l + 0.7827717662*m - 0.8086757660*s,
	}
}

// okLabToRGB converts OKLab to sRGB. Linear channels are clamped before
// transfer encoding so out-of-gamut intermediate values are deterministic.
func okLabToRGB(color oklab) renderer.RGB {
	l := cube(color.L + 0.3963377774*color.A + 0.2158037573*color.B)
	m := cube(color.L - 0.1055613458*color.A - 0.0638541728*color.B)
	s := cube(color.L - 0.0894841775*color.A - 1.2914855480*color.B)

	return renderer.RGB{
		R: linearToSRGB(4.0767416621*l - 3.3077115913*m + 0.2309699292*s),
		G: linearToSRGB(-1.2684380046*l + 2.6097574011*m - 0.3413193965*s),
		B: linearToSRGB(-0.0041960863*l - 0.7034186147*m + 1.7076147010*s),
	}
}

func okLabDistance(a, b oklab) float64 {
	return math.Sqrt(square(a.L-b.L) + square(a.A-b.A) + square(a.B-b.B))
}

func okLabChroma(color oklab) float64 {
	return math.Hypot(color.A, color.B)
}

// oklch is the polar (lightness, chroma, hue) form of oklab. Hue is degrees
// in [0, 360).
type oklch struct {
	L float64
	C float64
	H float64
}

// oklabToOKLCh converts to the polar form used to isolate hue from
// lightness and chroma.
func oklabToOKLCh(color oklab) oklch {
	hue := math.Atan2(color.B, color.A) * 180 / math.Pi
	if hue < 0 {
		hue += 360
	}
	return oklch{L: color.L, C: okLabChroma(color), H: hue}
}

// oklchToOKLab converts back from the polar form.
func oklchToOKLab(color oklch) oklab {
	rad := color.H * math.Pi / 180
	return oklab{L: color.L, A: color.C * math.Cos(rad), B: color.C * math.Sin(rad)}
}

// shiftHue replaces color's OKLCh hue with targetDegrees while preserving
// its lightness and chroma, so the result stays in the same value/intensity
// band as the source. A near-neutral source (chroma close to zero) yields a
// near-neutral result regardless of the target: hue is not meaningful there.
func shiftHue(color renderer.RGB, targetDegrees float64) renderer.RGB {
	lch := oklabToOKLCh(rgbToOKLab(color))
	lch.H = targetDegrees
	return okLabToRGB(oklchToOKLab(lch))
}

// okLabLerp interpolates sRGB endpoints in OKLab. Exact endpoints avoid
// round-trip quantization changing terminal-provided colors.
func okLabLerp(a, b renderer.RGB, weight float64) renderer.RGB {
	weight = clampUnit(weight)
	if weight == 0 {
		return a
	}
	if weight == 1 {
		return b
	}
	start := rgbToOKLab(a)
	end := rgbToOKLab(b)
	return okLabToRGB(oklab{
		L: start.L + (end.L-start.L)*weight,
		A: start.A + (end.A-start.A)*weight,
		B: start.B + (end.B-start.B)*weight,
	})
}

func srgbToLinear(channel uint8) float64 {
	value := float64(channel) / 255
	if value <= 0.04045 {
		return value / 12.92
	}
	return math.Pow((value+0.055)/1.055, 2.4)
}

func linearToSRGB(value float64) uint8 {
	value = clampUnit(value)
	if value <= 0.0031308 {
		value *= 12.92
	} else {
		value = 1.055*math.Pow(value, 1/2.4) - 0.055
	}
	return uint8(value*255 + 0.5)
}

func clampUnit(value float64) float64 {
	if math.IsNaN(value) || value <= 0 {
		return 0
	}
	if value >= 1 {
		return 1
	}
	return value
}

func square(value float64) float64 {
	return value * value
}

func cube(value float64) float64 {
	return value * value * value
}
