package theme

import (
	"math"
	"testing"

	"github.com/bnema/vev/pkg/renderer"
)

func TestRGBToOKLabKnownValues(t *testing.T) {
	tests := []struct {
		name string
		rgb  renderer.RGB
		want oklab
	}{
		{name: "black", rgb: renderer.RGB{}, want: oklab{}},
		{name: "white", rgb: renderer.RGB{R: 255, G: 255, B: 255}, want: oklab{L: 1}},
		{name: "red", rgb: renderer.RGB{R: 255}, want: oklab{L: 0.627955, A: 0.224863, B: 0.125846}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := rgbToOKLab(tt.rgb)
			if math.Abs(got.L-tt.want.L) > 0.000001 || math.Abs(got.A-tt.want.A) > 0.000001 || math.Abs(got.B-tt.want.B) > 0.000001 {
				t.Fatalf("rgbToOKLab(%+v) = %+v, want %+v", tt.rgb, got, tt.want)
			}
		})
	}
}

func TestOKLabRGBRoundTrip(t *testing.T) {
	colors := []renderer.RGB{
		{},
		{R: 255, G: 255, B: 255},
		{R: 255},
		{G: 255},
		{B: 255},
		{R: 0x7d, G: 0xb5, B: 0xb5},
		{R: 0x6c, G: 0x9b, B: 0xd9},
	}
	for _, color := range colors {
		t.Run("round_trip", func(t *testing.T) {
			got := okLabToRGB(rgbToOKLab(color))
			if channelDelta(got.R, color.R) > 1 || channelDelta(got.G, color.G) > 1 || channelDelta(got.B, color.B) > 1 {
				t.Fatalf("round trip %v = %v", color, got)
			}
		})
	}
}

func TestOKLabDistance(t *testing.T) {
	a := rgbToOKLab(renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5})
	b := rgbToOKLab(renderer.RGB{R: 0x6c, G: 0x9b, B: 0xd9})
	if got := okLabDistance(a, a); got != 0 {
		t.Fatalf("self distance = %v, want 0", got)
	}
	if got, want := okLabDistance(a, b), okLabDistance(b, a); math.Abs(got-want) > 1e-15 || got <= 0 {
		t.Fatalf("distance symmetry: %v != %v", got, want)
	}
}

func TestOKLabLerpEndpointsAndMonotonicity(t *testing.T) {
	background := renderer.RGB{R: 0x08, G: 0x09, B: 0x0a}
	accent := renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}

	if got := okLabLerp(background, accent, 0); got != background {
		t.Fatalf("weight 0 = %v, want %v", got, background)
	}
	if got := okLabLerp(background, accent, 1); got != accent {
		t.Fatalf("weight 1 = %v, want %v", got, accent)
	}
	if got := okLabLerp(background, accent, -1); got != background {
		t.Fatalf("clamped low weight = %v, want %v", got, background)
	}
	if got := okLabLerp(background, accent, 2); got != accent {
		t.Fatalf("clamped high weight = %v, want %v", got, accent)
	}

	previous := background
	for _, weight := range []float64{0.25, 0.5, 0.75, 1} {
		got := okLabLerp(background, accent, weight)
		if got.R < previous.R || got.G < previous.G || got.B < previous.B {
			t.Fatalf("weight %v = %v is not monotonic after %v", weight, got, previous)
		}
		previous = got
	}
}

func TestOKLabToRGBClampsOutOfGamutChannels(t *testing.T) {
	got := okLabToRGB(oklab{L: 0.7, A: 0.5, B: 0.5})
	if got.R != 255 || got.G != 0 || got.B != 0 {
		t.Fatalf("out-of-gamut color = %v, want deterministic per-channel clamp to {255 0 0}", got)
	}
}

func TestOKLabOKLChRoundTrip(t *testing.T) {
	colors := []oklab{
		rgbToOKLab(renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}),
		rgbToOKLab(renderer.RGB{R: 0xff}),
		rgbToOKLab(renderer.RGB{B: 0xff}),
		rgbToOKLab(renderer.RGB{R: 128, G: 128, B: 128}),
	}
	for _, want := range colors {
		got := oklchToOKLab(oklabToOKLCh(want))
		if math.Abs(got.L-want.L) > 1e-9 || math.Abs(got.A-want.A) > 1e-9 || math.Abs(got.B-want.B) > 1e-9 {
			t.Fatalf("oklch round trip %+v = %+v, want %+v", want, got, want)
		}
	}
}

func TestOKLabToOKLChHueIsDegreesInRange(t *testing.T) {
	tests := []struct {
		name string
		rgb  renderer.RGB
		want float64
	}{
		// Known reference hues, computed with the same conversion this
		// package uses: red sits near 29 degrees, blue near 264, amber
		// (#FFBF00) near 84.
		{name: "red", rgb: renderer.RGB{R: 255}, want: 29.2},
		{name: "amber #FFBF00", rgb: renderer.RGB{R: 0xff, G: 0xbf, B: 0x00}, want: 84.1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := oklabToOKLCh(rgbToOKLab(tt.rgb))
			if got.H < 0 || got.H >= 360 {
				t.Fatalf("hue %v out of [0, 360) range", got.H)
			}
			if math.Abs(got.H-tt.want) > 0.5 {
				t.Fatalf("hue = %v, want ~%v", got.H, tt.want)
			}
		})
	}
}

func TestShiftHuePreservesLightnessAndChromaButReplacesHue(t *testing.T) {
	tests := []struct {
		name   string
		source renderer.RGB
	}{
		{name: "blue accent", source: renderer.RGB{R: 0x6c, G: 0x9b, B: 0xd9}},
		{name: "red accent", source: renderer.RGB{R: 0xcc, G: 0x66, B: 0x66}},
		{name: "green accent", source: renderer.RGB{R: 0x6c, G: 0xae, B: 0x6c}},
	}
	const target = warnHueDegrees

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sourceLCh := oklabToOKLCh(rgbToOKLab(tt.source))
			shifted := shiftHue(tt.source, target)
			shiftedLCh := oklabToOKLCh(rgbToOKLab(shifted))

			// sRGB gamut clamping in okLabToRGB can perturb L/C slightly for
			// out-of-gamut combinations; allow a small tolerance. These three
			// sources sit safely inside the gamut at the amber target hue
			// (verified independently), so a tight tolerance still catches a
			// broken implementation.
			if math.Abs(shiftedLCh.L-sourceLCh.L) > 0.03 {
				t.Fatalf("lightness drifted: got %v, want ~%v", shiftedLCh.L, sourceLCh.L)
			}
			if math.Abs(shiftedLCh.C-sourceLCh.C) > 0.03 {
				t.Fatalf("chroma drifted: got %v, want ~%v", shiftedLCh.C, sourceLCh.C)
			}
			if math.Abs(shiftedLCh.H-target) > 1 {
				t.Fatalf("hue = %v, want ~%v", shiftedLCh.H, target)
			}
		})
	}
}

func channelDelta(a, b uint8) uint8 {
	if a > b {
		return a - b
	}
	return b - a
}
