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

func channelDelta(a, b uint8) uint8 {
	if a > b {
		return a - b
	}
	return b - a
}
