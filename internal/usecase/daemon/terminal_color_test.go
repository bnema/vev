package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestAdaptFrameColors(t *testing.T) {
	cases := []struct {
		name      string
		trueColor bool
		style     renderer.Style
		want      renderer.Style
	}{
		{
			name:      "truecolor passthrough",
			trueColor: true,
			style: renderer.Style{
				Foreground:           17,
				Background:           18,
				HasForegroundRGB:     true,
				ForegroundRGB:        renderer.RGB{R: 12, G: 34, B: 56},
				HasBackgroundRGB:     true,
				BackgroundRGB:        renderer.RGB{R: 78, G: 90, B: 123},
				UnderlineStyle:       renderer.UnderlineCurly,
				HasUnderlineColor:    true,
				UnderlineColor:       19,
				HasUnderlineColorRGB: true,
				UnderlineColorRGB:    renderer.RGB{R: 145, G: 167, B: 189},
			},
			want: renderer.Style{
				Foreground:           17,
				Background:           18,
				HasForegroundRGB:     true,
				ForegroundRGB:        renderer.RGB{R: 12, G: 34, B: 56},
				HasBackgroundRGB:     true,
				BackgroundRGB:        renderer.RGB{R: 78, G: 90, B: 123},
				UnderlineStyle:       renderer.UnderlineCurly,
				HasUnderlineColor:    true,
				UnderlineColor:       19,
				HasUnderlineColorRGB: true,
				UnderlineColorRGB:    renderer.RGB{R: 145, G: 167, B: 189},
			},
		},
		{
			name:      "indexed colors convert foreground background and underline",
			trueColor: false,
			style: renderer.Style{
				Foreground:           1,
				Background:           2,
				HasForegroundRGB:     true,
				ForegroundRGB:        renderer.RGB{R: 255, G: 0, B: 0},
				HasBackgroundRGB:     true,
				BackgroundRGB:        renderer.RGB{R: 0, G: 255, B: 0},
				UnderlineStyle:       renderer.UnderlineSingle,
				HasUnderlineColor:    true,
				UnderlineColor:       3,
				HasUnderlineColorRGB: true,
				UnderlineColorRGB:    renderer.RGB{R: 0, G: 0, B: 255},
			},
			want: renderer.Style{
				Foreground:           196,
				Background:           46,
				HasForegroundRGB:     false,
				ForegroundRGB:        renderer.RGB{R: 255, G: 0, B: 0},
				HasBackgroundRGB:     false,
				BackgroundRGB:        renderer.RGB{R: 0, G: 255, B: 0},
				UnderlineStyle:       renderer.UnderlineSingle,
				HasUnderlineColor:    true,
				UnderlineColor:       21,
				HasUnderlineColorRGB: false,
				UnderlineColorRGB:    renderer.RGB{R: 0, G: 0, B: 255},
			},
		},
		{
			name:      "neutral grays use the grayscale ramp",
			trueColor: false,
			style: renderer.Style{
				Foreground:           1,
				Background:           2,
				HasForegroundRGB:     true,
				ForegroundRGB:        renderer.RGB{R: 128, G: 128, B: 128},
				HasBackgroundRGB:     true,
				BackgroundRGB:        renderer.RGB{R: 100, G: 100, B: 100},
				UnderlineStyle:       renderer.UnderlineDashed,
				HasUnderlineColor:    true,
				UnderlineColor:       3,
				HasUnderlineColorRGB: true,
				UnderlineColorRGB:    renderer.RGB{R: 50, G: 50, B: 50},
			},
			want: renderer.Style{
				Foreground:           244,
				Background:           241,
				HasForegroundRGB:     false,
				ForegroundRGB:        renderer.RGB{R: 128, G: 128, B: 128},
				HasBackgroundRGB:     false,
				BackgroundRGB:        renderer.RGB{R: 100, G: 100, B: 100},
				UnderlineStyle:       renderer.UnderlineDashed,
				HasUnderlineColor:    true,
				UnderlineColor:       236,
				HasUnderlineColorRGB: false,
				UnderlineColorRGB:    renderer.RGB{R: 50, G: 50, B: 50},
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			frame := renderer.NewFrame(1, 1)
			frame.Cells[0].Style = tc.style
			composed := frame.Clone()

			got := adaptFrameColors(frame, tc.trueColor)

			require.Equal(t, tc.want, got.Cells[0].Style)
			require.Equal(t, composed, frame, "adaptation must not mutate composed frame state")
		})
	}
}

func TestXterm256Color(t *testing.T) {
	cases := []struct {
		name string
		rgb  renderer.RGB
		want int
	}{
		{
			name: "exact neutral gray",
			rgb:  renderer.RGB{R: 128, G: 128, B: 128},
			want: 244,
		},
		{
			name: "nearest neutral gray",
			rgb:  renderer.RGB{R: 100, G: 100, B: 100},
			want: 241,
		},
		{
			name: "color between cube levels",
			rgb:  renderer.RGB{R: 100, G: 100, B: 200},
			want: 62,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, xterm256Color(tc.rgb))
		})
	}
}
