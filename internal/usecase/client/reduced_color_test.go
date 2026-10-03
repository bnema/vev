package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
)

func TestPickerStaysLegibleInReducedColor(t *testing.T) {
	loop := pickerLoopFixture(t)
	size := domain.Size{Cols: 100, Rows: 30}

	ansi16 := string(newPickerRenderer(ansirenderer.ColorProfileANSI16).render(loop, size, emptyPickerPreview()))
	require.NotRegexp(t, `(?:38|48|58);[25];`, ansi16, "ansi16 picker emitted extended colors")
	require.Regexp(t, `\x1b\[0;[0-9;]*7`, ansi16, "the selected row must stay reverse video")

	mono := string(newPickerRenderer(ansirenderer.ColorProfileMonochrome).render(loop, size, emptyPickerPreview()))
	require.NotRegexp(t, `\x1b\[[0-9;:]*(?:38|48|58|3[0-7]|4[0-7]|9[0-7]|10[0-7])[;:m]`, mono, "monochrome picker emitted a color")
	require.Regexp(t, `\x1b\[0;[0-9;]*7`, mono, "the selected row must stay reverse video")
	require.Contains(t, mono, "work")
}

func TestToastBorderFollowsColorProfile(t *testing.T) {
	tests := []struct {
		name     string
		profile  ansirenderer.ColorProfile
		severity domain.NoticeSeverity
		want     string
	}{
		{name: "truecolor error", profile: ansirenderer.ColorProfileTrueColor, severity: domain.NoticeError, want: "\x1b[38;5;167m"},
		{name: "256 warn", profile: ansirenderer.ColorProfileANSI256, severity: domain.NoticeWarn, want: "\x1b[38;5;179m"},
		{name: "256 info", profile: ansirenderer.ColorProfileANSI256, severity: domain.NoticeInfo},
		{name: "16 error", profile: ansirenderer.ColorProfileANSI16, severity: domain.NoticeError, want: "\x1b[31m"},
		{name: "16 warn", profile: ansirenderer.ColorProfileANSI16, severity: domain.NoticeWarn, want: "\x1b[33m"},
		{name: "16 info", profile: ansirenderer.ColorProfileANSI16, severity: domain.NoticeInfo},
		{name: "mono error", profile: ansirenderer.ColorProfileMonochrome, severity: domain.NoticeError, want: "\x1b[1m"},
		{name: "mono warn", profile: ansirenderer.ColorProfileMonochrome, severity: domain.NoticeWarn, want: "\x1b[1m"},
		{name: "mono info", profile: ansirenderer.ColorProfileMonochrome, severity: domain.NoticeInfo},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, toastBorderSGRFor(tt.severity, tt.profile))
		})
	}
	// Monochrome borders are closed with normal intensity, not default color.
	bounds := domain.Rect{Width: 12, Height: 3}
	lines := clientToastLines(bounds, "hi", toastBorderSGRFor(domain.NoticeError, ansirenderer.ColorProfileMonochrome))
	require.Contains(t, lines[0], "\x1b[22m")
	require.NotContains(t, lines[0], "\x1b[39m")
}
