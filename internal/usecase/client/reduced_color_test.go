package client

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/require"

	ansirenderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/picker"
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

// TestPickerProblemDotsKeepMeaningPerProfile checks how each profile encodes
// the problem dots: truecolor and 256 keep the fixed indexed colors, ANSI-16 is
// quantized by the renderer, and monochrome marks error and warning dots bold
// since no color survives. Informational statuses never gain weight.
func TestPickerProblemDotsKeepMeaningPerProfile(t *testing.T) {
	snapshot := pickerSnapshotFixture()
	snapshot.Lines = []protocol.PickerLine{
		{Key: "aa/err", Kind: protocol.PickerLineSession, Label: "err", Focusable: true, Actions: protocol.PickerCanNavigate, Status: protocol.PickerLineStatusError},
		{Key: "bb/warn", Kind: protocol.PickerLineSession, Label: "warn", Focusable: true, Actions: protocol.PickerCanNavigate, Status: protocol.PickerLineStatusStale},
		{Key: "cc/ver", Kind: protocol.PickerLineSession, Label: "ver", Focusable: true, Actions: protocol.PickerCanNavigate, Status: protocol.PickerLineStatusVersion},
	}
	snapshot.Cursor = protocol.PickerCursor{Key: "aa/err"}
	// dot returns the SGR group in effect for the dot on the row labelled label.
	dot := func(out, label string) string {
		row := regexp.MustCompile(`(?s)` + label + `[^●]*(\x1b\[[0-9;]*m)[^●\x1b]*●`)
		m := row.FindStringSubmatch(out)
		require.NotNil(t, m, "no problem dot after %q in %q", label, out)
		return m[1]
	}
	tests := []struct {
		name    string
		profile ansirenderer.ColorProfile
		want    map[string]string // label -> exact SGR before the dot
	}{
		{name: "truecolor", profile: ansirenderer.ColorProfileTrueColor, want: map[string]string{"err": "\x1b[0;38;5;167m", "warn": "\x1b[0;38;5;179m", "ver": "\x1b[0;38;5;170m"}},
		{name: "ansi256", profile: ansirenderer.ColorProfileANSI256, want: map[string]string{"err": "\x1b[0;38;5;167m", "warn": "\x1b[0;38;5;179m", "ver": "\x1b[0;38;5;170m"}},
		{name: "ansi16", profile: ansirenderer.ColorProfileANSI16, want: map[string]string{"err": "", "warn": "", "ver": ""}},
		{name: "monochrome", profile: ansirenderer.ColorProfileMonochrome, want: map[string]string{"err": "\x1b[0;1m", "warn": "\x1b[0;1m", "ver": "\x1b[0m"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loop := pickerLoopFromSnapshot(snapshot, protocol.PickerIntentNavigation, picker.SortRecent)
			out := string(newPickerRenderer(tt.profile).render(loop, domain.Size{Cols: 100, Rows: 30}, emptyPickerPreview()))
			for label, want := range tt.want {
				got := dot(out, label)
				if want == "" {
					// ANSI-16 colors are chosen by the renderer's quantizer: only
					// require a basic or bright palette slot, never extended color.
					require.Regexp(t, `^\x1b\[0;(3[0-7]|9[0-7])m$`, got, label)
					continue
				}
				require.Equal(t, want, got, label)
			}
		})
	}
}
