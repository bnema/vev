package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	vt "bnema.dev/vev-vt"

	"github.com/bnema/vev/internal/domain"
)

func TestParseAgentsSpinner(t *testing.T) {
	tests := []struct {
		name      string
		value     string
		want      []rune
		wantValid bool
	}{
		{name: "circle preset", value: "circle", want: []rune("\U000f0a9e\U000f0a9f\U000f0aa0\U000f0aa1\U000f0aa2\U000f0aa3\U000f0aa4\U000f0aa5"), wantValid: true},
		{name: "jump preset", value: "jump", want: []rune("⢄⢂⢁⡁⡈⡐⡠"), wantValid: true},
		{name: "case-insensitive preset", value: "MiniDot", want: []rune("⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"), wantValid: true},
		{name: "dot preset", value: "dot", want: []rune("⣾⣽⣻⢿⡿⣟⣯⣷"), wantValid: true},
		{name: "line preset", value: "line", want: []rune(`|/-\`), wantValid: true},
		{name: "hamburger preset", value: "hamburger", want: []rune("☱☲☴☲"), wantValid: true},
		{name: "star preset", value: "star", want: []rune("✶✸✹✺✹✷"), wantValid: true},
		{name: "off", value: "off", want: []rune{}, wantValid: true},
		{name: "custom frames", value: "a b c", want: []rune("abc"), wantValid: true},
		{name: "quoted custom frames", value: `"◐ ◓ ◑ ◒"`, want: []rune("◐◓◑◒"), wantValid: true},
		{name: "wide rune", value: "中 a", wantValid: false},
		{name: "combining mark", value: "\u0301 a", wantValid: false},
		{name: "multi-rune frame", value: "ab c", wantValid: false},
		{name: "single frame", value: "x", wantValid: false},
		{name: "too many frames", value: strings.Repeat("x ", maxSpinnerFrames+1), wantValid: false},
		{name: "empty", value: "", wantValid: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseSpinner(tt.value)
			require.Equal(t, tt.wantValid, ok)
			if tt.wantValid {
				require.Equal(t, tt.want, got)
			}
		})
	}
}

func TestParseAgentsConfig(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		want         domain.AgentsConfig
		wantWarnings []string
	}{
		{name: "defaults leave the spinner unset", input: "", want: domain.AgentsConfig{Activity: true}},
		{name: "activity off", input: "agents.activity = off\n", want: domain.AgentsConfig{Activity: false}},
		{name: "explicit jump is set", input: "agents.spinner = jump\n", want: domain.AgentsConfig{Activity: true, Spinner: []rune("⢄⢂⢁⡁⡈⡐⡠"), SpinnerSet: true}},
		{name: "explicit circle is set", input: "agents.spinner = circle\n", want: domain.AgentsConfig{Activity: true, Spinner: domain.DefaultAgentSpinner(), SpinnerSet: true}},
		{name: "spinner preset", input: "agents.spinner = line\n", want: domain.AgentsConfig{Activity: true, Spinner: []rune(`|/-\`), SpinnerSet: true}},
		{name: "spinner off", input: "agents.spinner = off\n", want: domain.AgentsConfig{Activity: true, Spinner: []rune{}, SpinnerSet: true}},
		{
			name:         "invalid spinner falls back to the glyph set",
			input:        "agents.spinner = 中 文\n",
			want:         domain.AgentsConfig{Activity: true},
			wantWarnings: []string{`invalid agents.spinner "中 文"; using the ui.glyphs spinner`},
		},
		{
			name:         "invalid activity keeps default",
			input:        "agents.activity = maybe\n",
			want:         domain.AgentsConfig{Activity: true},
			wantWarnings: []string{`invalid agents.activity "maybe"`},
		},
		{
			name:         "invalid spinner after a valid one resets to unset",
			input:        "agents.spinner = dot\nagents.spinner = 中 文\n",
			want:         domain.AgentsConfig{Activity: true},
			wantWarnings: []string{`duplicate key "agents.spinner"`, `invalid agents.spinner "中 文"; using the ui.glyphs spinner`},
		},
		{
			name:         "duplicate key warns and last wins",
			input:        "agents.spinner = dot\nagents.spinner = line\n",
			want:         domain.AgentsConfig{Activity: true, Spinner: []rune(`|/-\`), SpinnerSet: true},
			wantWarnings: []string{`duplicate key "agents.spinner"`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.Agents)
			msgs := make([]string, 0, len(warnings))
			for _, w := range warnings {
				msgs = append(msgs, w.Msg)
			}
			if tt.wantWarnings == nil {
				require.Empty(t, msgs)
				return
			}
			require.Equal(t, tt.wantWarnings, msgs)
		})
	}
}

func TestParseUIGlyphs(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		want         domain.GlyphMode
		wantWarnings []string
	}{
		{name: "default is auto", input: "", want: domain.GlyphsAuto},
		{name: "auto", input: "ui.glyphs = auto\n", want: domain.GlyphsAuto},
		{name: "nerd", input: "ui.glyphs = nerd\n", want: domain.GlyphsNerd},
		{name: "unicode", input: "ui.glyphs = unicode\n", want: domain.GlyphsUnicode},
		{name: "ascii is case-insensitive", input: "ui.glyphs = ASCII\n", want: domain.GlyphsASCII},
		{name: "quoted", input: "ui.glyphs = \"ascii\"\n", want: domain.GlyphsASCII},
		{
			name:         "invalid warns and uses auto",
			input:        "ui.glyphs = nerd\nui.glyphs = emoji\n",
			want:         domain.GlyphsAuto,
			wantWarnings: []string{`duplicate key "ui.glyphs"`, `invalid ui.glyphs "emoji" (want auto, nerd, unicode, or ascii); using auto`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, warnings, err := Parse(strings.NewReader(tt.input))
			require.NoError(t, err)
			require.Equal(t, tt.want, cfg.UI.Glyphs)
			msgs := make([]string, 0, len(warnings))
			for _, w := range warnings {
				msgs = append(msgs, w.Msg)
			}
			if tt.wantWarnings == nil {
				require.Empty(t, msgs)
				return
			}
			require.Equal(t, tt.wantWarnings, msgs)
		})
	}
}

func TestSpinnerPresetFramesAreOneCell(t *testing.T) {
	for name, frames := range spinnerPresets {
		t.Run(name, func(t *testing.T) {
			for _, r := range frames {
				require.Equal(t, 1, vt.RuneWidth(r), "%U", r)
			}
		})
	}
}
