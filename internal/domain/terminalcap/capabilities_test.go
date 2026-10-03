package terminalcap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestColorCapabilities(t *testing.T) {
	tests := []struct {
		name       string
		in         ColorCapabilities
		wantRGB    bool
		wantColors int
	}{
		{name: "zero value is truecolor", in: ColorCapabilities{}, wantRGB: true, wantColors: 16777216},
		{name: "truecolor", in: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, wantRGB: true, wantColors: 16777216},
		{name: "ansi256", in: ColorCapabilities{Mode: ANSI256}, wantRGB: false, wantColors: 256},
		{name: "ansi16", in: ColorCapabilities{Mode: ANSI16, Source: SourceForced}, wantRGB: false, wantColors: 16},
		{name: "monochrome", in: ColorCapabilities{Mode: Monochrome, Source: SourceForced}, wantRGB: false, wantColors: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantRGB, tt.in.RGB())
			require.Equal(t, tt.wantColors, tt.in.Colors())
		})
	}
}

func TestColorCapabilitiesValid(t *testing.T) {
	tests := []struct {
		name string
		in   ColorCapabilities
		want bool
	}{
		{name: "zero value", in: ColorCapabilities{}, want: true},
		{name: "every mode and source", in: ColorCapabilities{Mode: Monochrome, Source: SourceForced}, want: true},
		{name: "unknown mode", in: ColorCapabilities{Mode: Monochrome + 1}},
		{name: "unknown source", in: ColorCapabilities{Source: SourceForced + 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.in.Valid())
		})
	}
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name     string
		env      []string
		declared ColorCapabilities
		want     ColorCapabilities
		wantApp  Application
	}{
		{name: "unknown claim falls back to detection", env: []string{"TERM=xterm-256color"}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "unknown truecolor claim falls back to detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: TrueColor}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "unknown claim with detected truecolor", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declared truecolor overrides indexed detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declared truecolor on unknown terminal", env: []string{"TERM=unknown"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declared 256 overrides detected truecolor verbatim", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, declared: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "forced 16 colors", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, declared: ColorCapabilities{Mode: ANSI16, Source: SourceForced}, want: ColorCapabilities{Mode: ANSI16, Source: SourceForced}},
		{name: "forced monochrome", env: nil, declared: ColorCapabilities{Mode: Monochrome, Source: SourceForced}, want: ColorCapabilities{Mode: Monochrome, Source: SourceForced}},
		{name: "heuristic truecolor upgrades weaker detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, want: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}},
		{name: "heuristic truecolor keeps stronger detection", env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, want: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, wantApp: ApplicationKitty},
		{name: "heuristic 16 colors falls back to detection", env: []string{"TERM=xterm-256color"}, declared: ColorCapabilities{Mode: ANSI16, Source: SourceHeuristic}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "declared claim keeps detected application", env: []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"}, declared: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, wantApp: ApplicationKitty},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Resolve(tt.env, tt.declared)
			require.Equal(t, tt.want, got.Color)
			require.Equal(t, tt.wantApp, got.Application)
			require.False(t, got.KittyGraphics)
		})
	}
}

func TestDetect(t *testing.T) {
	tests := []struct {
		name         string
		env          []string
		wantMode     ColorMode
		wantSource   Source
		wantApp      Application
		wantGraphics bool
	}{
		{name: "declared truecolor", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "declared 24bit", env: []string{"TERM=xterm-256color", "COLORTERM=24bit"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "direct terminfo entry", env: []string{"TERM=foot-direct"}, wantMode: TrueColor, wantSource: SourceDeclared},
		{name: "kitty signals infer truecolor", env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, wantMode: TrueColor, wantSource: SourceHeuristic, wantApp: ApplicationKitty},
		{name: "kitty environment with declared truecolor remains color-only", env: []string{"TERM=xterm-kitty", "COLORTERM=truecolor", "KITTY_WINDOW_ID=1"}, wantMode: TrueColor, wantSource: SourceDeclared, wantApp: ApplicationKitty},
		{name: "256 color terminal remains a constrained attachment", env: []string{"TERM=xterm-256color"}, wantMode: ANSI256, wantSource: SourceDeclared},
		{name: "unknown terminal is conservatively indexed", env: []string{"TERM=unknown"}, wantMode: ANSI256, wantSource: SourceUnknown},
		{name: "kitty environment behind a multiplexer is not graphics evidence", env: []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"}, wantMode: ANSI256, wantSource: SourceDeclared, wantApp: ApplicationKitty},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Detect(tt.env)
			require.Equal(t, tt.wantMode, got.Color.Mode)
			require.Equal(t, tt.wantSource, got.Color.Source)
			require.Equal(t, tt.wantApp, got.Application)
			require.Equal(t, tt.wantGraphics, got.SupportsKittyGraphics())
		})
	}
}
