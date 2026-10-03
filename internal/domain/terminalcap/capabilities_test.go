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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.wantRGB, tt.in.RGB())
			require.Equal(t, tt.wantColors, tt.in.Colors())
		})
	}
}

func TestResolve(t *testing.T) {
	tests := []struct {
		name     string
		env      []string
		declared bool
		want     ColorCapabilities
		wantApp  Application
	}{
		{name: "256 color without declaration stays indexed", env: []string{"TERM=xterm-256color"}, want: ColorCapabilities{Mode: ANSI256, Source: SourceDeclared}},
		{name: "declaration upgrades indexed terminal", env: []string{"TERM=xterm-256color"}, declared: true, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declaration upgrades unknown terminal", env: []string{"TERM=unknown"}, declared: true, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declaration keeps environment truecolor detection", env: []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"}, declared: true, want: ColorCapabilities{Mode: TrueColor, Source: SourceHeuristic}, wantApp: ApplicationKitty},
		{name: "environment truecolor without declaration", env: []string{"TERM=xterm-256color", "COLORTERM=truecolor"}, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}},
		{name: "declaration keeps application", env: []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"}, declared: true, want: ColorCapabilities{Mode: TrueColor, Source: SourceDeclared}, wantApp: ApplicationKitty},
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
