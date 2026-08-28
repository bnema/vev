package terminalcap

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectTrueColorPreservesKittyEnvironment(t *testing.T) {
	require.True(t, DetectTrueColor("xterm-kitty", "", []string{"KITTY_WINDOW_ID=1"}))
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
		{name: "256 color terminal remains a constrained attachment", env: []string{"TERM=xterm-256color"}, wantMode: Indexed256, wantSource: SourceDeclared},
		{name: "unknown terminal is conservatively indexed", env: []string{"TERM=unknown"}, wantMode: Indexed256, wantSource: SourceUnknown},
		{name: "kitty environment behind a multiplexer is not graphics evidence", env: []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"}, wantMode: Indexed256, wantSource: SourceDeclared, wantApp: ApplicationKitty},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := Detect(tt.env)
			require.Equal(t, tt.wantMode, got.ColorMode)
			require.Equal(t, tt.wantSource, got.ColorSource)
			require.Equal(t, tt.wantApp, got.Application)
			require.Equal(t, tt.wantGraphics, got.SupportsKittyGraphics())
		})
	}
}
