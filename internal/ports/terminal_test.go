package ports

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDetectTerminalCapabilities(t *testing.T) {
	tests := []struct {
		name         string
		env          []string
		wantMode     TerminalColorMode
		wantSource   TerminalCapabilitySource
		wantApp      TerminalApplication
		wantGraphics bool
	}{
		{
			name:       "declared truecolor",
			env:        []string{"TERM=xterm-256color", "COLORTERM=truecolor"},
			wantMode:   TerminalColorTrueColor,
			wantSource: TerminalCapabilityDeclared,
		},
		{
			name:         "kitty signals infer truecolor",
			env:          []string{"TERM=xterm-kitty", "KITTY_WINDOW_ID=1"},
			wantMode:     TerminalColorTrueColor,
			wantSource:   TerminalCapabilityHeuristic,
			wantApp:      TerminalApplicationKitty,
			wantGraphics: true,
		},
		{
			name:         "kitty graphics survives declared truecolor",
			env:          []string{"TERM=xterm-kitty", "COLORTERM=truecolor", "KITTY_WINDOW_ID=1"},
			wantMode:     TerminalColorTrueColor,
			wantSource:   TerminalCapabilityDeclared,
			wantApp:      TerminalApplicationKitty,
			wantGraphics: true,
		},
		{
			name:       "256 color terminal remains a constrained attachment",
			env:        []string{"TERM=xterm-256color"},
			wantMode:   TerminalColorIndexed256,
			wantSource: TerminalCapabilityDeclared,
		},
		{
			name:       "unknown terminal is conservatively indexed",
			env:        []string{"TERM=unknown"},
			wantMode:   TerminalColorIndexed256,
			wantSource: TerminalCapabilityUnknown,
		},
		{
			name:       "kitty environment behind a multiplexer is not graphics evidence",
			env:        []string{"TERM=tmux-256color", "KITTY_WINDOW_ID=1"},
			wantMode:   TerminalColorIndexed256,
			wantSource: TerminalCapabilityDeclared,
			wantApp:    TerminalApplicationKitty,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := DetectTerminalCapabilities(tt.env)
			require.Equal(t, tt.wantMode, got.ColorMode)
			require.Equal(t, tt.wantSource, got.ColorSource)
			require.Equal(t, tt.wantApp, got.Application)
			require.Equal(t, tt.wantGraphics, got.SupportsKittyGraphics())
		})
	}
}
