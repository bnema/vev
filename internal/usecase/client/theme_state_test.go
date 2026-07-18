package client

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

func TestTerminalThemeStateClearPalettePreservesReportedColors(t *testing.T) {
	state := &terminalThemeState{}
	state.update(func(theme *ports.Theme) {
		theme.HasForeground = true
		theme.Foreground = renderer.RGB{R: 1, G: 2, B: 3}
		theme.HasBackground = true
		theme.Background = renderer.RGB{R: 4, G: 5, B: 6}
		theme.SchemeKnown = true
		theme.Light = true
		theme.PaletteKnown = 1<<1 | 1<<14
		theme.Palette[1] = renderer.RGB{R: 7, G: 8, B: 9}
		theme.Palette[14] = renderer.RGB{R: 10, G: 11, B: 12}
	})

	got := state.clearPalette()
	require.True(t, got.HasForeground)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, got.Foreground)
	require.True(t, got.HasBackground)
	require.Equal(t, renderer.RGB{R: 4, G: 5, B: 6}, got.Background)
	require.True(t, got.SchemeKnown)
	require.True(t, got.Light)
	require.Zero(t, got.PaletteKnown)
	require.Equal(t, [16]renderer.RGB{}, got.Palette)

	_, reported := state.reportedTheme()
	require.True(t, reported, "foreground/background remain usable after palette invalidation")
}
