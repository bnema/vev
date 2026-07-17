package client

import (
	"bytes"
	"context"
	"io"
	"log/slog"
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

func TestStdinPumpCoalescesPaletteAndBackgroundThemePerRead(t *testing.T) {
	state := &terminalThemeState{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	out := make(chan ports.Frame, 3)
	pump := stdinPump{
		ctx:        ctx,
		cancel:     cancel,
		in:         bytes.NewReader([]byte("\x1b]4;1;#112233\a\x1b]11;rgb:0404/0505/0606\a\x1b]4;14;#778899\a")),
		out:        out,
		themeState: state,
		logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	pump.run()

	first := <-out
	require.Equal(t, ports.MsgTheme, first.Type)
	theme, err := ports.UnmarshalTheme(first.Payload)
	require.NoError(t, err)
	require.True(t, theme.HasBackground)
	require.Equal(t, renderer.RGB{R: 4, G: 5, B: 6}, theme.Background)
	require.Equal(t, uint16(1<<1|1<<14), theme.PaletteKnown)
	require.Equal(t, renderer.RGB{R: 0x11, G: 0x22, B: 0x33}, theme.Palette[1])
	require.Equal(t, renderer.RGB{R: 0x77, G: 0x88, B: 0x99}, theme.Palette[14])

	second := <-out
	require.Equal(t, ports.MsgDetach, second.Type)
	select {
	case extra := <-out:
		t.Fatalf("got extra frame after one input chunk: %v", extra.Type)
	default:
	}
}
