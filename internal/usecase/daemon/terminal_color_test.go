package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"
)

func TestAdaptFrameColors(t *testing.T) {
	frame := renderer.NewFrame(1, 1)
	frame.Cells[0].Style.HasForegroundRGB = true
	frame.Cells[0].Style.ForegroundRGB = renderer.RGB{R: 255, G: 0, B: 0}
	frame.Cells[0].Style.HasBackgroundRGB = true
	frame.Cells[0].Style.BackgroundRGB = renderer.RGB{R: 0, G: 255, B: 0}
	frame.Cells[0].Style.HasUnderlineColor = true
	frame.Cells[0].Style.HasUnderlineColorRGB = true
	frame.Cells[0].Style.UnderlineColorRGB = renderer.RGB{R: 0, G: 0, B: 255}

	got := adaptFrameColors(frame, false)

	require.False(t, got.Cells[0].Style.HasForegroundRGB)
	require.Equal(t, 196, got.Cells[0].Style.Foreground)
	require.False(t, got.Cells[0].Style.HasBackgroundRGB)
	require.Equal(t, 46, got.Cells[0].Style.Background)
	require.False(t, got.Cells[0].Style.HasUnderlineColorRGB)
	require.Equal(t, 21, got.Cells[0].Style.UnderlineColor)
	require.True(t, frame.Cells[0].Style.HasForegroundRGB, "adaptation must not mutate composed state")
}
