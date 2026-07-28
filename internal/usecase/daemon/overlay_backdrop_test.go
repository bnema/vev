package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

func backdropTheme() themeui.Theme {
	return themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 220, G: 220, B: 220}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
}

func TestApplyOverlayBackdropDimsCompleteFrame(t *testing.T) {
	frame := renderer.NewFrame(4, 3)
	original := renderer.Cell{Rune: '界', Style: renderer.DefaultStyle()}
	for y := range frame.Height {
		for x := range frame.Width {
			frame.Set(x, y, renderer.Cell{Rune: rune('a' + y*frame.Width + x), Style: renderer.DefaultStyle()})
		}
	}
	frame.Set(1, 1, original)
	frame.Set(2, 1, renderer.Cell{Continuation: true, Style: renderer.DefaultStyle()})

	applyOverlayBackdrop(frame, backdropTheme())
	dimmed := themeui.NewDimmer(backdropTheme()).Dim(renderer.DefaultStyle())
	for y := range frame.Height {
		for x := range frame.Width {
			require.Equal(t, dimmed, frame.At(x, y).Style, "cell (%d,%d)", x, y)
		}
	}
	require.Equal(t, '界', frame.At(1, 1).Rune)
	require.True(t, frame.At(2, 1).Continuation)
	require.Zero(t, frame.At(2, 1).Rune)
}

func TestDimFrameRectClipsToFrame(t *testing.T) {
	frame := renderer.NewFrame(4, 4)
	before := frame.Clone()
	dimFrameRect(frame, domain.Rect{X: 2, Y: 2, Width: 4, Height: 4}, backdropTheme())
	dimmed := themeui.NewDimmer(backdropTheme()).Dim(renderer.DefaultStyle())

	for _, point := range [][2]int{{2, 2}, {3, 2}, {2, 3}, {3, 3}} {
		require.Equal(t, dimmed, frame.At(point[0], point[1]).Style)
	}
	for _, point := range [][2]int{{0, 0}, {1, 1}, {3, 1}, {1, 3}} {
		require.Equal(t, before.At(point[0], point[1]).Style, frame.At(point[0], point[1]).Style)
	}
}
