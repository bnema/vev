package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// applyOverlayBackdrop dims every cell already composed into the complete
// renderer frame. Modal composition calls it immediately before painting each
// active layer so bars, panes, toasts, and lower overlays share one backdrop.
func applyOverlayBackdrop(frame renderer.Frame, theme themeui.Theme) {
	dimFrameRect(frame, domain.Rect{Width: frame.Width, Height: frame.Height}, theme)
}

func dimFrameRect(frame renderer.Frame, rect domain.Rect, theme themeui.Theme) {
	dimmer := themeui.NewDimmer(theme)
	x0 := max(rect.X, 0)
	y0 := max(rect.Y, 0)
	x1 := min(rect.X+rect.Width, frame.Width)
	y1 := min(rect.Y+rect.Height, frame.Height)
	for y := y0; y < y1; y++ {
		for x := x0; x < x1; x++ {
			cell := frame.At(x, y)
			cell.Style = dimmer.Dim(cell.Style)
			frame.Set(x, y, cell)
		}
	}
}
