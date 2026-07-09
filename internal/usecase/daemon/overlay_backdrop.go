package daemon

import (
	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// overlayBackdrop controls optional dimming of pane content behind an overlay.
// Chrome (bars, dividers, and pane titles) is deliberately not part of the
// backdrop.
type overlayBackdrop struct {
	DimPaneContents bool
}

// apply dims the content rectangles from a tab-content-relative layout
// snapshot. A failed solve follows the tab renderer's focused-pane fallback by
// dimming the complete content area.
func (b overlayBackdrop) apply(frame renderer.Frame, contentArea domain.Rect, layoutSnap tabLayoutSnapshot, theme themeui.Theme) {
	if !b.DimPaneContents || contentArea.Width <= 0 || contentArea.Height <= 0 {
		return
	}
	if !layoutSnap.ok {
		dimFrameRect(frame, contentArea, theme)
		return
	}
	for _, placement := range layoutSnap.placements {
		if placement.Collapsed || placement.Content.Width <= 0 || placement.Content.Height <= 0 {
			continue
		}
		r := placement.Content
		r.X += contentArea.X
		r.Y += contentArea.Y
		dimFrameRect(frame, r, theme)
	}
}

func dimFrameRect(frame renderer.Frame, rect domain.Rect, theme themeui.Theme) {
	x0 := max(rect.X, 0)
	y0 := max(rect.Y, 0)
	x1 := min(rect.X+rect.Width, frame.Width)
	y1 := min(rect.Y+rect.Height, frame.Height)
	for y := y0; y < y1; y++ {
		for x := x0; x < x1; x++ {
			cell := frame.At(x, y)
			cell.Style = themeui.DimStyle(cell.Style, theme)
			frame.Set(x, y, cell)
		}
	}
}
