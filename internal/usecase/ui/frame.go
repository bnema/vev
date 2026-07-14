package ui

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// VerticalAnchor controls which end of a source is retained when its height
// differs from the destination rectangle. Source and destination aliasing is
// unsupported.
type VerticalAnchor uint8

const (
	VerticalAnchorTop VerticalAnchor = iota
	VerticalAnchorBottom
)

// FrameView is a bounded, row-oriented view of cells.
type FrameView struct {
	Rows          [][]renderer.Cell
	Width, Height int
}

// BlitFrame copies the valid intersection of src and rect into dst. Uncovered
// destination cells are left untouched. Invalid dimensions, views, rectangles,
// or anchors are ignored.
func BlitFrame(dst renderer.Frame, rect domain.Rect, src FrameView, anchor VerticalAnchor) {
	if anchor != VerticalAnchorTop && anchor != VerticalAnchorBottom {
		return
	}
	if dst.Validate() != nil || rect.Width <= 0 || rect.Height <= 0 || src.Width <= 0 || src.Height <= 0 {
		return
	}
	if len(src.Rows) != src.Height {
		return
	}
	for _, row := range src.Rows {
		if len(row) != src.Width {
			return
		}
	}
	if rect.X+rect.Width <= 0 || rect.Y+rect.Height <= 0 || rect.X >= dst.Width || rect.Y >= dst.Height {
		return
	}

	srcY, dstY := 0, 0
	if anchor == VerticalAnchorBottom {
		if src.Height > rect.Height {
			srcY = src.Height - rect.Height
		} else {
			dstY = rect.Height - src.Height
		}
	}
	for dy := 0; dy < src.Height && dy+dstY < rect.Height; dy++ {
		sy := srcY + dy
		y := rect.Y + dstY + dy
		if y < 0 || y >= dst.Height {
			continue
		}
		for dx := 0; dx < rect.Width; dx++ {
			x := rect.X + dx
			if x < 0 || x >= dst.Width || dx >= src.Width {
				continue
			}
			dst.Set(x, y, src.Rows[sy][dx])
		}
	}
}
