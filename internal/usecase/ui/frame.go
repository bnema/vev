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

type frameSpan struct {
	source, destination, length int
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

	width := min(src.Width, rect.Width)
	x, ok := clippedFrameSpan(rect.X, 0, width, dst.Width)
	if !ok {
		return
	}

	height := min(src.Height, rect.Height)
	sourceY, destinationOffset := 0, 0
	if anchor == VerticalAnchorBottom {
		if src.Height > rect.Height {
			sourceY = src.Height - rect.Height
		} else {
			destinationOffset = rect.Height - src.Height
		}
	}
	destinationY, ok := frameDestinationOrigin(rect.Y, destinationOffset, dst.Height)
	if !ok {
		return
	}
	y, ok := clippedFrameSpan(destinationY, sourceY, height, dst.Height)
	if !ok {
		return
	}

	for dy := range y.length {
		row := src.Rows[y.source+dy]
		for dx := range x.length {
			sourceX := x.source + dx
			current := row[sourceX]
			if current.Continuation {
				continue
			}
			if renderer.RuneWidth(current.Rune) == 2 {
				next := sourceX + 1
				if dx+1 >= x.length || !row[next].Continuation {
					continue
				}
				dst.Set(x.destination+dx, y.destination+dy, current)
				dst.Set(x.destination+dx+1, y.destination+dy, row[next])
				continue
			}
			dst.Set(x.destination+dx, y.destination+dy, current)
		}
	}
}

// frameDestinationOrigin adds an in-rectangle offset only after proving that
// a non-negative origin remains in the destination's bounded coordinate range.
func frameDestinationOrigin(origin, offset, limit int) (int, bool) {
	if origin >= 0 {
		if origin >= limit || offset >= limit-origin {
			return 0, false
		}
		return origin + offset, true
	}
	// A negative integer plus a non-negative offset cannot overflow.
	return origin + offset, true
}

// clippedFrameSpan intersects a source span placed at origin with [0, limit).
// It avoids forming an unbounded endpoint before both operands are bounded.
func clippedFrameSpan(origin, source, length, limit int) (frameSpan, bool) {
	if origin < 0 {
		if origin <= -length {
			return frameSpan{}, false
		}
		trim := -origin
		source += trim
		length -= trim
		origin = 0
	}
	if origin >= limit {
		return frameSpan{}, false
	}
	length = min(length, limit-origin)
	return frameSpan{source: source, destination: origin, length: length}, length > 0
}
