package ui

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/pkg/renderer"
)

// SeparatorOrientation selects the axis of a separator slice.
type SeparatorOrientation uint8

const (
	SeparatorHorizontal SeparatorOrientation = iota
	SeparatorVertical
)

// DrawSeparator draws a styled, clipped separator slice. It contains no layout
// or theme policy; callers provide both the rectangle and style.
func DrawSeparator(dst renderer.Frame, rect domain.Rect, orientation SeparatorOrientation, style renderer.Style) {
	if dst.Validate() != nil || rect.Width <= 0 || rect.Height <= 0 {
		return
	}
	var cell renderer.Cell
	switch orientation {
	case SeparatorHorizontal:
		cell = renderer.Cell{Rune: '─', Style: style}
		FillRect(dst, domain.Rect{X: rect.X, Y: rect.Y, Width: rect.Width, Height: 1}, cell)
	case SeparatorVertical:
		cell = renderer.Cell{Rune: '│', Style: style}
		FillRect(dst, domain.Rect{X: rect.X, Y: rect.Y, Width: 1, Height: rect.Height}, cell)
	}
}
