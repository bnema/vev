package ui

import "github.com/bnema/vev/internal/domain"

// Margins specifies the distance between an anchored rectangle and each base edge.
type Margins struct {
	Top, Right, Bottom, Left int
}

// Place positions content in base according to anchor. Negative margins and
// content dimensions are normalized to zero. The returned rectangle is always
// contained within a positive base; a nonpositive base returns an empty rect.
func Place(base, content domain.Size, anchor domain.Anchor, margins Margins) domain.Rect {
	if !base.Valid() {
		return domain.Rect{}
	}

	width := clamp(content.Cols, 0, base.Cols)
	height := clamp(content.Rows, 0, base.Rows)
	margins.Top = max(0, margins.Top)
	margins.Right = max(0, margins.Right)
	margins.Bottom = max(0, margins.Bottom)
	margins.Left = max(0, margins.Left)

	x := (base.Cols - width) / 2
	y := (base.Rows - height) / 2
	switch anchor {
	case domain.AnchorTopLeft:
		x, y = margins.Left, margins.Top
	case domain.AnchorTop:
		y = margins.Top
	case domain.AnchorTopRight:
		x, y = base.Cols-margins.Right-width, margins.Top
	case domain.AnchorLeft:
		x = margins.Left
	case domain.AnchorRight:
		x = base.Cols - margins.Right - width
	case domain.AnchorBottomLeft:
		x, y = margins.Left, base.Rows-margins.Bottom-height
	case domain.AnchorBottom:
		y = base.Rows - margins.Bottom - height
	case domain.AnchorBottomRight:
		x, y = base.Cols-margins.Right-width, base.Rows-margins.Bottom-height
	case domain.AnchorCenter:
		// Center is the zero value and intentionally ignores all margins.
	default:
		// Invalid anchors fall back to center.
	}

	return domain.Rect{
		X:      clamp(x, 0, base.Cols-width),
		Y:      clamp(y, 0, base.Rows-height),
		Width:  width,
		Height: height,
	}
}

func clamp(n, low, high int) int {
	if n < low {
		return low
	}
	if n > high {
		return high
	}
	return n
}
