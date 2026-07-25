package layout

import "github.com/bnema/vev/internal/domain"

// Divider is the geometry of a single gap between two sibling panes in a Split
// node. Dir mirrors the owning Split's Dir: Horizontal produces a
// 1-column-wide vertical divider rect; Vertical produces a 1-row-high
// horizontal divider rect.
type Divider struct {
	Rect domain.Rect
	Dir  SplitDir
}

// SolveWithDividers behaves like Solve but also returns the gaps between
// sibling panes. Both come from the same traversal, so a divider can never sit
// anywhere but in the gap Solve left for it. Leaf and Stack nodes contribute no
// dividers.
func SolveWithDividers(root *Node, area domain.Rect) ([]Placement, []Divider, bool) {
	return solveArea(root, area, true)
}

// dividerBetween returns the one-cell gap splitChildRects left after prev
// inside parent.
func dividerBetween(dir SplitDir, parent, prev domain.Rect) Divider {
	if dir == Horizontal {
		return Divider{Rect: domain.Rect{X: prev.X + prev.Width, Y: parent.Y, Width: 1, Height: parent.Height}, Dir: Horizontal}
	}
	return Divider{Rect: domain.Rect{X: parent.X, Y: prev.Y + prev.Height, Width: parent.Width, Height: 1}, Dir: Vertical}
}
