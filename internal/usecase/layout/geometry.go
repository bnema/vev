package layout

import "github.com/bnema/vev/internal/domain"

// Solve returns deterministic pane placements for root inside area.
func Solve(root *Node, area domain.Rect) ([]Placement, bool) {
	if root == nil || area.Width <= 0 || area.Height <= 0 {
		return nil, false
	}
	var out []Placement
	if !solve(root, area, &out) {
		return nil, false
	}
	return out, true
}

func solve(n *Node, r domain.Rect, out *[]Placement) bool {
	switch n.Kind {
	case Leaf:
		if r.Width < MinPaneCols || r.Height < MinPaneRows {
			return false
		}
		*out = append(*out, Placement{ID: n.Leaf, Content: r})
		return true
	case Split:
		return solveSplit(n, r, out)
	case Stack:
		return solveStack(n, r, out)
	default:
		return false
	}
}

func solveSplit(n *Node, r domain.Rect, out *[]Placement) bool {
	count := len(n.Children)
	if count == 0 {
		return false
	}
	if count == 1 {
		return solve(n.Children[0], r, out)
	}
	if n.Dir == Horizontal {
		usable := r.Width - (count - 1)
		if usable < count*MinPaneCols || r.Height < MinPaneRows {
			return false
		}
		base, rem := usable/count, usable%count
		x := r.X
		for i, child := range n.Children {
			w := base
			if i < rem {
				w++
			}
			if !solve(child, domain.Rect{X: x, Y: r.Y, Width: w, Height: r.Height}, out) {
				return false
			}
			x += w + 1
		}
		return true
	}

	usable := r.Height - (count - 1)
	if usable < count*MinPaneRows || r.Width < MinPaneCols {
		return false
	}
	base, rem := usable/count, usable%count
	y := r.Y
	for i, child := range n.Children {
		h := base
		if i < rem {
			h++
		}
		if !solve(child, domain.Rect{X: r.X, Y: y, Width: r.Width, Height: h}, out) {
			return false
		}
		y += h + 1
	}
	return true
}

func solveStack(n *Node, r domain.Rect, out *[]Placement) bool {
	count := len(n.Children)
	if count == 0 || r.Width < MinPaneCols || r.Height < count+1 {
		return false
	}
	idx := -1
	for i, child := range n.Children {
		if child.Kind != Leaf {
			return false
		}
		if child.Leaf == n.Expanded {
			idx = i
		}
	}
	if idx < 0 {
		return false
	}
	contentRows := r.Height - (count - 1)
	for i, child := range n.Children {
		if i < idx {
			titleY := r.Y + i
			*out = append(*out, Placement{ID: child.Leaf, TitleBar: domain.Rect{X: r.X, Y: titleY, Width: r.Width, Height: 1}, Collapsed: true, InStack: true})
			continue
		}
		if i == idx {
			*out = append(*out, Placement{
				ID:      child.Leaf,
				Content: domain.Rect{X: r.X, Y: r.Y + idx, Width: r.Width, Height: contentRows},
				InStack: true,
			})
			continue
		}
		titleY := r.Y + idx + contentRows + (i - idx - 1)
		*out = append(*out, Placement{ID: child.Leaf, TitleBar: domain.Rect{X: r.X, Y: titleY, Width: r.Width, Height: 1}, Collapsed: true, InStack: true})
	}
	return true
}
