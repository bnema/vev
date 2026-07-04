package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/mouse"
)

func focusedPlacementLocked(tb *tab) (layout.Placement, bool) {
	if tb == nil || tb.tree == nil {
		return layout.Placement{}, false
	}
	placements, ok := solvedPlacementsLocked(tb)
	if !ok {
		return layout.Placement{}, false
	}
	for _, pl := range placements {
		if pl.ID == tb.tree.Focus {
			return pl, true
		}
	}
	return layout.Placement{}, false
}

func hitTestPlacementLocked(tb *tab, col, row int) (layout.Placement, bool) {
	if tb == nil || tb.tree == nil || tb.tree.Root == nil || !tb.size.Valid() {
		return layout.Placement{}, false
	}
	placements, ok := solvedPlacementsLocked(tb)
	if !ok {
		return layout.Placement{}, false
	}
	for _, pl := range placements {
		if pointInRect(col, row, pl.TitleBar) || pointInRect(col, row, pl.Content) {
			return pl, true
		}
	}
	return layout.Placement{}, false
}

func solvedPlacementsLocked(tb *tab) ([]layout.Placement, bool) {
	if tb == nil || tb.tree == nil || tb.tree.Root == nil || !tb.size.Valid() {
		return nil, false
	}
	return layout.Solve(tb.tree.Root, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
}

func pointInRect(col, row int, r domain.Rect) bool {
	return r.Width > 0 && r.Height > 0 && col >= r.X && col < r.X+r.Width && row >= r.Y && row < r.Y+r.Height
}

func focusPlacementLocked(tb *tab, id layout.PaneID) {
	if tb == nil || tb.tree == nil {
		return
	}
	tb.tree.Focus = id
	setExpandedLocked(tb.tree.Root, id)
}

func setExpandedLocked(n *layout.Node, id layout.PaneID) bool {
	if n == nil {
		return false
	}
	if n.Kind == layout.Stack {
		for _, child := range n.Children {
			if child.Kind == layout.Leaf && child.Leaf == id {
				n.Expanded = id
				return true
			}
		}
	}
	for _, child := range n.Children {
		if setExpandedLocked(child, id) {
			return true
		}
	}
	return false
}

func translateMouseEvent(ev mouse.Event, colOffset, contentYOffset int) mouse.Event {
	ev.Col -= colOffset
	ev.Row -= contentYOffset
	ev.Raw = sgrOffset(ev.Raw, -colOffset, -contentYOffset-1)
	return ev
}

func (d *Daemon) exitCopyMode(ac *attachedClient) {
	if ac == nil {
		return
	}
	ac.copyMu.Lock()
	ac.copyMode = nil
	ac.copyPressRowValid = false
	ac.copyDragging = false
	ac.normalMousePressValid = false
	ac.copyMu.Unlock()
}
