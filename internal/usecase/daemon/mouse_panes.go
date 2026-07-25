package daemon

import (
	"strconv"
	"strings"

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

type tabLayoutSnapshot struct {
	root        *layout.Node
	fingerprint string
	area        domain.Rect
	focus       layout.PaneID
	placements  []layout.Placement
	ok          bool
}

func solvedPlacementsLocked(tb *tab) ([]layout.Placement, bool) {
	snap := solveTabLayoutLocked(tb)
	return snap.placements, snap.ok
}

func solveTabLayoutLocked(tb *tab) tabLayoutSnapshot {
	if tb == nil || tb.tree == nil || tb.tree.Root == nil || !tb.size.Valid() {
		return tabLayoutSnapshot{}
	}
	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	placements, ok := layout.Solve(tb.tree.Root, area)
	return tabLayoutSnapshot{root: tb.tree.Root, fingerprint: layoutFingerprint(tb.tree.Root), area: area, focus: tb.tree.Focus, placements: placements, ok: ok}
}

func layoutFingerprint(root *layout.Node) string {
	var b strings.Builder
	writeLayoutFingerprint(&b, root)
	return b.String()
}

func writeLayoutFingerprint(b *strings.Builder, n *layout.Node) {
	if n == nil {
		b.WriteByte('0')
		return
	}
	b.WriteByte(byte('0' + n.Kind))
	b.WriteByte(byte('0' + n.Dir))
	writePaneIDFingerprint(b, n.Leaf)
	b.WriteByte('|')
	writePaneIDFingerprint(b, n.Expanded)
	b.WriteByte('[')
	for _, child := range n.Children {
		writeLayoutFingerprint(b, child)
		b.WriteByte(',')
	}
	b.WriteByte(']')
}

func writePaneIDFingerprint(b *strings.Builder, id layout.PaneID) {
	s := string(id)
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

func pointInRect(col, row int, r domain.Rect) bool {
	return r.Width > 0 && r.Height > 0 && col >= r.X && col < r.X+r.Width && row >= r.Y && row < r.Y+r.Height
}

func focusPlacementLocked(tb *tab, id layout.PaneID) bool {
	if tb == nil || tb.tree == nil {
		return false
	}
	oldFocus := tb.tree.Focus
	oldLayout := layoutFingerprint(tb.tree.Root)
	tb.tree.Focus = id
	setExpandedLocked(tb.tree.Root, id)
	changed := oldFocus != tb.tree.Focus || oldLayout != layoutFingerprint(tb.tree.Root)
	if changed {
		tb.bumpLayoutGenerationLocked()
	}
	return changed
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
	rt := ac.overlays
	if rt == nil {
		return
	}
	rt.copyMu.Lock()
	rt.clearCopyModeLocked()
	rt.copyMu.Unlock()
}
