package daemon

import (
	"math"
	"strconv"
	"strings"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/mouse"
)

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
	fingerprint string
	area        domain.Rect
	focus       layout.PaneID
	placements  []layout.Placement
	dividers    []layout.Divider
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
	placements, dividers, ok := layout.SolveWithDividers(tb.tree.Root, area)
	return tabLayoutSnapshot{fingerprint: layoutFingerprint(tb.tree.Root), area: area, focus: tb.tree.Focus, placements: placements, dividers: dividers, ok: ok}
}

func layoutFingerprint(root *layout.Node) string {
	var b strings.Builder
	b.Grow(layoutFingerprintLength(root))
	writeLayoutFingerprint(&b, root)
	return b.String()
}

const weightFingerprintMaxLength = 16

func layoutFingerprintLength(n *layout.Node) int {
	if n == nil {
		return 1
	}
	length := 2 + weightFingerprintMaxLength + 1 + paneIDFingerprintLength(n.Leaf) + 1 + paneIDFingerprintLength(n.Expanded) + 2
	for _, child := range n.Children {
		length += layoutFingerprintLength(child) + 1
	}
	return length
}

func paneIDFingerprintLength(id layout.PaneID) int {
	length := len(id)
	return decimalDigits(length) + 1 + length
}

func decimalDigits(value int) int {
	if value == 0 {
		return 1
	}
	digits := 0
	for value > 0 {
		value /= 10
		digits++
	}
	return digits
}

func writeLayoutFingerprint(b *strings.Builder, n *layout.Node) {
	if n == nil {
		b.WriteByte('0')
		return
	}
	b.WriteByte(byte('0' + n.Kind))
	b.WriteByte(byte('0' + n.Dir))
	writeWeightFingerprint(b, n.Weight)
	b.WriteByte('|')
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

func writeWeightFingerprint(b *strings.Builder, weight float64) {
	const hex = "0123456789abcdef"
	bits := math.Float64bits(weight)
	started := false
	for shift := uint(60); ; shift -= 4 {
		digit := byte(bits >> shift & 0xf)
		if digit != 0 || started || shift == 0 {
			b.WriteByte(hex[digit])
			started = true
		}
		if shift == 0 {
			return
		}
	}
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
	ev.Raw = sgrOffset(ev.Raw, -colOffset, -contentYOffset-clientTopBarRows)
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
