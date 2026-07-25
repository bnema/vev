package layout

import (
	"errors"
	"sort"

	"github.com/bnema/vev/internal/domain"
)

var (
	ErrNotFound      = errors.New("pane not found")
	ErrTooSmall      = errors.New("layout too small")
	ErrNoPane        = errors.New("no pane in direction")
	ErrNotToggleable = errors.New("layout node is not toggleable")
)

// Split adds newID next to target. The after flag controls whether the new pane
// is placed after target on the chosen axis.
func (t *Tree) Split(target PaneID, dir Direction, after bool, newID PaneID, area domain.Rect) error {
	candidate := t.clone()
	if candidate == nil || candidate.Root == nil || !insertSplit(candidate.Root, target, axisFor(dir), after, newID) {
		return ErrNotFound
	}
	candidate.Focus = newID
	if _, ok := Solve(candidate.Root, area); !ok {
		return ErrTooSmall
	}
	*t = *candidate
	return nil
}

func insertSplit(n *Node, target PaneID, axis SplitDir, after bool, newID PaneID) bool {
	if n == nil {
		return false
	}
	for i, child := range n.Children {
		if child.Kind == Leaf && child.Leaf == target {
			newLeaf := NewLeaf(newID)
			if n.Kind == Split && n.Dir == axis {
				at := i
				if after {
					at++
				}
				n.Children = append(n.Children, nil)
				copy(n.Children[at+1:], n.Children[at:])
				n.Children[at] = newLeaf
				return true
			}
			if n.Kind == Stack {
				stack := n.clone()
				children := []*Node{stack, newLeaf}
				if !after {
					children = []*Node{newLeaf, stack}
				}
				*n = Node{Kind: Split, Dir: axis, Children: children}
				return true
			}
			children := []*Node{child, newLeaf}
			if !after {
				children = []*Node{newLeaf, child}
			}
			n.Children[i] = &Node{Kind: Split, Dir: axis, Children: children}
			return true
		}
		if insertSplit(child, target, axis, after, newID) {
			return true
		}
	}
	if n.Kind == Leaf && n.Leaf == target {
		children := []*Node{n.clone(), NewLeaf(newID)}
		if !after {
			children = []*Node{NewLeaf(newID), n.clone()}
		}
		*n = Node{Kind: Split, Dir: axis, Children: children}
		return true
	}
	return false
}

// StackNew puts newID in target's stack, creating one if target is not stacked.
func (t *Tree) StackNew(target, newID PaneID, area domain.Rect) error {
	candidate := t.clone()
	if candidate == nil || !stackNew(candidate.Root, target, newID) {
		return ErrNotFound
	}
	candidate.Focus = newID
	if _, ok := Solve(candidate.Root, area); !ok {
		return ErrTooSmall
	}
	*t = *candidate
	return nil
}

func stackNew(n *Node, target, newID PaneID) bool {
	if n == nil {
		return false
	}
	if n.Kind == Stack {
		for _, child := range n.Children {
			if child.Kind == Leaf && child.Leaf == target {
				n.Children = append(n.Children, NewLeaf(newID))
				n.Expanded = newID
				return true
			}
		}
	}
	for i, child := range n.Children {
		if child.Kind == Leaf && child.Leaf == target {
			n.Children[i] = &Node{Kind: Stack, Children: []*Node{child, NewLeaf(newID)}, Expanded: newID}
			return true
		}
		if stackNew(child, target, newID) {
			return true
		}
	}
	if n.Kind == Leaf && n.Leaf == target {
		*n = Node{Kind: Stack, Children: []*Node{NewLeaf(target), NewLeaf(newID)}, Expanded: newID}
		return true
	}
	return false
}

// ToggleStack converts a stack back to a vertical split. For an unstacked leaf in
// a two-child split, it converts that split to a stack.
func (t *Tree) ToggleStack(target PaneID, area domain.Rect) error {
	candidate := t.clone()
	result := toggleStack(candidate.Root, target)
	switch result {
	case toggleNotFound:
		return ErrNotFound
	case toggleNotToggleable:
		return ErrNotToggleable
	}
	if _, ok := Solve(candidate.Root, area); !ok {
		return ErrTooSmall
	}
	*t = *candidate
	return nil
}

type toggleResult int

const (
	toggleNotFound toggleResult = iota
	toggleToggled
	toggleNotToggleable
)

func toggleStack(n *Node, target PaneID) toggleResult {
	if n == nil {
		return toggleNotFound
	}
	for _, child := range n.Children {
		if child.Kind == Leaf || !containsLeaf(child, target) {
			continue
		}
		result := toggleStack(child, target)
		if result != toggleNotFound {
			return result
		}
	}
	if !containsLeaf(n, target) {
		return toggleNotFound
	}
	if n.Kind == Stack {
		if stackDirectLeafIndex(n, target) < 0 {
			return toggleNotToggleable
		}
		n.Kind = Split
		n.Dir = Vertical
		n.Expanded = ""
		return toggleToggled
	}
	if n.Kind == Split && len(n.Children) == 2 && splitDirectLeavesContain(n, target) {
		n.Kind = Stack
		n.Expanded = target
		return toggleToggled
	}
	return toggleNotToggleable
}

func stackDirectLeafIndex(n *Node, target PaneID) int {
	for i, child := range n.Children {
		if child.Kind == Leaf && child.Leaf == target {
			return i
		}
	}
	return -1
}

func splitDirectLeavesContain(n *Node, target PaneID) bool {
	for _, child := range n.Children {
		if child.Kind != Leaf {
			return false
		}
	}
	return n.Children[0].Leaf == target || n.Children[1].Leaf == target
}

// Close removes target and dissolves single-child containers.
func (t *Tree) Close(target PaneID) error {
	candidate := t.clone()
	if candidate == nil || candidate.Root == nil || !containsLeaf(candidate.Root, target) {
		return ErrNotFound
	}
	if candidate.Root.Kind == Leaf && candidate.Root.Leaf == target {
		candidate.Root = nil
		candidate.Focus = ""
		*t = *candidate
		return nil
	}
	refocus := closeRefocusCandidate(candidate.Root, target)
	root, removed := closeNode(candidate.Root, target)
	if !removed {
		return ErrNotFound
	}
	candidate.Root = root
	if candidate.Focus == target {
		if refocus != "" && containsLeaf(candidate.Root, refocus) {
			candidate.Focus = refocus
		} else {
			ids := leafIDs(candidate.Root)
			if len(ids) > 0 {
				candidate.Focus = ids[0]
			} else {
				candidate.Focus = ""
			}
		}
		setExpanded(candidate.Root, candidate.Focus)
	}
	*t = *candidate
	return nil
}

func closeNode(n *Node, target PaneID) (*Node, bool) {
	if n.Kind == Leaf {
		if n.Leaf == target {
			return nil, true
		}
		return n, false
	}
	removed := false
	children := n.Children[:0]
	for _, child := range n.Children {
		newChild, didRemove := closeNode(child, target)
		removed = removed || didRemove
		if newChild != nil {
			children = append(children, newChild)
		}
	}
	n.Children = children
	if !removed {
		return n, false
	}
	if n.Kind == Stack {
		if n.Expanded == target && len(n.Children) > 0 {
			n.Expanded = firstLeaf(n.Children[0])
		}
		if len(n.Children) == 1 {
			return n.Children[0], true
		}
	}
	if n.Kind == Split && len(n.Children) == 1 {
		return n.Children[0], true
	}
	return n, true
}

func closeRefocusCandidate(n *Node, target PaneID) PaneID {
	if n == nil || n.Kind == Leaf {
		return ""
	}
	for i, child := range n.Children {
		if !containsLeaf(child, target) {
			continue
		}
		if child.Kind == Leaf && child.Leaf == target {
			if i > 0 {
				return lastLeaf(n.Children[i-1])
			}
			if i+1 < len(n.Children) {
				return firstLeaf(n.Children[i+1])
			}
			return ""
		}
		if id := closeRefocusCandidate(child, target); id != "" {
			return id
		}
		if i > 0 {
			return lastLeaf(n.Children[i-1])
		}
		if i+1 < len(n.Children) {
			return firstLeaf(n.Children[i+1])
		}
		return ""
	}
	return ""
}

// ContainsLeaf reports whether n contains a leaf with id.
func ContainsLeaf(n *Node, id PaneID) bool { return containsLeaf(n, id) }

func containsLeaf(n *Node, id PaneID) bool {
	if n == nil {
		return false
	}
	if n.Kind == Leaf {
		return n.Leaf == id
	}
	for _, child := range n.Children {
		if containsLeaf(child, id) {
			return true
		}
	}
	return false
}

func firstLeaf(n *Node) PaneID {
	if n.Kind == Leaf {
		return n.Leaf
	}
	for _, child := range n.Children {
		if id := firstLeaf(child); id != "" {
			return id
		}
	}
	return ""
}

func lastLeaf(n *Node) PaneID {
	if n.Kind == Leaf {
		return n.Leaf
	}
	for i := len(n.Children) - 1; i >= 0; i-- {
		if id := lastLeaf(n.Children[i]); id != "" {
			return id
		}
	}
	return ""
}

func setExpanded(n *Node, id PaneID) bool {
	if n == nil {
		return false
	}
	if n.Kind == Stack && stackDirectLeafIndex(n, id) >= 0 {
		n.Expanded = id
		return true
	}
	for _, child := range n.Children {
		if setExpanded(child, id) {
			return true
		}
	}
	return false
}

// LeafIDs returns the leaf ids under n in layout order.
func LeafIDs(n *Node) []PaneID { return leafIDs(n) }

func leafIDs(n *Node) []PaneID {
	if n == nil {
		return nil
	}
	if n.Kind == Leaf {
		return []PaneID{n.Leaf}
	}
	var ids []PaneID
	for _, child := range n.Children {
		ids = append(ids, leafIDs(child)...)
	}
	return ids
}

// FocusSpan returns the solved focus rectangle of the focused pane.
func (t *Tree) FocusSpan(area domain.Rect) (domain.Rect, error) {
	if t == nil || t.Root == nil {
		return domain.Rect{}, ErrNotFound
	}
	placements, ok := Solve(t.Root, area)
	if !ok {
		return domain.Rect{}, ErrTooSmall
	}
	for _, placement := range placements {
		if placement.ID == t.Focus {
			return focusRect(placement), nil
		}
	}
	return domain.Rect{}, ErrNotFound
}

// EntryPane returns the pane on the facing edge that best preserves span on
// the perpendicular axis. It does not mutate the tree.
func (t *Tree) EntryPane(dir Direction, span, area domain.Rect) (PaneID, error) {
	if t == nil || t.Root == nil {
		return "", ErrNotFound
	}
	placements, ok := Solve(t.Root, area)
	if !ok {
		return "", ErrTooSmall
	}

	outside := span
	switch dir {
	case Left:
		outside.X = area.X + area.Width
	case Right:
		outside.X = area.X - span.Width
	case Up:
		outside.Y = area.Y + area.Height
	case Down:
		outside.Y = area.Y - span.Height
	default:
		return "", ErrNoPane
	}

	return selectFacingCandidate(placements, outside, dir, facingGapFirst)
}

// FocusEnter focuses the best pane on the facing edge and expands it when it
// is a collapsed stack member.
func (t *Tree) FocusEnter(dir Direction, span, area domain.Rect) error {
	id, err := t.EntryPane(dir, span, area)
	if err != nil {
		return err
	}
	t.Focus = id
	setExpanded(t.Root, id)
	return nil
}

// FocusDir moves focus in dir. Vertical movement within a stack walks stack
// members before falling back to geometric focus.
func (t *Tree) FocusDir(dir Direction, area domain.Rect) error {
	if t.Root == nil {
		return ErrNotFound
	}
	if (dir == Up || dir == Down) && focusStackLocal(t.Root, t.Focus, dir, &t.Focus) {
		return nil
	}
	placements, ok := Solve(t.Root, area)
	if !ok {
		return ErrTooSmall
	}
	currentIdx := -1
	for i, p := range placements {
		if p.ID == t.Focus {
			currentIdx = i
			break
		}
	}
	if currentIdx < 0 {
		return ErrNotFound
	}
	current := focusRect(placements[currentIdx])
	candidate, err := selectFacingCandidate(placements, current, dir, facingOverlapFirst)
	if err != nil {
		return err
	}
	t.Focus = candidate
	setExpanded(t.Root, t.Focus)
	return nil
}

type facingMetricPriority uint8

const (
	facingGapFirst facingMetricPriority = iota
	facingOverlapFirst
)

type facingCandidate struct {
	id      PaneID
	gap     int
	overlap int
}

func selectFacingCandidate(placements []Placement, from domain.Rect, dir Direction, priority facingMetricPriority) (PaneID, error) {
	candidates := make([]facingCandidate, 0, len(placements))
	for _, placement := range placements {
		if facing, gap, perpendicularOverlap := sideGapOverlap(from, focusRect(placement), dir); facing {
			candidates = append(candidates, facingCandidate{id: placement.ID, gap: gap, overlap: perpendicularOverlap})
		}
	}
	if len(candidates) == 0 {
		return "", ErrNoPane
	}
	sort.Slice(candidates, func(i, j int) bool {
		if priority == facingGapFirst {
			if candidates[i].gap != candidates[j].gap {
				return candidates[i].gap < candidates[j].gap
			}
			if candidates[i].overlap != candidates[j].overlap {
				return candidates[i].overlap > candidates[j].overlap
			}
		} else {
			if candidates[i].overlap != candidates[j].overlap {
				return candidates[i].overlap > candidates[j].overlap
			}
			if candidates[i].gap != candidates[j].gap {
				return candidates[i].gap < candidates[j].gap
			}
		}
		return candidates[i].id < candidates[j].id
	})
	return candidates[0].id, nil
}

func focusStackLocal(n *Node, id PaneID, dir Direction, focus *PaneID) bool {
	if n.Kind == Stack {
		idx := stackDirectLeafIndex(n, id)
		if idx >= 0 {
			if next := adjacentStackLeaf(n, idx, dir); next != "" {
				n.Expanded = next
				*focus = next
				return true
			}
			return false
		}
	}
	for _, child := range n.Children {
		if focusStackLocal(child, id, dir, focus) {
			return true
		}
	}
	return false
}

func adjacentStackLeaf(n *Node, idx int, dir Direction) PaneID {
	step := 0
	switch dir {
	case Up:
		step = -1
	case Down:
		step = 1
	default:
		return ""
	}
	for i := idx + step; i >= 0 && i < len(n.Children); i += step {
		if n.Children[i].Kind == Leaf {
			return n.Children[i].Leaf
		}
	}
	return ""
}

func focusRect(p Placement) domain.Rect {
	if p.TitleBar.Height == 0 {
		return p.Content
	}
	if p.Content.Height == 0 {
		return p.TitleBar
	}
	y := p.TitleBar.Y
	height := p.Content.Y + p.Content.Height - y
	return domain.Rect{X: p.TitleBar.X, Y: y, Width: p.TitleBar.Width, Height: height}
}

func sideGapOverlap(a, b domain.Rect, dir Direction) (bool, int, int) {
	ar, ab := a.X+a.Width, a.Y+a.Height
	br, bb := b.X+b.Width, b.Y+b.Height
	switch dir {
	case Left:
		if br > a.X {
			return false, 0, 0
		}
		return true, a.X - br, overlap(a.Y, ab, b.Y, bb)
	case Right:
		if b.X < ar {
			return false, 0, 0
		}
		return true, b.X - ar, overlap(a.Y, ab, b.Y, bb)
	case Up:
		if bb > a.Y {
			return false, 0, 0
		}
		return true, a.Y - bb, overlap(a.X, ar, b.X, br)
	case Down:
		if b.Y < ab {
			return false, 0, 0
		}
		return true, b.Y - ab, overlap(a.X, ar, b.X, br)
	default:
		return false, 0, 0
	}
}

func overlap(a0, a1, b0, b1 int) int {
	lo := max(a0, b0)
	hi := min(a1, b1)
	if hi <= lo {
		return 0
	}
	return hi - lo
}
