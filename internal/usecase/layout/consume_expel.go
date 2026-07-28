package layout

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
)

var (
	ErrUnsupportedColumnLayout = errors.New("unsupported column layout")
	ErrInvalidDirection        = errors.New("invalid direction")
)

// ConsumeOrExpelPane moves target horizontally between canonical root columns.
// A singleton column is consumed into its immediate neighbor; a member of a
// vertical or stack column is expelled into a new singleton column.
func (t *Tree) ConsumeOrExpelPane(target PaneID, dir Direction, area domain.Rect) (bool, error) {
	if dir != Left && dir != Right {
		return false, ErrInvalidDirection
	}
	if !validCanonicalColumnTree(t) {
		return false, ErrUnsupportedColumnLayout
	}

	columns := canonicalColumns(t.Root)
	columnIndex, _, found := findColumnMember(columns, target)
	if !found {
		return false, ErrNotFound
	}

	if columns[columnIndex].Kind == Leaf {
		destinationIndex := columnIndex - 1
		if dir == Right {
			destinationIndex = columnIndex + 1
		}
		if destinationIndex < 0 || destinationIndex >= len(columns) {
			return false, nil
		}
	}

	candidate := t.clone()
	if _, solved := Solve(candidate.Root, area); !solved {
		return false, ErrTooSmall
	}

	columns = canonicalColumns(candidate.Root)
	columnIndex, memberIndex, _ := findColumnMember(columns, target)
	if candidate.Root.Kind == Split && candidate.Root.Dir == Horizontal {
		normalizeChildWeightsFromArea(candidate.Root, area)
	}

	if columns[columnIndex].Kind == Leaf {
		consumeSingletonColumn(candidate, columnIndex, dir, area)
	} else {
		expelColumnMember(candidate, columnIndex, memberIndex, dir)
	}
	candidate.Focus = target

	if _, solved := Solve(candidate.Root, area); !solved {
		return false, ErrTooSmall
	}
	*t = *candidate
	return true, nil
}

func validCanonicalColumnTree(t *Tree) bool {
	if t == nil || t.Root == nil {
		return false
	}

	columns := canonicalColumns(t.Root)
	if len(columns) == 0 {
		return false
	}
	if t.Root.Kind == Split && t.Root.Dir == Horizontal && len(columns) < 2 {
		return false
	}

	ids := make(map[PaneID]struct{})
	for _, column := range columns {
		if !validCanonicalColumn(column, ids) {
			return false
		}
	}
	if t.Focus == "" {
		return false
	}
	_, focused := ids[t.Focus]
	return focused
}

func canonicalColumns(root *Node) []*Node {
	if root == nil {
		return nil
	}
	if root.Kind == Split && root.Dir == Horizontal {
		return root.Children
	}
	return []*Node{root}
}

func validCanonicalColumn(column *Node, ids map[PaneID]struct{}) bool {
	if column == nil {
		return false
	}

	switch column.Kind {
	case Leaf:
		if len(column.Children) != 0 {
			return false
		}
		return addUniqueLeafID(ids, column.Leaf)
	case Split:
		if column.Dir != Vertical || len(column.Children) < 2 {
			return false
		}
	case Stack:
		if len(column.Children) < 2 {
			return false
		}
	default:
		return false
	}

	expanded := column.Kind != Stack
	for _, member := range column.Children {
		if member == nil || member.Kind != Leaf || len(member.Children) != 0 || !addUniqueLeafID(ids, member.Leaf) {
			return false
		}
		if column.Kind == Stack && member.Leaf == column.Expanded {
			expanded = true
		}
	}
	return expanded
}

func addUniqueLeafID(ids map[PaneID]struct{}, id PaneID) bool {
	if id == "" {
		return false
	}
	if _, exists := ids[id]; exists {
		return false
	}
	ids[id] = struct{}{}
	return true
}

func findColumnMember(columns []*Node, target PaneID) (int, int, bool) {
	for columnIndex, column := range columns {
		if column.Kind == Leaf {
			if column.Leaf == target {
				return columnIndex, -1, true
			}
			continue
		}
		for memberIndex, member := range column.Children {
			if member.Leaf == target {
				return columnIndex, memberIndex, true
			}
		}
	}
	return 0, 0, false
}

func consumeSingletonColumn(t *Tree, sourceIndex int, dir Direction, area domain.Rect) {
	root := t.Root
	destinationIndex := sourceIndex - 1
	if dir == Right {
		destinationIndex = sourceIndex + 1
	}

	columnAreas, _ := splitChildRects(root, area)
	moved := root.Children[sourceIndex]
	moved.Weight = 0
	destination := root.Children[destinationIndex]

	switch destination.Kind {
	case Leaf:
		weight := destination.Weight
		destination.Weight = 0
		destination = &Node{
			Kind:     Split,
			Dir:      Vertical,
			Children: []*Node{destination, moved},
			Weight:   weight,
		}
	case Split:
		normalizeChildWeightsFromArea(destination, columnAreas[destinationIndex])
		destination.Children = append(destination.Children, moved)
	case Stack:
		destination.Children = append(destination.Children, moved)
		destination.Expanded = moved.Leaf
		clearChildWeights(destination)
	}
	root.Children[destinationIndex] = destination
	root.Children = removeNodeAt(root.Children, sourceIndex)

	if len(root.Children) == 1 {
		promoted := root.Children[0]
		promoted.Weight = root.Weight
		t.Root = promoted
	}
}

func expelColumnMember(t *Tree, columnIndex, memberIndex int, dir Direction) {
	root := t.Root
	rootIsHorizontal := root.Kind == Split && root.Dir == Horizontal
	columns := canonicalColumns(root)
	source := columns[columnIndex]
	wrapperWeight := source.Weight
	if !rootIsHorizontal {
		wrapperWeight = root.Weight
	}

	moved := source.Children[memberIndex]
	moved.Weight = 0
	source.Children = removeNodeAt(source.Children, memberIndex)
	if source.Kind == Stack {
		if source.Expanded == moved.Leaf {
			source.Expanded = source.Children[0].Leaf
		}
		clearChildWeights(source)
	}

	remaining := source
	if len(source.Children) == 1 {
		remaining = source.Children[0]
		remaining.Weight = source.Weight
	}

	if !rootIsHorizontal {
		remaining.Weight = 0
		children := []*Node{remaining, moved}
		if dir == Left {
			children[0], children[1] = children[1], children[0]
		}
		t.Root = &Node{Kind: Split, Dir: Horizontal, Children: children, Weight: wrapperWeight}
		return
	}

	root.Children[columnIndex] = remaining
	insertionIndex := columnIndex
	if dir == Right {
		insertionIndex++
	}
	root.Children = insertNodeAt(root.Children, insertionIndex, moved)
}

func removeNodeAt(nodes []*Node, index int) []*Node {
	copy(nodes[index:], nodes[index+1:])
	nodes[len(nodes)-1] = nil
	return nodes[:len(nodes)-1]
}

func insertNodeAt(nodes []*Node, index int, node *Node) []*Node {
	nodes = append(nodes, nil)
	copy(nodes[index+1:], nodes[index:])
	nodes[index] = node
	return nodes
}
