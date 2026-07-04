package layout

import "github.com/bnema/vev/internal/domain"

// PaneID is an opaque pane identifier owned by higher layers.
type PaneID string

// Kind identifies the topology role of a Node.
type Kind int

const (
	Leaf Kind = iota
	Split
	Stack
)

// Direction is used for split and focus operations.
type Direction int

const (
	Left Direction = iota
	Right
	Up
	Down
)

// SplitDir is the axis used by a split container.
type SplitDir int

const (
	Horizontal SplitDir = iota // children are laid out left-to-right
	Vertical                   // children are laid out top-to-bottom
)

const (
	MinPaneCols = 20
	MinPaneRows = 2
)

// Node is a recursive pane layout node. Leaf is meaningful for Leaf nodes; Dir
// and Children are meaningful for Split nodes; Children and Expanded are
// meaningful for Stack nodes.
type Node struct {
	Kind     Kind
	Dir      SplitDir
	Children []*Node
	Leaf     PaneID
	Expanded PaneID
}

// Tree is a pane layout plus its focused pane.
type Tree struct {
	Root  *Node
	Focus PaneID
}

// Placement is the solved geometry for a pane.
type Placement struct {
	ID        PaneID
	Content   domain.Rect
	TitleBar  domain.Rect
	Collapsed bool
	InStack   bool
}

func NewLeaf(id PaneID) *Node { return &Node{Kind: Leaf, Leaf: id} }

func NewTree(id PaneID) *Tree { return &Tree{Root: NewLeaf(id), Focus: id} }

func axisFor(dir Direction) SplitDir {
	if dir == Left || dir == Right {
		return Horizontal
	}
	return Vertical
}

func (n *Node) clone() *Node {
	if n == nil {
		return nil
	}
	out := *n
	if len(n.Children) > 0 {
		out.Children = make([]*Node, len(n.Children))
		for i, child := range n.Children {
			out.Children[i] = child.clone()
		}
	}
	return &out
}

func (t *Tree) clone() *Tree {
	return &Tree{Root: t.Root.clone(), Focus: t.Focus}
}
