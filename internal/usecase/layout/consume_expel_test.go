package layout

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestConsumeOrExpelPaneTransitions(t *testing.T) {
	t.Parallel()

	area := domain.Rect{Width: 101, Height: 12}
	tests := []struct {
		name       string
		tree       *Tree
		target     PaneID
		dir        Direction
		want       *Tree
		wantChange bool
	}{
		{
			name:       "consume singleton left into leaf",
			tree:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "b"},
			target:     "b",
			dir:        Left,
			want:       &Tree{Root: vertical("a", "b"), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "consume singleton right into leaf",
			tree:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			target:     "a",
			dir:        Right,
			want:       &Tree{Root: vertical("b", "a"), Focus: "a"},
			wantChange: true,
		},
		{
			name:   "consume singleton left into vertical column",
			tree:   &Tree{Root: horizontal(vertical("a", "b"), NewLeaf("c")), Focus: "c"},
			target: "c",
			dir:    Left,
			want: &Tree{Root: verticalNodes(
				consumeExpelWeightedLeaf("a", 6),
				consumeExpelWeightedLeaf("b", 5),
				NewLeaf("c"),
			), Focus: "c"},
			wantChange: true,
		},
		{
			name:   "consume singleton right into vertical column",
			tree:   &Tree{Root: horizontal(NewLeaf("a"), vertical("b", "c")), Focus: "a"},
			target: "a",
			dir:    Right,
			want: &Tree{Root: verticalNodes(
				consumeExpelWeightedLeaf("b", 6),
				consumeExpelWeightedLeaf("c", 5),
				NewLeaf("a"),
			), Focus: "a"},
			wantChange: true,
		},
		{
			name:       "consume singleton left into stack appends and expands",
			tree:       &Tree{Root: horizontal(stack("a", "a", "b"), NewLeaf("c")), Focus: "c"},
			target:     "c",
			dir:        Left,
			want:       &Tree{Root: stack("c", "a", "b", "c"), Focus: "c"},
			wantChange: true,
		},
		{
			name:       "consume singleton right into stack appends and expands",
			tree:       &Tree{Root: horizontal(NewLeaf("a"), stack("b", "b", "c")), Focus: "a"},
			target:     "a",
			dir:        Right,
			want:       &Tree{Root: stack("a", "b", "c", "a"), Focus: "a"},
			wantChange: true,
		},
		{
			name:       "expel middle vertical member left from sole root column",
			tree:       &Tree{Root: vertical("a", "b", "c"), Focus: "b"},
			target:     "b",
			dir:        Left,
			want:       &Tree{Root: horizontal(NewLeaf("b"), vertical("a", "c")), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "expel middle vertical member right from sole root column",
			tree:       &Tree{Root: vertical("a", "b", "c"), Focus: "b"},
			target:     "b",
			dir:        Right,
			want:       &Tree{Root: horizontal(vertical("a", "c"), NewLeaf("b")), Focus: "b"},
			wantChange: true,
		},
		{
			name:   "expel vertical member left beside its source column",
			tree:   &Tree{Root: horizontal(vertical("a", "b", "c"), NewLeaf("d")), Focus: "b"},
			target: "b",
			dir:    Left,
			want: &Tree{Root: horizontal(
				NewLeaf("b"),
				consumeExpelWeightedNode(vertical("a", "c"), 50),
				consumeExpelWeightedLeaf("d", 50),
			), Focus: "b"},
			wantChange: true,
		},
		{
			name:   "expel vertical member right beside its source column",
			tree:   &Tree{Root: horizontal(vertical("a", "b", "c"), NewLeaf("d")), Focus: "b"},
			target: "b",
			dir:    Right,
			want: &Tree{Root: horizontal(
				consumeExpelWeightedNode(vertical("a", "c"), 50),
				NewLeaf("b"),
				consumeExpelWeightedLeaf("d", 50),
			), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "expel stack member left from sole root column",
			tree:       &Tree{Root: stack("a", "a", "b", "c"), Focus: "b"},
			target:     "b",
			dir:        Left,
			want:       &Tree{Root: horizontal(NewLeaf("b"), stack("a", "a", "c")), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "expel stack member right from sole root column",
			tree:       &Tree{Root: stack("a", "a", "b", "c"), Focus: "b"},
			target:     "b",
			dir:        Right,
			want:       &Tree{Root: horizontal(stack("a", "a", "c"), NewLeaf("b")), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "expelling expanded stack member repairs source expansion",
			tree:       &Tree{Root: stack("b", "a", "b", "c"), Focus: "b"},
			target:     "b",
			dir:        Right,
			want:       &Tree{Root: horizontal(stack("a", "a", "c"), NewLeaf("b")), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "expel collapses two-member vertical column",
			tree:       &Tree{Root: vertical("a", "b"), Focus: "a"},
			target:     "a",
			dir:        Left,
			want:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			wantChange: true,
		},
		{
			name:       "expel collapses two-member stack column",
			tree:       &Tree{Root: stack("b", "a", "b"), Focus: "b"},
			target:     "b",
			dir:        Right,
			want:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "real move focuses explicitly targeted non-focused pane",
			tree:       &Tree{Root: vertical("a", "b", "c"), Focus: "a"},
			target:     "b",
			dir:        Right,
			want:       &Tree{Root: horizontal(vertical("a", "c"), NewLeaf("b")), Focus: "b"},
			wantChange: true,
		},
		{
			name:       "left edge singleton is exact no-op and preserves different focus",
			tree:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "b"},
			target:     "a",
			dir:        Left,
			want:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "b"},
			wantChange: false,
		},
		{
			name:       "right edge singleton is exact no-op and preserves different focus",
			tree:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			target:     "b",
			dir:        Right,
			want:       &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			wantChange: false,
		},
		{
			name:       "sole singleton root is exact no-op",
			tree:       NewTree("a"),
			target:     "a",
			dir:        Left,
			want:       NewTree("a"),
			wantChange: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.tree.Clone()
			beforeIDs := LeafIDs(tt.tree.Root)
			requireUniquePaneIDs(t, beforeIDs)

			changed, err := tt.tree.ConsumeOrExpelPane(tt.target, tt.dir, area)
			require.NoError(t, err)
			require.Equal(t, tt.wantChange, changed)
			require.Equal(t, tt.want, tt.tree)
			if !tt.wantChange {
				require.Equal(t, before, tt.tree, "a successful edge no-op must preserve the complete tree")
			}

			afterIDs := LeafIDs(tt.tree.Root)
			require.ElementsMatch(t, beforeIDs, afterIDs)
			require.Len(t, afterIDs, len(beforeIDs), "tree surgery must neither create nor lose pane IDs")
			requireUniquePaneIDs(t, afterIDs)
			_, solved := Solve(tt.tree.Root, area)
			require.True(t, solved, "every successful result must solve")
		})
	}
}

func TestConsumeOrExpelPaneRejectsMalformedOrUnsolvableLayoutsAtomically(t *testing.T) {
	t.Parallel()

	area := domain.Rect{Width: 101, Height: 12}
	tests := []struct {
		name   string
		tree   *Tree
		target PaneID
		dir    Direction
		area   domain.Rect
		want   error
	}{
		{name: "nil tree", tree: nil, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "nil root", tree: &Tree{}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "missing target", tree: NewTree("a"), target: "missing", dir: Left, area: area, want: ErrNotFound},
		{name: "empty leaf ID", tree: &Tree{Root: NewLeaf(""), Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "duplicate leaf ID including duplicate target", tree: &Tree{Root: horizontal(NewLeaf("a"), NewLeaf("a")), Focus: "a"}, target: "a", dir: Right, area: area, want: ErrUnsupportedColumnLayout},
		{name: "missing focus", tree: &Tree{Root: NewLeaf("a")}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "dangling focus", tree: &Tree{Root: NewLeaf("a"), Focus: "missing"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "empty horizontal root", tree: &Tree{Root: &Node{Kind: Split, Dir: Horizontal}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "empty vertical column", tree: &Tree{Root: &Node{Kind: Split, Dir: Vertical}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "empty stack", tree: &Tree{Root: &Node{Kind: Stack}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "nested horizontal root column", tree: &Tree{Root: horizontal(NewLeaf("a"), horizontal(NewLeaf("b"), NewLeaf("c"))), Focus: "b"}, target: "b", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "vertical column has non-leaf member", tree: &Tree{Root: verticalNodes(NewLeaf("a"), horizontal(NewLeaf("b"), NewLeaf("c"))), Focus: "b"}, target: "b", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "stack has non-leaf member", tree: &Tree{Root: &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), vertical("b", "c")}, Expanded: "a"}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "stack has nil member", tree: &Tree{Root: &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), nil}, Expanded: "a"}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "stack expanded member is missing", tree: &Tree{Root: stack("missing", "a", "b"), Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "unsupported root split direction", tree: &Tree{Root: &Node{Kind: Split, Dir: SplitDir(99), Children: []*Node{NewLeaf("a"), NewLeaf("b")}}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "unsupported node kind", tree: &Tree{Root: &Node{Kind: Kind(99), Children: []*Node{NewLeaf("a")}}, Focus: "a"}, target: "a", dir: Left, area: area, want: ErrUnsupportedColumnLayout},
		{name: "invalid up direction", tree: NewTree("a"), target: "a", dir: Up, area: area, want: ErrInvalidDirection},
		{name: "invalid down direction", tree: NewTree("a"), target: "a", dir: Down, area: area, want: ErrInvalidDirection},
		{name: "invalid unknown direction", tree: NewTree("a"), target: "a", dir: Direction(99), area: area, want: ErrInvalidDirection},
		{name: "too small after expel", tree: &Tree{Root: vertical("a", "b"), Focus: "a"}, target: "a", dir: Right, area: domain.Rect{Width: 40, Height: 5}, want: ErrTooSmall},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.tree.Clone()
			changed, err := tt.tree.ConsumeOrExpelPane(tt.target, tt.dir, tt.area)
			require.False(t, changed)
			require.ErrorIs(t, err, tt.want)
			require.Equal(t, before, tt.tree, "every refusal must preserve the exact original tree")
		})
	}
}

func horizontal(columns ...*Node) *Node {
	return &Node{Kind: Split, Dir: Horizontal, Children: columns}
}

func vertical(ids ...PaneID) *Node {
	children := make([]*Node, 0, len(ids))
	for _, id := range ids {
		children = append(children, NewLeaf(id))
	}
	return verticalNodes(children...)
}

func verticalNodes(children ...*Node) *Node {
	return &Node{Kind: Split, Dir: Vertical, Children: children}
}

func stack(expanded PaneID, ids ...PaneID) *Node {
	children := make([]*Node, 0, len(ids))
	for _, id := range ids {
		children = append(children, NewLeaf(id))
	}
	return &Node{Kind: Stack, Children: children, Expanded: expanded}
}

func consumeExpelWeightedLeaf(id PaneID, weight float64) *Node {
	return &Node{Kind: Leaf, Leaf: id, Weight: weight}
}

func consumeExpelWeightedNode(node *Node, weight float64) *Node {
	node.Weight = weight
	return node
}

func requireUniquePaneIDs(t *testing.T, ids []PaneID) {
	t.Helper()
	seen := make(map[PaneID]struct{}, len(ids))
	for _, id := range ids {
		require.NotEmpty(t, id)
		_, duplicate := seen[id]
		require.False(t, duplicate, "pane ID %q occurs more than once", id)
		seen[id] = struct{}{}
	}
}
