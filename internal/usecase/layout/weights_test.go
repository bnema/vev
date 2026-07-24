package layout

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestWeightedSplitAllocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		root *Node
		area domain.Rect
		want map[PaneID]int
	}{
		{
			name: "zero weights preserve equal split and remainder order",
			root: split(Horizontal, NewLeaf("a"), NewLeaf("b"), NewLeaf("c")),
			area: domain.Rect{Width: 64, Height: 2},
			want: map[PaneID]int{"a": 21, "b": 21, "c": 20},
		},
		{
			name: "two to one weights",
			root: split(Horizontal, weightedLeaf("a", 2), weightedLeaf("b", 1)),
			area: domain.Rect{Width: 61, Height: 2},
			want: map[PaneID]int{"a": 40, "b": 20},
		},
		{
			name: "largest remainder ties use child order",
			root: split(Horizontal, weightedLeaf("a", 1), weightedLeaf("b", 1), weightedLeaf("c", 2)),
			area: domain.Rect{Width: 84, Height: 2},
			want: map[PaneID]int{"a": 21, "b": 20, "c": 41},
		},
		{
			name: "minimum constrained children pin and remainder redistributes proportionally",
			root: split(Horizontal, weightedLeaf("a", 1), weightedLeaf("b", 1), weightedLeaf("c", 8)),
			area: domain.Rect{Width: 102, Height: 2},
			want: map[PaneID]int{"a": 20, "b": 20, "c": 60},
		},
		{
			name: "negative and zero in memory weights use default",
			root: split(Horizontal, weightedLeaf("a", -4), weightedLeaf("b", 0), weightedLeaf("c", 2)),
			area: domain.Rect{Width: 82, Height: 2},
			want: map[PaneID]int{"a": 20, "b": 20, "c": 40},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			placements, ok := Solve(tt.root, tt.area)
			require.True(t, ok)
			for id, want := range tt.want {
				require.Equal(t, want, contents(placements)[id].Width)
			}
		})
	}
}

func TestWeightedSplitRecursiveMinima(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		root   *Node
		area   domain.Rect
		wantOK bool
		want   map[PaneID]domain.Rect
	}{
		{
			name: "same axis nested minima sum children and separators",
			root: split(Horizontal,
				weightedNode(split(Horizontal, NewLeaf("a"), NewLeaf("b")), 1),
				weightedLeaf("c", 9),
			),
			area:   domain.Rect{Width: 102, Height: 2},
			wantOK: true,
			want: map[PaneID]domain.Rect{
				"a": {Width: 20, Height: 2},
				"b": {X: 21, Width: 20, Height: 2},
				"c": {X: 42, Width: 60, Height: 2},
			},
		},
		{
			name: "cross axis nested minima take maximum",
			root: split(Horizontal,
				split(Vertical, NewLeaf("a"), NewLeaf("b")),
				NewLeaf("c"),
			),
			area:   domain.Rect{Width: 41, Height: 5},
			wantOK: true,
		},
		{
			name: "stack vertical minimum is titles plus one content row",
			root: split(Vertical,
				weightedNode(&Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b"), NewLeaf("c")}, Expanded: "b"}, 1),
				weightedLeaf("d", 9),
			),
			area:   domain.Rect{Width: 20, Height: 7},
			wantOK: true,
		},
		{
			name: "insufficient same axis recursive minimum fails without placements",
			root: split(Horizontal,
				split(Horizontal, NewLeaf("a"), NewLeaf("b")),
				NewLeaf("c"),
			),
			area:   domain.Rect{Width: 61, Height: 2},
			wantOK: false,
		},
		{
			name: "insufficient cross axis recursive minimum fails without placements",
			root: split(Horizontal,
				split(Vertical, NewLeaf("a"), NewLeaf("b")),
				NewLeaf("c"),
			),
			area:   domain.Rect{Width: 41, Height: 4},
			wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			placements, ok := Solve(tt.root, tt.area)
			require.Equal(t, tt.wantOK, ok)
			if !tt.wantOK {
				require.Nil(t, placements)
				return
			}
			if tt.want != nil {
				require.Equal(t, tt.want, contents(placements))
			}
		})
	}
}

func TestWeightSameAxisSplitNormalizesSolvedExtentsBeforeInsertion(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 2), weightedLeaf("b", 1)), Focus: "a"}
	area := domain.Rect{Width: 82, Height: 2}
	require.NoError(t, tr.Split("b", Right, true, "c", area))

	require.Equal(t, 54.0, tr.Root.Children[0].Weight)
	require.Equal(t, 27.0, tr.Root.Children[1].Weight)
	require.Zero(t, tr.Root.Children[2].Weight)
	placements, ok := Solve(tr.Root, area)
	require.True(t, ok)
	require.Equal(t, map[PaneID]int{"a": 40, "b": 20, "c": 20}, placementWidths(placements))
}

func TestWeightCrossAxisSplitTransfersParentShareAndClearsMembers(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 3), weightedLeaf("b", 1)), Focus: "a"}
	require.NoError(t, tr.Split("a", Down, true, "c", domain.Rect{Width: 81, Height: 5}))

	wrapper := tr.Root.Children[0]
	require.Equal(t, Vertical, wrapper.Dir)
	require.Equal(t, 3.0, wrapper.Weight)
	require.Zero(t, wrapper.Children[0].Weight)
	require.Zero(t, wrapper.Children[1].Weight)
	placements, ok := Solve(tr.Root, domain.Rect{Width: 81, Height: 5})
	require.True(t, ok)
	require.Equal(t, 60, contents(placements)["a"].Width)
	require.Equal(t, 20, contents(placements)["b"].Width)
}

func TestWeightStackNewTransfersParentShareAndClearsMembers(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 3), weightedLeaf("b", 1)), Focus: "a"}
	require.NoError(t, tr.StackNew("a", "c", domain.Rect{Width: 81, Height: 3}))

	stackNode := tr.Root.Children[0]
	require.Equal(t, Stack, stackNode.Kind)
	require.Equal(t, 3.0, stackNode.Weight)
	require.Zero(t, stackNode.Children[0].Weight)
	require.Zero(t, stackNode.Children[1].Weight)
	placements, ok := Solve(tr.Root, domain.Rect{Width: 81, Height: 3})
	require.True(t, ok)
	require.Equal(t, 60, contents(placements)["c"].Width)
	require.Equal(t, 20, contents(placements)["b"].Width)
}

func TestWeightStackNewClearsExistingStackMemberWeights(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: &Node{Kind: Stack, Children: []*Node{weightedLeaf("a", 3), weightedLeaf("b", 1)}, Expanded: "a", Weight: 4}, Focus: "a"}
	require.NoError(t, tr.StackNew("a", "c", domain.Rect{Width: 20, Height: 4}))

	require.Equal(t, 4.0, tr.Root.Weight)
	for _, child := range tr.Root.Children {
		require.Zero(t, child.Weight)
	}
}

func TestWeightToggleStackClearsMemberWeights(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		root     *Node
		wantKind Kind
	}{
		{
			name: "split to stack",
			root: &Node{Kind: Split, Dir: Vertical, Weight: 3, Children: []*Node{
				weightedLeaf("a", 2), weightedLeaf("b", 1),
			}},
			wantKind: Stack,
		},
		{
			name: "stack to split",
			root: &Node{Kind: Stack, Weight: 3, Children: []*Node{
				weightedLeaf("a", 2), weightedLeaf("b", 1),
			}, Expanded: "a"},
			wantKind: Split,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Root: split(Horizontal, tt.root, weightedLeaf("c", 1)), Focus: "a"}
			area := domain.Rect{Width: 81, Height: 5}
			require.NoError(t, tr.ToggleStack("a", area))
			container := tr.Root.Children[0]
			require.Equal(t, tt.wantKind, container.Kind)
			require.Equal(t, 3.0, container.Weight)
			for _, child := range container.Children {
				require.Zero(t, child.Weight)
			}
			placements, ok := Solve(tr.Root, area)
			require.True(t, ok)
			require.Equal(t, 60, contents(placements)["a"].Width)
			require.Equal(t, 20, contents(placements)["c"].Width)
			if tt.wantKind == Split {
				require.Equal(t, map[PaneID]int{"a": 2, "b": 2, "c": 5}, placementHeights(placements))
			}
		})
	}
}

func TestWeightClosePromotionTransfersContainerShare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		container *Node
	}{
		{
			name:      "split",
			container: split(Vertical, NewLeaf("a"), NewLeaf("b")),
		},
		{
			name: "stack",
			container: &Node{Kind: Stack, Children: []*Node{
				NewLeaf("a"), NewLeaf("b"),
			}, Expanded: "a"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Root: split(Horizontal,
				weightedNode(tt.container, 3),
				weightedLeaf("c", 1),
			), Focus: "a"}
			area := domain.Rect{Width: 81, Height: 5}
			before, ok := Solve(tr.Root, area)
			require.True(t, ok)
			require.Equal(t, 60, contents(before)["a"].Width)

			require.NoError(t, tr.Close("a"))
			require.Equal(t, PaneID("b"), tr.Root.Children[0].Leaf)
			require.Equal(t, 3.0, tr.Root.Children[0].Weight)
			after, ok := Solve(tr.Root, area)
			require.True(t, ok)
			require.Equal(t, 60, contents(after)["b"].Width)
			require.Equal(t, 20, contents(after)["c"].Width)
		})
	}
}

func TestWeightSplitStackWrapperTransfersShare(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: &Node{Kind: Stack, Weight: 4, Children: []*Node{
		weightedLeaf("a", 2), weightedLeaf("b", 1),
	}, Expanded: "a"}, Focus: "a"}
	require.NoError(t, tr.Split("a", Right, true, "c", domain.Rect{Width: 41, Height: 3}))

	require.Equal(t, 4.0, tr.Root.Weight)
	require.Zero(t, tr.Root.Children[0].Weight)
	require.Equal(t, Stack, tr.Root.Children[0].Kind)
	for _, member := range tr.Root.Children[0].Children {
		require.Zero(t, member.Weight)
	}
}

func TestWeightCloneCopiesWeightIndependently(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 2), weightedLeaf("b", 1)), Focus: "a"}
	clone := tr.Clone()
	require.Equal(t, tr, clone)
	clone.Root.Children[0].Weight = 9
	require.Equal(t, 2.0, tr.Root.Children[0].Weight)
}

func split(dir SplitDir, children ...*Node) *Node {
	return &Node{Kind: Split, Dir: dir, Children: children}
}

func weightedLeaf(id PaneID, weight float64) *Node {
	return &Node{Kind: Leaf, Leaf: id, Weight: weight}
}

func weightedNode(n *Node, weight float64) *Node {
	n.Weight = weight
	return n
}

func placementWidths(placements []Placement) map[PaneID]int {
	out := make(map[PaneID]int, len(placements))
	for _, placement := range placements {
		out[placement.ID] = placement.Content.Width
	}
	return out
}

func placementHeights(placements []Placement) map[PaneID]int {
	out := make(map[PaneID]int, len(placements))
	for _, placement := range placements {
		out[placement.ID] = placement.Content.Height
	}
	return out
}
