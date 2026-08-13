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

func TestDistributePinsChildrenFromOneQuotaSnapshot(t *testing.T) {
	t.Parallel()

	allocations, ok := distribute(100, []int{20, 33, 42}, []float64{1, 4, 5})
	require.True(t, ok)
	require.Equal(t, []int{20, 36, 44}, allocations)
	require.Equal(t, 100, allocations[0]+allocations[1]+allocations[2])
}

func TestWeightedSplitNestedMinimumRedistributesWithFreshDenominator(t *testing.T) {
	t.Parallel()

	root := split(Horizontal,
		weightedNode(split(Horizontal, NewLeaf("a"), NewLeaf("b")), 2),
		weightedLeaf("c", 2),
		weightedLeaf("d", 6),
	)

	rects, ok := splitChildRects(root, domain.Rect{Width: 132, Height: 2})
	require.True(t, ok)
	require.Equal(t, []int{41, 22, 67}, []int{rects[0].Width, rects[1].Width, rects[2].Width})
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

func TestWeightSameAxisSplitHalvesTargetSolvedExtent(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 2), weightedLeaf("b", 1)), Focus: "a"}
	area := domain.Rect{Width: 82, Height: 2}
	require.NoError(t, tr.Split("b", Right, true, "c", area))

	require.Equal(t, 54.0, tr.Root.Children[0].Weight)
	require.Equal(t, 13.0, tr.Root.Children[1].Weight)
	require.Equal(t, 13.0, tr.Root.Children[2].Weight)
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

func TestClosePreservesFreedEqualizedShareUntilNextEqualize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		dir        SplitDir
		area       domain.Rect
		before     map[PaneID]int
		afterClose map[PaneID]int
		afterEQP   map[PaneID]int
		extents    func([]Placement) map[PaneID]int
	}{
		{
			name:       "horizontal",
			dir:        Horizontal,
			area:       domain.Rect{Width: 92, Height: 5},
			before:     map[PaneID]int{"a": 30, "b": 30, "c": 30},
			afterClose: map[PaneID]int{"a": 30, "b": 61},
			afterEQP:   map[PaneID]int{"a": 46, "b": 45},
			extents:    placementWidths,
		},
		{
			name:       "vertical",
			dir:        Vertical,
			area:       domain.Rect{Width: 40, Height: 17},
			before:     map[PaneID]int{"a": 5, "b": 5, "c": 5},
			afterClose: map[PaneID]int{"a": 5, "b": 11},
			afterEQP:   map[PaneID]int{"a": 8, "b": 8},
			extents:    placementHeights,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Root: split(tt.dir, NewLeaf("a"), NewLeaf("b"), NewLeaf("c")), Focus: "c"}
			require.NoError(t, tr.Equalize(tt.area))
			before, ok := Solve(tr.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.before, tt.extents(before))

			require.NoError(t, tr.Close("c"))
			afterClose, ok := Solve(tr.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.afterClose, tt.extents(afterClose))

			require.NoError(t, tr.Equalize(tt.area))
			afterEQP, ok := Solve(tr.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.afterEQP, tt.extents(afterEQP))
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

func TestWeightConsumeOrExpelPaneSemantics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		tree   *Tree
		target PaneID
		dir    Direction
		area   domain.Rect
		check  func(*testing.T, *Tree, []Placement, []Placement)
	}{
		{
			name: "horizontal insertion preserves normalized sibling shares and gives expelled column fair-share weight",
			tree: &Tree{Root: weightedNode(horizontal(
				weightedNode(vertical("a", "b"), 3),
				weightedLeaf("c", 1),
			), 11), Focus: "b"},
			target: "b",
			dir:    Right,
			area:   domain.Rect{Width: 202, Height: 5},
			check: func(t *testing.T, tree *Tree, before, after []Placement) {
				require.Equal(t, map[PaneID]int{"a": 151, "b": 151, "c": 50}, placementWidths(before))
				require.Equal(t, 11.0, tree.Root.Weight)
				require.Equal(t, []float64{151, 100.5, 50}, childWeights(tree.Root))
				require.Equal(t, []PaneID{"a", "b", "c"}, LeafIDs(tree.Root))
				require.Equal(t, map[PaneID]int{"a": 100, "b": 67, "c": 33}, placementWidths(after))
			},
		},
		{
			name: "vertical consume preserves solved member shares and gives moved pane fair-share weight",
			tree: &Tree{Root: weightedNode(horizontal(
				weightedNode(verticalNodes(weightedLeaf("a", 3), weightedLeaf("b", 1)), 2),
				weightedLeaf("c", 1),
			), 9), Focus: "c"},
			target: "c",
			dir:    Left,
			area:   domain.Rect{Width: 101, Height: 20},
			check: func(t *testing.T, tree *Tree, before, after []Placement) {
				require.Equal(t, map[PaneID]int{"a": 14, "b": 5, "c": 20}, placementHeights(before))
				require.Equal(t, Vertical, tree.Root.Dir)
				require.Equal(t, 9.0, tree.Root.Weight, "root promotion retains the removed horizontal wrapper share")
				require.Equal(t, []float64{14, 5, 9.5}, childWeights(tree.Root))
				require.Equal(t, map[PaneID]int{"a": 9, "b": 3, "c": 6}, placementHeights(after))
			},
		},
		{
			name: "stack consume clears member weights and preserves normalized outer column share",
			tree: &Tree{Root: horizontal(
				weightedLeaf("x", 1),
				weightedNode(&Node{Kind: Stack, Children: []*Node{weightedLeaf("a", 8), weightedLeaf("b", 4)}, Expanded: "a"}, 3),
				weightedLeaf("c", 1),
			), Focus: "c"},
			target: "c",
			dir:    Left,
			area:   domain.Rect{Width: 103, Height: 4},
			check: func(t *testing.T, tree *Tree, before, after []Placement) {
				require.Equal(t, map[PaneID]int{"x": 20, "a": 61, "b": 0, "c": 20}, placementWidths(before))
				require.Len(t, tree.Root.Children, 2)
				stackColumn := tree.Root.Children[1]
				require.Equal(t, Stack, stackColumn.Kind)
				require.Equal(t, 61.0, stackColumn.Weight)
				require.Equal(t, PaneID("c"), stackColumn.Expanded)
				require.Equal(t, []float64{0, 0, 0}, childWeights(stackColumn))
				require.Equal(t, 77, placementSpanWidth(after, "a"))
				require.Equal(t, 77, placementSpanWidth(after, "b"))
				require.Equal(t, 77, placementSpanWidth(after, "c"))
			},
		},
		{
			name: "stack expel clears remaining weights and repairs expanded member",
			tree: &Tree{Root: weightedNode(&Node{Kind: Stack, Children: []*Node{
				weightedLeaf("a", 5), weightedLeaf("b", 4), weightedLeaf("c", 3),
			}, Expanded: "b"}, 7), Focus: "b"},
			target: "b",
			dir:    Right,
			area:   domain.Rect{Width: 101, Height: 4},
			check: func(t *testing.T, tree *Tree, _, after []Placement) {
				require.Equal(t, Horizontal, tree.Root.Dir)
				require.Equal(t, 7.0, tree.Root.Weight)
				require.Equal(t, []float64{0, 0}, childWeights(tree.Root))
				stackColumn := tree.Root.Children[0]
				require.Equal(t, PaneID("a"), stackColumn.Expanded)
				require.Equal(t, []float64{0, 0}, childWeights(stackColumn))
				require.Equal(t, 50, placementSpanWidth(after, "a"))
				require.Equal(t, 50, placementSpanWidth(after, "c"))
				require.Equal(t, 50, placementSpanWidth(after, "b"))
			},
		},
		{
			name: "column collapse copies normalized container weight to promoted leaf",
			tree: &Tree{Root: horizontal(
				weightedLeaf("x", 1),
				weightedNode(vertical("a", "b"), 3),
				weightedLeaf("y", 1),
			), Focus: "a"},
			target: "a",
			dir:    Left,
			area:   domain.Rect{Width: 103, Height: 5},
			check: func(t *testing.T, tree *Tree, _, after []Placement) {
				require.Equal(t, []PaneID{"x", "a", "b", "y"}, LeafIDs(tree.Root))
				require.Equal(t, Leaf, tree.Root.Children[2].Kind)
				require.Equal(t, PaneID("b"), tree.Root.Children[2].Leaf)
				require.Equal(t, 61.0, tree.Root.Children[2].Weight)
				weights := childWeights(tree.Root)
				require.Equal(t, 20.0, weights[0])
				require.InDelta(t, 101.0/3.0, weights[1], 1e-9)
				require.Equal(t, 61.0, weights[2])
				require.Equal(t, 20.0, weights[3])
				require.Equal(t, map[PaneID]int{"x": 20, "a": 21, "b": 39, "y": 20}, placementWidths(after))
			},
		},
		{
			name:   "sole-column expel creates equal default horizontal children and retains wrapper weight",
			tree:   &Tree{Root: weightedNode(verticalNodes(weightedLeaf("a", 3), weightedLeaf("b", 1)), 9), Focus: "a"},
			target: "a",
			dir:    Left,
			area:   domain.Rect{Width: 101, Height: 5},
			check: func(t *testing.T, tree *Tree, _, after []Placement) {
				require.Equal(t, Horizontal, tree.Root.Dir)
				require.Equal(t, 9.0, tree.Root.Weight)
				require.Equal(t, []float64{0, 0}, childWeights(tree.Root))
				require.Equal(t, map[PaneID]int{"a": 50, "b": 50}, placementWidths(after))
			},
		},
		{
			name: "expel from equal columns yields three homogeneous columns",
			tree: &Tree{Root: horizontal(
				NewLeaf("a"),
				verticalNodes(NewLeaf("b"), NewLeaf("c")),
			), Focus: "c"},
			target: "c",
			dir:    Right,
			area:   domain.Rect{Width: 122, Height: 10},
			check: func(t *testing.T, tree *Tree, _, after []Placement) {
				require.Equal(t, []PaneID{"a", "b", "c"}, LeafIDs(tree.Root))
				require.Equal(t, map[PaneID]int{"a": 40, "b": 40, "c": 40}, placementWidths(after))
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			beforeIDs := LeafIDs(tt.tree.Root)
			requireUniquePaneIDs(t, beforeIDs)
			before, solved := Solve(tt.tree.Root, tt.area)
			require.True(t, solved, "weight fixture must solve before surgery")

			changed, err := tt.tree.ConsumeOrExpelPane(tt.target, tt.dir, tt.area)
			require.NoError(t, err)
			require.True(t, changed)
			require.Equal(t, tt.target, tt.tree.Focus)

			afterIDs := LeafIDs(tt.tree.Root)
			require.ElementsMatch(t, beforeIDs, afterIDs)
			require.Len(t, afterIDs, len(beforeIDs))
			requireUniquePaneIDs(t, afterIDs)
			after, solved := Solve(tt.tree.Root, tt.area)
			require.True(t, solved, "weighted result must solve")
			tt.check(t, tt.tree, before, after)
		})
	}
}

func childWeights(node *Node) []float64 {
	weights := make([]float64, len(node.Children))
	for i, child := range node.Children {
		weights[i] = child.Weight
	}
	return weights
}

func placementSpanWidth(placements []Placement, id PaneID) int {
	for _, placement := range placements {
		if placement.ID != id {
			continue
		}
		if placement.Content.Width > 0 {
			return placement.Content.Width
		}
		return placement.TitleBar.Width
	}
	return 0
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
