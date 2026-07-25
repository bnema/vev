package layout

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestResizeFocusTransfersSolvedExtent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		root  *Node
		focus PaneID
		axis  Axis
		delta int
		area  domain.Rect
		want  map[PaneID]domain.Rect
	}{
		{
			name:  "grow width takes from right neighbor",
			root:  split(Horizontal, NewLeaf("a"), NewLeaf("b")),
			focus: "a",
			axis:  Width,
			delta: 5,
			area:  domain.Rect{Width: 51, Height: 2},
			want: map[PaneID]domain.Rect{
				"a": {Width: 30, Height: 2},
				"b": {X: 31, Width: 20, Height: 2},
			},
		},
		{
			name:  "shrink width gives to right neighbor",
			root:  split(Horizontal, weightedLeaf("a", 30), weightedLeaf("b", 20)),
			focus: "a",
			axis:  Width,
			delta: -5,
			area:  domain.Rect{Width: 51, Height: 2},
			want: map[PaneID]domain.Rect{
				"a": {Width: 25, Height: 2},
				"b": {X: 26, Width: 25, Height: 2},
			},
		},
		{
			name: "grow height takes from bottom neighbor",
			root: split(Vertical,
				weightedLeaf("a", 4), weightedLeaf("b", 3),
			),
			focus: "a",
			axis:  Height,
			delta: 1,
			area:  domain.Rect{Width: 20, Height: 8},
			want: map[PaneID]domain.Rect{
				"a": {Width: 20, Height: 5},
				"b": {Y: 6, Width: 20, Height: 2},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Root: tt.root, Focus: tt.focus}
			require.NoError(t, tr.ResizeFocus(tt.axis, tt.delta, tt.area))
			placements, ok := Solve(tr.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.want, contents(placements))
		})
	}
}

func TestResizeFocusSelectsNearestTargetAndAdjacentSibling(t *testing.T) {
	t.Parallel()

	t.Run("nearest matching ancestor", func(t *testing.T) {
		inner := split(Horizontal, NewLeaf("a"), NewLeaf("b"))
		tr := &Tree{Root: split(Horizontal, weightedNode(inner, 2), weightedLeaf("c", 1)), Focus: "a"}
		area := domain.Rect{Width: 122, Height: 2}
		before, ok := Solve(tr.Root, area)
		require.True(t, ok)

		require.NoError(t, tr.ResizeFocus(Width, 5, area))
		after, ok := Solve(tr.Root, area)
		require.True(t, ok)
		require.Equal(t, contents(before)["c"], contents(after)["c"])
		require.Equal(t, contents(before)["a"].Width+5, contents(after)["a"].Width)
		require.Equal(t, contents(before)["b"].Width-5, contents(after)["b"].Width)
	})

	t.Run("direct child subtree is resized as one target", func(t *testing.T) {
		subtree := split(Vertical, NewLeaf("a"), NewLeaf("d"))
		tr := &Tree{Root: split(Horizontal, subtree, NewLeaf("b")), Focus: "a"}
		area := domain.Rect{Width: 51, Height: 5}
		require.NoError(t, tr.ResizeFocus(Width, 5, area))
		placements, ok := Solve(tr.Root, area)
		require.True(t, ok)
		require.Equal(t, 30, contents(placements)["a"].Width)
		require.Equal(t, 30, contents(placements)["d"].Width)
		require.Equal(t, 20, contents(placements)["b"].Width)
	})

	t.Run("stack is resized as one target", func(t *testing.T) {
		stackNode := &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("d")}, Expanded: "a"}
		tr := &Tree{Root: split(Horizontal, stackNode, NewLeaf("b")), Focus: "d"}
		area := domain.Rect{Width: 51, Height: 3}
		require.NoError(t, tr.ResizeFocus(Width, 5, area))
		placements, ok := Solve(tr.Root, area)
		require.True(t, ok)
		require.Equal(t, 30, contents(placements)["a"].Width)
		var collapsedTitleWidth int
		for _, placement := range placements {
			if placement.ID == "d" {
				collapsedTitleWidth = placement.TitleBar.Width
			}
		}
		require.Equal(t, 30, collapsedTitleWidth)
		require.Equal(t, 20, contents(placements)["b"].Width)
	})

	t.Run("right donor at minimum falls back to left", func(t *testing.T) {
		tr := &Tree{Root: split(Horizontal,
			weightedLeaf("a", 40), weightedLeaf("b", 20), weightedLeaf("c", 20),
		), Focus: "b"}
		area := domain.Rect{Width: 82, Height: 2}
		require.NoError(t, tr.ResizeFocus(Width, 5, area))
		placements, ok := Solve(tr.Root, area)
		require.True(t, ok)
		require.Equal(t, map[PaneID]int{"a": 35, "b": 25, "c": 20}, placementWidths(placements))
	})

	t.Run("last target falls back to left neighbor", func(t *testing.T) {
		tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 30), weightedLeaf("b", 20)), Focus: "b"}
		area := domain.Rect{Width: 51, Height: 2}
		require.NoError(t, tr.ResizeFocus(Width, 5, area))
		placements, ok := Solve(tr.Root, area)
		require.True(t, ok)
		require.Equal(t, map[PaneID]int{"a": 25, "b": 25}, placementWidths(placements))
	})
}

func TestResizeFocusMinimumAndTransactionalFailures(t *testing.T) {
	t.Parallel()

	minInt := -int(^uint(0)>>1) - 1
	tests := []struct {
		name  string
		tree  *Tree
		axis  Axis
		delta int
		area  domain.Rect
		err   error
		want  map[PaneID]int
	}{
		{
			name:  "partial growth uses every available donor cell",
			tree:  &Tree{Root: split(Horizontal, weightedLeaf("a", 20), weightedLeaf("b", 22)), Focus: "a"},
			axis:  Width,
			delta: 5,
			area:  domain.Rect{Width: 43, Height: 2},
			want:  map[PaneID]int{"a": 22, "b": 20},
		},
		{
			name:  "minimum integer clamps without overflow",
			tree:  &Tree{Root: split(Horizontal, weightedLeaf("a", 30), weightedLeaf("b", 20)), Focus: "a"},
			axis:  Width,
			delta: minInt,
			area:  domain.Rect{Width: 51, Height: 2},
			want:  map[PaneID]int{"a": 20, "b": 30},
		},
		{
			name: "partial growth stops at recursive donor minimum",
			tree: &Tree{Root: split(Horizontal,
				weightedLeaf("a", 20),
				weightedNode(split(Horizontal, NewLeaf("b"), NewLeaf("d")), 43),
			), Focus: "a"},
			axis:  Width,
			delta: 5,
			area:  domain.Rect{Width: 64, Height: 2},
			want:  map[PaneID]int{"a": 22, "b": 20, "d": 20},
		},
		{
			name:  "no donor capacity",
			tree:  &Tree{Root: split(Horizontal, NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			axis:  Width,
			delta: 2,
			area:  domain.Rect{Width: 41, Height: 2},
			err:   ErrTooSmall,
		},
		{
			name:  "target cannot shrink",
			tree:  &Tree{Root: split(Horizontal, NewLeaf("a"), weightedLeaf("b", 2)), Focus: "a"},
			axis:  Width,
			delta: -2,
			area:  domain.Rect{Width: 61, Height: 2},
			err:   ErrTooSmall,
		},
		{
			name:  "wrong axis",
			tree:  &Tree{Root: split(Horizontal, NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			axis:  Height,
			delta: 1,
			area:  domain.Rect{Width: 41, Height: 2},
			err:   ErrNotInSplit,
		},
		{
			name:  "single pane",
			tree:  NewTree("a"),
			axis:  Width,
			delta: 2,
			area:  domain.Rect{Width: 20, Height: 2},
			err:   ErrNotInSplit,
		},
		{
			name:  "invalid area rolls back",
			tree:  &Tree{Root: split(Horizontal, NewLeaf("a"), NewLeaf("b")), Focus: "a"},
			axis:  Width,
			delta: 2,
			area:  domain.Rect{Width: 20, Height: 2},
			err:   ErrTooSmall,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.tree.Clone()
			err := tt.tree.ResizeFocus(tt.axis, tt.delta, tt.area)
			if tt.err != nil {
				require.ErrorIs(t, err, tt.err)
				require.Equal(t, before, tt.tree, "failed resize must not mutate the tree")
				return
			}
			require.NoError(t, err)
			placements, ok := Solve(tt.tree.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.want, placementWidths(placements))
		})
	}
}

func TestResizeFocusRetainsProportionsAcrossTerminalSizes(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, NewLeaf("a"), NewLeaf("b")), Focus: "a"}
	small := domain.Rect{Width: 81, Height: 2}
	require.NoError(t, tr.ResizeFocus(Width, 10, small))

	placements, ok := Solve(tr.Root, domain.Rect{Width: 161, Height: 2})
	require.True(t, ok)
	require.Equal(t, map[PaneID]int{"a": 100, "b": 60}, placementWidths(placements))
	placements, ok = Solve(tr.Root, small)
	require.True(t, ok)
	require.Equal(t, map[PaneID]int{"a": 50, "b": 30}, placementWidths(placements))
}

func TestEqualize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tree *Tree
		area domain.Rect
		want map[PaneID]int
	}{
		{
			name: "weighted siblings become equal",
			tree: &Tree{Root: split(Horizontal, weightedLeaf("a", 3), weightedLeaf("b", 1)), Focus: "a"},
			area: domain.Rect{Width: 81, Height: 2},
			want: map[PaneID]int{"a": 40, "b": 40},
		},
		{
			name: "recursive minimum constrains only the large child",
			tree: &Tree{Root: split(Horizontal,
				weightedNode(split(Horizontal, weightedLeaf("a", 5), weightedLeaf("b", 1)), 9),
				weightedLeaf("c", 1), weightedLeaf("d", 1),
			), Focus: "a"},
			area: domain.Rect{Width: 102, Height: 2},
			want: map[PaneID]int{"a": 20, "b": 20, "c": 30, "d": 29},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.NoError(t, tt.tree.Equalize(tt.area))
			placements, ok := Solve(tt.tree.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.want, placementWidths(placements))
		})
	}
}

func TestEqualizeFailureIsTransactional(t *testing.T) {
	t.Parallel()

	tr := &Tree{Root: split(Horizontal, weightedLeaf("a", 3), weightedLeaf("b", 1)), Focus: "a"}
	before := tr.Clone()
	require.ErrorIs(t, tr.Equalize(domain.Rect{Width: 20, Height: 2}), ErrTooSmall)
	require.Equal(t, before, tr)
}

func TestCanResize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		tree *Tree
		want bool
	}{
		{name: "single pane", tree: NewTree("a"), want: false},
		{name: "focused split child", tree: &Tree{Root: split(Horizontal, NewLeaf("a"), NewLeaf("b")), Focus: "a"}, want: true},
		{name: "focused stack member under split", tree: &Tree{Root: split(Horizontal, &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b")}, Expanded: "a"}, NewLeaf("c")), Focus: "b"}, want: true},
		{name: "stack without split", tree: &Tree{Root: &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b")}, Expanded: "a"}, Focus: "a"}, want: false},
		{name: "missing focus", tree: &Tree{Root: split(Horizontal, NewLeaf("a"), NewLeaf("b")), Focus: "missing"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.tree.CanResize())
		})
	}
}
