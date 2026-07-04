package layout

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestSolveSplitGeometry(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		root *Node
		area domain.Rect
		want map[PaneID]domain.Rect
	}{
		{
			name: "horizontal remainder to first children",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{NewLeaf("a"), NewLeaf("b"), NewLeaf("c")}},
			area: domain.Rect{Width: 64, Height: 5},
			want: map[PaneID]domain.Rect{"a": {Width: 21, Height: 5}, "b": {X: 22, Width: 21, Height: 5}, "c": {X: 44, Width: 20, Height: 5}},
		},
		{
			name: "vertical remainder to first children",
			root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("a"), NewLeaf("b")}},
			area: domain.Rect{Width: 20, Height: 7},
			want: map[PaneID]domain.Rect{"a": {Width: 20, Height: 3}, "b": {Y: 4, Width: 20, Height: 3}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := Solve(tt.root, tt.area)
			require.True(t, ok)
			require.Len(t, got, len(tt.want))
			for _, p := range got {
				require.Equal(t, tt.want[p.ID], p.Content)
			}
		})
	}
}

func TestSolveNestedTrees(t *testing.T) {
	t.Parallel()
	root := &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
		NewLeaf("left"),
		{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("top"), NewLeaf("bottom")}},
	}}
	got, ok := Solve(root, domain.Rect{Width: 61, Height: 5})
	require.True(t, ok)
	require.Equal(t, map[PaneID]domain.Rect{
		"left":   {Width: 30, Height: 5},
		"top":    {X: 31, Width: 30, Height: 2},
		"bottom": {X: 31, Y: 3, Width: 30, Height: 2},
	}, contents(got))
}

func TestSolveStackFitAndOverflow(t *testing.T) {
	t.Parallel()
	stack := &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b"), NewLeaf("c")}, Expanded: "b"}
	got, ok := Solve(stack, domain.Rect{Width: 20, Height: 4})
	require.True(t, ok)
	require.Equal(t, []Placement{
		{ID: "a", TitleBar: domain.Rect{Width: 20, Height: 1}, Collapsed: true, InStack: true},
		{ID: "b", Content: domain.Rect{Y: 2, Width: 20, Height: 1}, TitleBar: domain.Rect{Y: 1, Width: 20, Height: 1}, InStack: true},
		{ID: "c", TitleBar: domain.Rect{Y: 3, Width: 20, Height: 1}, Collapsed: true, InStack: true},
	}, got)

	_, ok = Solve(stack, domain.Rect{Width: 20, Height: 3})
	require.False(t, ok)
}

func TestTooSmallRefusalsDoNotMutate(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   func(*Tree) error
	}{
		{
			name: "split refuses below min width",
			op: func(tr *Tree) error {
				return tr.Split("a", Right, true, "b", domain.Rect{Width: 40, Height: 2})
			},
		},
		{
			name: "stack refuses overflow",
			op: func(tr *Tree) error {
				require.NoError(t, tr.StackNew("a", "b", domain.Rect{Width: 20, Height: 3}))
				return tr.StackNew("b", "c", domain.Rect{Width: 20, Height: 3})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewTree("a")
			before := tr.clone()
			err := tt.op(tr)
			require.ErrorIs(t, err, ErrTooSmall)
			if tt.name == "stack refuses overflow" {
				require.Equal(t, PaneID("b"), tr.Focus)
				require.Equal(t, []PaneID{"a", "b"}, leafIDs(tr.Root))
				return
			}
			require.Equal(t, before, tr)
		})
	}
}

func TestTreeSurgeryShapes(t *testing.T) {
	t.Parallel()

	t.Run("split preserves and dissolves containers", func(t *testing.T) {
		tr := NewTree("a")
		require.NoError(t, tr.Split("a", Right, true, "b", domain.Rect{Width: 41, Height: 2}))
		require.Equal(t, Split, tr.Root.Kind)
		require.Equal(t, Horizontal, tr.Root.Dir)
		require.Equal(t, []PaneID{"a", "b"}, leafIDs(tr.Root))
		require.NoError(t, tr.Close("a"))
		require.Equal(t, Leaf, tr.Root.Kind)
		require.Equal(t, PaneID("b"), tr.Root.Leaf)
	})

	t.Run("nested split inserts on matching axis", func(t *testing.T) {
		tr := NewTree("a")
		require.NoError(t, tr.Split("a", Right, true, "b", domain.Rect{Width: 41, Height: 4}))
		require.NoError(t, tr.Split("b", Right, true, "c", domain.Rect{Width: 62, Height: 4}))
		require.Len(t, tr.Root.Children, 3)
		require.Equal(t, []PaneID{"a", "b", "c"}, leafIDs(tr.Root))
		require.NoError(t, tr.Split("b", Down, true, "d", domain.Rect{Width: 62, Height: 5}))
		require.Equal(t, Vertical, tr.Root.Children[1].Dir)
		require.Equal(t, []PaneID{"a", "b", "d", "c"}, leafIDs(tr.Root))
	})
}

func TestFocusDir(t *testing.T) {
	t.Parallel()
	area := domain.Rect{Width: 62, Height: 5}
	tests := []struct {
		name  string
		focus PaneID
		dir   Direction
		want  PaneID
	}{
		{"right chooses overlapping neighbor", "a", Right, "b"},
		{"left chooses overlapping neighbor", "b", Left, "a"},
		{"down chooses vertical neighbor", "b", Down, "c"},
		{"up chooses vertical neighbor", "c", Up, "b"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Focus: tt.focus, Root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				NewLeaf("a"),
				{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("b"), NewLeaf("c")}},
			}}}
			require.NoError(t, tr.FocusDir(tt.dir, area))
			require.Equal(t, tt.want, tr.Focus)
		})
	}

	t.Run("stack local vertical focus expands member", func(t *testing.T) {
		tr := &Tree{Focus: "b", Root: &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b"), NewLeaf("c")}, Expanded: "b"}}
		require.NoError(t, tr.FocusDir(Down, domain.Rect{Width: 20, Height: 4}))
		require.Equal(t, PaneID("c"), tr.Focus)
		require.Equal(t, PaneID("c"), tr.Root.Expanded)
		require.NoError(t, tr.FocusDir(Up, domain.Rect{Width: 20, Height: 4}))
		require.Equal(t, PaneID("b"), tr.Focus)
	})

	t.Run("no pane in direction", func(t *testing.T) {
		tr := NewTree("a")
		err := tr.FocusDir(Left, domain.Rect{Width: 20, Height: 2})
		require.True(t, errors.Is(err, ErrNoPane))
	})
}

func contents(ps []Placement) map[PaneID]domain.Rect {
	out := make(map[PaneID]domain.Rect, len(ps))
	for _, p := range ps {
		out[p.ID] = p.Content
	}
	return out
}
