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

func TestRepeatedSplitHalvesFocusedPane(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		dir    Direction
		area   domain.Rect
		first  map[PaneID]domain.Rect
		second map[PaneID]domain.Rect
	}{
		{
			name: "horizontal",
			dir:  Right,
			area: domain.Rect{Width: 123, Height: 5},
			first: map[PaneID]domain.Rect{
				"a": {Width: 61, Height: 5},
				"b": {X: 62, Width: 61, Height: 5},
			},
			second: map[PaneID]domain.Rect{
				"a": {Width: 61, Height: 5},
				"b": {X: 62, Width: 30, Height: 5},
				"c": {X: 93, Width: 30, Height: 5},
			},
		},
		{
			name: "vertical",
			dir:  Down,
			area: domain.Rect{Width: 40, Height: 15},
			first: map[PaneID]domain.Rect{
				"a": {Width: 40, Height: 7},
				"b": {Y: 8, Width: 40, Height: 7},
			},
			second: map[PaneID]domain.Rect{
				"a": {Width: 40, Height: 7},
				"b": {Y: 8, Width: 40, Height: 3},
				"c": {Y: 12, Width: 40, Height: 3},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := NewTree("a")
			require.NoError(t, tr.Split("a", tt.dir, true, "b", tt.area))
			first, ok := Solve(tr.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.first, contents(first))

			require.NoError(t, tr.Split("b", tt.dir, true, "c", tt.area))
			second, ok := Solve(tr.Root, tt.area)
			require.True(t, ok)
			require.Equal(t, tt.second, contents(second))
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
		{ID: "b", Content: domain.Rect{Y: 1, Width: 20, Height: 2}, InStack: true},
		{ID: "c", TitleBar: domain.Rect{Y: 3, Width: 20, Height: 1}, Collapsed: true, InStack: true},
	}, got)

	_, ok = Solve(stack, domain.Rect{Width: 20, Height: 3})
	require.False(t, ok)
}

func TestSolveStackExpandedMemberHasNoTitleBar(t *testing.T) {
	t.Parallel()
	area := domain.Rect{X: 5, Y: 2, Width: 20, Height: 5}
	tests := []struct {
		name     string
		expanded PaneID
		want     []Placement
	}{
		{
			name:     "edge member expanded",
			expanded: "a",
			want: []Placement{
				{ID: "a", Content: domain.Rect{X: 5, Y: 2, Width: 20, Height: 3}, InStack: true},
				{ID: "b", TitleBar: domain.Rect{X: 5, Y: 5, Width: 20, Height: 1}, Collapsed: true, InStack: true},
				{ID: "c", TitleBar: domain.Rect{X: 5, Y: 6, Width: 20, Height: 1}, Collapsed: true, InStack: true},
			},
		},
		{
			name:     "middle member expanded",
			expanded: "b",
			want: []Placement{
				{ID: "a", TitleBar: domain.Rect{X: 5, Y: 2, Width: 20, Height: 1}, Collapsed: true, InStack: true},
				{ID: "b", Content: domain.Rect{X: 5, Y: 3, Width: 20, Height: 3}, InStack: true},
				{ID: "c", TitleBar: domain.Rect{X: 5, Y: 6, Width: 20, Height: 1}, Collapsed: true, InStack: true},
			},
		},
		{
			name:     "other edge member expanded",
			expanded: "c",
			want: []Placement{
				{ID: "a", TitleBar: domain.Rect{X: 5, Y: 2, Width: 20, Height: 1}, Collapsed: true, InStack: true},
				{ID: "b", TitleBar: domain.Rect{X: 5, Y: 3, Width: 20, Height: 1}, Collapsed: true, InStack: true},
				{ID: "c", Content: domain.Rect{X: 5, Y: 4, Width: 20, Height: 3}, InStack: true},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stack := &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b"), NewLeaf("c")}, Expanded: tt.expanded}
			got, ok := Solve(stack, area)
			require.True(t, ok)
			require.Equal(t, tt.want, got)

			// Invariant: total consumed rows equal area.Height, and rows are
			// contiguous and non-overlapping across title bars and content.
			rows := make(map[int]PaneID, area.Height)
			for _, p := range got {
				if p.TitleBar.Height > 0 {
					require.Zero(t, p.Content.Height, "expanded member must not have both a title bar and content")
					for y := p.TitleBar.Y; y < p.TitleBar.Y+p.TitleBar.Height; y++ {
						_, dup := rows[y]
						require.False(t, dup, "row %d already occupied", y)
						rows[y] = p.ID
					}
				} else {
					require.Equal(t, tt.expanded, p.ID, "only the expanded member may omit its title bar")
					for y := p.Content.Y; y < p.Content.Y+p.Content.Height; y++ {
						_, dup := rows[y]
						require.False(t, dup, "row %d already occupied", y)
						rows[y] = p.ID
					}
				}
			}
			require.Len(t, rows, area.Height)
			for y := area.Y; y < area.Y+area.Height; y++ {
				_, ok := rows[y]
				require.True(t, ok, "row %d not covered", y)
			}
		})
	}
}

func TestSplitStackedLeafSplitsWholeStack(t *testing.T) {
	t.Parallel()
	tr := &Tree{
		Root:  &Node{Kind: Stack, Children: []*Node{NewLeaf("one"), NewLeaf("two")}, Expanded: "two"},
		Focus: "two",
	}
	area := domain.Rect{Width: 41, Height: 4}

	require.NoError(t, tr.Split("two", Right, true, "three", area))
	placements, ok := Solve(tr.Root, area)
	require.True(t, ok)
	require.Equal(t, PaneID("three"), tr.Focus)
	require.Equal(t, domain.Rect{Y: 1, Width: 20, Height: 3}, contents(placements)["two"], "existing stack should remain visible on the left")
	require.Equal(t, domain.Rect{X: 21, Width: 20, Height: 4}, contents(placements)["three"], "new split pane should appear to the right of the stack")
}

func TestStackNewEmptyTreeReturnsError(t *testing.T) {
	t.Parallel()
	tr := &Tree{}
	require.ErrorIs(t, tr.StackNew("missing", "new", domain.Rect{Width: 20, Height: 2}), ErrNotFound)
	require.Nil(t, tr.Root)
	require.Empty(t, tr.Focus)
}

func TestSplitEmptyTreeReturnsError(t *testing.T) {
	t.Parallel()
	tr := &Tree{}
	require.ErrorIs(t, tr.Split("missing", Right, true, "new", domain.Rect{Width: 41, Height: 2}), ErrNotFound)
	require.Nil(t, tr.Root)
	require.Empty(t, tr.Focus)
}

func TestNilTreeCloneIsSafe(t *testing.T) {
	t.Parallel()
	var tr *Tree
	require.Nil(t, tr.clone())
	require.Nil(t, tr.Clone())
}

func TestTreeCloneReturnsIndependentCopy(t *testing.T) {
	t.Parallel()
	tr := &Tree{
		Root: &Node{
			Kind: Split,
			Dir:  Horizontal,
			Children: []*Node{
				NewLeaf("a"),
				NewLeaf("b"),
			},
		},
		Focus: "b",
	}

	clone := tr.Clone()
	require.Equal(t, tr, clone)

	clone.Root.Children[0].Leaf = "changed"
	clone.Focus = "changed"
	require.Equal(t, PaneID("a"), tr.Root.Children[0].Leaf)
	require.Equal(t, PaneID("b"), tr.Focus)
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

func TestToggleStackNestedShapes(t *testing.T) {
	t.Parallel()
	area := domain.Rect{Width: 62, Height: 5}
	tests := []struct {
		name       string
		root       *Node
		target     PaneID
		wantKind   Kind
		wantDir    SplitDir
		wantErr    error
		wantLeaves []PaneID
	}{
		{
			name: "nested stack toggles directly containing stack",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				NewLeaf("a"),
				{Kind: Stack, Children: []*Node{NewLeaf("b"), NewLeaf("c")}, Expanded: "b"},
			}},
			target:     "b",
			wantKind:   Split,
			wantDir:    Vertical,
			wantLeaves: []PaneID{"b", "c"},
		},
		{
			name: "nested split toggles directly containing two-leaf split",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				NewLeaf("a"),
				{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("b"), NewLeaf("c")}},
			}},
			target:     "b",
			wantKind:   Stack,
			wantLeaves: []PaneID{"b", "c"},
		},
		{
			name: "nested non toggleable split reports not toggleable",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				NewLeaf("a"),
				{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("b"), NewLeaf("c"), NewLeaf("d")}},
			}},
			target:  "b",
			wantErr: ErrNotToggleable,
		},
		{
			name: "three leaf split reports not toggleable not too small",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				NewLeaf("a"), NewLeaf("b"), NewLeaf("c"),
			}},
			target:  "b",
			wantErr: ErrNotToggleable,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Root: tt.root, Focus: tt.target}
			err := tr.ToggleStack(tt.target, area)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				require.NotErrorIs(t, err, ErrTooSmall)
				return
			}
			require.NoError(t, err)
			container := tr.Root.Children[1]
			require.Equal(t, tt.wantKind, container.Kind)
			if tt.wantKind == Split {
				require.Equal(t, tt.wantDir, container.Dir)
			}
			require.Equal(t, tt.wantLeaves, LeafIDs(container))
		})
	}
}

func TestCloseRefocusPrefersSibling(t *testing.T) {
	t.Parallel()
	tr := &Tree{Focus: "c", Root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
		NewLeaf("a"),
		{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("b"), NewLeaf("c")}},
		NewLeaf("d"),
	}}}
	require.NoError(t, tr.Close("c"))
	require.Equal(t, PaneID("b"), tr.Focus)
}

func TestCloseNotFoundDoesNotMutate(t *testing.T) {
	t.Parallel()
	tr := &Tree{Focus: "b", Root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
		NewLeaf("a"),
		{Kind: Stack, Children: []*Node{NewLeaf("b"), NewLeaf("c")}, Expanded: "b"},
	}}}
	before := tr.clone()
	require.ErrorIs(t, tr.Close("missing"), ErrNotFound)
	require.Equal(t, before, tr)
}

func TestSolveRejectsStackWithMissingExpanded(t *testing.T) {
	t.Parallel()
	_, ok := Solve(&Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b")}, Expanded: "missing"}, domain.Rect{Width: 20, Height: 4})
	require.False(t, ok)
}

func TestExportedLeafHelpers(t *testing.T) {
	t.Parallel()
	root := &Node{Kind: Split, Dir: Horizontal, Children: []*Node{NewLeaf("a"), NewLeaf("b")}}
	require.True(t, ContainsLeaf(root, "b"))
	require.False(t, ContainsLeaf(root, "c"))
	require.Equal(t, []PaneID{"a", "b"}, LeafIDs(root))
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
		require.Equal(t, PaneID("b"), tr.Root.Expanded)
	})

	t.Run("no pane in direction", func(t *testing.T) {
		tr := NewTree("a")
		err := tr.FocusDir(Left, domain.Rect{Width: 20, Height: 2})
		require.True(t, errors.Is(err, ErrNoPane))
	})
}

func TestFocusDirEnteringStackExpandsMember(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		root  *Node
		area  domain.Rect
		focus PaneID
		dir   Direction
		want  PaneID
	}{
		{
			name:  "from left",
			area:  domain.Rect{Width: 62, Height: 8},
			focus: "a",
			dir:   Right,
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("x"), NewLeaf("a")}},
				{Kind: Stack, Children: []*Node{NewLeaf("b"), NewLeaf("c"), NewLeaf("d"), NewLeaf("e")}, Expanded: "b"},
			}},
			want: "c",
		},
		{
			name:  "from right",
			area:  domain.Rect{Width: 62, Height: 8},
			focus: "a",
			dir:   Left,
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Stack, Children: []*Node{NewLeaf("b"), NewLeaf("c"), NewLeaf("d"), NewLeaf("e")}, Expanded: "b"},
				{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("x"), NewLeaf("a")}},
			}},
			want: "c",
		},
		{
			name:  "from above",
			area:  domain.Rect{Width: 41, Height: 7},
			focus: "a",
			dir:   Down,
			root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{
				NewLeaf("a"),
				{Kind: Stack, Children: []*Node{NewLeaf("b"), NewLeaf("c")}, Expanded: "c"},
			}},
			want: "b",
		},
		{
			name:  "from below",
			area:  domain.Rect{Width: 41, Height: 7},
			focus: "a",
			dir:   Up,
			root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{
				{Kind: Stack, Children: []*Node{NewLeaf("b"), NewLeaf("c")}, Expanded: "b"},
				NewLeaf("a"),
			}},
			want: "c",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr := &Tree{Root: tt.root, Focus: tt.focus}
			var stack *Node
			findStack(tr.Root, &stack)
			require.NotEqual(t, tt.want, stack.Expanded)
			require.NoError(t, tr.FocusDir(tt.dir, tt.area))
			require.Equal(t, tt.want, tr.Focus)
			require.Equal(t, tt.want, stack.Expanded)
		})
	}
}

func TestFocusDirStackLocalSkipsNonLeafChildren(t *testing.T) {
	t.Parallel()
	tr := &Tree{
		Root: &Node{Kind: Stack, Children: []*Node{
			NewLeaf("a"),
			{Kind: Split, Dir: Horizontal, Children: []*Node{NewLeaf("nested")}},
			NewLeaf("b"),
		}, Expanded: "a"},
		Focus: "a",
	}

	require.NoError(t, tr.FocusDir(Down, domain.Rect{Width: 80, Height: 6}))
	require.Equal(t, PaneID("b"), tr.Focus)
	require.Equal(t, PaneID("b"), tr.Root.Expanded)
}

func contents(ps []Placement) map[PaneID]domain.Rect {
	out := make(map[PaneID]domain.Rect, len(ps))
	for _, p := range ps {
		out[p.ID] = p.Content
	}
	return out
}

func findStack(n *Node, out **Node) {
	if n == nil || *out != nil {
		return
	}
	if n.Kind == Stack {
		*out = n
		return
	}
	for _, child := range n.Children {
		findStack(child, out)
	}
}
