package layout

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestFocusSpan(t *testing.T) {
	t.Parallel()

	area := domain.Rect{X: 5, Y: 7, Width: 41, Height: 5}
	tests := []struct {
		name    string
		tree    *Tree
		area    domain.Rect
		want    domain.Rect
		wantErr error
	}{
		{
			name:    "nil root",
			tree:    &Tree{},
			area:    area,
			wantErr: ErrNotFound,
		},
		{
			name:    "layout too small",
			tree:    NewTree("a"),
			area:    domain.Rect{Width: MinPaneCols - 1, Height: MinPaneRows},
			wantErr: ErrTooSmall,
		},
		{
			name:    "focused pane unknown",
			tree:    &Tree{Root: NewLeaf("a"), Focus: "missing"},
			area:    area,
			wantErr: ErrNotFound,
		},
		{
			name: "returns collapsed stack title bar",
			tree: &Tree{
				Root:  &Node{Kind: Stack, Children: []*Node{NewLeaf("a"), NewLeaf("b")}, Expanded: "a"},
				Focus: "b",
			},
			area: area,
			want: domain.Rect{X: 5, Y: 11, Width: 41, Height: 1},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.tree.FocusSpan(tt.area)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
		})
	}
}

func TestEntryPane(t *testing.T) {
	t.Parallel()

	area := domain.Rect{Width: 41, Height: 5}
	tests := []struct {
		name    string
		tree    *Tree
		dir     Direction
		span    domain.Rect
		area    domain.Rect
		want    PaneID
		wantErr error
	}{
		{
			name:    "nil root",
			tree:    &Tree{},
			dir:     Right,
			area:    area,
			wantErr: ErrNotFound,
		},
		{
			name:    "layout too small",
			tree:    NewTree("a"),
			dir:     Right,
			area:    domain.Rect{Width: MinPaneCols - 1, Height: MinPaneRows},
			wantErr: ErrTooSmall,
		},
		{
			name:    "unknown direction has no pane",
			tree:    NewTree("a"),
			dir:     Direction(99),
			area:    area,
			wantErr: ErrNoPane,
		},
		{
			name: "right enters left edge",
			tree: &Tree{Root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{NewLeaf("left"), NewLeaf("right")}}, Focus: "right"},
			dir:  Right,
			span: domain.Rect{Y: 1, Width: 20, Height: 2},
			area: area,
			want: "left",
		},
		{
			name: "left enters right edge",
			tree: &Tree{Root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{NewLeaf("left"), NewLeaf("right")}}, Focus: "left"},
			dir:  Left,
			span: domain.Rect{Y: 1, Width: 20, Height: 2},
			area: area,
			want: "right",
		},
		{
			name: "down enters top edge",
			tree: &Tree{Root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("top"), NewLeaf("bottom")}}, Focus: "bottom"},
			dir:  Down,
			span: domain.Rect{X: 5, Width: 10, Height: 2},
			area: area,
			want: "top",
		},
		{
			name: "up enters bottom edge",
			tree: &Tree{Root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("top"), NewLeaf("bottom")}}, Focus: "top"},
			dir:  Up,
			span: domain.Rect{X: 5, Width: 10, Height: 2},
			area: area,
			want: "bottom",
		},
		{
			name: "minimum edge gap precedes overlap",
			tree: &Tree{Root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("edge-top"), NewLeaf("edge-bottom")}},
				NewLeaf("far"),
			}}},
			dir:  Right,
			span: domain.Rect{Y: 1, Width: 20, Height: 4},
			area: domain.Rect{Width: 62, Height: 5},
			want: "edge-bottom",
		},
		{
			name: "maximum perpendicular overlap breaks equal gap",
			tree: &Tree{Root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{NewLeaf("top"), NewLeaf("bottom")}}},
			dir:  Right,
			span: domain.Rect{Y: 3, Width: 20, Height: 2},
			area: domain.Rect{Width: 20, Height: 5},
			want: "bottom",
		},
		{
			name: "pane id deterministically breaks full tie",
			tree: &Tree{Root: &Node{Kind: Stack, Children: []*Node{NewLeaf("z"), NewLeaf("a")}, Expanded: "z"}},
			dir:  Right,
			span: domain.Rect{Y: 10, Width: 20, Height: 1},
			area: domain.Rect{Width: 20, Height: 4},
			want: "a",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := tt.tree.Clone()
			got, err := tt.tree.EntryPane(tt.dir, tt.span, tt.area)
			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, tt.want, got)
			require.Equal(t, before, tt.tree, "EntryPane must be pure")
		})
	}
}

func TestFocusEnterExpandsCollapsedStackMember(t *testing.T) {
	t.Parallel()

	area := domain.Rect{Width: 20, Height: 4}
	tr := &Tree{
		Root:  &Node{Kind: Stack, Children: []*Node{NewLeaf("expanded"), NewLeaf("collapsed")}, Expanded: "expanded"},
		Focus: "expanded",
	}

	err := tr.FocusEnter(Right, domain.Rect{Y: 3, Width: 20, Height: 1}, area)
	require.NoError(t, err)
	require.Equal(t, PaneID("collapsed"), tr.Focus)
	require.Equal(t, PaneID("collapsed"), tr.Root.Expanded)
}
