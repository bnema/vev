package layout

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func rectsOverlap(a, b domain.Rect) bool {
	if a.Width <= 0 || a.Height <= 0 || b.Width <= 0 || b.Height <= 0 {
		return false
	}
	return a.X < b.X+b.Width && b.X < a.X+a.Width && a.Y < b.Y+b.Height && b.Y < a.Y+a.Height
}

func TestDividersMatchSolvedGaps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		root     *Node
		area     domain.Rect
		expected []Divider
	}{
		{
			name: "equal weights",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 1},
				{Kind: Leaf, Leaf: "b", Weight: 1},
			}},
			area:     domain.Rect{Width: 61, Height: 10},
			expected: []Divider{{Rect: domain.Rect{X: 30, Width: 1, Height: 10}, Dir: Horizontal}},
		},
		{
			name: "unequal weights 2:1",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 2},
				{Kind: Leaf, Leaf: "b", Weight: 1},
			}},
			area:     domain.Rect{Width: 61, Height: 10},
			expected: []Divider{{Rect: domain.Rect{X: 40, Width: 1, Height: 10}, Dir: Horizontal}},
		},
		{
			name: "unequal weights 1:3",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 1},
				{Kind: Leaf, Leaf: "b", Weight: 3},
			}},
			area:     domain.Rect{Width: 81, Height: 10},
			expected: []Divider{{Rect: domain.Rect{X: 20, Width: 1, Height: 10}, Dir: Horizontal}},
		},
		{
			name: "nested split",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 1},
				{Kind: Split, Dir: Vertical, Weight: 1, Children: []*Node{
					{Kind: Leaf, Leaf: "b", Weight: 1},
					{Kind: Leaf, Leaf: "c", Weight: 1},
				}},
			}},
			area: domain.Rect{Width: 61, Height: 20},
			expected: []Divider{
				{Rect: domain.Rect{X: 30, Width: 1, Height: 20}, Dir: Horizontal},
				{Rect: domain.Rect{X: 31, Y: 10, Width: 30, Height: 1}, Dir: Vertical},
			},
		},
		{
			name: "three or more children",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 1},
				{Kind: Leaf, Leaf: "b", Weight: 1},
				{Kind: Leaf, Leaf: "c", Weight: 1},
			}},
			area: domain.Rect{Width: 62, Height: 10},
			expected: []Divider{
				{Rect: domain.Rect{X: 20, Width: 1, Height: 10}, Dir: Horizontal},
				{Rect: domain.Rect{X: 41, Width: 1, Height: 10}, Dir: Horizontal},
			},
		},
		{
			name: "minimum pins a child",
			root: &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 1},
				{Kind: Leaf, Leaf: "b", Weight: 100},
			}},
			area:     domain.Rect{Width: 61, Height: 10},
			expected: []Divider{{Rect: domain.Rect{X: 20, Width: 1, Height: 10}, Dir: Horizontal}},
		},
		{
			name: "vertical split",
			root: &Node{Kind: Split, Dir: Vertical, Children: []*Node{
				{Kind: Leaf, Leaf: "a", Weight: 1},
				{Kind: Leaf, Leaf: "b", Weight: 2},
			}},
			area:     domain.Rect{Width: 41, Height: 10},
			expected: []Divider{{Rect: domain.Rect{Y: 3, Width: 41, Height: 1}, Dir: Vertical}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			solved, solveOK := Solve(tt.root, tt.area)
			placements, dividers, dividersOK := SolveWithDividers(tt.root, tt.area)
			require.Equal(t, solveOK, dividersOK, "SolveWithDividers must succeed/fail exactly when Solve does")
			require.True(t, solveOK)
			require.Equal(t, solved, placements, "collecting dividers must not change the solved placements")

			require.Equal(t, tt.expected, dividers, "divider geometry and direction must match the solved gaps exactly")

			for _, d := range dividers {
				require.True(t, d.Rect.Width == 1 || d.Rect.Height == 1, "divider rect must be a single row or column")
				for _, p := range placements {
					require.False(t, rectsOverlap(d.Rect, p.Content), "divider %+v overlaps pane %s content %+v", d.Rect, p.ID, p.Content)
				}
			}
		})
	}
}

func TestDividersFailsLikeSolve(t *testing.T) {
	t.Parallel()

	root := &Node{Kind: Split, Dir: Horizontal, Children: []*Node{
		{Kind: Leaf, Leaf: "a", Weight: 1},
		{Kind: Leaf, Leaf: "b", Weight: 1},
	}}
	area := domain.Rect{Width: MinPaneCols, Height: MinPaneRows}

	_, solveOK := Solve(root, area)
	require.False(t, solveOK)

	_, _, dividersOK := SolveWithDividers(root, area)
	require.False(t, dividersOK)
}

func TestDividersNilRoot(t *testing.T) {
	t.Parallel()

	placements, dividers, ok := SolveWithDividers(nil, domain.Rect{Width: 40, Height: 10})
	require.False(t, ok)
	require.Nil(t, placements)
	require.Nil(t, dividers)
}
