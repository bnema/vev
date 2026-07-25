package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

// TestComposeFrameDividerFollowsWeights proves the rendered divider glyph
// tracks the weight-aware pane geometry from layout.Solve, rather than an
// equal-split recomputation. On a 61-wide area, equal weights place the
// divider at x=30; a 2:1 weight split moves the real gap to x=40. The
// divider must move with it and must not linger at the old column.
func TestComposeFrameDividerFollowsWeights(t *testing.T) {
	root := &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		{Kind: layout.Leaf, Leaf: "left", Weight: 1},
		{Kind: layout.Leaf, Leaf: "right", Weight: 1},
	}}
	area := domain.Rect{Width: 61, Height: 5}

	stateFor := func() capturedRenderState {
		placements, dividers, ok := layout.SolveWithDividers(root, area)
		require.True(t, ok)
		panes := make([]capturedPaneRenderState, 0, len(placements))
		for _, placement := range placements {
			frame := cachePaneFrame(placement.Content.Width, placement.Content.Height, rune(placement.ID[0]))
			panes = append(panes, capturedPaneRenderState{
				id: placement.ID, frame: frame, placement: placement, focused: placement.ID == "left",
				damage: []renderer.Damage{renderer.FullRedraw()},
			})
		}
		return capturedRenderState{
			reset:  true,
			layout: capturedTabLayout{root: root, area: area, focus: "left", placements: placements, dividers: dividers, fingerprint: layoutFingerprint(root), valid: true},
			panes:  panes, styles: resolveStyles(nil),
		}
	}

	equal := stateFor()
	out := composeFrame(equal, composeCacheInput{})
	require.Equal(t, '│', out.frame.At(30, 1).Rune, "equal weights must place the divider at the equal-split column")

	root.Children[0].Weight = 2
	weighted := stateFor()
	require.NotEqual(t, equal.layout.fingerprint, weighted.layout.fingerprint)
	require.Equal(t, 41, weighted.layout.placements[1].Content.X, "sanity check: left pane is 40 wide, so the right pane starts at x=41")

	out = composeFrame(weighted, out.cache, composeCacheInput{})
	require.Equal(t, '│', out.frame.At(40, 1).Rune, "the divider must follow the weight-derived gap")
	require.NotEqual(t, '│', out.frame.At(30, 1).Rune, "the divider must not linger at the old equal-split column")
}
