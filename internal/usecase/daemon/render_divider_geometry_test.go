package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

// TestComposeFrameDividerFollowsWeights proves rendered divider glyphs track
// weight-aware pane geometry rather than an equal-split recomputation.
func TestComposeFrameDividerFollowsWeights(t *testing.T) {
	tests := []struct {
		name                 string
		direction            layout.SplitDir
		area                 domain.Rect
		glyph                rune
		equalX, equalY       int
		weightedX, weightedY int
	}{
		{
			name:      "horizontal split",
			direction: layout.Horizontal,
			area:      domain.Rect{Width: 61, Height: 5},
			glyph:     '│',
			equalX:    30,
			equalY:    1,
			weightedX: 40,
			weightedY: 1,
		},
		{
			name:      "vertical split",
			direction: layout.Vertical,
			area:      domain.Rect{Width: 41, Height: 10},
			glyph:     '─',
			equalX:    1,
			equalY:    6,
			weightedX: 1,
			weightedY: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			root := &layout.Node{Kind: layout.Split, Dir: tt.direction, Children: []*layout.Node{
				{Kind: layout.Leaf, Leaf: "first", Weight: 1},
				{Kind: layout.Leaf, Leaf: "second", Weight: 1},
			}}
			stateFor := func() capturedRenderState {
				placements, dividers, ok := layout.SolveWithDividers(root, tt.area)
				require.True(t, ok)
				panes := make([]capturedPaneRenderState, 0, len(placements))
				for _, placement := range placements {
					frame := cachePaneFrame(placement.Content.Width, placement.Content.Height, rune(placement.ID[0]))
					panes = append(panes, capturedPaneRenderState{
						id: placement.ID, frame: frame, placement: placement, focused: placement.ID == "first",
						damage: []renderer.Damage{renderer.FullRedraw()},
					})
				}
				return capturedRenderState{
					reset:  true,
					layout: capturedTabLayout{area: tt.area, focus: "first", placements: placements, dividers: dividers, fingerprint: layoutFingerprint(root), valid: true},
					panes:  panes, styles: resolveStyles(nil),
				}
			}

			equal := stateFor()
			out := composeFrame(equal, composeCacheInput{})
			require.Equal(t, tt.glyph, out.frame.At(tt.equalX, tt.equalY).Rune, "equal weights must place the divider at the equal-split gap")

			root.Children[0].Weight = 2
			weighted := stateFor()
			require.NotEqual(t, equal.layout.fingerprint, weighted.layout.fingerprint)

			out = composeFrame(weighted, out.cache, composeCacheInput{})
			require.Equal(t, tt.glyph, out.frame.At(tt.weightedX, tt.weightedY).Rune, "the divider must follow the weight-derived gap")
			require.NotEqual(t, tt.glyph, out.frame.At(tt.equalX, tt.equalY).Rune, "the divider must not linger at the old equal-split gap")
		})
	}
}

// TestComposeFrameClearsCellsOutsideSharedLayout pins that a window larger
// than the shared session layout blanks the cells the layout no longer covers
// instead of keeping a wider earlier layout's cells on screen.
func TestComposeFrameClearsCellsOutsideSharedLayout(t *testing.T) {
	tests := []struct {
		name       string
		wide, next domain.Rect
		x, y       int // stale content cell outside next, frame coordinates
	}{
		{name: "narrower layout clears the right side", wide: domain.Rect{Width: 6, Height: 2}, next: domain.Rect{Width: 4, Height: 2}, x: 5, y: 1},
		{name: "shorter layout clears the bottom rows", wide: domain.Rect{Width: 6, Height: 2}, next: domain.Rect{Width: 6, Height: 1}, x: 0, y: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stateFor := func(area domain.Rect, fingerprint string) capturedRenderState {
				pane := renderer.NewFrame(area.Width, area.Height)
				for y := range area.Height {
					for x := range area.Width {
						pane.Set(x, y, renderer.Cell{Rune: 'X', Style: renderer.DefaultStyle()})
					}
				}
				placement := layout.Placement{ID: "p", Content: area}
				return capturedRenderState{
					window: domain.Size{Cols: 6, Rows: 2 + tabChromeRows},
					layout: capturedTabLayout{area: area, focus: "p", valid: true, fingerprint: fingerprint, placements: []layout.Placement{placement}},
					panes:  []capturedPaneRenderState{{id: "p", frame: pane, focused: true, placement: placement, damage: []renderer.Damage{renderer.FullRedraw()}}},
				}
			}
			committed := composeFrame(stateFor(tt.wide, "wide"), composeCacheInput{})
			require.Equal(t, 'X', committed.frame.At(tt.x, tt.y).Rune)

			out := composeFrame(stateFor(tt.next, "next"), committed.cache, composeCacheInput{})
			require.Equal(t, ' ', out.frame.At(tt.x, tt.y).Rune, "cell outside the shared layout must be blanked")
		})
	}
}
