package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

func backdropTheme() themeui.Theme {
	return themeui.Theme{Known: true, TrueColor: true, HasFG: true, HasBG: true, Foreground: renderer.RGB{R: 220, G: 220, B: 220}, Background: renderer.RGB{R: 10, G: 10, B: 10}}
}

func TestOverlayBackdropDimsOnlyTranslatedPaneContents(t *testing.T) {
	frame := renderer.NewFrame(8, 6)
	original := renderer.Cell{Rune: '界', Style: renderer.DefaultStyle()}
	frame.Set(2, 2, original)
	frame.Set(3, 2, renderer.Cell{Continuation: true, Style: renderer.DefaultStyle()})
	frame.Set(2, 1, renderer.Cell{Rune: 'T', Style: renderer.DefaultStyle()}) // title
	frame.Set(0, 0, renderer.Cell{Rune: 'B', Style: renderer.DefaultStyle()}) // top bar
	frame.Set(0, 5, renderer.Cell{Rune: 'S', Style: renderer.DefaultStyle()}) // bottom bar
	frame.Set(6, 3, renderer.Cell{Rune: 'C', Style: renderer.DefaultStyle()}) // collapsed

	snap := tabLayoutSnapshot{ok: true, placements: []layout.Placement{
		{ID: "visible", TitleBar: domain.Rect{X: 1, Y: 0, Width: 4, Height: 1}, Content: domain.Rect{X: 1, Y: 1, Width: 4, Height: 2}},
		{ID: "collapsed", Collapsed: true, Content: domain.Rect{X: 6, Y: 2, Width: 1, Height: 1}},
	}}
	(overlayBackdrop{DimPaneContents: true}).apply(frame, domain.Rect{Y: 1, Width: 8, Height: 4}, snap, backdropTheme())

	require.Equal(t, '界', frame.At(2, 2).Rune, "content is translated by the content-area origin")
	require.True(t, frame.At(2, 2).Style.HasForegroundRGB)
	require.True(t, frame.At(3, 2).Continuation, "continuation markers must survive dimming")
	require.Zero(t, frame.At(3, 2).Rune)
	require.False(t, frame.At(2, 1).Style.HasForegroundRGB, "pane title must remain crisp")
	require.False(t, frame.At(0, 0).Style.HasForegroundRGB, "top bar must remain crisp")
	require.False(t, frame.At(0, 5).Style.HasForegroundRGB, "bottom bar must remain crisp")
	require.False(t, frame.At(6, 3).Style.HasForegroundRGB, "collapsed pane content must not be dimmed")
}

func TestOverlayBackdropClipsPlacementsToFrame(t *testing.T) {
	tests := []struct {
		name       string
		content    domain.Rect
		affected   [][2]int
		unaffected [][2]int
	}{
		{
			name:       "negative origin",
			content:    domain.Rect{X: -1, Y: -1, Width: 3, Height: 3},
			affected:   [][2]int{{0, 0}, {1, 1}},
			unaffected: [][2]int{{2, 2}, {3, 3}},
		},
		{
			name:       "right and bottom overflow",
			content:    domain.Rect{X: 2, Y: 2, Width: 4, Height: 4},
			affected:   [][2]int{{2, 2}, {3, 3}},
			unaffected: [][2]int{{0, 0}, {1, 1}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			frame := renderer.NewFrame(4, 4)
			original := renderer.DefaultStyle()
			snap := tabLayoutSnapshot{ok: true, placements: []layout.Placement{{Content: tt.content}}}

			(overlayBackdrop{DimPaneContents: true}).apply(frame, domain.Rect{Width: 4, Height: 4}, snap, backdropTheme())

			for _, point := range tt.affected {
				require.Equal(t, themeui.NewDimmer(backdropTheme()).Dim(original), frame.At(point[0], point[1]).Style)
			}
			for _, point := range tt.unaffected {
				require.Equal(t, original, frame.At(point[0], point[1]).Style)
			}
		})
	}
}

func TestOverlayBackdropDisabledAndFallback(t *testing.T) {
	frame := renderer.NewFrame(4, 4)
	frame.Set(1, 1, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	before := frame.Clone()
	area := domain.Rect{Y: 1, Width: 4, Height: 2}
	(overlayBackdrop{}).apply(frame, area, tabLayoutSnapshot{}, backdropTheme())
	require.True(t, sameCells(before.Cells, frame.Cells), "backdrops are disabled by default")

	(overlayBackdrop{DimPaneContents: true}).apply(frame, area, tabLayoutSnapshot{}, backdropTheme())
	require.True(t, frame.At(0, 1).Style.HasForegroundRGB, "failed layout dims the focused-pane fallback area")
	require.True(t, frame.At(3, 2).Style.HasForegroundRGB)
	require.False(t, frame.At(0, 0).Style.HasForegroundRGB)
	require.False(t, frame.At(0, 3).Style.HasForegroundRGB)
}
