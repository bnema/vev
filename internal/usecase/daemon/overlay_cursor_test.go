package daemon

import (
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/stretchr/testify/require"
)

func TestOverlayInputCursor(t *testing.T) {
	inner := domain.Rect{X: 3, Y: 2, Width: 10, Height: 4}
	withCaret := func(col int) capturedModal {
		return capturedModal{active: true, presentation: ui.Presentation{Inner: inner}, caretCol: col, hasCaret: true}
	}
	notices := capturedModal{active: true, presentation: ui.Presentation{Inner: inner}}
	bar := cursorOut{valid: true, row: 5, col: 5, style: 6, hasStyle: true}
	for _, tt := range []struct {
		name     string
		overlays capturedOverlayRenderState
		pane     cursorOut
		want     cursorOut
	}{
		{
			name:     "palette caret owns the cursor and keeps the pane shape",
			overlays: capturedOverlayRenderState{palette: withCaret(4)},
			pane:     bar,
			want:     cursorOut{valid: true, row: 2, col: 7, style: 6, hasStyle: true},
		},
		{
			name:     "hidden pane cursor falls back to the default shape",
			overlays: capturedOverlayRenderState{palette: withCaret(4)},
			pane:     cursorOut{hidden: true},
			want:     cursorOut{valid: true, row: 2, col: 7, style: 1, hasStyle: true},
		},
		{
			name:     "prompt above palette owns the cursor",
			overlays: capturedOverlayRenderState{palette: withCaret(4), prompt: withCaret(6)},
			pane:     bar,
			want:     cursorOut{valid: true, row: 2, col: 9, style: 6, hasStyle: true},
		},
		{
			name:     "prompt above copy search owns the cursor",
			overlays: capturedOverlayRenderState{copySearch: withCaret(1), prompt: withCaret(6)},
			pane:     bar,
			want:     cursorOut{valid: true, row: 2, col: 9, style: 6, hasStyle: true},
		},
		{
			name:     "palette above notices owns the cursor",
			overlays: capturedOverlayRenderState{palette: withCaret(4), noticesOverlay: notices},
			pane:     bar,
			want:     cursorOut{valid: true, row: 2, col: 7, style: 6, hasStyle: true},
		},
		{
			name:     "copy search caret owns the cursor",
			overlays: capturedOverlayRenderState{copySearch: withCaret(1)},
			pane:     bar,
			want:     cursorOut{valid: true, row: 2, col: 4, style: 6, hasStyle: true},
		},
		{
			name:     "notices above copy search hide the cursor",
			overlays: capturedOverlayRenderState{copySearch: withCaret(1), noticesOverlay: notices},
			pane:     bar,
			want:     cursorOut{hidden: true},
		},
		{
			name:     "modal without caret hides the cursor",
			overlays: capturedOverlayRenderState{palette: capturedModal{active: true, presentation: ui.Presentation{Inner: inner}, caretCol: 4}},
			pane:     bar,
			want:     cursorOut{hidden: true},
		},
		{
			name:     "caret outside the frame hides the cursor",
			overlays: capturedOverlayRenderState{palette: capturedModal{active: true, presentation: ui.Presentation{Inner: domain.Rect{X: 18, Y: 2, Width: 10, Height: 1}}, hasCaret: true, caretCol: 4}},
			pane:     bar,
			want:     cursorOut{hidden: true},
		},
		{
			name:     "copy mode without a modal hides the cursor",
			overlays: capturedOverlayRenderState{copyActive: true},
			pane:     bar,
			want:     cursorOut{hidden: true},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, overlayInputCursor(tt.overlays, tt.pane, 20, 10))
		})
	}
}

// TestPaletteInputLineOwnsTerminalCursor drives the production render path:
// the open palette shows the real terminal cursor after its query, which is
// what lets client predictive echo type ahead in it.
func TestPaletteInputLineOwnsTerminalCursor(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	client := vt.NewScreen(80, 25)
	d.paint(sess, ac, true, nil)
	mustApplyOutput(t, client, awaitFrame(t, sends, "Output"))

	d.handleInput(sess, ac, []byte("\x1b "))
	mustApplyOutput(t, client, awaitFrame(t, sends, "Output"))
	require.True(t, client.CursorVisible(), "open palette shows the real cursor")
	row, col := client.CursorRow(), client.CursorCol()
	require.Equal(t, '>', client.Cell(col-2, row).Rune, "cursor sits after the query prompt")

	d.handleInput(sess, ac, []byte("ab"))
	mustApplyOutput(t, client, awaitFrame(t, sends, "Output"))
	require.True(t, client.CursorVisible())
	require.Equal(t, row, client.CursorRow())
	require.Equal(t, col+2, client.CursorCol(), "cursor follows typed query")
	require.Equal(t, 'b', client.Cell(col+1, row).Rune)
}
