package daemon

import (
	"fmt"
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/stretchr/testify/require"
)

func TestCopyRowCompositionMatchesFrameReference(t *testing.T) {
	for _, target := range []domain.Rect{
		{X: 2, Y: 2, Width: 12, Height: 4},
		{X: -3, Y: -1, Width: 12, Height: 4},
		{X: 15, Y: 5, Width: 12, Height: 4},
		{X: -20, Y: 2, Width: 12, Height: 4},
		{X: 25, Y: 2, Width: 12, Height: 4},
		{X: 2, Y: 20, Width: 12, Height: 4},
		{X: 2, Y: 2, Width: 3, Height: 1},
	} {
		t.Run(fmt.Sprint(target), func(t *testing.T) {
			// The viewport is taller than the document: blank rows must erase the
			// old pane rather than leak its contents. Selection/search share the
			// same row renderer as the owned-frame API.
			rows := [][]renderer.Cell{testRow("alpha beta"), testRow("second row")}
			mode := scopy.NewMode(scopy.NewDocument(scopy.NewSnapshotFromRows(rows, 12, 4), ""))
			mode.Search("alpha")
			require.True(t, mode.StartCharacterSelection(scopy.Pos{Col: 1}))
			require.True(t, mode.ExtendCharacterSelection(scopy.Pos{Row: 1, Col: 3}))
			styles := resolveStyles(nil)
			base := renderer.NewFrame(20, 7)
			for y := range base.Height {
				base.FillRow(y, 0, base.Width, renderer.Cell{Rune: '#'})
			}
			want := base.Clone()
			src := mode.Render(styles.CopyStatus, styles.Selection)
			// Reference placement deliberately uses independent per-cell clipping.
			for y := 0; y < src.Height-1 && y < target.Height; y++ {
				for x := 0; x < src.Width && x < target.Width; x++ {
					dx, dy := target.X+x, target.Y+y
					if dx >= 0 && dx < want.Width && dy >= 0 && dy < want.Height-1 {
						want.Set(dx, dy, src.Cell(x, y))
					}
				}
			}
			want.FillRow(want.Height-1, 0, want.Width, renderer.Cell{Rune: ' ', Style: styles.SurfaceBar})
			for x := range min(want.Width, src.Width) {
				want.Set(x, want.Height-1, src.Cell(x, src.Height-1))
			}
			got, _ := composeCopyClientFrame(mode, target, base, styles)
			require.Equal(t, captureTestFrame(want), captureTestFrame(got))
			require.NoError(t, got.CheckInvariants())
		})
	}
}

func TestCopyScrollKeepsBaseUnadornedAndExitRefreshesLiveState(t *testing.T) {
	f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 100})
	ack := func() { f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next) }
	f.d.enterCopyMode(f.sess, f.ac)
	ack()
	require.True(t, f.ac.pipelineCache.valid)
	committed := f.ac.pipelineCache.frame
	before := captureTestFrame(committed)
	f.d.copyWheel(f.sess, f.ac, -30)
	ack()
	require.True(t, f.ac.pipelineCache.valid)
	require.Equal(t, before, captureTestFrame(committed), "scroll must not mutate the committed buffer")
	require.NotContains(t, frameText(f.ac.pipelineCache.frame), "[SCROLL]")

	// Live damage still updates the base behind the immutable history viewport.
	f.activePane.mu.Lock()
	f.activePane.screen.Write([]byte("\x1b[1;1HLIVE-UPDATED"))
	f.activePane.mu.Unlock()
	f.d.copyWheel(f.sess, f.ac, -3)
	ack()
	require.Contains(t, frameText(f.ac.pipelineCache.frame), "LIVE-UPDATED")
	require.NotContains(t, frameText(f.ac.pipelineCache.frame), "[SCROLL]")

	f.d.copyWheel(f.sess, f.ac, 1000)
	ack()
	require.Nil(t, f.ac.overlays.copyMode)
	require.Contains(t, frameText(f.ac.pipelineCache.frame), "LIVE-UPDATED")
	require.NoError(t, f.ac.pipelineCache.frame.CheckInvariants())
}
