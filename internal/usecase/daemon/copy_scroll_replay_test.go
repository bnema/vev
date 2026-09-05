package daemon

import (
	"fmt"
	"strings"
	"testing"

	vt "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestCopyWheelReplayMatchesFullComposition(t *testing.T) {
	for _, panes := range []int{1, 2, 4} {
		t.Run(fmt.Sprint(panes), func(t *testing.T) {
			f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 120, Rows: 40}, panes: panes, historyRows: 200})
			f.d.enterCopyMode(f.sess, f.ac)
			base := f.ac.pipelineCache.frame
			terminal := vt.NewScreen(base.Width, base.Height)
			replay := func() []byte {
				output, err := wire.UnmarshalOutput(f.output.lastPayload())
				require.NoError(t, err)
				terminal.Write(output.Data)
				f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
				return output.Data
			}
			replay()
			for i, delta := range []int{-30, -3, 3, -1, 1, -60, 3, 1} {
				previous := f.ac.pipelineCache.copyViewport.frame
				var before vt.Frame
				if previous.Width > 0 {
					before = captureTestFrame(previous)
				}
				// Concurrent live output must update the unadorned base without
				// contaminating retained immutable copy rows.
				f.activePane.mu.Lock()
				f.activePane.screen.Write([]byte(fmt.Sprintf("\x1b[1;1HLIVE-%d", i)))
				f.activePane.mu.Unlock()
				f.d.copyWheel(f.sess, f.ac, delta)
				data := replay()
				if before.Width > 0 {
					require.Equal(t, before, captureTestFrame(previous))
				}
				cache := f.ac.pipelineCache
				mode := f.ac.overlays.copyMode
				require.NotNil(t, mode)
				target := cache.copyViewport.target
				require.NotNil(t, cache.copyViewport.document)
				want, _ := composeCopyClientFrame(mode, target, cache.frame.Clone(), resolveStyles(nil))
				require.Equal(t, captureTestFrame(want), captureTestFrame(cache.copyViewport.frame), "cached composition, step %d", i)
				for y := range want.Height {
					for x := range want.Width {
						require.True(t, want.Cell(x, y).Equal(terminal.Cell(x, y)), "replay step %d cell %d,%d", i, x, y)
					}
				}
				require.NoError(t, cache.copyViewport.frame.CheckInvariants())
				if panes == 1 && i == 1 {
					require.Contains(t, string(data), "\x1b[3T")
				}
				if panes == 1 && i == 2 {
					require.Contains(t, string(data), "\x1b[3S")
				}
				if target.Width != want.Width {
					require.False(t, strings.Contains(string(data), "\x1b[3T"))
				}
			}
		})
	}
}
