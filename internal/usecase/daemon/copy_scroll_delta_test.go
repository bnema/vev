package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/stretchr/testify/require"
)

func TestCopyWheelPreservesOutputEpoch(t *testing.T) {
	f := newPerformanceFixture(t, performanceConfig{size: domain.Size{Cols: 80, Rows: 24}, panes: 1, historyRows: 10000})
	f.d.enterCopyMode(f.sess, f.ac)
	f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
	epoch := f.ac.output.currentEpoch()
	for _, delta := range []int{-30, -3, 3} {
		f.d.copyWheel(f.sess, f.ac, delta)
		require.Equal(t, epoch, f.ac.output.currentEpoch(), "ordinary scrolling must not reset the output dependency chain")
		f.ac.ackOutputState(f.ac.output.currentEpoch(), f.ac.output.next)
	}
	viewport := f.ac.pipelineCache.copyViewport
	require.NotNil(t, viewport.document)
	require.Equal(t, viewport.document.Height(), viewport.target.Height)
	require.Equal(t, viewport.document.Width(), viewport.target.Width)
}
