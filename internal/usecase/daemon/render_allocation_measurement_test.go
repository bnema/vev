package daemon

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

var renderAllocationMeasurementSink []byte

func TestRenderByteMeasurementUsesBoundedRunsAndExcludesWarmup(t *testing.T) {
	defer func() { renderAllocationMeasurementSink = nil }()
	for _, size := range []int{128, 4096} {
		calls := 0
		procs := runtime.GOMAXPROCS(0)
		got := renderAllocatedBytesPerRun(func() {
			calls++
			if calls == 1 {
				renderAllocationMeasurementSink = make([]byte, 1<<20)
			} else {
				renderAllocationMeasurementSink = make([]byte, size)
			}
		})
		require.Equal(t, 21, calls, "one warmup and twenty samples, not a time-calibrated benchmark")
		require.Equal(t, procs, runtime.GOMAXPROCS(0))
		require.GreaterOrEqual(t, got, int64(size))
		require.Less(t, got, int64(size+256), "warmup bytes must not enter the per-operation measurement")
	}
}
