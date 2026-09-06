package daemon

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

// Measure bytes with the same bounded warmup/sampling strategy as
// testing.AllocsPerRun. These sequential allocation tests need a byte budget,
// not testing.Benchmark's default one-second timing calibration per assertion.
func renderAllocatedBytesPerRun(run func()) int64 {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	run()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const samples = 20
	for range samples {
		run()
	}
	runtime.ReadMemStats(&after)
	return int64((after.TotalAlloc - before.TotalAlloc) / samples)
}

// Setup and warmup remain outside the measured operation; existing byte
// ceilings are unchanged. Like AllocsPerRun, do not call from parallel tests.
func assertRenderByteBudget(t *testing.T, run func(), maximum int64) {
	t.Helper()
	require.LessOrEqual(t, renderAllocatedBytesPerRun(run), maximum, "rendering must preserve the compact byte-allocation benefit")
}
