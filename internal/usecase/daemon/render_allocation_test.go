package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Benchmark accounting measures allocated bytes, not retained heap. Setup and
// warm-up belong to the calling fixture, outside this measured operation.
func assertRenderByteBudget(t *testing.T, run func(), maximum int64) {
	t.Helper()
	result := testing.Benchmark(func(b *testing.B) {
		for b.Loop() {
			run()
		}
	})
	require.LessOrEqual(t, result.AllocedBytesPerOp(), maximum, "rendering must preserve the compact byte-allocation benefit")
}
