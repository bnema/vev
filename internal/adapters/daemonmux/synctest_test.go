package daemonmux

import (
	"runtime"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

// requireSettled checks a stable condition after all bubble workers block.
func requireSettled(t *testing.T, condition func() bool, msgAndArgs ...any) {
	t.Helper()
	synctest.Wait()
	require.True(t, condition(), msgAndArgs...)
}

// inBubble runs body in a synctest bubble and then drops streamframe's
// process-global pool of Send completion channels. A channel the bubble put
// there must not be reused by a later test: using a bubble-owned channel from
// outside its bubble (or from another bubble) is a fatal runtime error. Two
// collections empty a sync.Pool (primary and victim caches).
func inBubble(t *testing.T, body func(t *testing.T)) {
	t.Helper()
	runtime.GC()
	runtime.GC()
	synctest.Test(t, body)
	runtime.GC()
	runtime.GC()
}
