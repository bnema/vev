package daemonmux

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// awaitClosed fails the test when done stays open for budget. Real-I/O carriage
// tests call it outside any bubble, so the failure guard is a wall-clock timer;
// inside a bubble the same timer runs on fake time.
//
//testpolicy:realtime shared by real-I/O carriage tests that run outside a bubble
func awaitClosed(t *testing.T, done <-chan struct{}, budget time.Duration, msgAndArgs ...any) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(budget):
		require.FailNow(t, "terminal notification did not arrive", msgAndArgs...)
	}
}
