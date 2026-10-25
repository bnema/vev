package daemonmux

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func awaitClosed(t *testing.T, done <-chan struct{}, budget time.Duration, msgAndArgs ...any) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(budget):
		require.FailNow(t, "terminal notification did not arrive", msgAndArgs...)
	}
}
