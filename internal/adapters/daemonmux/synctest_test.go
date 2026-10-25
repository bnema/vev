package daemonmux

import (
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
