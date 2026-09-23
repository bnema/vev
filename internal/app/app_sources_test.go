package app

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// loadAppSources reads every Go file of this package, keyed by file name, so
// composition tests can assert which files own a given call.
func loadAppSources(t *testing.T, includeTests bool) map[string]string {
	t.Helper()

	entries, err := os.ReadDir(".")
	require.NoError(t, err, "read app package dir")
	sources := make(map[string]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".go") {
			continue
		}
		if !includeTests && strings.HasSuffix(name, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(name)
		require.NoError(t, err, "read app source %q", name)
		sources[name] = string(raw)
	}
	require.NotEmpty(t, sources, "app sources must not be empty")
	return sources
}
