package app

import (
	"encoding/base64"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWebTokenConcurrentCreation(t *testing.T) {
	t.Setenv("XDG_STATE_HOME", t.TempDir())
	const workers = 16
	var wg sync.WaitGroup
	tokens := make([]string, workers)
	errors := make([]error, workers)
	for i := range workers {
		wg.Go(func() { tokens[i], errors[i] = webToken() })
	}
	wg.Wait()
	for i := range workers {
		require.NoError(t, errors[i])
		require.Equal(t, tokens[0], tokens[i])
	}
	decoded, err := base64.RawURLEncoding.DecodeString(tokens[0])
	require.NoError(t, err)
	require.Len(t, decoded, 32)
	info, err := os.Stat(webTokenPath())
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0600), info.Mode().Perm())
	files, err := filepath.Glob(filepath.Join(filepath.Dir(webTokenPath()), ".web-token-*"))
	require.NoError(t, err)
	require.Empty(t, files)
}
