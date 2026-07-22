package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepositoryLoadLegacyCompletesAuthorizedDeleteAfterRestart(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, os.Mkdir(repo.dir, 0o700))
	path := filepath.Join(repo.dir, filenameForName("named"))
	require.NoError(t, os.WriteFile(path, []byte("legacy"), 0o600))

	injected := errors.New("unlink failed")
	repo.hooks.remove = func(path string) error {
		if filepath.Base(path) == filenameForName("named") {
			return injected
		}
		return os.Remove(path)
	}
	require.ErrorIs(t, repo.DeleteLegacy(context.Background(), "named"), injected)

	// A new repository has no in-memory retry state. The durable authorization
	// must fence import and complete deletion without another publication.
	repo = NewRepository(repo.dir)
	legacy, err := repo.LoadLegacy(context.Background())
	require.NoError(t, err)
	require.Empty(t, legacy)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}
