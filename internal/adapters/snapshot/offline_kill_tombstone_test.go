package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepositoryTombstoneBlocksIncrementalRestoreAndLegacyImport(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))))
	require.NoError(t, os.WriteFile(filepath.Join(repo.dir, filenameForName("named")), []byte("legacy"), 0o600))

	require.NoError(t, repo.Tombstone(context.Background(), "named"))

	names, err := repo.List(context.Background())
	require.NoError(t, err)
	require.NotContains(t, names, "named")
	legacy, err := repo.LoadLegacy(context.Background())
	require.NoError(t, err)
	for _, snapshot := range legacy {
		require.NotEqual(t, "named", snapshot.Name)
	}
}

func TestRepositoryDeleteLegacyRetriesDurabilityAfterTombstone(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, os.Mkdir(repo.dir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(repo.dir, filenameForName("named")), []byte("legacy"), 0o600))
	require.NoError(t, repo.Tombstone(context.Background(), "named"))

	calls := 0
	repo.hooks.syncDirectory = func(string) error {
		calls++
		if calls == 1 {
			return os.ErrPermission
		}
		return nil
	}
	require.ErrorIs(t, repo.DeleteLegacy(context.Background(), "named"), os.ErrPermission)

	// A retry after process restart has no in-memory pending-sync state.
	repo = NewRepository(repo.dir)
	repo.hooks.syncDirectory = func(string) error {
		calls++
		return nil
	}
	require.NoError(t, repo.DeleteLegacy(context.Background(), "named"))
	require.Equal(t, 2, calls, "explicit purge retries only the pending source-directory sync")
}
