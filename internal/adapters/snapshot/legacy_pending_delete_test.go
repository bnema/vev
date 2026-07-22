package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
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
	// The injected unlink fault is after receipt persistence, simulating a
	// crash boundary where publication was verified but source cleanup was not.
	require.ErrorIs(t, repo.DeleteVerifiedLegacy(context.Background(), ports.LegacySnapshot{Name: "named", Data: []byte("legacy")}), injected)

	// A new repository has no in-memory retry state. The durable receipt must
	// fence import and complete deletion without another publication.
	repo = NewRepository(repo.dir)
	legacy, err := repo.LoadLegacy(context.Background())
	require.NoError(t, err)
	require.Empty(t, legacy)
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestRepositoryReceiptDoesNotDeleteReplacedLegacySource(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, os.Mkdir(repo.dir, 0o700))
	path := filepath.Join(repo.dir, filenameForName("named"))
	require.NoError(t, os.WriteFile(path, []byte("verified"), 0o600))

	injected := errors.New("unlink failed")
	repo.hooks.remove = func(path string) error {
		if filepath.Base(path) == filenameForName("named") {
			return injected
		}
		return os.Remove(path)
	}
	require.ErrorIs(t, repo.DeleteVerifiedLegacy(context.Background(), ports.LegacySnapshot{Name: "named", Data: []byte("verified")}), injected)
	require.NoError(t, os.WriteFile(path, []byte("replacement"), 0o600))

	repo = NewRepository(repo.dir)
	legacy, err := repo.LoadLegacy(context.Background())
	require.NoError(t, err)
	require.Equal(t, []ports.LegacySnapshot{{Name: "named", Data: []byte("replacement")}}, legacy)
	require.FileExists(t, path)
}
