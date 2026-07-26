package snapshot

import (
	"context"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepositoryLoadPropagatesOperationalHeadErrors(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))))

	repo.hooks.beforeHeadRead = func(string) error { return os.ErrPermission }

	_, err := repo.Load(context.Background(), "named")
	require.ErrorIs(t, err, os.ErrPermission)
}

func TestRepositoryCurrentGenerationPropagatesOperationalHeadErrors(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))))

	repo.hooks.beforeHeadRead = func(string) error { return syscall.EIO }

	_, _, err := repo.currentGeneration(context.Background(), "named", legacyIncarnationID("named").String())
	require.ErrorIs(t, err, syscall.EIO)
}

func TestRepositoryLoadUsesEstablishedLegacySessionKey(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publication := repositoryPublication(t, "named", 1, []byte("legacy-state"))
	require.NoError(t, repo.Publish(context.Background(), publication))
	require.NoError(t, os.Rename(repo.sessionPath(publication.IncarnationID), filepath.Join(repo.dir, repositorySessionsDir, sessionKey(publication.Name))))

	got, err := repo.Load(context.Background(), publication.Name)
	require.NoError(t, err)
	require.Equal(t, publication.Objects[0].Data, got.Objects[publication.Objects[0].Digest])
}

func TestRepositoryLoadFallsBackFromInvalidHead(t *testing.T) {
	repo := NewRepository(privateDir(t))
	publication := repositoryPublication(t, "named", 1, []byte("state"))
	require.NoError(t, repo.Publish(context.Background(), publication))
	require.NoError(t, os.WriteFile(repo.legacyHeadPath(legacyIncarnationID("named").String()), []byte("invalid"), 0o600))

	got, err := repo.Load(context.Background(), "named")
	require.NoError(t, err)
	require.Equal(t, uint64(1), got.Generation)
	require.Equal(t, publication.Objects[0].Data, got.Objects[publication.Objects[0].Digest])
}

func TestRepositoryLoadFallsBackAcrossMultipleCorruptCandidates(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= 4; generation++ {
		require.NoError(t, repo.Publish(context.Background(), repositoryPublicationAfter(t, repo, "named", generation, []byte{byte(generation)})))
	}
	key := legacyIncarnationID("named").String()
	for _, generation := range []uint64{4, 3} {
		publication := repositoryPublication(t, "named", generation, []byte{byte(generation)})
		require.NoError(t, os.Remove(repo.legacyObjectPath(key, publication.Objects[0].Digest)))
	}

	got, err := repo.Load(context.Background(), "named")
	require.NoError(t, err)
	require.Equal(t, uint64(2), got.Generation)
	fallback := repositoryPublication(t, "named", 2, []byte{2})
	require.Equal(t, fallback.Objects[0].Data, got.Objects[fallback.Objects[0].Digest])
}

func TestRepositoryLoadHoldsSessionLockAcrossTombstoneCheck(t *testing.T) {
	repo := NewRepository(privateDir(t))
	require.NoError(t, repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))))

	tombstoneCheckStarted := make(chan struct{})
	releaseTombstoneCheck := make(chan struct{})
	repo.hooks.beforeTombstoneCheck = func(string) {
		close(tombstoneCheckStarted)
		<-releaseTombstoneCheck
	}
	loadDone := make(chan error, 1)
	go func() {
		_, err := repo.Load(context.Background(), "named")
		loadDone <- err
	}()
	<-tombstoneCheckStarted

	tombstoneDone := make(chan error, 1)
	go func() { tombstoneDone <- repo.Tombstone(context.Background(), "named") }()
	select {
	case err := <-tombstoneDone:
		t.Fatalf("Tombstone completed before Load released the session lock: %v", err)
	default:
	}

	close(releaseTombstoneCheck)
	require.NoError(t, <-loadDone)
	require.NoError(t, <-tombstoneDone)
}
