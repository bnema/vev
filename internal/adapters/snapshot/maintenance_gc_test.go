package snapshot

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"slices"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestCollectGarbageClearsUncommittedCheckpointHead(t *testing.T) {
	t.Parallel()
	repository := NewRepository(privateDir(t))
	publication := repositoryPublication(t, "work", 1, []byte("first checkpoint"))
	require.NoError(t, repository.Publish(context.Background(), publication))

	// Repository publication completed, but the catalogue commit did not.
	// Startup GC knows the incarnation but has no committed checkpoint for it.
	keep := map[domain.IncarnationID]domain.CheckpointRef{publication.IncarnationID: {}}
	require.NoError(t, repository.CollectGarbage(context.Background(), keep))
	_, err := os.Stat(repository.headPath(publication.IncarnationID))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.Empty(t, garbageCollectionGenerations(t, repository, publication.IncarnationID))

	// Retrying starts from an empty repository, then the resulting checkpoint
	// can be committed to the catalogue and restored.
	require.NoError(t, repository.Publish(context.Background(), publication))
	committed := domain.CheckpointRef{Generation: publication.Generation, ManifestDigest: sha256.Sum256(publication.Manifest)}
	keep[publication.IncarnationID] = committed
	require.NoError(t, repository.CollectGarbage(context.Background(), keep))
	_, err = repository.LoadCheckpoint(context.Background(), publication.IncarnationID, publication.Name, committed)
	require.NoError(t, err)
}

func TestCollectGarbageRejectsMalformedAuthorityWithoutMutation(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*testing.T, *Repository, domain.IncarnationID, domain.CheckpointRef) domain.CheckpointRef
	}{
		{
			name: "committed digest mismatch",
			mutate: func(_ *testing.T, _ *Repository, _ domain.IncarnationID, committed domain.CheckpointRef) domain.CheckpointRef {
				committed.ManifestDigest[0]++
				return committed
			},
		},
		{
			name: "committed manifest missing",
			mutate: func(t *testing.T, repository *Repository, id domain.IncarnationID, committed domain.CheckpointRef) domain.CheckpointRef {
				t.Helper()
				require.NoError(t, os.Remove(repository.manifestPath(id, committed.Generation)))
				return committed
			},
		},
		{
			name: "committed manifest corrupt",
			mutate: func(t *testing.T, repository *Repository, id domain.IncarnationID, committed domain.CheckpointRef) domain.CheckpointRef {
				t.Helper()
				require.NoError(t, os.WriteFile(repository.manifestPath(id, committed.Generation), []byte("corrupt"), 0o600))
				return committed
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repository := NewRepository(privateDir(t))
			publications := publishGarbageCollectionGenerations(t, repository, "work", 3)
			id := publications[0].IncarnationID
			committed := domain.CheckpointRef{Generation: 3, ManifestDigest: sha256.Sum256(publications[2].Manifest)}
			committed = tt.mutate(t, repository, id, committed)
			before := snapshotRepositoryFiles(t, repository.dir)

			require.Error(t, repository.CollectGarbage(t.Context(), map[domain.IncarnationID]domain.CheckpointRef{id: committed}))
			require.Equal(t, before, snapshotRepositoryFiles(t, repository.dir), "malformed catalogue authority must fail closed")
		})
	}
}

func TestCollectGarbage(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		committed       uint64
		known           bool
		wantGenerations []uint64
	}{
		{name: "incarnation absent from catalogue is removed whole"},
		{name: "committed checkpoint prunes forward orphan and keeps predecessor", committed: 3, known: true, wantGenerations: []uint64{2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repository := NewRepository(privateDir(t))
			publications := publishGarbageCollectionGenerations(t, repository, "work", 4)
			id := publications[0].IncarnationID
			keep := make(map[domain.IncarnationID]domain.CheckpointRef)
			if tt.known {
				publication := publications[tt.committed-1]
				keep[id] = domain.CheckpointRef{Generation: tt.committed, ManifestDigest: sha256.Sum256(publication.Manifest)}
			}

			require.NoError(t, repository.CollectGarbage(context.Background(), keep))
			if !tt.known {
				_, err := os.Stat(repository.sessionPath(id))
				require.ErrorIs(t, err, os.ErrNotExist)
				return
			}
			require.Equal(t, tt.wantGenerations, garbageCollectionGenerations(t, repository, id))
			retainedObjects := make(map[ports.SnapshotDigest]struct{})
			for _, publication := range publications[tt.committed-2 : tt.committed] {
				for _, object := range publication.Objects {
					retainedObjects[object.Digest] = struct{}{}
				}
			}
			for _, publication := range publications {
				for _, object := range publication.Objects {
					_, retained := retainedObjects[object.Digest]
					_, err := os.Stat(repository.objectPath(id, object.Digest))
					if retained {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, os.ErrNotExist)
					}
				}
			}
		})
	}
}

func publishGarbageCollectionGenerations(t *testing.T, repository *Repository, name string, count uint64) []ports.SnapshotPublication {
	t.Helper()
	publications := make([]ports.SnapshotPublication, 0, count)
	for generation := uint64(1); generation <= count; generation++ {
		publication := repositoryPublicationAfter(t, repository, name, generation, []byte{byte(generation)})
		require.NoError(t, repository.Publish(context.Background(), publication))
		publications = append(publications, publication)
	}
	return publications
}

func garbageCollectionGenerations(t *testing.T, repository *Repository, id domain.IncarnationID) []uint64 {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(repository.sessionPath(id), repositoryGenerations))
	require.NoError(t, err)
	generations := make([]uint64, 0, len(entries))
	for _, entry := range entries {
		if generation, ok := parseGenerationFilename(entry.Name()); ok {
			generations = append(generations, generation)
		}
	}
	slices.Sort(generations)
	return generations
}
