package snapshot

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	codec "github.com/bnema/vev/internal/snapshotcodec"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// real I/O: publishes and reads checkpoint files.
func TestIncarnationNamespace(t *testing.T) {
	repo := NewRepository(privateDir(t))
	id := domain.IncarnationID{1}
	pub := incarnationPublication(t, id, "old", 1, nil)
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	parent := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(pub.Manifest)}
	pub = incarnationPublication(t, id, "new", 2, &parent)
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	if got := filepath.Base(repo.sessionPath(id)); got != id.String() {
		t.Fatalf("namespace = %q", got)
	}

	wrong := pub
	wrong.IncarnationID = domain.IncarnationID{2}
	if err := repo.Publish(context.Background(), wrong); err == nil {
		t.Fatal("mismatched publication incarnation accepted")
	}
}

// real I/O: publishes and reads checkpoint files.
func TestDirectCheckpointLoad(t *testing.T) {
	repo := NewRepository(privateDir(t))
	id := domain.IncarnationID{1}
	pub := incarnationPublication(t, id, "work", 1, nil)
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	generations := filepath.Dir(repo.manifestPath(id, 1))
	for i := 2; i <= 5001; i++ {
		if err := os.WriteFile(filepath.Join(generations, generationFilename(uint64(i))), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	ref := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(pub.Manifest)}
	got, err := repo.LoadCheckpoint(context.Background(), id, "work", ref)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 1 || got.IncarnationID != id {
		t.Fatalf("generation = %#v", got)
	}
}

// real I/O: publishes and reads checkpoint files.
func TestLoadCheckpointPreservesManifestVersionError(t *testing.T) {
	for _, tt := range []struct {
		name    string
		version uint16
	}{
		{name: "dense VT checkpoint", version: 3},
		{name: "future", version: codec.ManifestVersion + 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			id := domain.IncarnationID{1}
			pub := incarnationPublication(t, id, "work", 1, nil)
			require.NoError(t, repo.Publish(t.Context(), pub))

			manifest := append([]byte(nil), pub.Manifest...)
			binary.BigEndian.PutUint16(manifest[4:6], tt.version)
			require.NoError(t, os.WriteFile(repo.manifestPath(id, 1), manifest, 0o600))
			ref := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(manifest)}

			_, err := repo.LoadCheckpoint(t.Context(), id, "work", ref)
			require.ErrorIs(t, err, codec.ErrBadVersion)
		})
	}
}

// real I/O: publishes and reads checkpoint files.
func TestLoadCheckpointDoesNotClassifyCorruptionAsManifestVersion(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*testing.T, *Repository, ports.SnapshotPublication, *domain.CheckpointRef)
	}{
		{
			name: "digest mismatch precedes version decode",
			mutate: func(t *testing.T, repo *Repository, pub ports.SnapshotPublication, _ *domain.CheckpointRef) {
				t.Helper()
				manifest := append([]byte(nil), pub.Manifest...)
				binary.BigEndian.PutUint16(manifest[4:6], codec.ManifestVersion-1)
				require.NoError(t, os.WriteFile(repo.manifestPath(pub.IncarnationID, pub.Generation), manifest, 0o600))
			},
		},
		{
			name: "bad magic",
			mutate: func(t *testing.T, repo *Repository, pub ports.SnapshotPublication, ref *domain.CheckpointRef) {
				t.Helper()
				manifest := append([]byte(nil), pub.Manifest...)
				manifest[0] ^= 1
				ref.ManifestDigest = sha256.Sum256(manifest)
				require.NoError(t, os.WriteFile(repo.manifestPath(pub.IncarnationID, pub.Generation), manifest, 0o600))
			},
		},
		{
			name: "bad crc",
			mutate: func(t *testing.T, repo *Repository, pub ports.SnapshotPublication, ref *domain.CheckpointRef) {
				t.Helper()
				manifest := append([]byte(nil), pub.Manifest...)
				manifest[12] ^= 1
				ref.ManifestDigest = sha256.Sum256(manifest)
				require.NoError(t, os.WriteFile(repo.manifestPath(pub.IncarnationID, pub.Generation), manifest, 0o600))
			},
		},
		{
			name: "wrong identity",
			mutate: func(t *testing.T, repo *Repository, pub ports.SnapshotPublication, ref *domain.CheckpointRef) {
				t.Helper()
				manifest, err := codec.UnmarshalManifest(pub.Manifest)
				require.NoError(t, err)
				manifest.IncarnationID = domain.IncarnationID{2}
				encoded, err := codec.MarshalManifest(manifest)
				require.NoError(t, err)
				ref.ManifestDigest = sha256.Sum256(encoded)
				require.NoError(t, os.WriteFile(repo.manifestPath(pub.IncarnationID, pub.Generation), encoded, 0o600))
			},
		},
		{
			name: "invalid object",
			mutate: func(t *testing.T, repo *Repository, pub ports.SnapshotPublication, _ *domain.CheckpointRef) {
				t.Helper()
				require.NoError(t, os.WriteFile(repo.objectPath(pub.IncarnationID, pub.Objects[0].Digest), []byte("corrupt"), 0o600))
			},
		},
		{
			name: "well-formed object with wrong digest",
			mutate: func(t *testing.T, repo *Repository, pub ports.SnapshotPublication, _ *domain.CheckpointRef) {
				t.Helper()
				object := pub.Objects[0]
				kind, payload, err := codec.UnmarshalObject(object.Data)
				require.NoError(t, err)
				payload = append([]byte(nil), payload...)
				payload[len(payload)-1] ^= 1
				replacement, err := codec.MarshalObject(kind, payload)
				require.NoError(t, err)
				require.Len(t, replacement.Data, len(object.Data))
				require.NoError(t, os.WriteFile(repo.objectPath(pub.IncarnationID, object.Digest), replacement.Data, 0o600))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			id := domain.IncarnationID{1}
			pub := incarnationPublication(t, id, "work", 1, nil)
			require.NoError(t, repo.Publish(t.Context(), pub))
			ref := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(pub.Manifest)}
			tt.mutate(t, repo, pub, &ref)

			_, err := repo.LoadCheckpoint(t.Context(), id, "work", ref)
			require.Error(t, err)
			require.NotErrorIs(t, err, codec.ErrBadVersion)
		})
	}
}

// real I/O: publishes and reads checkpoint files.
func TestReconcileCheckpoint(t *testing.T) {
	repo := NewRepository(privateDir(t))
	id := domain.IncarnationID{1}
	pub := incarnationPublication(t, id, "work", 1, nil)
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	valid := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(pub.Manifest)}
	if err := repo.ReconcileCheckpoint(context.Background(), id, valid); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(repo.headPath(id))
	if err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.ManifestDigest[0]++
	if err := repo.ReconcileCheckpoint(context.Background(), id, invalid); err == nil {
		t.Fatal("invalid checkpoint repaired HEAD")
	}
	after, err := os.ReadFile(repo.headPath(id))
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("invalid repair changed HEAD")
	}
}

// real I/O: publishes and reads checkpoint files.
func TestReconcileCheckpointRepairsOnlyDivergentHead(t *testing.T) {
	tests := []struct {
		name      string
		prepare   func(t *testing.T, repo *Repository, id domain.IncarnationID)
		wantWrite bool
	}{
		{name: "matching HEAD is left untouched", prepare: func(*testing.T, *Repository, domain.IncarnationID) {}},
		{name: "missing HEAD is repaired", wantWrite: true, prepare: func(t *testing.T, repo *Repository, id domain.IncarnationID) {
			require.NoError(t, os.Remove(repo.headPath(id)))
		}},
		{name: "corrupt HEAD is repaired", wantWrite: true, prepare: func(t *testing.T, repo *Repository, id domain.IncarnationID) {
			require.NoError(t, os.WriteFile(repo.headPath(id), []byte("garbage"), 0o600))
		}},
		{name: "stale HEAD is repaired", wantWrite: true, prepare: func(t *testing.T, repo *Repository, id domain.IncarnationID) {
			require.NoError(t, os.WriteFile(repo.headPath(id), marshalHead(1, [32]byte{9}), 0o600))
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			id := domain.IncarnationID{1}
			pub := incarnationPublication(t, id, "work", 1, nil)
			require.NoError(t, repo.Publish(t.Context(), pub))
			ref := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(pub.Manifest)}
			tt.prepare(t, repo, id)
			before, _ := os.Stat(repo.headPath(id))

			require.NoError(t, repo.ReconcileCheckpoint(t.Context(), id, ref))

			generation, digest, err := repo.readHead(id)
			require.NoError(t, err)
			require.Equal(t, ref.Generation, generation)
			require.Equal(t, ports.SnapshotDigest(ref.ManifestDigest), digest)
			after, err := os.Stat(repo.headPath(id))
			require.NoError(t, err)
			require.Equal(t, tt.wantWrite, before == nil || !os.SameFile(before, after), "HEAD rewrite")
		})
	}
}

// real I/O: publishes and reads checkpoint files.
func TestForwardOrphanRetryValidatesCheckpointBeforeCatalogueAdvance(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*testing.T, *Repository, ports.SnapshotPublication)
	}{
		{
			name: "missing object",
			mutate: func(t *testing.T, repository *Repository, publication ports.SnapshotPublication) {
				t.Helper()
				require.NoError(t, os.Remove(repository.objectPath(publication.IncarnationID, publication.Objects[0].Digest)))
			},
		},
		{
			name: "corrupt object",
			mutate: func(t *testing.T, repository *Repository, publication ports.SnapshotPublication) {
				t.Helper()
				require.NoError(t, os.WriteFile(repository.objectPath(publication.IncarnationID, publication.Objects[0].Digest), []byte("corrupt"), 0o600))
			},
		},
		{
			name: "corrupt manifest digest",
			mutate: func(t *testing.T, repository *Repository, publication ports.SnapshotPublication) {
				t.Helper()
				head, err := os.ReadFile(repository.headPath(publication.IncarnationID))
				require.NoError(t, err)
				head[len(head)-1]++
				require.NoError(t, os.WriteFile(repository.headPath(publication.IncarnationID), head, 0o600))
			},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id := domain.IncarnationID{1}
			publication := incarnationPublication(t, id, "work", 1, nil)
			commitError := errors.New("catalogue commit failed")
			record := domain.CatalogueRecord{Name: publication.Name, IncarnationID: id}
			catalogue := portsmocks.NewMockCatalogue(t)
			catalogue.EXPECT().Record(publication.Name).RunAndReturn(func(string) (domain.CatalogueRecord, bool, error) { return record, true, nil })
			replaceCalls := 0
			catalogue.EXPECT().Replace(publication.Name, mock.Anything).RunAndReturn(func(name string, next domain.CatalogueRecord) error {
				require.Equal(t, record.Name, name)
				require.Equal(t, record.Name, next.Name)
				replaceCalls++
				if replaceCalls == 1 {
					return commitError
				}
				record = next
				return nil
			})
			repository := NewRepository(privateDir(t))
			coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil)

			_, err := coordinator.PublishCheckpoint(t.Context(), publication.Name, publication)
			require.ErrorIs(t, err, commitError)
			tt.mutate(t, repository, publication)
			_, err = coordinator.PublishCheckpoint(t.Context(), publication.Name, publication)
			require.Error(t, err)
			require.Nil(t, record.Committed, "an invalid forward orphan must not become authoritative")
			require.Equal(t, 1, replaceCalls, "retry must not advance the catalogue")
		})
	}
}

func incarnationPublication(t *testing.T, id domain.IncarnationID, name string, generation uint64, parent *domain.CheckpointRef) ports.SnapshotPublication {
	t.Helper()
	tail, err := codec.MarshalObject(codec.HistoryTail, canonicalHistoryBlob(t, "tail"))
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := codec.MarshalObject(codec.RecoveryTranscript, canonicalHistoryBlob(t, "transcript"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.MarshalManifest(codec.Manifest{
		Generation: generation, IncarnationID: id, ParentCheckpoint: parent, Name: name,
		Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Transcript: codec.ObjectRef{Kind: codec.RecoveryTranscript, Digest: transcript.Digest, Size: uint32(len(transcript.Data))}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.SnapshotPublication{IncarnationID: id, Name: name, Generation: generation, ParentCheckpoint: parent, Manifest: manifest, Objects: []ports.SnapshotObject{tail, transcript}}
}
