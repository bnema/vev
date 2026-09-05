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
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/stretchr/testify/require"
)

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
			catalogue := &forwardOrphanCatalogue{
				record:       domain.CatalogueRecord{Name: publication.Name, IncarnationID: id},
				replaceError: commitError,
			}
			repository := NewRepository(privateDir(t))
			coordinator := recoveryusecase.NewCoordinator(catalogue, repository, nil)

			_, err := coordinator.PublishCheckpoint(t.Context(), publication.Name, publication)
			require.ErrorIs(t, err, commitError)
			tt.mutate(t, repository, publication)
			_, err = coordinator.PublishCheckpoint(t.Context(), publication.Name, publication)
			require.Error(t, err)
			require.Nil(t, catalogue.record.Committed, "an invalid forward orphan must not become authoritative")
			require.Equal(t, 1, catalogue.replaceCalls, "retry must not advance the catalogue")
		})
	}
}

type forwardOrphanCatalogue struct {
	record       domain.CatalogueRecord
	replaceError error
	replaceCalls int
}

func (c *forwardOrphanCatalogue) Records() ([]domain.CatalogueRecord, error) {
	return []domain.CatalogueRecord{c.record}, nil
}

func (c *forwardOrphanCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	return c.record, c.record.Name == name, nil
}

func (c *forwardOrphanCatalogue) Create(domain.CatalogueRecord) error {
	return errors.New("unexpected create")
}
func (c *forwardOrphanCatalogue) UpdateMetadata(domain.CatalogueMetadataUpdate) error {
	return errors.New("unexpected metadata update")
}
func (c *forwardOrphanCatalogue) Sync() error { return nil }
func (c *forwardOrphanCatalogue) Rename(string, domain.CatalogueRecord) error {
	return errors.New("unexpected rename")
}
func (c *forwardOrphanCatalogue) Delete(string) error { return errors.New("unexpected delete") }
func (c *forwardOrphanCatalogue) Close() error        { return nil }

func (c *forwardOrphanCatalogue) Replace(name string, record domain.CatalogueRecord) error {
	if name != c.record.Name || name != record.Name {
		return errors.New("catalogue key mismatch")
	}
	c.replaceCalls++
	if c.replaceError != nil {
		err := c.replaceError
		c.replaceError = nil
		return err
	}
	c.record = record
	return nil
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
