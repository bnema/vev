package snapshot

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bnema/vev/internal/domain"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestRepositoryPublishesAndLoadsCompleteGeneration(t *testing.T) {
	t.Parallel()
	dir := privateDir(t)
	repo := NewRepository(dir)
	pub := repositoryPublication(t, "named", 1, []byte("state"))
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	got, err := loadPublication(context.Background(), repo, pub)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "named" || got.Generation != 1 || len(got.Objects) != 2 {
		t.Fatalf("Load = %#v", got)
	}
}

func loadPublication(ctx context.Context, repo *Repository, publication ports.SnapshotPublication) (ports.SnapshotGeneration, error) {
	return repo.LoadCheckpoint(ctx, publication.IncarnationID, publication.Name, ports.CheckpointRef{
		Generation:     publication.Generation,
		ManifestDigest: codec.ManifestDigest(publication.Manifest),
	})
}

func repositoryPublicationAfter(t *testing.T, repo *Repository, name string, generation uint64, payload []byte) ports.SnapshotPublication {
	t.Helper()
	return publicationWithCurrentParent(t, repo, repositoryPublication(t, name, generation, payload))
}

// publicationWithCurrentParent requires repositories to contain HEAD for every
// publication after generation one. Callers such as prepareHeadStage and
// seedCompletePublication must seed or publish the parent generation first.
func publicationWithCurrentParent(t *testing.T, repo *Repository, publication ports.SnapshotPublication) ports.SnapshotPublication {
	t.Helper()
	if publication.Generation == 1 {
		return publication
	}
	currentGeneration, digest, err := repo.readHead(publication.IncarnationID)
	if err != nil {
		t.Fatalf("read parent HEAD: %v", err)
	}
	parent := &domain.CheckpointRef{Generation: currentGeneration, ManifestDigest: digest}
	manifest, err := codec.UnmarshalManifest(publication.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.ParentCheckpoint = parent
	publication.Manifest, err = codec.MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	publication.ParentCheckpoint = parent
	return publication
}

func repositoryPublication(t *testing.T, name string, generation uint64, payload []byte) ports.SnapshotPublication {
	t.Helper()
	id := testIncarnationID(name)
	tail, err := codec.MarshalObject(codec.HistoryTail, canonicalHistoryBlob(t, string(payload)))
	if err != nil {
		t.Fatal(err)
	}
	transcript, err := codec.MarshalObject(codec.RecoveryTranscript, canonicalHistoryBlob(t, "transcript"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.MarshalManifest(codec.Manifest{Generation: generation, IncarnationID: id, Name: name, Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Transcript: codec.ObjectRef{Kind: codec.RecoveryTranscript, Digest: transcript.Digest, Size: uint32(len(transcript.Data))}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return ports.SnapshotPublication{IncarnationID: id, Name: name, Generation: generation, Manifest: manifest, Objects: []ports.SnapshotObject{tail, transcript}}
}

func testIncarnationID(name string) domain.IncarnationID {
	digest := sha256.Sum256([]byte("test incarnation: " + name))
	var id domain.IncarnationID
	copy(id[:], digest[:])
	return id
}
