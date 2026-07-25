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
	got, err := repo.Load(context.Background(), "named")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Name != "named" || got.Generation != 1 || len(got.Objects) != 2 {
		t.Fatalf("Load = %#v", got)
	}
}

func TestRepositoryDoesNotRewriteVerifiedImmutableBlob(t *testing.T) {
	t.Parallel()
	dir := privateDir(t)
	repo := NewRepository(dir)
	pub := repositoryPublication(t, "named", 1, []byte("state"))
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	writes := 0
	repo.hooks.beforeBlobWrite = func(string) error { writes++; return nil }
	pub.Generation = 2
	manifest, err := codec.UnmarshalManifest(pub.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Generation = 2
	parent := &domain.CheckpointRef{Generation: 1, ManifestDigest: codecDigestForTest(pub.Manifest)}
	manifest.ParentCheckpoint = parent
	pub.ParentCheckpoint = parent
	pub.Manifest, err = codec.MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	if writes != 0 {
		t.Fatalf("blob writes = %d, want 0", writes)
	}
}

func codecDigestForTest(data []byte) [32]byte {
	return sha256.Sum256(data)
}

func repositoryPublicationAfter(t *testing.T, repo *Repository, name string, generation uint64, payload []byte) ports.SnapshotPublication {
	t.Helper()
	return publicationWithCurrentParent(t, repo, repositoryPublication(t, name, generation, payload))
}

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
	id := legacyIncarnationID(name)
	tail, err := codec.MarshalObject(codec.HistoryTail, payload)
	if err != nil {
		t.Fatal(err)
	}
	visible, err := codec.MarshalObject(codec.Visible, []byte("visible"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.MarshalManifest(codec.Manifest{Generation: generation, IncarnationID: id, Name: name, Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Visible: codec.ObjectRef{Kind: codec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))}}}}}})
	if err != nil {
		t.Fatal(err)
	}
	return ports.SnapshotPublication{IncarnationID: id, Name: name, Generation: generation, Manifest: manifest, Objects: []ports.SnapshotObject{tail, visible}}
}
