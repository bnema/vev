package snapshot

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestRepositoryPublishSkipsUnchangedTenThousandObjectHistory(t *testing.T) {
	const historyObjects = 10_000

	repo := NewRepository(privateDir(t))
	first, second := largeIncrementalPublications(t, "named", historyObjects)
	seedCompletePublication(t, repo, first)

	var reads, hashes, copies int
	repo.hooks.beforeObjectRead = func(string) { reads++ }
	repo.hooks.beforeObjectHash = func([]byte) { hashes++ }
	repo.hooks.beforeObjectCopy = func([]byte) { copies++ }

	if err := repo.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	// Only the new mutable tail and visible objects need work. The 10k sealed
	// objects are retained history supplied again by the producer, not input
	// that Publish needs to read, hash, or copy.
	if reads != 2 || hashes != 2 || copies != 2 {
		t.Fatalf("unchanged 10k history reads/hashes/copies = %d/%d/%d, want 2/2/2", reads, hashes, copies)
	}
	got, err := repo.Load(context.Background(), second.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != second.Generation {
		t.Fatalf("published generation = %d, want %d", got.Generation, second.Generation)
	}
}

func TestRepositoryPublishVerifiesNecessaryExistingObjectOnce(t *testing.T) {
	repo := NewRepository(privateDir(t))
	first := repositoryPublication(t, "named", 1, []byte("first"))
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := repositoryPublication(t, "named", 2, []byte("second"))
	newTail := second.Objects[0]
	key := sessionKey(second.Name)
	if err := os.MkdirAll(filepath.Dir(repo.objectPath(key, newTail.Digest)), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(repo.objectPath(key, newTail.Digest), newTail.Data, 0o600); err != nil {
		t.Fatal(err)
	}

	var reads, hashes, copies int
	repo.hooks.beforeObjectRead = func(string) { reads++ }
	repo.hooks.beforeObjectHash = func([]byte) { hashes++ }
	repo.hooks.beforeObjectCopy = func([]byte) { copies++ }
	if err := repo.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	// The existing new tail is checked from disk once. The unchanged visible
	// object is retained from generation one and needs no further work.
	if reads != 1 || hashes != 1 || copies != 0 {
		t.Fatalf("necessary object reads/hashes/copies = %d/%d/%d, want 1/1/0", reads, hashes, copies)
	}
}

func largeIncrementalPublications(t *testing.T, name string, count int) (ports.SnapshotPublication, ports.SnapshotPublication) {
	t.Helper()
	sealed := make([]codec.ObjectRef, 0, count)
	objects := make([]ports.SnapshotObject, 0, count+2)
	for i := range count {
		object, err := codec.MarshalObject(codec.HistoryChunk, []byte(fmt.Sprintf("history-%d", i)))
		if err != nil {
			t.Fatal(err)
		}
		sealed = append(sealed, codec.ObjectRef{Kind: codec.HistoryChunk, Digest: object.Digest, Size: uint32(len(object.Data))})
		objects = append(objects, object)
	}
	makePublication := func(generation uint64, tailPayload, visiblePayload string) ports.SnapshotPublication {
		t.Helper()
		tail, err := codec.MarshalObject(codec.HistoryTail, []byte(tailPayload))
		if err != nil {
			t.Fatal(err)
		}
		visible, err := codec.MarshalObject(codec.Visible, []byte(visiblePayload))
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := codec.MarshalManifest(codec.Manifest{Generation: generation, Name: name, Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Sealed: sealed, Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Visible: codec.ObjectRef{Kind: codec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))}}}}}})
		if err != nil {
			t.Fatal(err)
		}
		return ports.SnapshotPublication{Name: name, Generation: generation, Manifest: manifest, Objects: append(append([]ports.SnapshotObject(nil), objects...), tail, visible)}
	}
	return makePublication(1, "tail-1", "visible-1"), makePublication(2, "tail-2", "visible-2")
}

func seedCompletePublication(t *testing.T, repo *Repository, publication ports.SnapshotPublication) {
	t.Helper()
	key := sessionKey(publication.Name)
	if err := repo.ensureSession(key); err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.UnmarshalManifest(publication.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range publication.Objects {
		if err := os.MkdirAll(filepath.Dir(repo.objectPath(key, object.Digest)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(repo.objectPath(key, object.Digest), object.Data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(repo.manifestPath(key, publication.Generation), publication.Manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(repo.headPath(key), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest)), 0o600); err != nil {
		t.Fatal(err)
	}
	if refs := manifestRefs(manifest); refs == nil {
		t.Fatal("invalid seeded manifest")
	}
}
