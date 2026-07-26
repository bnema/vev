package snapshot

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestRepositoryPublishSkipsUnchangedTenThousandObjectHistory(t *testing.T) {
	const historyObjects = 10_000

	repo := NewRepository(privateDir(t))
	first, second := largeIncrementalPublications(t, "named", historyObjects)
	seedCompletePublication(t, repo, first)

	accounting := installFilesystemPublishAccounting(repo)
	if err := repo.Publish(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	// Only the new mutable tail and visible objects need work. The 10k sealed
	// objects are retained history supplied again by the producer, not input
	// that Publish needs to read, hash, copy, or write.
	requireFilesystemPublishAccounting(t, accounting, filesystemPublishAccounting{
		objectReads: 2, objectHashes: 2, objectCopies: 2,
		tempCreates: 4, tempWrites: 4, fileSyncs: 4,
		objectWrites: 2, manifestWrites: 1, headWrites: 1,
		immutableInstalls: 3, mutableRenames: 1, tempRemoves: 3, directorySyncs: 4,
	})

	// Replaying the now-current generation still parses the 10k references, but
	// must not allocate one object-sized buffer per retained history entry.
	// The limit is 450, not lower: the os.Root-based fast path pays a constant
	// allocation cost for guarded reads and root identity validation, and -race
	// adds further overhead. A per-retained-object regression would exceed this
	// by orders of magnitude.
	allocations := testing.AllocsPerRun(3, func() {
		if err := repo.Publish(context.Background(), second); err != nil {
			panic(err)
		}
	})
	if allocations > 450 {
		t.Fatalf("unchanged 10k history Publish allocations = %.0f, want <= 450; retained objects were likely copied", allocations)
	}

	got, err := repo.Load(context.Background(), second.Name)
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != second.Generation {
		t.Fatalf("published generation = %d, want %d", got.Generation, second.Generation)
	}
}

type filesystemPublishAccounting struct {
	objectReads, objectHashes, objectCopies                        int
	tempCreates, tempWrites, fileSyncs                             int
	objectWrites, manifestWrites, headWrites                       int
	immutableInstalls, mutableRenames, tempRemoves, directorySyncs int
}

func installFilesystemPublishAccounting(repo *Repository) *filesystemPublishAccounting {
	accounting := &filesystemPublishAccounting{}
	repo.hooks.beforeObjectRead = func(string) { accounting.objectReads++ }
	repo.hooks.beforeObjectHash = func([]byte) { accounting.objectHashes++ }
	repo.hooks.beforeObjectCopy = func([]byte) { accounting.objectCopies++ }
	repo.hooks.createTemp = func(string) error { accounting.tempCreates++; return nil }
	repo.hooks.writeTemp = func(string) error { accounting.tempWrites++; return nil }
	repo.hooks.syncFile = func(string) error { accounting.fileSyncs++; return nil }
	repo.hooks.beforeBlobWrite = func(string) error { accounting.objectWrites++; return nil }
	repo.hooks.beforeManifestWrite = func(string) error { accounting.manifestWrites++; return nil }
	repo.hooks.beforeHeadWrite = func(string) error { accounting.headWrites++; return nil }
	repo.hooks.installImmutable = func(string) error { accounting.immutableInstalls++; return nil }
	repo.hooks.rename = func(string) error { accounting.mutableRenames++; return nil }
	repo.hooks.remove = func(string) error { accounting.tempRemoves++; return nil }
	repo.hooks.syncDirectory = func(string) error { accounting.directorySyncs++; return nil }
	return accounting
}

func requireFilesystemPublishAccounting(t *testing.T, got *filesystemPublishAccounting, want filesystemPublishAccounting) {
	t.Helper()
	if *got != want {
		t.Fatalf("filesystem Publish accounting = %+v, want %+v", *got, want)
	}
}

func BenchmarkRepositoryPublishUnchangedTenThousandObjectHistory(b *testing.B) {
	first, second := largeIncrementalPublications(b, "named", 10_000)
	repo := NewRepository(filepath.Join(b.TempDir(), "vev"))
	seedCompletePublication(b, repo, first)
	accounting := installFilesystemPublishAccounting(repo)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := repo.Publish(context.Background(), second); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	// The benchmark deliberately replays the current generation so it can run
	// repeatedly without fixture writes. Its bounds catch a regression that
	// reads, hashes, or copies all 10k retained objects on the fast path.
	if accounting.objectReads > 2 || accounting.objectHashes > 2 || accounting.objectCopies > 2 {
		b.Fatalf("unchanged 10k history per-run object work = %d/%d/%d, want <= 2/2/2", accounting.objectReads, accounting.objectHashes, accounting.objectCopies)
	}
}

func TestRepositoryPublishVerifiesNecessaryExistingObjectOnce(t *testing.T) {
	repo := NewRepository(privateDir(t))
	first := repositoryPublication(t, "named", 1, []byte("first"))
	if err := repo.Publish(context.Background(), first); err != nil {
		t.Fatal(err)
	}
	second := repositoryPublicationAfter(t, repo, "named", 2, []byte("second"))
	newTail := second.Objects[0]
	path := repo.objectPath(second.IncarnationID, newTail.Digest)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, newTail.Data, 0o600); err != nil {
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

func largeIncrementalPublications(t testing.TB, name string, count int) (ports.SnapshotPublication, ports.SnapshotPublication) {
	t.Helper()
	id := legacyIncarnationID(name)
	sealed := make([]codec.ObjectRef, 0, count)
	objects := make([]ports.SnapshotObject, 0, count+2)
	for i := range count {
		object, err := codec.MarshalObject(codec.HistoryChunk, fmt.Appendf(nil, "history-%d", i))
		if err != nil {
			t.Fatal(err)
		}
		sealed = append(sealed, codec.ObjectRef{Kind: codec.HistoryChunk, Digest: object.Digest, Size: uint32(len(object.Data))})
		objects = append(objects, object)
	}
	makePublication := func(generation uint64, parent *domain.CheckpointRef, tailPayload, visiblePayload string) ports.SnapshotPublication {
		t.Helper()
		tail, err := codec.MarshalObject(codec.HistoryTail, []byte(tailPayload))
		if err != nil {
			t.Fatal(err)
		}
		visible, err := codec.MarshalObject(codec.Visible, []byte(visiblePayload))
		if err != nil {
			t.Fatal(err)
		}
		manifest, err := codec.MarshalManifest(codec.Manifest{Generation: generation, IncarnationID: id, ParentCheckpoint: parent, Name: name, Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Sealed: sealed, Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Visible: codec.ObjectRef{Kind: codec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))}}}}}})
		if err != nil {
			t.Fatal(err)
		}
		return ports.SnapshotPublication{IncarnationID: id, Name: name, Generation: generation, ParentCheckpoint: parent, Manifest: manifest, Objects: append(append([]ports.SnapshotObject(nil), objects...), tail, visible)}
	}
	first := makePublication(1, nil, "tail-1", "visible-1")
	parent := &domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(first.Manifest)}
	return first, makePublication(2, parent, "tail-2", "visible-2")
}

func seedCompletePublication(t testing.TB, repo *Repository, publication ports.SnapshotPublication) {
	t.Helper()
	if err := repo.ensureSession(publication.IncarnationID); err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.UnmarshalManifest(publication.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	for _, object := range publication.Objects {
		if err := os.MkdirAll(filepath.Dir(repo.objectPath(publication.IncarnationID, object.Digest)), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(repo.objectPath(publication.IncarnationID, object.Digest), object.Data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(repo.manifestPath(publication.IncarnationID, publication.Generation), publication.Manifest, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(repo.headPath(publication.IncarnationID), marshalHead(publication.Generation, sha256.Sum256(publication.Manifest)), 0o600); err != nil {
		t.Fatal(err)
	}
	if refs := manifestRefs(manifest); refs == nil {
		t.Fatal("invalid seeded manifest")
	}
}
