package snapshot

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	codec "github.com/bnema/vev/internal/usecase/snapshot"
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
	var generationDirectoryReadCount atomic.Int64
	repo.hooks.beforeDirectoryRead = func(path string) {
		if path == generations {
			generationDirectoryReadCount.Add(1)
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
	if generationDirectoryReadCount.Load() != 0 {
		t.Fatal("direct load enumerated generation directory")
	}
}

func TestRepairHEAD(t *testing.T) {
	repo := NewRepository(privateDir(t))
	id := domain.IncarnationID{1}
	pub := incarnationPublication(t, id, "work", 1, nil)
	if err := repo.Publish(context.Background(), pub); err != nil {
		t.Fatal(err)
	}
	valid := domain.CheckpointRef{Generation: 1, ManifestDigest: sha256.Sum256(pub.Manifest)}
	if err := repo.RepairHEAD(context.Background(), id, valid); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(repo.headPath(id))
	if err != nil {
		t.Fatal(err)
	}
	invalid := valid
	invalid.ManifestDigest[0]++
	if err := repo.RepairHEAD(context.Background(), id, invalid); err == nil {
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

func incarnationPublication(t *testing.T, id domain.IncarnationID, name string, generation uint64, parent *domain.CheckpointRef) ports.SnapshotPublication {
	t.Helper()
	tail, err := codec.MarshalObject(codec.HistoryTail, []byte("tail"))
	if err != nil {
		t.Fatal(err)
	}
	visible, err := codec.MarshalObject(codec.Visible, []byte("visible"))
	if err != nil {
		t.Fatal(err)
	}
	manifest, err := codec.MarshalManifest(codec.Manifest{
		Generation: generation, IncarnationID: id, ParentCheckpoint: parent, Name: name,
		Tabs: []codec.ManifestTab{{Cols: 1, Rows: 1, Panes: []codec.ManifestPane{{ID: "p", Tail: codec.ObjectRef{Kind: codec.HistoryTail, Digest: tail.Digest, Size: uint32(len(tail.Data))}, Visible: codec.ObjectRef{Kind: codec.Visible, Digest: visible.Digest, Size: uint32(len(visible.Data))}}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return ports.SnapshotPublication{IncarnationID: id, Name: name, Generation: generation, ParentCheckpoint: parent, Manifest: manifest, Objects: []ports.SnapshotObject{tail, visible}}
}
