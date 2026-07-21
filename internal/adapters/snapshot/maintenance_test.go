package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryDeleteQuarantinesCanonicalSessionBeforeCleanup(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(context.Background(), "named"); err != nil {
		t.Fatal(err)
	}

	canonical := repo.sessionPath(sessionKey("named"))
	if _, err := os.Lstat(canonical); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical session after Delete = %v, want not exist", err)
	}
	entries, err := os.ReadDir(filepath.Join(repo.dir, repositorySessionsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || !isQuarantine(entries[0].Name()) || !entries[0].IsDir() {
		t.Fatalf("session entries after Delete = %#v, want one quarantine", entries)
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err = os.ReadDir(filepath.Join(repo.dir, repositorySessionsDir))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Fatalf("session entries after Maintain = %#v, want none", entries)
	}
}

func TestRepositoryDeleteReturnsRootSyncFailureAfterQuarantine(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("sessions sync")
	repo.hooks.syncDirectory = func(string) error { return injected }
	if err := repo.Delete(context.Background(), "named"); !errors.Is(err, injected) {
		t.Fatalf("Delete error = %v, want root sync failure", err)
	}
	if _, err := os.Lstat(repo.sessionPath(sessionKey("named"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical session after failed sync = %v, want not exist", err)
	}
}

func TestRepositoryDeleteWaitsForPublication(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("first"))); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	release := make(chan struct{})
	repo.hooks.beforeHeadWrite = func(string) error {
		close(entered)
		<-release
		return nil
	}
	publishDone := make(chan error, 1)
	go func() {
		publishDone <- repo.Publish(context.Background(), repositoryPublication(t, "named", 2, []byte("second")))
	}()
	<-entered
	deleteDone := make(chan error, 1)
	go func() { deleteDone <- repo.Delete(context.Background(), "named") }()
	select {
	case err := <-deleteDone:
		t.Fatalf("Delete returned before Publish released its lock: %v", err)
	default:
	}
	close(release)
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	if err := <-deleteDone; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(repo.sessionPath(sessionKey("named"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("canonical session after concurrent Delete = %v, want not exist", err)
	}
}

func TestRepositoryMaintainRetainsNewestTwoCompleteGenerations(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= 3; generation++ {
		if err := repo.Publish(context.Background(), repositoryPublication(t, "named", generation, []byte{byte(generation)})); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	key := sessionKey("named")
	for _, generation := range []uint64{2, 3} {
		if _, err := os.Lstat(repo.manifestPath(key, generation)); err != nil {
			t.Fatalf("retained manifest %d: %v", generation, err)
		}
	}
	if _, err := os.Lstat(repo.manifestPath(key, 1)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("old manifest after Maintain = %v, want not exist", err)
	}
	for _, generation := range []uint64{2, 3} {
		if _, err := repo.Load(context.Background(), "named"); err != nil {
			t.Fatalf("Load after collecting generation %d: %v", generation, err)
		}
	}
}

func TestRepositoryMaintainPreservesIncompleteManifestReferences(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= 3; generation++ {
		if err := repo.Publish(context.Background(), repositoryPublication(t, "named", generation, []byte{byte(generation)})); err != nil {
			t.Fatal(err)
		}
	}
	key := sessionKey("named")
	manifest := repositoryPublication(t, "named", 3, []byte{3})
	if err := os.Remove(repo.objectPath(key, manifest.Objects[1].Digest)); err != nil {
		t.Fatal(err)
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(repo.manifestPath(key, 3)); err != nil {
		t.Fatalf("incomplete manifest removed: %v", err)
	}
	if _, err := os.Lstat(repo.objectPath(key, manifest.Objects[0].Digest)); err != nil {
		t.Fatalf("object referenced by incomplete manifest removed: %v", err)
	}
}

func TestRepositoryMaintainUsesFixedRemovalBatch(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maintenanceBatch+1; i++ {
		if err := os.WriteFile(filepath.Join(repo.dir, ".tmp-"+string(rune('a'+i))), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(repo.dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("remaining stale entries = %d, want 1 after one batch", len(entries))
	}
}
