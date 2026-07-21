package snapshot

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
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

func TestRepositoryCancellationAfterSessionLockWaitPreventsMutations(t *testing.T) {
	t.Run("publish", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		key := sessionKey("named")
		lock := repo.sessionLock(key)
		lock.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() {
			close(started)
			done <- repo.Publish(ctx, repositoryPublication(t, "named", 1, []byte("state")))
		}()
		<-started
		cancel()
		lock.Unlock()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish error = %v, want canceled", err)
		}
		if _, err := os.Lstat(repo.dir); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("Publish mutated canceled repository: %v", err)
		}
	})
	t.Run("load and delete", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
			t.Fatal(err)
		}
		key := sessionKey("named")
		for _, operation := range []struct {
			name string
			run  func(context.Context) error
		}{
			{"load", func(ctx context.Context) error { _, err := repo.Load(ctx, "named"); return err }},
			{"delete", func(ctx context.Context) error { return repo.Delete(ctx, "named") }},
		} {
			t.Run(operation.name, func(t *testing.T) {
				lock := repo.sessionLock(key)
				lock.Lock()
				ctx, cancel := context.WithCancel(context.Background())
				started := make(chan struct{})
				done := make(chan error, 1)
				go func() { close(started); done <- operation.run(ctx) }()
				<-started
				cancel()
				lock.Unlock()
				if err := <-done; !errors.Is(err, context.Canceled) {
					t.Fatalf("%s error = %v, want canceled", operation.name, err)
				}
				if _, err := repo.Load(context.Background(), "named"); err != nil {
					t.Fatalf("%s mutated session: %v", operation.name, err)
				}
			})
		}
	})
	t.Run("maintain", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
			t.Fatal(err)
		}
		lock := repo.sessionLock(sessionKey("named"))
		lock.Lock()
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		done := make(chan error, 1)
		go func() { close(started); done <- repo.Maintain(ctx) }()
		<-started
		cancel()
		lock.Unlock()
		if err := <-done; !errors.Is(err, context.Canceled) {
			t.Fatalf("Maintain error = %v, want canceled", err)
		}
		if _, err := repo.Load(context.Background(), "named"); err != nil {
			t.Fatalf("Maintain mutated session: %v", err)
		}
	})
}

func TestRepositoryDeleteRetriesPendingQuarantineSyncWithoutDeletingRecreatedSession(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("old"))); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("sessions sync")
	repo.hooks.syncDirectory = func(string) error { return injected }
	if err := repo.Delete(context.Background(), "named"); !errors.Is(err, injected) {
		t.Fatalf("Delete error = %v, want sync failure", err)
	}
	repo.hooks.syncDirectory = nil
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("new"))); err != nil {
		t.Fatal(err)
	}
	if err := repo.Delete(context.Background(), "named"); err != nil {
		t.Fatalf("retry Delete: %v", err)
	}
	if _, err := repo.Load(context.Background(), "named"); err != nil {
		t.Fatalf("recreated session was deleted: %v", err)
	}
}

func TestRepositoryMaintainResumesNestedQuarantineWithinBatch(t *testing.T) {
	repo := NewRepository(privateDir(t))
	quarantine := filepath.Join(repo.dir, repositorySessionsDir, ".deleting-named-test")
	for i := 0; i < maintenanceBatch+1; i++ {
		path := filepath.Join(quarantine, "nested", string(rune('a'+i)))
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(quarantine); errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quarantine after one bounded call = %v, want remaining tree", err)
	}
	for i := 0; i < 4; i++ {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Lstat(quarantine); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("quarantine after resumed maintenance = %v, want removed", err)
	}
}

func TestRepositoryMaintainDoesNotStarveLaterTemporaryEntries(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	// The live entries precede the stale entries in directory insertion order.
	// A fresh ReadDir from the start on every call would never reach the latter.
	for i := 0; i < maintenanceBatch+1; i++ {
		if err := os.WriteFile(filepath.Join(repo.dir, "live-"+string(rune('a'+i))), []byte("live"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < maintenanceBatch+1; i++ {
		if err := os.WriteFile(filepath.Join(repo.dir, ".tmp-later-"+string(rune('a'+i))), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 4; i++ {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(repo.dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".tmp-") {
			t.Fatalf("stale entry %q was starved", entry.Name())
		}
	}
}

func TestRepositoryMaintainMarksLargeGenerationSetBeforeSweeping(t *testing.T) {
	repo := NewRepository(privateDir(t))
	var newest ports.SnapshotPublication
	for generation := uint64(1); generation <= maintenanceBatch+2; generation++ {
		publication := repositoryPublication(t, "named", generation, []byte(fmt.Sprintf("state-%d", generation)))
		if err := repo.Publish(context.Background(), publication); err != nil {
			t.Fatal(err)
		}
		newest = publication
	}
	key := sessionKey("named")
	stale := sha256.Sum256([]byte("stale object"))
	stalePath := repo.objectPath(key, stale)
	if err := os.MkdirAll(filepath.Dir(stalePath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(stalePath, []byte("stale object"), 0o600); err != nil {
		t.Fatal(err)
	}

	// The first call has classified only one manifest batch. It must not sweep
	// even an otherwise stale object, nor a blob owned by a later manifest.
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(stalePath); err != nil {
		t.Fatalf("stale object swept before complete mark pass: %v", err)
	}
	for _, object := range newest.Objects {
		if _, err := os.Lstat(repo.objectPath(key, object.Digest)); err != nil {
			t.Fatalf("object referenced by not-yet-classified manifest removed: %v", err)
		}
	}

	for i := 0; i < 600; i++ {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := os.Lstat(stalePath); errors.Is(err, os.ErrNotExist) {
			break
		}
		if i == 599 {
			t.Fatal("stale object was not eventually swept")
		}
	}
	for _, generation := range []uint64{maintenanceBatch + 1, maintenanceBatch + 2} {
		if _, err := os.Lstat(repo.manifestPath(key, generation)); err != nil {
			t.Fatalf("newest complete manifest %d removed: %v", generation, err)
		}
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
