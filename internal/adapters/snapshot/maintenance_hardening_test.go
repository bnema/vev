package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryMaintainBoundsCursorsAcrossHostileObjectShards(t *testing.T) {
	repo := NewRepository(privateDir(t))
	key := sessionKey("named")
	objects := filepath.Join(repo.sessionPath(key), repositoryObjectsDir)
	const shards = maintenanceBatch * 2
	for i := 0; i < shards; i++ {
		shard := filepath.Join(objects, fmt.Sprintf("%03d", i))
		if err := os.MkdirAll(shard, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(shard, ".tmp-stale"), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
		// Force a resumable cursor in every attacker-controlled shard.
		for j := 0; j < maintenanceBatch+1; j++ {
			if err := os.WriteFile(filepath.Join(shard, fmt.Sprintf("live-%03d", j)), []byte("live"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}

	for pass := 0; pass < shards*4; pass++ {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if got := len(repo.maintenanceCursors); got > maxMaintenanceCursors {
			t.Fatalf("retained maintenance cursors = %d, want at most %d", got, maxMaintenanceCursors)
		}
	}
	for i := 0; i < shards; i++ {
		_, err := os.Lstat(filepath.Join(objects, fmt.Sprintf("%03d", i), ".tmp-stale"))
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("stale temporary object in shard %d = %v, want removed", i, err)
		}
	}
}

func TestRepositoryMaintainBoundsAndResumesDeepWideQuarantine(t *testing.T) {
	repo := NewRepository(privateDir(t))
	quarantine := filepath.Join(repo.dir, repositorySessionsDir, ".deleting-hostile")
	deep := quarantine
	for i := 0; i < maintenanceBatch*2; i++ {
		deep = filepath.Join(deep, "d")
	}
	leaf := filepath.Join(deep, "leaf")
	if err := os.MkdirAll(filepath.Dir(leaf), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(leaf, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maintenanceBatch*2; i++ {
		if err := os.WriteFile(filepath.Join(quarantine, fmt.Sprintf("wide-%03d", i)), []byte("stale"), 0o600); err != nil {
			t.Fatal(err)
		}
	}

	work := 0
	repo.hooks.beforeMaintenanceWork = func(string) { work++ }
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	if work == 0 || work > maintenanceBatch {
		t.Fatalf("quarantine work in one maintenance call = %d, want 1..%d", work, maintenanceBatch)
	}
	if _, err := os.Lstat(leaf); err != nil {
		t.Fatalf("deep quarantine leaf removed despite bounded traversal: %v", err)
	}

	for pass := 0; pass < 1000; pass++ {
		work = 0
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
		if work > maintenanceBatch {
			t.Fatalf("quarantine work in pass %d = %d, want at most %d", pass, work, maintenanceBatch)
		}
		if _, err := os.Lstat(quarantine); errors.Is(err, os.ErrNotExist) {
			return
		}
	}
	t.Fatal("deep and wide quarantine was not eventually removed")
}
