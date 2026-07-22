package snapshot

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func TestRepositoryPublishDeleteManySessionsRaceFree(t *testing.T) {
	repo := NewRepository(privateDir(t))
	const sessions = 128

	publications := make([]ports.SnapshotPublication, sessions)
	for i := range publications {
		name := fmt.Sprintf("named-%03d", i)
		publications[i] = repositoryPublication(t, name, 1, []byte(name))
		if err := repo.Publish(context.Background(), publications[i]); err != nil {
			t.Fatalf("initial Publish(%q): %v", name, err)
		}
	}

	var wg sync.WaitGroup
	for _, publication := range publications {
		publication := publication
		wg.Add(2)
		go func() {
			defer wg.Done()
			if err := repo.Publish(context.Background(), publication); err != nil {
				t.Errorf("Publish(%q): %v", publication.Name, err)
			}
		}()
		go func() {
			defer wg.Done()
			if err := repo.Delete(context.Background(), publication.Name); err != nil {
				t.Errorf("Delete(%q): %v", publication.Name, err)
			}
		}()
	}
	wg.Wait()

	locks, epochs := repo.sessionStateCounts()
	if locks != 0 || epochs != 0 {
		t.Fatalf("retained session state = %d locks, %d epochs, want none", locks, epochs)
	}
}

func TestRepositoryBoundsAndEvictsPartialMaintenanceSessionState(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for _, key := range []string{"named-000", "named-001"} {
		generations := filepath.Join(repo.sessionPath(key), repositoryGenerations)
		for i := 0; i < maintenanceBatch+1; i++ {
			if err := os.MkdirAll(generations, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(generations, fmt.Sprintf("unclassified-%03d", i)), []byte("state"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := repo.Maintain(context.Background()); err != nil {
		t.Fatal(err)
	}
	locks, epochs := repo.sessionStateCounts()
	if locks != 1 || epochs != 0 {
		t.Fatalf("partial maintenance state = %d locks, %d epochs, want one lock and no epochs", locks, epochs)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := repo.Maintain(ctx); err != context.Canceled {
		t.Fatalf("canceled Maintain error = %v, want context canceled", err)
	}
	locks, epochs = repo.sessionStateCounts()
	if locks != 0 || epochs != 0 {
		t.Fatalf("session state after maintenance reset = %d locks, %d epochs, want none", locks, epochs)
	}
}

func TestRepositoryDeleteUsesDeterministicPendingQuarantine(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.Publish(context.Background(), repositoryPublication(t, "named", 1, []byte("state"))); err != nil {
		t.Fatal(err)
	}
	sessions := filepath.Join(repo.dir, repositorySessionsDir)
	for i := 0; i < maxDirectoryTraversalEntries*2; i++ {
		if err := os.Mkdir(filepath.Join(sessions, fmt.Sprintf("unrelated-%05d", i)), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	var checks atomic.Int32
	repo.hooks.beforePendingQuarantineCheck = func(string) { checks.Add(1) }
	if err := repo.Delete(context.Background(), "named"); err != nil {
		t.Fatal(err)
	}
	if got := checks.Load(); got != 1 {
		t.Fatalf("pending quarantine checks = %d, want one deterministic lookup", got)
	}
	if _, err := os.Lstat(filepath.Join(sessions, deletingSessionName(sessionKey("named")))); err != nil {
		t.Fatalf("deterministic quarantine missing: %v", err)
	}
}
