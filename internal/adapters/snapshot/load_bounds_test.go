package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryListStopsAtTraversalBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	sessions := filepath.Join(repo.dir, repositorySessionsDir)
	if err := os.MkdirAll(sessions, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxDirectoryTraversalEntries+1; i++ {
		if err := os.Mkdir(filepath.Join(sessions, fmt.Sprintf("hostile-%05d", i)), 0o700); err != nil {
			t.Fatal(err)
		}
	}

	_, err := repo.List(context.Background())
	if !errors.Is(err, ErrDirectoryTraversalBudget) {
		t.Fatalf("List error = %v, want directory traversal budget", err)
	}
}

func TestRepositoryLoadStopsAtGenerationTraversalBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	key := sessionKey("named")
	generations := filepath.Join(repo.sessionPath(key), repositoryGenerations)
	if err := os.MkdirAll(generations, 0o700); err != nil {
		t.Fatal(err)
	}
	for generation := uint64(1); generation <= uint64(maxDirectoryTraversalEntries+1); generation++ {
		if err := os.WriteFile(filepath.Join(generations, generationFilename(generation)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	_, err := repo.Load(context.Background(), "named")
	if !errors.Is(err, ErrDirectoryTraversalBudget) {
		t.Fatalf("Load error = %v, want directory traversal budget", err)
	}
}

func TestRepositoryLoadFallsBackNewestToOldestWithoutGenerationEnumeration(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= 3; generation++ {
		if err := repo.Publish(context.Background(), repositoryPublication(t, "named", generation, []byte(fmt.Sprintf("state-%d", generation)))); err != nil {
			t.Fatal(err)
		}
	}
	key := sessionKey("named")
	publication := repositoryPublication(t, "named", 3, []byte("state-3"))
	if err := os.Remove(repo.objectPath(key, publication.Objects[0].Digest)); err != nil {
		t.Fatal(err)
	}

	got, err := repo.Load(context.Background(), "named")
	if err != nil {
		t.Fatal(err)
	}
	if got.Generation != 2 || !got.Fallback {
		t.Fatalf("Load = generation %d fallback %v, want generation 2 fallback true", got.Generation, got.Fallback)
	}
}

func TestRepositoryMaintainDoesNotCollectBeyondRetainedMetadataBudget(t *testing.T) {
	repo := NewRepository(privateDir(t))
	for generation := uint64(1); generation <= uint64(maxMaintenanceMarkedGenerations+1); generation++ {
		if err := repo.Publish(context.Background(), repositoryPublication(t, "named", generation, []byte(fmt.Sprintf("state-%d", generation)))); err != nil {
			t.Fatal(err)
		}
	}
	for pass := 0; pass < 3; pass++ {
		if err := repo.Maintain(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := os.Lstat(repo.manifestPath(sessionKey("named"), 1)); err != nil {
		t.Fatalf("old manifest collected after mark budget overflow: %v", err)
	}
	if state := repo.maintenanceSessions[sessionKey("named")]; state != nil {
		t.Fatalf("overflow maintenance state retained = %#v, want reset", state)
	}
}

func TestMaintenanceMarkStateIsCapped(t *testing.T) {
	references := &sessionMaintenance{marked: make(map[uint64]manifestMaintenance)}
	if !references.canRetainManifest(maxMaintenanceReferences) {
		t.Fatal("state rejected references at its documented ceiling")
	}
	if references.canRetainManifest(1) {
		t.Fatal("state accepted references beyond its documented ceiling")
	}
	generations := &sessionMaintenance{marked: make(map[uint64]manifestMaintenance)}
	for generation := uint64(1); generation <= maxMaintenanceMarkedGenerations; generation++ {
		generations.marked[generation] = manifestMaintenance{}
	}
	if generations.canRetainManifest(0) {
		t.Fatal("state accepted generations beyond its documented ceiling")
	}
}
