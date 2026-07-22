package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryRejectsSymlinkedGenerationAndObjectShards(t *testing.T) {
	for _, target := range []string{repositoryGenerations, repositoryObjectsDir} {
		t.Run(target, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			publication := repositoryPublication(t, "named", 1, []byte("state"))
			if err := repo.Publish(context.Background(), publication); err != nil {
				t.Fatal(err)
			}
			key := sessionKey(publication.Name)
			inside := filepath.Join(repo.sessionPath(key), target)
			outside := t.TempDir()
			guard := filepath.Join(outside, "must-not-change")
			if err := os.WriteFile(guard, []byte("guard"), 0o600); err != nil {
				t.Fatal(err)
			}
			if err := os.Rename(inside, inside+"-real"); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, inside); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.Load(context.Background(), publication.Name); err == nil {
				t.Fatal("Load succeeded through symlinked repository component")
			}
			// Maintenance must reject the replacement rather than following it.
			_ = repo.Maintain(context.Background())
			got, err := os.ReadFile(guard)
			if err != nil || string(got) != "guard" {
				t.Fatalf("outside guard = %q, %v", got, err)
			}
		})
	}
}

func TestRepositoryLoadLegacyChargesUnrelatedRootEntries(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.MkdirAll(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i <= maxDirectoryTraversalEntries; i++ {
		if err := os.WriteFile(filepath.Join(repo.dir, fmt.Sprintf("unrelated-%05d", i)), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := repo.LoadLegacy(context.Background()); !errors.Is(err, ErrDirectoryTraversalBudget) {
		t.Fatalf("LoadLegacy error = %v, want traversal budget error", err)
	}
}
