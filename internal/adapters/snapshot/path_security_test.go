package snapshot

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
			if err := repo.Maintain(context.Background()); err == nil {
				t.Fatal("Maintain succeeded through symlinked repository component")
			}
			got, err := os.ReadFile(guard)
			if err != nil || string(got) != "guard" {
				t.Fatalf("outside guard = %q, %v", got, err)
			}
		})
	}
}

func TestFinalSymlinksRejectedByRootOperations(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.ensurePrivateDirectory(repo.dir); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(repo.dir, "target")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(repo.dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	if _, err := repo.openDirectory(link); !errors.Is(err, syscall.ELOOP) || strings.Contains(err.Error(), repo.dir) {
		t.Fatalf("openDirectory error = %v, want sanitized ELOOP", err)
	}
	if _, err := repo.readBounded(link); !errors.Is(err, syscall.ELOOP) || strings.Contains(err.Error(), repo.dir) {
		t.Fatalf("readBounded error = %v, want sanitized ELOOP", err)
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
