package snapshot

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestRepositoryLegacyReadsOnlyBoundedRootSnapshots(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.MkdirAll(filepath.Join(repo.dir, repositorySessionsDir), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, "named.snap"), []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, filenameForName("unsafe/name")), []byte("hashed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, repositorySessionsDir, "ignored.snap"), []byte("incremental"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := repo.LoadLegacy(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Name != "" || got[1].Name != "named" {
		t.Fatalf("LoadLegacy = %#v, want root safe and hashed snapshots", got)
	}
}

func TestRepositoryDeleteLegacyMissingFileDoesNotRequireRoot(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := repo.DeleteLegacy(context.Background(), "missing"); err != nil {
		t.Fatalf("DeleteLegacy missing file: %v", err)
	}
}

func TestRepositoryDeleteLegacySurfacesRootSyncAndCanRetry(t *testing.T) {
	repo := NewRepository(privateDir(t))
	if err := os.Mkdir(repo.dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(repo.dir, filenameForName("named")), []byte("legacy"), 0o600); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("root sync")
	repo.hooks.syncDirectory = func(string) error { return injected }
	if err := repo.DeleteLegacy(context.Background(), "named"); !errors.Is(err, injected) {
		t.Fatalf("DeleteLegacy error = %v, want root sync error", err)
	}
	repo.hooks.syncDirectory = nil
	if err := repo.DeleteLegacy(context.Background(), "named"); err != nil {
		t.Fatalf("retry DeleteLegacy: %v", err)
	}
}
