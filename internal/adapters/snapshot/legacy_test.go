package snapshot

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

func TestRepositoryLoadLegacyReportsDirectoryCloseError(t *testing.T) {
	for _, tt := range []struct {
		name      string
		failOpen  int
		operation string
	}{
		{name: "pending deletion markers", failOpen: 1, operation: "close legacy deletion marker directory"},
		{name: "legacy snapshots", failOpen: 2, operation: "close legacy snapshot directory"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			closeErr := errors.New("injected directory close failure")
			repo := NewRepository(privateDir(t))
			openCalls := 0
			repo.hooks.openLegacyDirectory = func(string) (legacyDirectory, error) {
				openCalls++
				if openCalls == tt.failOpen {
					return fakeLegacyDirectory{closeErr: closeErr}, nil
				}
				return fakeLegacyDirectory{}, nil
			}

			_, err := repo.LoadLegacy(context.Background())
			if !errors.Is(err, closeErr) {
				t.Fatalf("LoadLegacy error = %v, want close error", err)
			}
			if !strings.Contains(err.Error(), tt.operation) {
				t.Fatalf("LoadLegacy error = %v, want %q", err, tt.operation)
			}
		})
	}
}

type fakeLegacyDirectory struct {
	closeErr error
}

func (d fakeLegacyDirectory) ReadDir(int) ([]os.DirEntry, error) { return nil, io.EOF }
func (d fakeLegacyDirectory) Close() error                       { return d.closeErr }

func TestRepositoryLegacyRejectsTooManyAndTooLargeAggregateSnapshots(t *testing.T) {
	t.Run("count", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		if err := os.Mkdir(repo.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < maxLegacySnapshotFiles+1; i++ {
			if err := os.WriteFile(filepath.Join(repo.dir, fmt.Sprintf("%03d.snap", i)), []byte("legacy"), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := repo.LoadLegacy(context.Background()); err == nil || !strings.Contains(err.Error(), "too many") {
			t.Fatalf("LoadLegacy error = %v, want actionable count limit", err)
		}
	})
	t.Run("aggregate bytes", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		if err := os.Mkdir(repo.dir, 0o700); err != nil {
			t.Fatal(err)
		}
		for i, size := range []int{maxLegacySnapshotBytes / 2, maxLegacySnapshotBytes/2 + 1} {
			if err := os.WriteFile(filepath.Join(repo.dir, fmt.Sprintf("%d.snap", i)), make([]byte, size), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := repo.LoadLegacy(context.Background()); err == nil || !strings.Contains(err.Error(), "aggregate") {
			t.Fatalf("LoadLegacy error = %v, want actionable aggregate limit", err)
		}
	})
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
	syncCalls := 0
	repo.hooks.syncDirectory = func(string) error {
		syncCalls++
		if syncCalls == 1 {
			return injected
		}
		return nil
	}
	if err := repo.DeleteLegacy(context.Background(), "named"); !errors.Is(err, injected) {
		t.Fatalf("DeleteLegacy error = %v, want root sync error", err)
	}
	if err := repo.DeleteLegacy(context.Background(), "named"); err != nil {
		t.Fatalf("retry DeleteLegacy: %v", err)
	}
	if syncCalls != 4 {
		t.Fatalf("sync calls = %d, want retry to sync absent file", syncCalls)
	}
}
