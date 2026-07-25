package snapshot

import (
	"context"
	"encoding/hex"
	"errors"
	"path/filepath"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestDeletionTombstoneCodec(t *testing.T) {
	want := domain.DeletionTombstone{Name: "work", IncarnationID: domain.IncarnationID{1}}
	encoded, err := encodeDeletionTombstone(want)
	if err != nil {
		t.Fatal(err)
	}
	if got := hex.EncodeToString(encoded); got != "5645564400010004776f726b01000000000000000000000000000000" {
		t.Fatalf("encoded tombstone = %s", got)
	}
	got, err := decodeDeletionTombstone(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("round trip = %#v", got)
	}
	for n := range len(encoded) {
		if _, err := decodeDeletionTombstone(encoded[:n]); err == nil {
			t.Fatalf("prefix %d accepted", n)
		}
	}
	if _, err := decodeDeletionTombstone(append(encoded, 0)); err == nil {
		t.Fatal("trailing byte accepted")
	}

	for _, boundary := range []string{"file-sync", "install", "directory-sync"} {
		t.Run(boundary, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			dir := filepath.Join(repo.dir, deletionTombstonesDir)
			if err := repo.ensurePrivateDirectory(dir); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected tombstone persistence failure")
			switch boundary {
			case "file-sync":
				repo.hooks.syncFile = func(string) error { return injected }
			case "install":
				repo.hooks.installImmutable = func(string) error { return injected }
			case "directory-sync":
				repo.hooks.syncDirectory = func(path string) error {
					if path == dir {
						return injected
					}
					return nil
				}
			}
			if err := repo.WriteDeletionTombstone(context.Background(), want); !errors.Is(err, injected) {
				t.Fatalf("WriteDeletionTombstone() error = %v", err)
			}
			repo.hooks = repositoryHooks{}
			if err := repo.WriteDeletionTombstone(context.Background(), want); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}
}

func TestDeletionTombstoneListing(t *testing.T) {
	repo := NewRepository(privateDir(t))
	ctx := context.Background()
	first := domain.DeletionTombstone{Name: "a", IncarnationID: domain.IncarnationID{1}}
	second := domain.DeletionTombstone{Name: "b", IncarnationID: domain.IncarnationID{2}}
	if err := repo.WriteDeletionTombstone(ctx, second); err != nil {
		t.Fatal(err)
	}
	if err := repo.WriteDeletionTombstone(ctx, first); err != nil {
		t.Fatal(err)
	}
	page, err := repo.ListDeletionTombstones(ctx, ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 3, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if page.Done || len(page.Tombstones) != 1 || page.Tombstones[0] != first || page.Next.After == "" {
		t.Fatalf("first page = %#v", page)
	}
	page, err = repo.ListDeletionTombstones(ctx, page.Next, ports.MaintenanceBudget{Entries: 3, Bytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if !page.Done || len(page.Tombstones) != 1 || page.Tombstones[0] != second || page.Next.After == "" {
		t.Fatalf("second page = %#v", page)
	}
	if _, err := repo.ListDeletionTombstones(ctx, ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 3, Bytes: 1}); err == nil {
		t.Fatal("non-advancing budget accepted")
	}

	reads := 0
	repo.hooks.beforeDeletionTombstoneRead = func(string) { reads++ }
	page, err = repo.ListDeletionTombstones(ctx, ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 1, Bytes: 1024})
	if err != nil || reads != 0 || len(page.Tombstones) != 0 || page.Next.After == "" {
		t.Fatalf("bounded scan read %d objects: page=%#v err=%v", reads, page, err)
	}

	injected := errors.New("injected directory sync failure")
	tombstoneDir := filepath.Join(repo.dir, deletionTombstonesDir)
	repo.hooks.syncDirectory = func(path string) error {
		if path == tombstoneDir {
			return injected
		}
		return nil
	}
	if err := repo.DeleteDeletionTombstone(ctx, first.IncarnationID); !errors.Is(err, injected) {
		t.Fatalf("DeleteDeletionTombstone() error = %v", err)
	}
	repo.hooks = repositoryHooks{}
	if err := repo.DeleteDeletionTombstone(ctx, first.IncarnationID); err != nil {
		t.Fatalf("DeleteDeletionTombstone() retry = %v", err)
	}
}
