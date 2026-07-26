package snapshot

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func TestDiscardCrashMatrix(t *testing.T) {
	ctx := context.Background()
	old := domain.IncarnationID{1}
	replacement := domain.IncarnationID{2}
	record := domain.CatalogueRecord{
		Name: "broken", IncarnationID: old, RecoveryState: domain.RecoveryDegraded,
		Committed: &domain.CheckpointRef{Generation: 1, ManifestDigest: [32]byte{1}}, DegradedReason: "corrupt",
	}
	descriptor := domain.QuarantineDescriptor{
		OldRecord: record, OldIncarnation: old, ReplacementIncarnation: replacement,
		SessionName: record.Name, Reason: "explicit discard",
	}

	t.Run("descriptor is immutable and idempotent", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		if err := repo.SaveQuarantineDescriptor(ctx, descriptor); err != nil {
			t.Fatal(err)
		}
		if err := repo.SaveQuarantineDescriptor(ctx, descriptor); err != nil {
			t.Fatalf("idempotent retry: %v", err)
		}
		path := filepath.Join(repo.dir, "quarantine", old.String(), "record")
		encoded, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		got, err := decodeQuarantineDescriptor(encoded)
		if err != nil || !reflect.DeepEqual(got, descriptor) {
			t.Fatalf("descriptor round trip = %#v, %v", got, err)
		}
		changed := descriptor
		changed.Reason = "different"
		if err := repo.SaveQuarantineDescriptor(ctx, changed); err == nil {
			t.Fatal("descriptor overwrite accepted")
		}
		if err := os.WriteFile(path, append(encoded, 0), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := repo.SaveQuarantineDescriptor(ctx, descriptor); err == nil {
			t.Fatal("malformed existing descriptor accepted")
		}
	})

	for _, boundary := range []string{"descriptor-file-sync", "descriptor-install", "descriptor-directory-sync"} {
		t.Run(boundary, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			dir := filepath.Join(repo.dir, "quarantine", old.String())
			if err := repo.ensurePrivateDirectory(dir); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected descriptor durability failure")
			switch boundary {
			case "descriptor-file-sync":
				repo.hooks.syncFile = func(string) error { return injected }
			case "descriptor-install":
				repo.hooks.installImmutable = func(string) error { return injected }
			case "descriptor-directory-sync":
				repo.hooks.syncDirectory = func(path string) error {
					if path == dir {
						return injected
					}
					return nil
				}
			}
			if err := repo.SaveQuarantineDescriptor(ctx, descriptor); !errors.Is(err, injected) {
				t.Fatalf("SaveQuarantineDescriptor() error = %v", err)
			}
			repo.hooks = repositoryHooks{}
			if err := repo.SaveQuarantineDescriptor(ctx, descriptor); err != nil {
				t.Fatalf("retry: %v", err)
			}
		})
	}

	for _, boundary := range []string{"rename", "source-directory-sync", "destination-directory-sync"} {
		t.Run(boundary, func(t *testing.T) {
			repo := NewRepository(privateDir(t))
			source := repo.sessionPath(old)
			destinationDir := filepath.Join(repo.dir, "quarantine", old.String())
			if err := repo.ensurePrivateDirectory(source); err != nil {
				t.Fatal(err)
			}
			if err := repo.ensurePrivateDirectory(destinationDir); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(source, "payload"), []byte("uncertain"), 0o600); err != nil {
				t.Fatal(err)
			}
			injected := errors.New("injected quarantine durability failure")
			switch boundary {
			case "rename":
				repo.hooks.rename = func(string) error { return injected }
			case "source-directory-sync":
				repo.hooks.syncDirectory = func(path string) error {
					if path == filepath.Dir(source) {
						return injected
					}
					return nil
				}
			case "destination-directory-sync":
				repo.hooks.syncDirectory = func(path string) error {
					if path == destinationDir {
						return injected
					}
					return nil
				}
			}
			if err := repo.QuarantineIncarnation(ctx, old); !errors.Is(err, injected) {
				t.Fatalf("QuarantineIncarnation() error = %v", err)
			}
			repo.hooks = repositoryHooks{}
			if err := repo.QuarantineIncarnation(ctx, old); err != nil {
				t.Fatalf("retry: %v", err)
			}
			got, err := os.ReadFile(filepath.Join(destinationDir, "snapshot", "payload"))
			if err != nil || string(got) != "uncertain" {
				t.Fatalf("quarantined payload = %q, %v", got, err)
			}
		})
	}

	t.Run("quarantine never removes source data", func(t *testing.T) {
		repo := NewRepository(privateDir(t))
		source := repo.sessionPath(old)
		if err := repo.ensurePrivateDirectory(source); err != nil {
			t.Fatal(err)
		}
		payload := filepath.Join(source, "payload")
		if err := os.WriteFile(payload, []byte("uncertain"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := repo.SaveQuarantineDescriptor(ctx, descriptor); err != nil {
			t.Fatal(err)
		}
		if err := repo.QuarantineIncarnation(ctx, old); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(repo.dir, "quarantine", old.String(), "snapshot", "payload")
		got, err := os.ReadFile(target)
		if err != nil || string(got) != "uncertain" {
			t.Fatalf("quarantined payload = %q, %v", got, err)
		}
		if err := repo.QuarantineIncarnation(ctx, old); err != nil {
			t.Fatalf("idempotent retry: %v", err)
		}
	})
}

func TestQuarantineDeletionSourcesSerializesConcurrentPublication(t *testing.T) {
	repo := NewRepository(privateDir(t))
	id := domain.IncarnationID{9}
	tombstone := domain.DeletionTombstone{Name: "work", IncarnationID: id}
	publication := incarnationPublication(t, id, "work", 1, nil)
	if err := repo.Publish(context.Background(), publication); err != nil {
		t.Fatal(err)
	}

	quarantined := make(chan struct{})
	release := make(chan struct{})
	repo.hooks.afterDeletionIncarnationQuarantine = func() error {
		close(quarantined)
		<-release
		return nil
	}
	quarantineDone := make(chan error, 1)
	go func() { quarantineDone <- repo.QuarantineDeletionSources(context.Background(), tombstone, false) }()
	<-quarantined

	publishAttempted := make(chan struct{})
	repo.hooks.beforeSessionLock = func(string) { close(publishAttempted) }
	publishDone := make(chan error, 1)
	go func() { publishDone <- repo.Publish(context.Background(), publication) }()
	select {
	case <-publishAttempted:
	case <-time.After(time.Second):
		t.Fatal("Publish did not attempt the incarnation lock")
	}
	select {
	case err := <-publishDone:
		t.Fatalf("Publish completed while quarantine held the incarnation lock: %v", err)
	case <-time.After(25 * time.Millisecond):
	}
	close(release)
	if err := <-quarantineDone; err != nil {
		t.Fatal(err)
	}
	if err := <-publishDone; err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(repo.headPath(id)); err != nil {
		t.Fatalf("republished HEAD: %v", err)
	}
	if _, err := os.Stat(filepath.Join(repo.dir, "quarantine", id.String(), "snapshot", repositoryHead)); err != nil {
		t.Fatalf("quarantined HEAD: %v", err)
	}
}

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
	for _, test := range []struct {
		name   string
		mutate func([]byte) []byte
	}{
		{name: "wrong magic", mutate: func(data []byte) []byte { data[0] ^= 0xff; return data }},
		{name: "wrong version", mutate: func(data []byte) []byte { data[5]++; return data }},
		{name: "invalid name", mutate: func(data []byte) []byte { copy(data[8:12], "../x"); return data }},
		{name: "zero ID", mutate: func(data []byte) []byte { clear(data[len(data)-16:]); return data }},
	} {
		t.Run(test.name, func(t *testing.T) {
			malformed := test.mutate(append([]byte(nil), encoded...))
			if _, err := decodeDeletionTombstone(malformed); err == nil {
				t.Fatal("malformed tombstone accepted")
			}
		})
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

func TestDeletionTombstoneListingResumesAcrossRepositoryRecreation(t *testing.T) {
	dir := privateDir(t)
	ctx := context.Background()
	want := []domain.DeletionTombstone{
		{Name: "first", IncarnationID: domain.IncarnationID{15: 1}},
		{Name: "second", IncarnationID: domain.IncarnationID{15: 2}},
		{Name: "third", IncarnationID: domain.IncarnationID{15: 3}},
	}
	repo := NewRepository(dir)
	for _, index := range []int{2, 0, 1} {
		if err := repo.WriteDeletionTombstone(ctx, want[index]); err != nil {
			t.Fatal(err)
		}
	}

	var got []domain.DeletionTombstone
	cursor := ports.DeletionTombstoneCursor{}
	reads := 0
	for calls := 0; ; calls++ {
		if calls == 32 {
			t.Fatal("listing did not complete")
		}
		// A new Repository has no in-memory scan state. Every continuation must
		// therefore carry everything needed to resume the bounded directory scan.
		repo = NewRepository(dir)
		repo.hooks.beforeDeletionTombstoneRead = func(string) { reads++ }
		page, err := repo.ListDeletionTombstones(ctx, cursor, ports.MaintenanceBudget{Entries: 1, Bytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page.Tombstones...)
		if page.Done {
			break
		}
		if page.Next.After == "" || page.Next.After == cursor.After {
			t.Fatalf("continuation did not advance: before=%q after=%q", cursor.After, page.Next.After)
		}
		cursor = page.Next
	}
	if !slices.Equal(got, want) {
		t.Fatalf("tombstones = %#v, want %#v", got, want)
	}
	if reads != len(want) {
		t.Fatalf("tombstone object reads = %d, want %d", reads, len(want))
	}
}

func TestDeletionTombstoneListingBatchesSortedCandidatesWithinScanBudget(t *testing.T) {
	dir := privateDir(t)
	repo := NewRepository(dir)
	ctx := context.Background()
	want := make([]domain.DeletionTombstone, 5)
	for i := range want {
		want[i] = domain.DeletionTombstone{Name: fmt.Sprintf("session-%d", i), IncarnationID: domain.IncarnationID{15: byte(i + 1)}}
	}
	for _, i := range []int{4, 1, 3, 0, 2} {
		if err := repo.WriteDeletionTombstone(ctx, want[i]); err != nil {
			t.Fatal(err)
		}
	}

	cursor := ports.DeletionTombstoneCursor{}
	var got []domain.DeletionTombstone
	reads := 0
	for calls := range 64 {
		repo = NewRepository(dir)
		repo.hooks.beforeDeletionTombstoneRead = func(string) { reads++ }
		entryBudget := uint64(2)
		if calls > 0 {
			entryBudget = 1 // A resumed caller may reduce its per-pass budget.
		}
		page, err := repo.ListDeletionTombstones(ctx, cursor, ports.MaintenanceBudget{Entries: entryBudget, Bytes: 4096})
		if err != nil {
			t.Fatal(err)
		}
		if uint64(len(page.Tombstones)) > entryBudget {
			t.Fatalf("page %d returned %d tombstones over entry budget %d", calls, len(page.Tombstones), entryBudget)
		}
		got = append(got, page.Tombstones...)
		if page.Done {
			break
		}
		cursor = page.Next
	}
	if !slices.Equal(got, want) {
		t.Fatalf("tombstones = %#v, want %#v", got, want)
	}
	if reads != len(want) {
		t.Fatalf("payload reads = %d, want %d", reads, len(want))
	}
}

func TestMalformedDeletionTombstone(t *testing.T) {
	dir := privateDir(t)
	repo := NewRepository(dir)
	tombstone := domain.DeletionTombstone{Name: "work", IncarnationID: domain.IncarnationID{7}}
	if err := repo.WriteDeletionTombstone(context.Background(), tombstone); err != nil {
		t.Fatal(err)
	}
	path := repo.deletionTombstonePath(tombstone.IncarnationID)
	if err := os.WriteFile(path, []byte("malformed"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.ListDeletionTombstones(context.Background(), ports.DeletionTombstoneCursor{}, ports.MaintenanceBudget{Entries: 64, Bytes: 1024}); err == nil {
		t.Fatal("malformed tombstone was skipped")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("malformed tombstone was removed: %v", err)
	}
}

func TestDeletionTombstoneListingRejectsMalformedContinuation(t *testing.T) {
	repo := NewRepository(privateDir(t))
	ctx := context.Background()
	after := domain.IncarnationID{15: 2}.String() + ".tombstone"

	tests := []struct {
		name   string
		cursor ports.DeletionTombstoneCursor
	}{
		{name: "repository-memory token", cursor: ports.DeletionTombstoneCursor{After: "@1"}},
		{name: "non-advancing empty state", cursor: rawDeletionListingCursor(t, `{}`)},
		{name: "non-canonical key", cursor: rawDeletionListingCursor(t, `{"after":"ABC.tombstone"}`)},
		{name: "candidate does not advance", cursor: rawDeletionListingCursor(t, `{"after":"`+after+`","candidate":"`+after+`","found":1}`)},
		{name: "unknown field", cursor: rawDeletionListingCursor(t, `{"unknown":1}`)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := repo.ListDeletionTombstones(ctx, test.cursor, ports.MaintenanceBudget{Entries: 1, Bytes: 1024}); err == nil {
				t.Fatal("malformed continuation accepted")
			}
		})
	}
}

func rawDeletionListingCursor(t *testing.T, state string) ports.DeletionTombstoneCursor {
	t.Helper()
	return ports.DeletionTombstoneCursor{After: deletionListingCursorPrefix + base64.RawURLEncoding.EncodeToString([]byte(state))}
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
	if !page.Done || !slices.Equal(page.Tombstones, []domain.DeletionTombstone{first, second}) || page.Next.After == "" {
		t.Fatalf("batched page = %#v", page)
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
