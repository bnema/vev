package snapshot

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

func TestWriteImmutableUsesOneFinalDirectorySync(t *testing.T) {
	repo := NewRepository(privateDir(t))
	dir := filepath.Join(repo.dir, "immutable")
	if err := repo.ensurePrivateDirectory(dir); err != nil {
		t.Fatal(err)
	}

	syncs := 0
	repo.hooks.syncDirectory = func(got string) error {
		if got == dir {
			syncs++
		}
		return nil
	}
	data := []byte("immutable")
	if err := repo.writeImmutable(filepath.Join(dir, "object"), data, func(existing []byte) error {
		if !bytes.Equal(existing, data) {
			return errors.New("unexpected immutable data")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if syncs != 1 {
		t.Fatalf("successful immutable directory syncs = %d, want 1", syncs)
	}
}

func TestWithAtomicTempPreservesLifecycleOrder(t *testing.T) {
	repo := NewRepository(privateDir(t))
	dir := filepath.Join(repo.dir, "temporary")
	if err := repo.ensurePrivateDirectory(dir); err != nil {
		t.Fatal(err)
	}

	var events []string
	repo.hooks.createTemp = func(string) error { events = append(events, "create"); return nil }
	repo.hooks.writeTemp = func(string) error { events = append(events, "write"); return nil }
	repo.hooks.syncFile = func(string) error { events = append(events, "sync file"); return nil }
	repo.hooks.closeFile = func(string) error { events = append(events, "close"); return nil }
	repo.hooks.remove = func(string) error { events = append(events, "remove"); return nil }
	repo.hooks.syncDirectory = func(string) error { events = append(events, "sync directory"); return nil }

	if err := repo.withAtomicTemp(dir, []byte("data"), func(path string) (bool, error) {
		events = append(events, "publish")
		info, err := os.Stat(path)
		if err != nil {
			return true, err
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("temporary file mode = %o, want 600", got)
		}
		return true, nil
	}); err != nil {
		t.Fatal(err)
	}
	want := []string{"create", "write", "sync file", "close", "publish", "remove", "sync directory"}
	if len(events) != len(want) {
		t.Fatalf("lifecycle = %v, want %v", events, want)
	}
	for i := range want {
		if events[i] != want[i] {
			t.Fatalf("lifecycle = %v, want %v", events, want)
		}
	}
}

func TestInstallImmutableJoinsPrimaryAndParentCloseErrors(t *testing.T) {
	repo := NewRepository(privateDir(t))
	dir := filepath.Join(repo.dir, "objects")
	if err := repo.ensurePrivateDirectory(dir); err != nil {
		t.Fatal(err)
	}
	oldPath := filepath.Join(dir, "old")
	newPath := filepath.Join(dir, "new")
	if err := os.WriteFile(oldPath, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(newPath, []byte("new"), 0o600); err != nil {
		t.Fatal(err)
	}
	closeCause := errors.New("injected source parent close failure")
	repo.hooks.closeDescriptor = func(operation string) error {
		if operation == "close source snapshot parent directory" {
			return closeCause
		}
		return nil
	}

	err := repo.installImmutable(oldPath, newPath)
	if !errors.Is(err, syscall.EEXIST) || !errors.Is(err, closeCause) {
		t.Fatalf("installImmutable error = %v, want link and close failures", err)
	}
	if !strings.Contains(err.Error(), "close source snapshot parent directory") {
		t.Fatalf("installImmutable error = %v, want close operation context", err)
	}
}

func TestWriteMutableDoesNotRemoveConsumedTemporaryFile(t *testing.T) {
	repo := NewRepository(privateDir(t))
	path := filepath.Join(repo.dir, "HEAD")
	repo.hooks.remove = func(string) error { return errors.New("remove must not run") }

	if err := repo.writeMutable(path, []byte("head")); err != nil {
		t.Fatalf("writeMutable error = %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, []byte("head")) {
		t.Fatalf("HEAD = %q, want %q", got, "head")
	}
}
