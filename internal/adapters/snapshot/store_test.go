package snapshot

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"testing"
)

func TestStoreWriteLoadRoundTripAndSupersede(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := NewStore(dir)

	if err := store.Write("alpha", []byte("first")); err != nil {
		t.Fatalf("Write first: %v", err)
	}
	if err := store.Write("alpha", []byte("second")); err != nil {
		t.Fatalf("Write second: %v", err)
	}

	got := loadMap(t, store)
	if string(got["alpha"]) != "second" {
		t.Fatalf("loaded alpha = %q, want second", got["alpha"])
	}
	info, err := os.Stat(filepath.Join(dir, "alpha.snap"))
	if err != nil {
		t.Fatalf("stat snapshot: %v", err)
	}
	if gotMode := info.Mode().Perm(); gotMode != 0o600 {
		t.Fatalf("snapshot mode = %v, want 0600", gotMode)
	}
	if dirInfo, err := os.Stat(dir); err != nil {
		t.Fatalf("stat dir: %v", err)
	} else if gotMode := dirInfo.Mode().Perm(); gotMode != 0o700 {
		t.Fatalf("dir mode = %v, want 0700", gotMode)
	}
}

func TestStoreDelete(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := NewStore(dir)

	if err := store.Write("gone", []byte("data")); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := store.Delete("gone"); err != nil {
		t.Fatalf("Delete existing: %v", err)
	}
	if err := store.Delete("gone"); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}
	if got := loadMap(t, store); len(got) != 0 {
		t.Fatalf("Load after delete = %#v, want empty", got)
	}
}

func TestStoreUnsafeNameUsesDeterministicHash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := NewStore(dir)
	name := "../unsafe/session name"

	if err := store.Write(name, []byte("raw")); err != nil {
		t.Fatalf("Write unsafe: %v", err)
	}
	sum := sha256.Sum256([]byte(name))
	wantBase := "@" + hex.EncodeToString(sum[:])[:40] + ".snap"
	if _, err := os.Stat(filepath.Join(dir, wantBase)); err != nil {
		t.Fatalf("stat hashed snapshot %q: %v", wantBase, err)
	}
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(loaded) != 1 || loaded[0].Name != "" || string(loaded[0].Data) != "raw" {
		t.Fatalf("Load unsafe = %#v, want one nameless raw blob", loaded)
	}
}

func TestStoreMissingDirAndStaleTmpCleanup(t *testing.T) {
	t.Parallel()
	dir := filepath.Join(t.TempDir(), "missing")
	store := NewStore(dir)

	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load missing dir: %v", err)
	}
	if len(loaded) != 0 {
		t.Fatalf("Load missing dir len = %d, want 0", len(loaded))
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".tmp-stale"), []byte("tmp"), 0o600); err != nil {
		t.Fatalf("write stale tmp: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "keep.txt"), []byte("ignore"), 0o600); err != nil {
		t.Fatalf("write ignored: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "beta.snap"), []byte("beta"), 0o600); err != nil {
		t.Fatalf("write snap: %v", err)
	}

	got := loadMap(t, store)
	if string(got["beta"]) != "beta" || len(got) != 1 {
		t.Fatalf("Load = %#v, want only beta", got)
	}
	if _, err := os.Stat(filepath.Join(dir, ".tmp-stale")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale tmp stat err = %v, want not exist", err)
	}
}

func TestStoreLoadRejectsOversizedFileBeforeRead(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	store := NewStore(dir)
	if err := os.WriteFile(filepath.Join(dir, "ok.snap"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	tooLarge := filepath.Join(dir, "huge.snap")
	f, err := os.Create(tooLarge)
	if err != nil {
		t.Fatalf("create huge: %v", err)
	}
	if err := f.Truncate(maxSnapshotFileSize + 1); err != nil {
		_ = f.Close()
		t.Fatalf("truncate huge: %v", err)
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close huge: %v", err)
	}

	got := loadMap(t, store)
	if len(got) != 1 || string(got["ok"]) != "ok" {
		t.Fatalf("Load = %#v, want only ok", got)
	}
}

func TestStoreLoadSkipsUnreadableFile(t *testing.T) {
	if runtime.GOOS == "windows" || os.Geteuid() == 0 {
		t.Skip("permission bits do not make file unreadable on this platform/user")
	}
	t.Parallel()
	dir := t.TempDir()
	store := NewStore(dir)
	if err := os.WriteFile(filepath.Join(dir, "ok.snap"), []byte("ok"), 0o600); err != nil {
		t.Fatalf("write ok: %v", err)
	}
	bad := filepath.Join(dir, "bad.snap")
	if err := os.WriteFile(bad, []byte("bad"), 0o000); err != nil {
		t.Fatalf("write bad: %v", err)
	}
	t.Cleanup(func() { _ = os.Chmod(bad, 0o600) })

	got := loadMap(t, store)
	if len(got) != 1 || string(got["ok"]) != "ok" {
		t.Fatalf("Load = %#v, want only ok", got)
	}
}

func loadMap(t *testing.T, store *Store) map[string][]byte {
	t.Helper()
	loaded, err := store.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	sort.Slice(loaded, func(i, j int) bool { return strings.Compare(loaded[i].Name, loaded[j].Name) < 0 })
	out := make(map[string][]byte, len(loaded))
	for _, blob := range loaded {
		out[blob.Name] = blob.Data
	}
	return out
}
