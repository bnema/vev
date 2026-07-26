package snapshot

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestWriteImmutablePublishesAndReusesIdenticalData(t *testing.T) {
	repo := NewRepository(privateDir(t))
	dir := filepath.Join(repo.dir, "immutable")
	if err := repo.ensurePrivateDirectory(dir); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(dir, "object")
	data := []byte("immutable")
	verify := func(existing []byte) error {
		if !bytes.Equal(existing, data) {
			return errors.New("unexpected immutable data")
		}
		return nil
	}
	if err := repo.writeImmutable(path, data, verify); err != nil {
		t.Fatal(err)
	}
	if err := repo.writeImmutable(path, data, verify); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("immutable data = %q, want %q", got, data)
	}
}

func TestWithAtomicTempRemovesPublishedTemporaryFile(t *testing.T) {
	repo := NewRepository(privateDir(t))
	dir := filepath.Join(repo.dir, "temporary")
	if err := repo.ensurePrivateDirectory(dir); err != nil {
		t.Fatal(err)
	}

	var temporary string
	if err := repo.withAtomicTemp(dir, []byte("data"), func(path string) (bool, error) {
		temporary = path
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
	if _, err := os.Stat(temporary); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary file remains after publication: %v", err)
	}
}

func TestWriteMutableConsumesTemporaryFile(t *testing.T) {
	repo := NewRepository(privateDir(t))
	path := filepath.Join(repo.dir, "HEAD")

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
	matches, err := filepath.Glob(filepath.Join(repo.dir, ".tmp-*"))
	if err != nil || len(matches) != 0 {
		t.Fatalf("temporary files after mutable write = %v, glob error = %v", matches, err)
	}
}
