package snapshot

import (
	"errors"
	"path/filepath"
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
		if !equalBytes(existing, data) {
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
