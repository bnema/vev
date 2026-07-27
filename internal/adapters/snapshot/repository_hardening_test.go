package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	codec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestReadBoundedRejectsUnsafeFiles(t *testing.T) {
	dir := t.TempDir()
	regular := filepath.Join(dir, "regular")
	if err := os.WriteFile(regular, []byte("safe"), 0o600); err != nil {
		t.Fatal(err)
	}
	tooLarge := filepath.Join(dir, "large")
	f, err := os.Create(tooLarge)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxRepositoryRead + 1); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	unsafeMode := filepath.Join(dir, "mode")
	if err := os.WriteFile(unsafeMode, []byte("unsafe"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(regular, link); err != nil {
		t.Fatal(err)
	}
	fifo := filepath.Join(dir, "fifo")
	if err := syscall.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}

	for _, path := range []string{tooLarge, unsafeMode, link, fifo} {
		t.Run(filepath.Base(path), func(t *testing.T) {
			if _, err := readBounded(path); err == nil {
				t.Fatal("readBounded accepted unsafe file")
			}
		})
	}

	wrongOwner := filepath.Join(dir, "owner")
	if os.Geteuid() == 0 {
		if err := os.WriteFile(wrongOwner, []byte("unsafe"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Chown(wrongOwner, 1, -1); err != nil {
			t.Fatal(err)
		}
		if _, err := readBounded(wrongOwner); err == nil {
			t.Fatal("readBounded accepted wrong-owner file")
		}
	}
}

func TestValidatePublicationRejectsUnloadableAggregate(t *testing.T) {
	pub := repositoryPublication(t, "named", 1, []byte("state"))
	manifest, err := codec.UnmarshalManifest(pub.Manifest)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Tabs[0].Panes[0].Tail.Size = uint32(maxRepositoryRead)
	manifest.Tabs[0].Panes[0].Transcript.Size = uint32(maxRepositoryRead)
	pub.Manifest, err = codec.MarshalManifest(manifest)
	if err != nil {
		t.Fatal(err)
	}
	pub.Objects = nil // validation permits omitted objects for immutable reuse.
	if _, _, err := validatePublication(pub); err == nil {
		t.Fatal("validatePublication accepted unloadable aggregate")
	} else if errors.Is(err, os.ErrNotExist) {
		t.Fatalf("validatePublication returned unrelated error: %v", err)
	}
}
