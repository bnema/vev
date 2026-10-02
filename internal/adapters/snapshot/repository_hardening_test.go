package snapshot

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
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

func TestReadSizedFile(t *testing.T) {
	const limit = 16
	tests := []struct {
		name     string
		content  string
		hintSize int
		want     string
	}{
		{name: "empty", content: "", hintSize: 0, want: ""},
		{name: "exact size hint", content: "snapshot object", hintSize: 15, want: "snapshot object"},
		{name: "file grew after stat", content: "snapshot", hintSize: 4, want: "snapshot"},
		{name: "file shrank after stat", content: "tiny", hintSize: 64, want: "tiny"},
		{name: "file grew past limit stops one byte over", content: "snapshot object grew far", hintSize: 4, want: "snapshot object g"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readSizedFile(strings.NewReader(tt.content), tt.hintSize, limit)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != tt.want {
				t.Fatalf("readSizedFile = %q, want %q", got, tt.want)
			}
		})
	}

	// An exact size hint must cost one buffer, unlike io.ReadAll's growth.
	content := strings.Repeat("x", 1<<20)
	allocs := testing.AllocsPerRun(10, func() {
		_, _ = readSizedFile(strings.NewReader(content), len(content), maxRepositoryRead)
	})
	if allocs > 3 {
		t.Fatalf("readSizedFile allocations = %v, want at most 3", allocs)
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
