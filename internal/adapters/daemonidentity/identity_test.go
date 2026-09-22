package daemonidentity

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadOrCreateStableAndIncarnationFresh(t *testing.T) {
	root := t.TempDir()
	first, err := LoadOrCreate(root)
	if err != nil {
		t.Fatal(err)
	}
	second, err := LoadOrCreate(root)
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("identity changed: %q != %q", first, second)
	}
	a, err := NewIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewIncarnation()
	if err != nil {
		t.Fatal(err)
	}
	if a.IsZero() || b.IsZero() || a == b {
		t.Fatalf("incarnations not fresh: %x %x", a, b)
	}
}

func TestLoadFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(string) error
	}{
		{"corrupt", func(root string) error {
			if err := os.Mkdir(filepath.Dir(Path(root)), 0o700); err != nil {
				return err
			}
			return os.WriteFile(Path(root), []byte("bad"), 0o600)
		}},
		{"permissions", func(root string) error {
			if err := os.Mkdir(filepath.Dir(Path(root)), 0o700); err != nil {
				return err
			}
			return os.WriteFile(Path(root), []byte("00112233445566778899aabbccddeeff"), 0o644)
		}},
		{"symlink", func(root string) error {
			if err := os.Mkdir(filepath.Dir(Path(root)), 0o700); err != nil {
				return err
			}
			target := filepath.Join(root, "target")
			if err := os.WriteFile(target, []byte("00112233445566778899aabbccddeeff"), 0o600); err != nil {
				return err
			}
			return os.Symlink(target, Path(root))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			if err := tc.prepare(root); err != nil {
				t.Fatal(err)
			}
			if _, err := Load(root); !errors.Is(err, ErrInvalid) {
				t.Fatalf("error=%v, want ErrInvalid", err)
			}
		})
	}
}

func TestLoadAbsentDoesNotCreate(t *testing.T) {
	root := filepath.Join(t.TempDir(), "absent")
	if _, err := Load(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("error=%v", err)
	}
	if _, err := os.Stat(root); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("read created state: %v", err)
	}
}
