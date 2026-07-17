package ipc

import (
	"os"
	"path/filepath"
	"testing"
)

const darwinUnixSocketPathMax = 103

func shortSocketDir(t *testing.T, nested ...string) string {
	t.Helper()

	root, err := os.MkdirTemp("/tmp", "v")
	if err != nil {
		t.Fatalf("create short socket test root: %v", err)
	}
	t.Cleanup(func() {
		if err := os.RemoveAll(root); err != nil {
			t.Errorf("remove short socket test root %q: %v", root, err)
		}
	})

	dir := filepath.Join(append([]string{root}, nested...)...)
	assertDarwinSafeSocketPath(t, filepath.Join(dir, "daemon.sock"))
	return dir
}

func assertDarwinSafeSocketPath(t *testing.T, path string) {
	t.Helper()

	if len(path) >= darwinUnixSocketPathMax {
		t.Fatalf("test socket path %q is %d bytes; must be shorter than Darwin's %d-byte AF_UNIX pathname maximum", path, len(path), darwinUnixSocketPathMax)
	}
}
