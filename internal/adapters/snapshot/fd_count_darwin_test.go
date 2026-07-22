//go:build darwin

package snapshot

import (
	"os"
	"testing"
)

// macOS exposes the calling process descriptor table through /dev/fd.
// Readdirnames avoids os.ReadDir's fstatat calls for descriptors that can
// disappear while the directory is being enumerated.
func openDescriptorCount(t *testing.T) int {
	t.Helper()
	dir, err := os.Open("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	names, readErr := dir.Readdirnames(-1)
	closeErr := dir.Close()
	if readErr != nil {
		t.Fatal(readErr)
	}
	if closeErr != nil {
		t.Fatal(closeErr)
	}
	return len(names)
}
