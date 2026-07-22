//go:build darwin

package snapshot

import (
	"os"
	"testing"
)

// macOS exposes the calling process descriptor table through /dev/fd.
func openDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/dev/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
