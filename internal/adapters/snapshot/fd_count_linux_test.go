//go:build linux

package snapshot

import (
	"os"
	"testing"
)

// openDescriptorCount checks the actual Linux descriptor table, not a mocked
// lifecycle signal, so the maintenance leak assertion remains strict.
func openDescriptorCount(t *testing.T) int {
	t.Helper()
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}
