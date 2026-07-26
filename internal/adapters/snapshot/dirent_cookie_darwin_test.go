//go:build darwin

package snapshot

import (
	"syscall"
	"testing"
)

func TestDirectoryCookieUsesDescriptorEntryCount(t *testing.T) {
	file := fakeMaintenanceDirectory{seekOffset: 42}
	dirent := syscall.Dirent{Seekoff: 99}
	got, err := directoryCookie(file, &dirent, 0)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("directoryCookie() = %d, want descriptor entry count 42", got)
	}
}

func TestDirectoryCookieSubtractsBufferedEntries(t *testing.T) {
	file := fakeMaintenanceDirectory{seekOffset: 42}
	got, err := directoryCookie(file, &syscall.Dirent{}, 3)
	if err != nil {
		t.Fatal(err)
	}
	if got != 39 {
		t.Fatalf("directoryCookie() = %d, want per-entry cursor 39", got)
	}
}
