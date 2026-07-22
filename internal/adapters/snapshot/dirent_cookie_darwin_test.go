//go:build darwin

package snapshot

import (
	"syscall"
	"testing"
)

func TestDirectoryCookieUsesDescriptorEntryCount(t *testing.T) {
	file := fakeMaintenanceDirectory{seekOffset: 42}
	dirent := syscall.Dirent{Seekoff: 99}
	got, err := directoryCookie(file, &dirent)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("directoryCookie() = %d, want descriptor entry count 42", got)
	}
}

func TestDirectoryUnlinkRetryRequiresDarwinDirectoryEPERM(t *testing.T) {
	if directoryUnlinkRetry(syscall.EPERM, syscall.S_IFREG) {
		t.Fatal("directoryUnlinkRetry(EPERM, regular file) = true, want false")
	}
	if !directoryUnlinkRetry(syscall.EPERM, syscall.S_IFDIR) {
		t.Fatal("directoryUnlinkRetry(EPERM, directory) = false, want true")
	}
}
