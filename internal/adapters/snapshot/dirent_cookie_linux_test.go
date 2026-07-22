//go:build linux

package snapshot

import (
	"syscall"
	"testing"
)

func TestDirectoryCookieUsesLinuxOffset(t *testing.T) {
	dirent := syscall.Dirent{Off: 42}
	got, err := directoryCookie(fakeMaintenanceDirectory{}, &dirent)
	if err != nil {
		t.Fatal(err)
	}
	if got != 42 {
		t.Fatalf("directoryCookie() = %d, want 42", got)
	}
}

func TestDirectoryUnlinkRetryRequiresLinuxEISDIR(t *testing.T) {
	if directoryUnlinkRetry(syscall.EPERM, syscall.S_IFDIR) {
		t.Fatal("directoryUnlinkRetry(EPERM, directory) = true, want false")
	}
	if !directoryUnlinkRetry(syscall.EISDIR, syscall.S_IFDIR) {
		t.Fatal("directoryUnlinkRetry(EISDIR, directory) = false, want true")
	}
}
