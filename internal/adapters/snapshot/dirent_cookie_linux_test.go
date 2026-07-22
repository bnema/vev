//go:build linux

package snapshot

import (
	"syscall"
	"testing"
)

func TestDirectoryCookieUsesLinuxOffset(t *testing.T) {
	dirent := syscall.Dirent{Off: 42}
	if got := directoryCookie(&dirent); got != 42 {
		t.Fatalf("directoryCookie() = %d, want 42", got)
	}
}
