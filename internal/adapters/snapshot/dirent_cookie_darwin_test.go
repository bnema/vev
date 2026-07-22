//go:build darwin

package snapshot

import (
	"syscall"
	"testing"
)

func TestDirectoryCookieUsesDarwinSeekOffset(t *testing.T) {
	dirent := syscall.Dirent{Seekoff: 42}
	if got := directoryCookie(&dirent); got != 42 {
		t.Fatalf("directoryCookie() = %d, want 42", got)
	}
}
