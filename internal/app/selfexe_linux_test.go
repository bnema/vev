//go:build linux

package app

import "testing"

func TestSelfExePath(t *testing.T) {
	got, err := selfExePath()
	if err != nil {
		t.Fatalf("selfExePath: %v", err)
	}
	if got != "/proc/self/exe" {
		t.Fatalf("selfExePath = %q, want %q", got, "/proc/self/exe")
	}
}
