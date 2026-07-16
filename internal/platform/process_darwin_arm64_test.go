//go:build darwin && arm64

package platform

import (
	"os"
	"syscall"
	"testing"
)

func TestDarwinProcessInspectorCurrentProcess(t *testing.T) {
	inspector := NewProcessInspector()
	pid := os.Getpid()

	cwd, err := inspector.Cwd(pid)
	if err != nil {
		t.Fatalf("Cwd(%d): %v", pid, err)
	}
	if cwd == "" {
		t.Fatal("Cwd returned an empty path")
	}

	comm, err := inspector.Comm(pid)
	if err != nil {
		t.Fatalf("Comm(%d): %v", pid, err)
	}
	if comm == "" {
		t.Fatal("Comm returned an empty command")
	}

	argv, err := inspector.Argv(pid)
	if err != nil {
		t.Fatalf("Argv(%d): %v", pid, err)
	}
	if len(argv) == 0 {
		t.Fatal("Argv returned no arguments")
	}

	groupArgv, err := inspector.GroupArgv(syscall.Getpgrp(), -1)
	if err != nil {
		t.Fatalf("GroupArgv(current group): %v", err)
	}
	if len(groupArgv) == 0 {
		t.Fatal("GroupArgv returned no arguments for the current process group")
	}
}
