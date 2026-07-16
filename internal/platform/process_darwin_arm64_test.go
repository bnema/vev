//go:build darwin && arm64

package platform

import (
	"os"
	"syscall"
	"testing"
	"unsafe"
)

func TestDarwinKinfoProcLayout(t *testing.T) {
	tests := []struct {
		name string
		got  uintptr
		want uintptr
	}{
		{"Proc.Pid", unsafe.Offsetof(kinfoProc{}.Proc) + unsafe.Offsetof(externProc{}.Pid), 40},
		{"Proc.Comm", unsafe.Offsetof(kinfoProc{}.Proc) + unsafe.Offsetof(externProc{}.Comm), 243},
		{"Eproc.Pgid", unsafe.Offsetof(kinfoProc{}.Eproc) + unsafe.Offsetof(eproc{}.Pgid), 564},
		{"kinfoProc size", unsafe.Sizeof(kinfoProc{}), darwinKinfoProcSize},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Fatalf("%s = %d; want %d", tt.name, tt.got, tt.want)
			}
		})
	}
}

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
