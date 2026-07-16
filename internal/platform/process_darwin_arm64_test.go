//go:build darwin && arm64

package platform

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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
	cwdInfo, err := os.Stat(cwd)
	if err != nil {
		t.Fatalf("stat Cwd(%d) result %q: %v", pid, cwd, err)
	}
	currentDirInfo, err := os.Stat(".")
	if err != nil {
		t.Fatal(err)
	}
	if !os.SameFile(cwdInfo, currentDirInfo) {
		t.Fatalf("Cwd(%d) = %q; does not identify the current directory", pid, cwd)
	}

	comm, err := inspector.Comm(pid)
	if err != nil {
		t.Fatalf("Comm(%d): %v", pid, err)
	}
	wantComm := filepath.Base(os.Args[0])
	if len(wantComm) > len(externProc{}.Comm)-1 {
		wantComm = wantComm[:len(externProc{}.Comm)-1]
	}
	if comm != wantComm {
		t.Fatalf("Comm(%d) = %q; want %q", pid, comm, wantComm)
	}

	argv, err := inspector.Argv(pid)
	if err != nil {
		t.Fatalf("Argv(%d): %v", pid, err)
	}
	if len(argv) == 0 || argv[0] != os.Args[0] {
		t.Fatalf("Argv(%d) = %#v; want argv[0] %q", pid, argv, os.Args[0])
	}

	cmd := exec.Command("/bin/sleep", "5")
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()
	groupArgv, err := inspector.GroupArgv(cmd.Process.Pid, -1)
	if err != nil {
		t.Fatalf("GroupArgv(child group): %v", err)
	}
	if len(groupArgv) != 2 || groupArgv[0] != "/bin/sleep" || groupArgv[1] != "5" {
		t.Fatalf("GroupArgv(child group) = %#v; want [/bin/sleep 5]", groupArgv)
	}
}

func TestDarwinPIDRejectsUnrepresentableValues(t *testing.T) {
	for _, pid := range []int{0, -1, maxDarwinPID + 1} {
		if _, err := ProcessComm(pid); err == nil {
			t.Errorf("ProcessComm(%d) succeeded; want invalid PID error", pid)
		}
		if _, err := ProcessArgv(pid); err == nil {
			t.Errorf("ProcessArgv(%d) succeeded; want invalid PID error", pid)
		}
	}
}

func TestRawSysctlRetriesAfterENOMEM(t *testing.T) {
	var sizeCalls, readCalls int
	data, err := rawSysctlWith([]int32{1, 2}, func(_ []int32, old *byte, oldlen *uintptr) error {
		if old == nil {
			sizeCalls++
			*oldlen = 1
			return nil
		}
		readCalls++
		if readCalls == 1 {
			return syscall.ENOMEM
		}
		*old = 'x'
		*oldlen = 1
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "x" || sizeCalls != 2 || readCalls != 2 {
		t.Fatalf("rawSysctl calls = size %d, read %d, data %q; want 2, 2, x", sizeCalls, readCalls, data)
	}
}

func TestRawSysctlStopsRetryingAfterLimit(t *testing.T) {
	var sizeCalls, readCalls int
	_, err := rawSysctlWith([]int32{1, 2}, func(_ []int32, old *byte, oldlen *uintptr) error {
		if old == nil {
			sizeCalls++
			*oldlen = 1
			return nil
		}
		readCalls++
		return syscall.ENOMEM
	})
	if !errors.Is(err, syscall.ENOMEM) || !strings.Contains(err.Error(), "result grew after") {
		t.Fatalf("rawSysctl exhaustion error = %v; want contextual ENOMEM", err)
	}
	wantCalls := rawSysctlMaxRetries + 1
	if sizeCalls != wantCalls || readCalls != wantCalls {
		t.Fatalf("rawSysctl calls = size %d, read %d; want %d each", sizeCalls, readCalls, wantCalls)
	}
}
