//go:build linux

package platform

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
)

func TestProcessCwdRejectsInvalidPID(t *testing.T) {
	if _, err := processCwd("/proc", 0); err == nil {
		t.Fatal("processCwd accepted invalid pid")
	}
}

func TestProcessArgv(t *testing.T) {
	argv, err := ProcessArgv(os.Getpid())
	if err != nil {
		t.Fatalf("ProcessArgv current process: %v", err)
	}
	if len(argv) == 0 {
		t.Fatal("ProcessArgv returned empty argv")
	}
}

func TestProcessArgvRejectsEmptyCmdline(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cmdline")
	if err := os.WriteFile(path, []byte("\x00\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	if argv, err := processArgv(path, 123); err == nil || argv != nil {
		t.Fatalf("processArgv empty = %#v, %v; want nil error", argv, err)
	}
}

func TestSelectProcessGroupPID(t *testing.T) {
	tests := []struct {
		name    string
		recs    []processRecord
		pgid    int
		shell   int
		wantPID int
		wantOK  bool
	}{
		{
			name:    "selects other member",
			recs:    []processRecord{{pid: 10, pgrp: 7}, {pid: 12, pgrp: 7}, {pid: 20, pgrp: 9}},
			pgid:    7,
			shell:   10,
			wantPID: 12,
			wantOK:  true,
		},
		{
			name:    "bare shell has no candidate",
			recs:    []processRecord{{pid: 10, pgrp: 7}, {pid: 20, pgrp: 9}},
			pgid:    7,
			shell:   10,
			wantPID: 0,
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pid, ok := selectProcessGroupPID(tt.recs, tt.pgid, tt.shell)
			if ok != tt.wantOK || pid != tt.wantPID {
				t.Fatalf("selectProcessGroupPID = %d, %v; want %d, %v", pid, ok, tt.wantPID, tt.wantOK)
			}
		})
	}
}

func TestProcessInspectorUsesConfiguredRoot(t *testing.T) {
	root := t.TempDir()
	writeProcStat(t, root, 10, 7)
	pidDir := filepath.Join(root, "10")
	cwdTarget := t.TempDir()
	if err := os.Symlink(cwdTarget, filepath.Join(pidDir, "cwd")); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pidDir, "comm"), []byte("custom\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(pidDir, "cmdline"), []byte("custom\x00arg\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	ins := newProcessInspector(root)

	cwd, err := ins.Cwd(10)
	if err != nil || cwd != cwdTarget {
		t.Fatalf("Cwd = %q, %v; want %q, nil", cwd, err, cwdTarget)
	}
	comm, err := ins.Comm(10)
	if err != nil || comm != "custom" {
		t.Fatalf("Comm = %q, %v; want custom, nil", comm, err)
	}
	argv, err := ins.Argv(10)
	if err != nil || !reflect.DeepEqual(argv, []string{"custom", "arg"}) {
		t.Fatalf("Argv = %#v, %v; want custom argv, nil", argv, err)
	}
}

func TestProcessInspectorCachesProcessRecordsBriefly(t *testing.T) {
	root := t.TempDir()
	writeProcStat(t, root, 10, 7)
	ins := newProcessInspector(root)

	first, err := ins.processRecords()
	if err != nil {
		t.Fatal(err)
	}
	writeProcStat(t, root, 12, 7)
	second, err := ins.processRecords()
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("cached processRecords lengths = %d, %d; want 1, 1", len(first), len(second))
	}
}

func writeProcStat(t *testing.T, root string, pid int, pgrp int) {
	t.Helper()
	dir := filepath.Join(root, strconv.Itoa(pid))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	stat := fmt.Sprintf("%d (cmd) S 1 %d 1 0 0", pid, pgrp)
	if err := os.WriteFile(filepath.Join(dir, "stat"), []byte(stat), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestParseStatPgrp(t *testing.T) {
	got, err := parseStatPgrp("123 (cmd with spaces) S 1 456 456 0")
	if err != nil || got != 456 {
		t.Fatalf("parseStatPgrp = %d, %v; want 456, nil", got, err)
	}
}

func TestParseKernProcArgs2(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		want    []string
		wantErr bool
	}{
		{
			name: "argv after executable and padding",
			data: append([]byte{2, 0, 0, 0}, []byte("/bin/tool\x00\x00tool\x00arg\x00ENV=value\x00")...),
			want: []string{"tool", "arg"},
		},
		{
			name: "preserves empty argument",
			data: append([]byte{3, 0, 0, 0}, []byte("/bin/tool\x00tool\x00\x00arg\x00")...),
			want: []string{"tool", "", "arg"},
		},
		{
			name:    "truncated argc",
			data:    []byte{1, 0, 0},
			wantErr: true,
		},
		{
			name:    "unterminated executable path",
			data:    append([]byte{1, 0, 0, 0}, []byte("/bin/tool")...),
			wantErr: true,
		},
		{
			name:    "truncated argv",
			data:    append([]byte{1, 0, 0, 0}, []byte("/bin/tool\x00tool")...),
			wantErr: true,
		},
		{
			name:    "negative argc",
			data:    append([]byte{0xff, 0xff, 0xff, 0xff}, []byte("/bin/tool\x00tool\x00")...),
			wantErr: true,
		},
		{
			name:    "argc exceeds remaining data",
			data:    append([]byte{0xff, 0xff, 0xff, 0x7f}, []byte("/bin/tool\x00tool\x00")...),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseKernProcArgs2(tt.data)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parseKernProcArgs2 error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("parseKernProcArgs2 = %#v; want %#v", got, tt.want)
			}
		})
	}
}

func TestProcessArgvFileParsing(t *testing.T) {
	tests := []struct {
		name string
		data []byte
		want []string
	}{
		{
			name: "splits nul-delimited argv",
			data: []byte("cmd\x00two words\x00"),
			want: []string{"cmd", "two words"},
		},
		{
			name: "preserves empty argument",
			data: []byte("cmd\x00\x00arg\x00"),
			want: []string{"cmd", "", "arg"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "cmdline")
			if err := os.WriteFile(path, tt.data, 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := processArgv(path, 123)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("processArgv = %#v; want %#v", got, tt.want)
			}
		})
	}
}
