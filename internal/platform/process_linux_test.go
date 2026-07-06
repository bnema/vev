package platform

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

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

func TestProcessGroupArgvSelection(t *testing.T) {
	recs := []processRecord{{pid: 10, pgrp: 7}, {pid: 12, pgrp: 7}, {pid: 20, pgrp: 9}}
	pid, ok := selectProcessGroupPID(recs, 7, 10)
	if !ok || pid != 12 {
		t.Fatalf("selectProcessGroupPID = %d, %v; want 12, true", pid, ok)
	}
}

func TestProcessGroupArgvBareShellReturnsNoPID(t *testing.T) {
	recs := []processRecord{{pid: 10, pgrp: 7}, {pid: 20, pgrp: 9}}
	pid, ok := selectProcessGroupPID(recs, 7, 10)
	if ok || pid != 0 {
		t.Fatalf("selectProcessGroupPID bare shell = %d, %v; want 0, false", pid, ok)
	}
}

func TestParseStatPgrp(t *testing.T) {
	got, err := parseStatPgrp("123 (cmd with spaces) S 1 456 456 0")
	if err != nil || got != 456 {
		t.Fatalf("parseStatPgrp = %d, %v; want 456, nil", got, err)
	}
}

func TestProcessArgvFileParsing(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cmdline")
	if err := os.WriteFile(path, []byte("cmd\x00two words\x00"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := processArgv(path, 123)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"cmd", "two words"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("processArgv = %#v; want %#v", got, want)
	}
}
