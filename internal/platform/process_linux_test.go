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

func TestParseStatPgrp(t *testing.T) {
	got, err := parseStatPgrp("123 (cmd with spaces) S 1 456 456 0")
	if err != nil || got != 456 {
		t.Fatalf("parseStatPgrp = %d, %v; want 456, nil", got, err)
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
