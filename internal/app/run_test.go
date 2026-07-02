package app

import (
	"errors"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantKind   cmdKind
		wantIntent uint8
		wantName   string
		wantErr    bool
	}{
		{name: "no args -> ephemeral attach", args: nil, wantKind: kindAttach, wantIntent: ports.IntentEphemeral},
		{name: "empty slice -> ephemeral attach", args: []string{}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral},
		{name: "new named", args: []string{"new", "work"}, wantKind: kindAttach, wantIntent: ports.IntentNew, wantName: "work"},
		{name: "new without name", args: []string{"new"}, wantErr: true},
		{name: "new empty name", args: []string{"new", ""}, wantErr: true},
		{name: "attach named", args: []string{"attach", "work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work"},
		{name: "attach alias a", args: []string{"a", "work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work"},
		{name: "attach without name", args: []string{"attach"}, wantErr: true},
		{name: "ls", args: []string{"ls"}, wantKind: kindList},
		{name: "list", args: []string{"list"}, wantKind: kindList},
		{name: "kill named", args: []string{"kill", "work"}, wantKind: kindKill, wantName: "work"},
		{name: "kill without name", args: []string{"kill"}, wantErr: true},
		{name: "daemon", args: []string{"--daemon"}, wantKind: kindDaemon},
		{name: "help", args: []string{"--help"}, wantKind: kindHelp},
		{name: "help subcommand", args: []string{"help"}, wantKind: kindHelp},
		{name: "version", args: []string{"--version"}, wantKind: kindVersion},
		{name: "unknown", args: []string{"frobnicate"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseArgs(tt.args)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseArgs(%q) = %+v, want error", tt.args, got)
				}
				var ue *usageError
				if !errors.As(err, &ue) {
					t.Fatalf("parseArgs(%q) error = %T, want *usageError", tt.args, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseArgs(%q) unexpected error: %v", tt.args, err)
			}
			if got.kind != tt.wantKind {
				t.Errorf("kind = %v, want %v", got.kind, tt.wantKind)
			}
			if got.kind == kindAttach && got.intent != tt.wantIntent {
				t.Errorf("intent = %d, want %d", got.intent, tt.wantIntent)
			}
			if got.name != tt.wantName {
				t.Errorf("name = %q, want %q", got.name, tt.wantName)
			}
		})
	}
}
