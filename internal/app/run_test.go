package app

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/internal/usecase/confirm"
)

func TestParseArgs(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantKind   cmdKind
		wantIntent uint8
		wantName   string
		wantRemote string
		wantAll    bool
		wantDaemon bool
		wantErr    bool
	}{
		{name: "no args -> ephemeral attach", args: nil, wantKind: kindAttach, wantIntent: ports.IntentEphemeral},
		{name: "empty slice -> ephemeral attach", args: []string{}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral},
		{name: "new named", args: []string{"new", "work"}, wantKind: kindAttach, wantIntent: ports.IntentNew, wantName: "work"},
		{name: "new without name", args: []string{"new"}, wantErr: true},
		{name: "new empty name", args: []string{"new", ""}, wantErr: true},
		{name: "new command override unsupported", args: []string{"new", "work", "--", "sh"}, wantErr: true},
		{name: "attach named", args: []string{"attach", "work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work"},
		{name: "attach alias a", args: []string{"a", "work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work"},
		{name: "attach remote without session uses ephemeral", args: []string{"attach", "user@example.com"}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral, wantRemote: "user@example.com"},
		{name: "attach remote with empty session uses ephemeral", args: []string{"attach", "user@example.com:"}, wantKind: kindAttach, wantIntent: ports.IntentEphemeral, wantRemote: "user@example.com"},
		{name: "attach remote with session", args: []string{"attach", "user@example.com:work"}, wantKind: kindAttach, wantIntent: ports.IntentAttach, wantName: "work", wantRemote: "user@example.com"},
		{name: "attach extra arg", args: []string{"attach", "work", "extra"}, wantErr: true},
		{name: "attach without name", args: []string{"attach"}, wantErr: true},
		{name: "ls", args: []string{"ls"}, wantKind: kindList},
		{name: "list", args: []string{"list"}, wantKind: kindList},
		{name: "kill named", args: []string{"kill", "work"}, wantKind: kindKill, wantName: "work"},
		{name: "kill dashed name via terminator", args: []string{"kill", "--", "--all"}, wantKind: kindKill, wantName: "--all"},
		{name: "kill all", args: []string{"kill", "--all"}, wantKind: kindKill, wantAll: true},
		{name: "kill daemon", args: []string{"kill", "--daemon"}, wantKind: kindKill, wantDaemon: true},
		{name: "kill without name", args: []string{"kill"}, wantErr: true},
		{name: "kill terminator without name", args: []string{"kill", "--"}, wantErr: true},
		{name: "kill all rejects extra arg", args: []string{"kill", "--all", "extra"}, wantErr: true},
		{name: "kill daemon rejects extra arg", args: []string{"kill", "--daemon", "extra"}, wantErr: true},
		{name: "kill extra arg", args: []string{"kill", "work", "extra"}, wantErr: true},
		{name: "daemon", args: []string{"--daemon"}, wantKind: kindDaemon},
		{name: "stdio", args: []string{"_stdio"}, wantKind: kindStdio},
		{name: "stdio with session", args: []string{"_stdio", "work"}, wantKind: kindStdio, wantName: "work"},
		{name: "stdio too many args", args: []string{"_stdio", "work", "extra"}, wantErr: true},
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
			if got.remoteTarget != tt.wantRemote {
				t.Errorf("remoteTarget = %q, want %q", got.remoteTarget, tt.wantRemote)
			}
			if got.killAll != tt.wantAll {
				t.Errorf("killAll = %v, want %v", got.killAll, tt.wantAll)
			}
			if got.killDaemon != tt.wantDaemon {
				t.Errorf("killDaemon = %v, want %v", got.killDaemon, tt.wantDaemon)
			}
		})
	}
}

func TestRunLocalAttachPromptsAndRestartsOnProtocolMismatch(t *testing.T) {
	var prompts bytes.Buffer
	answers := strings.NewReader("y\n")
	attachCalls := 0
	killCalls := 0

	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(answers, &prompts),
		attach: func(context.Context, uint8, string) error {
			attachCalls++
			if attachCalls == 1 {
				return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
			}
			return nil
		},
		killDaemon: func(context.Context) error {
			killCalls++
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runLocalAttachWithRecovery returned error: %v", err)
	}
	if attachCalls != 2 {
		t.Fatalf("attach calls = %d, want 2", attachCalls)
	}
	if killCalls != 1 {
		t.Fatalf("kill calls = %d, want 1", killCalls)
	}
	if got := prompts.String(); !strings.Contains(got, "Your vev version differs") || !strings.Contains(got, "kill it") {
		t.Fatalf("prompt = %q, want version/kill prompt", got)
	}
}

func TestRunLocalAttachPropagatesPromptError(t *testing.T) {
	readErr := errors.New("read failed")
	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(errorReader{err: readErr}, &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
		},
		killDaemon: func(context.Context) error {
			t.Fatal("killDaemon should not be called after prompt error")
			return nil
		},
	})
	if !errors.Is(err, readErr) {
		t.Fatalf("error = %v, want wrapped %v", err, readErr)
	}
}

func TestRunLocalAttachPropagatesKillError(t *testing.T) {
	killErr := errors.New("kill failed")
	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(strings.NewReader("y\n"), &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
		},
		killDaemon: func(context.Context) error { return killErr },
	})
	if !errors.Is(err, killErr) {
		t.Fatalf("error = %v, want %v", err, killErr)
	}
}

func TestRunLocalAttachSettlesBeforeRetry(t *testing.T) {
	var order []string
	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(strings.NewReader("y\n"), &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			order = append(order, "attach")
			if len(order) == 1 {
				return &client.ProtocolError{Code: ports.ErrVersionMismatch, Text: "protocol version mismatch"}
			}
			return nil
		},
		killDaemon: func(context.Context) error {
			order = append(order, "kill")
			return nil
		},
		settleAfterKill: func(context.Context) error {
			order = append(order, "settle")
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runLocalAttachWithRecovery returned error: %v", err)
	}
	if got, want := strings.Join(order, ","), "attach,kill,settle,attach"; got != want {
		t.Fatalf("order = %s, want %s", got, want)
	}
}

type errorReader struct{ err error }

func (r errorReader) Read([]byte) (int, error) { return 0, r.err }

func TestRunLocalAttachDeclineKeepsOriginalError(t *testing.T) {
	answers := strings.NewReader("n\n")
	wantErr := &client.ProtocolError{Code: ports.ErrInternal, Text: "malformed hello"}
	attachCalls := 0
	killCalls := 0

	err := runLocalAttachWithRecovery(context.Background(), ports.IntentEphemeral, "", attachRecoveryDeps{
		confirmer: confirm.NewConfirmer(answers, &bytes.Buffer{}),
		attach: func(context.Context, uint8, string) error {
			attachCalls++
			return wantErr
		},
		killDaemon: func(context.Context) error {
			killCalls++
			return nil
		},
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want original %v", err, wantErr)
	}
	if attachCalls != 1 {
		t.Fatalf("attach calls = %d, want 1", attachCalls)
	}
	if killCalls != 0 {
		t.Fatalf("kill calls = %d, want 0", killCalls)
	}
}

func TestRunAttachRejectsNestedVEVBeforeDial(t *testing.T) {
	called := false
	err := runAttachWithDeps(context.Background(), ports.IntentEphemeral, "", "", "outer", runAttachDeps{
		attachLocal: func(context.Context, uint8, string) error {
			called = true
			return nil
		},
		createDetached: func(context.Context, string) error {
			called = true
			return nil
		},
	})
	if err == nil {
		t.Fatal("runAttach with VEV set returned nil error")
	}
	if called {
		t.Fatal("runAttach dialed while nested attach should be rejected")
	}
	if !strings.Contains(err.Error(), "sessions should be nested with care") {
		t.Fatalf("runAttach error = %q, want nested-session warning", err)
	}
}

func TestRunAttachNestedNewCreatesDetachedSession(t *testing.T) {
	var gotName string
	err := runAttachWithDeps(context.Background(), ports.IntentNew, "scratch", "", "outer", runAttachDeps{
		attachLocal: func(context.Context, uint8, string) error {
			t.Fatal("nested new should not attach to the session")
			return nil
		},
		createDetached: func(_ context.Context, name string) error {
			gotName = name
			return nil
		},
	})
	if err != nil {
		t.Fatalf("runAttachWithDeps returned error: %v", err)
	}
	if gotName != "scratch" {
		t.Fatalf("detached name = %q, want scratch", gotName)
	}
}
