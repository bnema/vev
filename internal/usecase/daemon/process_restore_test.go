package daemon

import (
	"testing"

	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestProcessRestorePlan(t *testing.T) {
	allow := map[string]struct{}{
		"less":     {},
		"pi":       {},
		"claude":   {},
		"opencode": {},
		"codex":    {},
	}
	tests := []struct {
		name    string
		proc    *snapcodec.Process
		restore bool
		command string
		reason  string
	}{
		{
			name:    "generic less allowed",
			proc:    &snapcodec.Process{Argv: []string{"/usr/bin/less", "README.md"}, Strategy: processStrategyGeneric},
			restore: true,
			command: "/usr/bin/less README.md",
		},
		{
			name:   "bash denied",
			proc:   &snapcodec.Process{Argv: []string{"bash", "-lc", "rm -rf /"}, Strategy: processStrategyGeneric},
			reason: "not_allowlisted",
		},
		{
			name:    "pi with ID resumes",
			proc:    &snapcodec.Process{Argv: []string{"pi", "cli"}, Strategy: processStrategyPi, Opts: snapcodec.ProcessOpts{AgentSessionID: "session-123"}},
			restore: true,
			command: "pi --resume session-123",
		},
		{
			name:    "pi no ID continues",
			proc:    &snapcodec.Process{Argv: []string{"pi", "cli"}, Strategy: processStrategyPi},
			restore: true,
			command: "pi --continue",
		},
		{
			name:    "opencode session command",
			proc:    &snapcodec.Process{Argv: []string{"opencode"}, Strategy: processStrategyOpenCode, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc"}},
			restore: true,
			command: "opencode --session abc",
		},
		{
			name:    "codex resume command",
			proc:    &snapcodec.Process{Argv: []string{"codex"}, Strategy: processStrategyCodex, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc"}},
			restore: true,
			command: "codex resume abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := planProcessRestore(tt.proc, allow)
			if got.Restore != tt.restore || got.Command != tt.command || got.Reason != tt.reason {
				t.Fatalf("planProcessRestore() = %+v, want restore=%v command=%q reason=%q", got, tt.restore, tt.command, tt.reason)
			}
		})
	}
}

func TestExtractAgentSessionID(t *testing.T) {
	tests := []struct {
		name     string
		strategy string
		argv     []string
		want     string
	}{
		{name: "claude session flag extraction", strategy: processStrategyClaude, argv: []string{"claude", "--session-id", "uuid-123"}, want: "uuid-123"},
		{name: "opencode short session flag extraction", strategy: processStrategyOpenCode, argv: []string{"opencode", "-s", "abc"}, want: "abc"},
		{name: "codex resume extraction", strategy: processStrategyCodex, argv: []string{"codex", "resume", "abc"}, want: "abc"},
		{name: "claude flag-looking ID rejected", strategy: processStrategyClaude, argv: []string{"claude", "--resume", "--continue"}},
		{name: "pi flag-looking ID rejected", strategy: processStrategyPi, argv: []string{"pi", "-r", "--last"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := extractAgentSessionID(tt.strategy, tt.argv); got != tt.want {
				t.Fatalf("extractAgentSessionID() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestShellQuoteArgv(t *testing.T) {
	got, ok := shellQuoteArgv([]string{"cmd", "two words", "it's"})
	if !ok {
		t.Fatal("shellQuoteArgv() returned !ok")
	}
	want := "cmd 'two words' 'it'\\''s'"
	if got != want {
		t.Fatalf("shellQuoteArgv() = %q, want %q", got, want)
	}
}
