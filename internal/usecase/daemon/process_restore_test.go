package daemon

import (
	"testing"

	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
)

func TestProcessRestorePlan(t *testing.T) {
	defaultAllow := map[string]struct{}{
		"less":     {},
		"pi":       {},
		"claude":   {},
		"opencode": {},
		"codex":    {},
	}
	tests := []struct {
		name    string
		proc    *snapcodec.Process
		allow   map[string]struct{}
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
			name:    "claude no ID continues",
			proc:    &snapcodec.Process{Argv: []string{"claude"}, Strategy: processStrategyClaude},
			restore: true,
			command: "claude --continue",
		},
		{
			name:    "opencode session command",
			proc:    &snapcodec.Process{Argv: []string{"opencode"}, Strategy: processStrategyOpenCode, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc"}},
			restore: true,
			command: "opencode --session abc",
		},
		{
			name:    "opencode no ID continues",
			proc:    &snapcodec.Process{Argv: []string{"opencode"}, Strategy: processStrategyOpenCode},
			restore: true,
			command: "opencode --continue",
		},
		{
			name:    "codex resume command",
			proc:    &snapcodec.Process{Argv: []string{"codex"}, Strategy: processStrategyCodex, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc"}},
			restore: true,
			command: "codex resume abc",
		},
		{
			name:    "codex no ID resumes last",
			proc:    &snapcodec.Process{Argv: []string{"codex"}, Strategy: processStrategyCodex},
			restore: true,
			command: "codex resume --last",
		},
		{
			name:   "agent strategy matches allowlist by strategy not argv basename",
			proc:   &snapcodec.Process{Argv: []string{"less"}, Strategy: processStrategyClaude, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc"}},
			allow:  map[string]struct{}{"less": {}},
			reason: "not_allowlisted",
		},
		{
			name:   "agent ID with control byte rejected",
			proc:   &snapcodec.Process{Argv: []string{"pi", "cli"}, Strategy: processStrategyPi, Opts: snapcodec.ProcessOpts{AgentSessionID: "session\rmalicious"}},
			reason: "invalid_agent_session_id",
		},
		{
			name:   "empty executable rejected",
			proc:   &snapcodec.Process{Argv: []string{""}, Strategy: processStrategyGeneric},
			reason: "missing_process",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			allow := tt.allow
			if allow == nil {
				allow = defaultAllow
			}
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
		{name: "claude short resume extraction", strategy: processStrategyClaude, argv: []string{"claude", "-r", "uuid-456"}, want: "uuid-456"},
		{name: "opencode short session flag extraction", strategy: processStrategyOpenCode, argv: []string{"opencode", "-s", "abc"}, want: "abc"},
		{name: "codex resume extraction", strategy: processStrategyCodex, argv: []string{"codex", "resume", "abc"}, want: "abc"},
		{name: "claude flag-looking ID rejected", strategy: processStrategyClaude, argv: []string{"claude", "--resume", "--continue"}},
		{name: "pi flag-looking ID rejected", strategy: processStrategyPi, argv: []string{"pi", "-r", "--last"}},
		{name: "control byte ID rejected", strategy: processStrategyPi, argv: []string{"pi", "--resume", "abc\nmalicious"}},
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
	tests := []struct {
		name string
		argv []string
		want string
		ok   bool
	}{
		{name: "empty argv", ok: false},
		{name: "spaces and single quote", argv: []string{"cmd", "two words", "it's"}, want: "cmd 'two words' 'it'\\''s'", ok: true},
		{name: "shell metacharacters", argv: []string{"cmd", "$(touch /tmp/pwned)", "a;b", "line\nbreak"}, want: "cmd '$(touch /tmp/pwned)' 'a;b' 'line\nbreak'", ok: true},
		{name: "empty argument", argv: []string{"cmd", ""}, want: "cmd ''", ok: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := shellQuoteArgv(tt.argv)
			if ok != tt.ok || got != tt.want {
				t.Fatalf("shellQuoteArgv() = %q, %v; want %q, %v", got, ok, tt.want, tt.ok)
			}
		})
	}
}
