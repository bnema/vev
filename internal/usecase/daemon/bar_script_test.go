package daemon

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
)

func TestBarScriptSanitizeOutput(t *testing.T) {
	tests := []struct {
		name  string
		raw   string
		limit int
		want  string
	}{
		{name: "first line only", raw: "one\ntwo\n", limit: barScriptOutputLimit, want: "one"},
		{name: "ansi stripped", raw: "\x1b[31mred\x1b[0m", limit: barScriptOutputLimit, want: "red"},
		{name: "controls stripped", raw: "a\x00b\x07c", limit: barScriptOutputLimit, want: "abc"},
		{name: "osc stripped", raw: "a\x1b]0;bad\ab", limit: barScriptOutputLimit, want: "ab"},
		{name: "charset escape stripped", raw: "a\x1b(Bb\x1b)0c", limit: barScriptOutputLimit, want: "abc"},
		{name: "non csi escape stripped", raw: "a\x1b7b\x1b8c", limit: barScriptOutputLimit, want: "abc"},
		{name: "c1 controls stripped", raw: "a\u009b31mb\u009d0;bad\ac", limit: barScriptOutputLimit, want: "abc"},
		{name: "utf8 and nerd font preserved", raw: "󰍛 14:32 ↑3", limit: barScriptOutputLimit, want: "󰍛 14:32 ↑3"},
		{name: "display capped", raw: strings.Repeat("a", barScriptDisplayLimit+10), limit: barScriptOutputLimit, want: strings.Repeat("a", barScriptDisplayLimit)},
		{name: "capture capped before first line", raw: strings.Repeat("a", 8) + "\nignored", limit: 4, want: "aaaa"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sanitizeBarScriptOutput([]byte(tt.raw), tt.limit); got != tt.want {
				t.Fatalf("sanitizeBarScriptOutput() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestBarScriptContextEnv(t *testing.T) {
	base := []string{
		"PATH=/bin",
		"VEV_ANCHOR=old",
		"VEV_SESSION=old",
		"VEV_TAB=old",
		"VEV_PANE=old",
		"VEV_PANE_CWD=/old",
		"VEV_COLS=1",
	}
	ctx := barScriptContext{Anchor: "top-right", Session: "work", Tab: "2", Pane: "pane-3", PaneCWD: "/repo", Cols: 120}
	got := ctx.env(base)

	want := map[string]string{
		"VEV_ANCHOR":   "top-right",
		"VEV_SESSION":  "work",
		"VEV_TAB":      "2",
		"VEV_PANE":     "pane-3",
		"VEV_PANE_CWD": "/repo",
		"VEV_COLS":     "120",
	}
	seen := map[string]int{}
	for _, entry := range got {
		key, val, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if _, isVEV := want[key]; isVEV {
			seen[key]++
			if val != want[key] {
				t.Fatalf("%s = %q, want %q in env %v", key, val, want[key], got)
			}
		}
	}
	for key := range want {
		if seen[key] != 1 {
			t.Fatalf("%s appears %d times, want 1 in env %v", key, seen[key], got)
		}
	}
}

func TestBarScriptRunner(t *testing.T) {
	t.Run("nil runner returns explicit error for command", func(t *testing.T) {
		runner := barScriptRunner{}
		got, err := runner.run(context.Background(), "date", nil, barScriptContext{})
		if err == nil || err.Error() != "bar script command runner is nil" {
			t.Fatalf("run err = %v, want explicit nil runner error", err)
		}
		if got != "" {
			t.Fatalf("run output = %q, want empty", got)
		}
	})

	t.Run("empty command skips port runner", func(t *testing.T) {
		runner := barScriptRunner{}
		got, err := runner.run(context.Background(), " \t\n ", nil, barScriptContext{})
		if err != nil {
			t.Fatalf("run err = %v, want nil", err)
		}
		if got != "" {
			t.Fatalf("run output = %q, want empty", got)
		}
	})

	t.Run("success sanitizes first line and passes command spec", func(t *testing.T) {
		portRunner := portsmocks.NewMockShellCommandRunner(t)
		portRunner.EXPECT().Run(mock.Anything, mock.MatchedBy(func(spec ports.CommandSpec) bool {
			return spec.Command == "printf 'ok\\nignored'" &&
				spec.Timeout == 50*time.Millisecond &&
				spec.StdoutLimit == barScriptOutputLimit &&
				containsEnv(spec.Env, "PATH=/bin") &&
				containsEnv(spec.Env, "VEV_ANCHOR=top-right") &&
				containsEnv(spec.Env, "VEV_SESSION=work") &&
				containsEnv(spec.Env, "VEV_TAB=tab-1") &&
				containsEnv(spec.Env, "VEV_PANE=pane-1") &&
				containsEnv(spec.Env, "VEV_PANE_CWD=/repo") &&
				containsEnv(spec.Env, "VEV_COLS=120") &&
				!containsEnv(spec.Env, "VEV_ANCHOR=old")
		})).Return(ports.ShellCommandResult{Stdout: []byte("\x1b[31mok\x1b[0m\nignored")}, nil)
		runner := barScriptRunner{runner: portRunner, timeout: 50 * time.Millisecond}

		got, err := runner.run(context.Background(), "printf 'ok\\nignored'",
			[]string{"PATH=/bin", "VEV_ANCHOR=old"},
			barScriptContext{Anchor: "top-right", Session: "work", Tab: "tab-1", Pane: "pane-1", PaneCWD: "/repo", Cols: 120})
		if err != nil {
			t.Fatalf("run err = %v, want nil", err)
		}
		if got != "ok" {
			t.Fatalf("run output = %q, want %q", got, "ok")
		}
	})

	t.Run("port failure returns sanitized partial stdout and error", func(t *testing.T) {
		wantErr := errors.New("exit 1")
		portRunner := portsmocks.NewMockShellCommandRunner(t)
		portRunner.EXPECT().Run(mock.Anything, mock.Anything).
			Return(ports.ShellCommandResult{Stdout: []byte("partial\x00 output\nignored"), ExitCode: 1}, wantErr)
		runner := barScriptRunner{runner: portRunner, timeout: time.Second}

		got, err := runner.run(context.Background(), "false", nil, barScriptContext{})
		if !errors.Is(err, wantErr) {
			t.Fatalf("run err = %v, want %v", err, wantErr)
		}
		if got != "partial output" {
			t.Fatalf("run output = %q, want %q", got, "partial output")
		}
	})

	t.Run("timeout propagates deadline", func(t *testing.T) {
		portRunner := portsmocks.NewMockShellCommandRunner(t)
		portRunner.EXPECT().Run(mock.Anything, mock.Anything).RunAndReturn(
			func(ctx context.Context, _ ports.CommandSpec) (ports.ShellCommandResult, error) {
				<-ctx.Done()
				return ports.ShellCommandResult{ExitCode: -1}, ctx.Err()
			})
		runner := barScriptRunner{runner: portRunner, timeout: time.Nanosecond}

		_, err := runner.run(context.Background(), "sleep 1", nil, barScriptContext{})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("run err = %v, want %v", err, context.DeadlineExceeded)
		}
	})
}

func TestBarScriptRunnerBoundsStdoutBeforeSanitize(t *testing.T) {
	portRunner := portsmocks.NewMockShellCommandRunner(t)
	portRunner.EXPECT().Run(mock.Anything, mock.MatchedBy(func(spec ports.CommandSpec) bool {
		return spec.StdoutLimit == barScriptOutputLimit
	})).Return(ports.ShellCommandResult{Stdout: []byte(strings.Repeat("a", 4096))}, nil)
	runner := barScriptRunner{runner: portRunner, timeout: time.Second}

	got, err := runner.run(context.Background(), "long-output", nil, barScriptContext{})
	if err != nil {
		t.Fatalf("run bounded output: %v", err)
	}
	if len(got) > barScriptDisplayLimit {
		t.Fatalf("output length = %d, want <= %d", len(got), barScriptDisplayLimit)
	}
}

func containsEnv(env []string, entry string) bool {
	return slices.Contains(env, entry)
}
