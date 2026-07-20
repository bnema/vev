package shellcmd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestRunnerRun(t *testing.T) {
	tests := []struct {
		name    string
		command string
		limit   int
		want    string
	}{
		{name: "success", command: "printf 'ok'", limit: 1024, want: "ok"},
		{name: "bounded stdout", command: "yes a | head -c 4096", limit: 8, want: "a\na\na\na\n"},
		{name: "stderr does not leak into stdout", command: "printf out; printf err >&2", limit: 1024, want: "out"},
		{name: "zero limit captures nothing", command: "printf output", limit: 0, want: ""},
		{name: "negative limit captures nothing", command: "printf output", limit: -1, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()

			got, err := New().Run(ctx, ports.CommandSpec{Command: tt.command, Timeout: time.Second, StdoutLimit: tt.limit})
			if err != nil {
				t.Fatalf("Run() error = %v", err)
			}
			if string(got.Stdout) != tt.want {
				t.Fatalf("Run() = %q, want %q", string(got.Stdout), tt.want)
			}
		})
	}
}

func TestRunnerRunReturnsPartialStdoutOnError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := New().Run(ctx, ports.CommandSpec{Command: "printf 'abcdef'; exit 7", Timeout: time.Second, StdoutLimit: 3})
	if err == nil {
		t.Fatal("Run() error = nil, want non-zero exit error")
	}
	if string(got.Stdout) != "abc" {
		t.Fatalf("Run() = %q, want bounded partial stdout %q", string(got.Stdout), "abc")
	}
}

func TestRunnerRunTimeoutReturnsPromptly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()

	_, err := New().Run(ctx, ports.CommandSpec{Command: "sleep 1", Timeout: 50 * time.Millisecond, StdoutLimit: 1024})

	if err == nil {
		t.Fatal("Run() error = nil, want timeout or wait error")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("context error = %v, want DeadlineExceeded", ctx.Err())
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Run() elapsed = %s, want <= 500ms", elapsed)
	}
}

func TestRunnerRunBackgroundChildDoesNotWedgeAfterTimeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()

	_, err := New().Run(ctx, ports.CommandSpec{Command: "sleep 1 &", Timeout: 50 * time.Millisecond, StdoutLimit: 1024})

	if err == nil {
		t.Fatal("Run() error = nil, want timeout or wait error")
	}
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("context error = %v, want DeadlineExceeded", ctx.Err())
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("Run() elapsed = %s, want <= 500ms", elapsed)
	}
}

func TestRunnerRunUsesProvidedEnvironment(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := New().Run(ctx, ports.CommandSpec{
		Command:     "printf '%s' \"$VEV_TEST_VALUE\"",
		Env:         []string{"PATH=/bin:/usr/bin", "VEV_TEST_VALUE=from-env"},
		Timeout:     time.Second,
		StdoutLimit: 1024,
	})
	if err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if strings.TrimSpace(string(got.Stdout)) != "from-env" {
		t.Fatalf("Run() env output = %q, want from-env", string(got.Stdout))
	}
}

func TestRunnerCapturesStderrAndExitCode(t *testing.T) {
	tests := []struct {
		name         string
		command      string
		wantExitCode int
		wantStderr   string
		wantStdout   string
	}{
		{
			name:         "command not found reports 127 and stderr",
			command:      "definitely-not-a-real-command-xyz",
			wantExitCode: 127,
			wantStderr:   "not found",
		},
		{
			name:         "explicit exit code with stderr",
			command:      "echo oops >&2; exit 3",
			wantExitCode: 3,
			wantStderr:   "oops",
		},
		{
			name:         "success reports zero and stdout",
			command:      "echo hello",
			wantExitCode: 0,
			wantStdout:   "hello",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := New().Run(context.Background(), ports.CommandSpec{
				Command:     tc.command,
				Timeout:     5 * time.Second,
				StdoutLimit: 1024,
				StderrLimit: 1024,
			})
			if tc.wantExitCode == 0 {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
			require.Equal(t, tc.wantExitCode, res.ExitCode)
			if tc.wantStderr != "" {
				require.Contains(t, string(res.Stderr), tc.wantStderr)
			}
			if tc.wantStdout != "" {
				require.Contains(t, string(res.Stdout), tc.wantStdout)
			}
		})
	}
}

func TestRunnerBoundsStderr(t *testing.T) {
	res, _ := New().Run(context.Background(), ports.CommandSpec{
		Command:     "yes badness 2>/dev/null | head -c 100000 >&2",
		Timeout:     5 * time.Second,
		StdoutLimit: 1024,
		StderrLimit: 64,
	})
	require.Equal(t, 64, len(res.Stderr), "stderr must be captured and truncated to StderrLimit")
}
