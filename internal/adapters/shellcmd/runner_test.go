package shellcmd

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
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
		{name: "stderr discarded", command: "printf out; printf err >&2", limit: 1024, want: "out"},
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
			if string(got) != tt.want {
				t.Fatalf("Run() = %q, want %q", string(got), tt.want)
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
	if string(got) != "abc" {
		t.Fatalf("Run() = %q, want bounded partial stdout %q", string(got), "abc")
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
	if strings.TrimSpace(string(got)) != "from-env" {
		t.Fatalf("Run() env output = %q, want from-env", string(got))
	}
}
