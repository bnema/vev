package main

import (
	"errors"
	"strings"
	"testing"
)

func TestBoundedStderrRetainsAtMostDiagnosticLimit(t *testing.T) {
	stderr := &boundedStderr{}
	input := strings.Repeat("x", commandStderrLimit+1)

	written, err := stderr.Write([]byte(input))

	if err != nil {
		t.Fatal(err)
	}
	if written != len(input) {
		t.Fatalf("written = %d, want %d", written, len(input))
	}
	if stderr.Len() != commandStderrLimit {
		t.Fatalf("stderr length = %d, want %d", stderr.Len(), commandStderrLimit)
	}
	if !strings.Contains(stderr.String(), "[stderr truncated]") {
		t.Fatalf("stderr = %q, want truncation marker", stderr.String())
	}
}

func TestCommandErrorWithStderr(t *testing.T) {
	cause := errors.New("command failed")
	if got := commandErrorWithStderr(cause, "  daemon could not start\n"); !errors.Is(got, cause) || !strings.Contains(got.Error(), "stderr: daemon could not start") {
		t.Fatalf("command error = %v", got)
	}
	if got := commandErrorWithStderr(cause, "\n"); got != cause {
		t.Fatalf("empty stderr error = %v, want original error", got)
	}
}

func TestRunCapturedCommandIncludesStderr(t *testing.T) {
	err := runCapturedCommand(t.Context(), "sh", "-c", "printf 'command diagnostics\\n' >&2; exit 1")
	if err == nil || !strings.Contains(err.Error(), "stderr: command diagnostics") {
		t.Fatalf("command error = %v", err)
	}
}
