package app

import (
	"context"
	"strings"
	"testing"
)

func TestLimitedBufferCapsBootstrapStderr(t *testing.T) {
	var b limitedBuffer
	chunk := strings.Repeat("x", maxBootstrapStderr+1024)
	n, err := b.Write([]byte(chunk))
	if err != nil {
		t.Fatal(err)
	}
	if n != len(chunk) {
		t.Fatalf("n=%d, want %d", n, len(chunk))
	}
	if got := len(b.String()); got != maxBootstrapStderr {
		t.Fatalf("captured=%d, want %d", got, maxBootstrapStderr)
	}
}

func TestRemoteDatagramDialerReportsUDPAndStdioFailures(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	_, err := (remoteDatagramDialer{target: "example.invalid"}).Dial(context.Background())
	if err == nil {
		t.Fatal("expected dial error")
	}
	msg := err.Error()
	if !strings.Contains(msg, "datagram dial failed") || !strings.Contains(msg, "stdio fallback failed") {
		t.Fatalf("error %q does not include both failures", msg)
	}
}

func TestParseRemoteAttachTarget(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		wantTarget  string
		wantSession string
		wantOK      bool
	}{
		{name: "local plain", input: "work"},
		{name: "local with colon", input: "work:logs"},
		{name: "missing user", input: "@host"},
		{name: "missing host", input: "user@"},
		{name: "remote no session", input: "user@example.com", wantTarget: "user@example.com", wantOK: true},
		{name: "remote with session", input: "user@example.com:work", wantTarget: "user@example.com", wantSession: "work", wantOK: true},
		{name: "remote empty session allowed", input: "user@example.com:", wantTarget: "user@example.com", wantOK: true},
		{name: "target with at in userinfo", input: "first@user@example.com:work", wantTarget: "first@user@example.com", wantSession: "work", wantOK: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTarget, gotSession, gotOK := parseRemoteAttachTarget(tt.input)
			if gotOK != tt.wantOK {
				t.Fatalf("ok = %v, want %v", gotOK, tt.wantOK)
			}
			if gotTarget != tt.wantTarget {
				t.Fatalf("target = %q, want %q", gotTarget, tt.wantTarget)
			}
			if gotSession != tt.wantSession {
				t.Fatalf("session = %q, want %q", gotSession, tt.wantSession)
			}
		})
	}
}
