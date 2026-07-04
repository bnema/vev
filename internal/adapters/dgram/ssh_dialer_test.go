package dgram

import (
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

func TestSSHTargetHost(t *testing.T) {
	tests := []struct {
		target string
		want   string
	}{
		{target: "example.com", want: "example.com"},
		{target: "user@example.com", want: "example.com"},
		{target: "example.com:2222", want: "example.com"},
		{target: "user@[2001:db8::1]:2222", want: "2001:db8::1"},
	}
	for _, tt := range tests {
		t.Run(tt.target, func(t *testing.T) {
			if got := sshTargetHost(tt.target); got != tt.want {
				t.Fatalf("sshTargetHost(%q) = %q, want %q", tt.target, got, tt.want)
			}
		})
	}
}
