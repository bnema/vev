package dgram

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

func TestRemoteDialerFactorySelectsExplicitModes(t *testing.T) {
	factory := NewRemoteDialerFactory()

	tests := []struct {
		name     string
		mode     ports.RemoteTransportMode
		wantType any
	}{
		{name: "udp", mode: ports.RemoteTransportUDP, wantType: RemoteDialer{}},
		{name: "stdio", mode: ports.RemoteTransportStdio, wantType: stdioDialer{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := factory.DialerForRemote("remote.example", "work", tt.mode, nil)
			if err != nil {
				t.Fatalf("DialerForRemote() error = %v", err)
			}
			switch tt.wantType.(type) {
			case RemoteDialer:
				got, ok := dialer.(RemoteDialer)
				if !ok {
					t.Fatalf("dialer type = %T, want %T", dialer, RemoteDialer{})
				}
				if got.Target != "remote.example" || got.Session != "work" {
					t.Fatalf("dialer = %+v, want target/session copied", got)
				}
			case stdioDialer:
				got, ok := dialer.(stdioDialer)
				if !ok {
					t.Fatalf("dialer type = %T, want %T", dialer, stdioDialer{})
				}
				if got.target != "remote.example" || got.session != "work" {
					t.Fatalf("dialer = %+v, want target/session copied", got)
				}
			}
		})
	}
}

func TestRemoteDialerFactoryRejectsUnsupportedMode(t *testing.T) {
	factory := NewRemoteDialerFactory()

	dialer, err := factory.DialerForRemote("remote.example", "work", ports.RemoteTransportMode("serial"), nil)
	if err == nil {
		t.Fatal("DialerForRemote() error = nil, want unsupported mode error")
	}
	if dialer != nil {
		t.Fatalf("DialerForRemote() dialer = %T, want nil", dialer)
	}
	if got, want := err.Error(), "vev: unsupported remote transport \"serial\""; got != want {
		t.Fatalf("error = %q, want %q", got, want)
	}
}

func TestStdioDialerUsesContextBeforeStartingSSH(t *testing.T) {
	dialer := stdioDialer{target: "remote.example", session: "work"}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	transport, err := dialer.Dial(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Dial() error = %v, want context canceled", err)
	}
	if transport != nil {
		t.Fatalf("Dial() transport = %T, want nil", transport)
	}
}

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
