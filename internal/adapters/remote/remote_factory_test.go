package remote

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/adapters/dgram"
	"github.com/bnema/vev/internal/ports"
)

func TestDialerFactorySelectsExplicitModes(t *testing.T) {
	factory := NewDialerFactory()

	tests := []struct {
		name     string
		mode     ports.RemoteTransportMode
		wantType any
	}{
		{name: "udp", mode: ports.RemoteTransportUDP, wantType: dgram.RemoteDialer{}},
		{name: "stdio", mode: ports.RemoteTransportStdio, wantType: stdioDialer{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := factory.DialerForRemote("remote.example", "work", tt.mode, nil)
			if err != nil {
				t.Fatalf("DialerForRemote() error = %v", err)
			}
			switch tt.wantType.(type) {
			case dgram.RemoteDialer:
				got, ok := dialer.(dgram.RemoteDialer)
				if !ok {
					t.Fatalf("dialer type = %T, want %T", dialer, dgram.RemoteDialer{})
				}
				if got.Target != "remote.example" {
					t.Fatalf("dialer target = %q, want %q", got.Target, "remote.example")
				}
			case stdioDialer:
				got, ok := dialer.(stdioDialer)
				if !ok {
					t.Fatalf("dialer type = %T, want %T", dialer, stdioDialer{})
				}
				if got.target != "remote.example" {
					t.Fatalf("dialer target = %q, want %q", got.target, "remote.example")
				}
			}
		})
	}
}

func TestDialerFactoryRejectsUnsupportedMode(t *testing.T) {
	factory := NewDialerFactory()

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
	dialer := stdioDialer{target: "remote.example"}
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
