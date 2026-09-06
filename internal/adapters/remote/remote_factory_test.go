package remote

import (
	"context"
	"errors"
	"testing"

	"github.com/bnema/vev/internal/adapters/dgram"
)

func TestDialerFactorySelectsExplicitModes(t *testing.T) {
	factory := NewDialerFactory()

	tests := []struct {
		name     string
		mode     TransportMode
		wantType any
	}{
		{name: "udp", mode: TransportUDP, wantType: dgram.RemoteDialer{}},
		{name: "stdio", mode: TransportStdio, wantType: stdioDialer{}},
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

func TestDialerFactoryRejectsIncompleteIsolatedLaunch(t *testing.T) {
	factory := NewDialerFactory()
	_, err := factory.DialerForRemoteWithLaunch("remote.example", "work", TransportStdio, nil, &EndpointLaunch{Binary: "/bin/vev"})
	if err == nil {
		t.Fatal("DialerForRemoteWithLaunch() error = nil, want incomplete owner rejection")
	}
}

func TestDialerFactoryRejectsUnsupportedMode(t *testing.T) {
	factory := NewDialerFactory()

	dialer, err := factory.DialerForRemote("remote.example", "work", TransportMode("serial"), nil)
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
