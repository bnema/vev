package remote

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/ports"
)

type recordingObserver struct {
	mu    sync.Mutex
	marks []ports.RuntimeMark
}

func (r *recordingObserver) ObserveRuntime(mark ports.RuntimeMark) {
	r.mu.Lock()
	r.marks = append(r.marks, mark)
	r.mu.Unlock()
}

func (r *recordingObserver) Flush() {}
func (r *recordingObserver) Close() {}

func (r *recordingObserver) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.marks)
}

func TestDialerFactorySelectsExplicitModes(t *testing.T) {
	factory := NewDialerFactory()

	tests := []struct {
		name     string
		mode     TransportMode
		wantType any
	}{
		{name: "quic", mode: TransportQUIC, wantType: quicDialer{}},
		{name: "stdio", mode: TransportStdio, wantType: stdioDialer{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dialer, err := factory.DialerForRemote("remote.example", "work", tt.mode, nil)
			if err != nil {
				t.Fatalf("DialerForRemote() error = %v", err)
			}
			switch tt.wantType.(type) {
			case quicDialer:
				got, ok := dialer.(quicDialer)
				if !ok {
					t.Fatalf("dialer type = %T, want %T", dialer, quicDialer{})
				}
				if got.target != "remote.example" {
					t.Fatalf("dialer target = %q, want %q", got.target, "remote.example")
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

func TestDialerFactoryPropagatesObserverToQUICTransport(t *testing.T) {
	observer := &recordingObserver{}
	factory := NewDialerFactoryWithRuntimeObserver(observer)
	dialer, err := factory.DialerForRemote("remote.example", "work", TransportQUIC, nil)
	if err != nil {
		t.Fatalf("DialerForRemote() error = %v", err)
	}
	got, ok := dialer.(quicDialer)
	if !ok {
		t.Fatalf("dialer type = %T, want %T", dialer, quicDialer{})
	}
	if got.observer != observer {
		t.Fatalf("quic dialer observer = %v, want the factory observer", got.observer)
	}
}

func TestDialerFactoryRejectsIncompleteIsolatedLaunch(t *testing.T) {
	factory := NewDialerFactory()
	for _, launch := range []*EndpointLaunch{
		{Binary: "/bin/vev"},
		{Binary: "/bin/vev", Root: "/tmp/root"},
		{Root: "/tmp/root"},
		{Root: "/tmp/root", OwnerToken: "token"},
	} {
		_, err := factory.DialerForRemoteWithLaunch("remote.example", "work", TransportStdio, nil, launch)
		if err == nil {
			t.Fatalf("DialerForRemoteWithLaunch(%#v) error = nil, want incomplete launch rejection", launch)
		}
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
