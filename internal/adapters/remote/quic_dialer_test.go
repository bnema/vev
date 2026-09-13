package remote

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/protocol/wire"
)

// stubBootstrapProcess replays one readiness line then exits, modelling
// the detached `_quic-bootstrap` child: the session outlives the child,
// so Wait succeeds promptly after the line is consumed.
type stubBootstrapProcess struct {
	line    []byte
	waitErr error

	killOnce sync.Once
	killed   bool
	waitOnce sync.Once
	waited   chan struct{}
}

func newStubBootstrapProcess(line []byte, waitErr error) *stubBootstrapProcess {
	return &stubBootstrapProcess{line: line, waitErr: waitErr, waited: make(chan struct{})}
}

func (p *stubBootstrapProcess) StdoutPipe() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(append(append([]byte(nil), p.line...), '\n'))), nil
}

func (p *stubBootstrapProcess) Start() error { return nil }

func (p *stubBootstrapProcess) Kill() error {
	p.killOnce.Do(func() { p.killed = true })
	return nil
}

func (p *stubBootstrapProcess) Wait() error {
	p.waitOnce.Do(func() { close(p.waited) })
	<-p.waited
	return p.waitErr
}

// stubQuicDialer runs quicDialer.Dial against the stub bootstrap child.
func stubQuicDialerForTest(target string, stub *stubBootstrapProcess) quicDialer {
	return quicDialer{
		target: target,
		startBootstrap: func(context.Context, string, *EndpointLaunch, *limitedBootstrapBuffer) bootstrapProcess {
			return stub
		},
	}
}

func TestQUICDialerComposesHostPortAndAuthenticates(t *testing.T) {
	server, readiness, err := quic.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	raw, err := quic.EncodeReadiness(readiness)
	if err != nil {
		t.Fatal(err)
	}

	accepted := make(chan wire.Transport, 1)
	go func() {
		transport, err := server.Accept(context.Background())
		if err == nil {
			accepted <- transport
		}
	}()

	// The SSH target carries a user prefix; the dial must resolve the
	// bare host and attach the published port, never a remote loopback.
	stub := newStubBootstrapProcess(raw, nil)
	dialer := stubQuicDialerForTest("user@127.0.0.1", stub)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	transport, err := dialer.Dial(ctx)
	if err != nil {
		t.Fatalf("Dial() error = %v", err)
	}
	defer func() { _ = transport.Close() }()

	select {
	case peer := <-accepted:
		defer func() { _ = peer.Close() }()
		payload := []byte("dialer session stays exact")
		if err := transport.Send(wire.Envelope{Payload: payload}); err != nil {
			t.Fatalf("Send() error = %v", err)
		}
		got, err := peer.Recv()
		if err != nil {
			t.Fatalf("Recv() error = %v", err)
		}
		if !bytes.Equal(got.Payload, payload) {
			t.Fatalf("payload = %q, want %q", got.Payload, payload)
		}
	case <-ctx.Done():
		t.Fatal("bootstrap accept timed out")
	}
	// The short-lived bootstrap child was reaped, never killed: the
	// session outlives the SSH channel by design.
	if stub.killed {
		t.Fatal("bootstrap child was killed; the detached proxy must outlive it")
	}
	select {
	case <-stub.waited:
	default:
		t.Fatal("bootstrap child was not reaped")
	}
}

func TestQUICDialerRejectsFailedBootstrapWait(t *testing.T) {
	server, readiness, err := quic.NewServer()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = server.Close() }()
	raw, err := quic.EncodeReadiness(readiness)
	if err != nil {
		t.Fatal(err)
	}
	dialer := stubQuicDialerForTest("127.0.0.1", newStubBootstrapProcess(raw, errors.New("exit status 1")))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	transport, err := dialer.Dial(ctx)
	if err == nil {
		_ = transport.Close()
		t.Fatal("Dial() error = nil, want bootstrap wait failure")
	}
	if transport != nil {
		t.Fatalf("Dial() transport = %T, want nil", transport)
	}
}

func TestResolveQUICPeer(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		port    int
		want    string
		wantErr bool
	}{
		{name: "bare host", target: "remote.example", port: 4433, want: "remote.example:4433"},
		{name: "user prefix", target: "user@remote.example", port: 4433, want: "remote.example:4433"},
		{name: "loopback", target: "127.0.0.1", port: 4433, want: "127.0.0.1:4433"},
		{name: "explicit port replaced", target: "remote.example:22", port: 4433, want: "remote.example:4433"},
		{name: "bracketed ipv6", target: "[::1]", port: 4433, want: "[::1]:4433"},
		{name: "ipv6 with port", target: "[::1]:22", port: 4433, want: "[::1]:4433"},
		{name: "zero port", target: "remote.example", port: 0, wantErr: true},
		{name: "port overflow", target: "remote.example", port: 65536, wantErr: true},
		{name: "empty host", target: "user@", port: 4433, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveQUICPeer(tt.target, tt.port)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("resolveQUICPeer() error = nil, want failure")
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveQUICPeer() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("resolveQUICPeer() = %q, want %q", got, tt.want)
			}
		})
	}
}
