package dgram

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
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

type fakeBootstrapProcess struct {
	stdout   io.ReadCloser
	startErr error
	killed   bool
}

func (p *fakeBootstrapProcess) StdoutPipe() (io.ReadCloser, error) { return p.stdout, nil }
func (p *fakeBootstrapProcess) Start() error                       { return p.startErr }
func (p *fakeBootstrapProcess) Kill() error                        { p.killed = true; return nil }

func withBootstrapStarter(t *testing.T, fn func(context.Context, string, string, io.Writer) bootstrapProcess) {
	t.Helper()
	old := startUDPBootstrap
	startUDPBootstrap = fn
	t.Cleanup(func() { startUDPBootstrap = old })
}

func withListenUDP(t *testing.T, fn func(context.Context) (net.PacketConn, error)) {
	t.Helper()
	old := listenUDPPacket
	listenUDPPacket = fn
	t.Cleanup(func() { listenUDPPacket = old })
}

func TestRemoteDialerUDPBootstrapFailures(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	tests := []struct {
		name   string
		stdout string
		start  error
		listen error
		want   string
		killed bool
	}{
		{name: "start failure", start: errors.New("ssh missing"), want: "vev: remote UDP transport unavailable: start bootstrap: ssh missing"},
		{name: "malformed readiness", stdout: "hello\n", want: "malformed readiness line", killed: true},
		{name: "packet listen failure", stdout: "VEV-UDP 4444 " + key + "\n", listen: errors.New("bind denied"), want: "listen UDP: bind denied", killed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proc := &fakeBootstrapProcess{stdout: io.NopCloser(strings.NewReader(tt.stdout)), startErr: tt.start}
			withBootstrapStarter(t, func(context.Context, string, string, io.Writer) bootstrapProcess { return proc })
			if tt.listen != nil {
				withListenUDP(t, func(context.Context) (net.PacketConn, error) { return nil, tt.listen })
			}
			tr, err := NewRemoteDialer("127.0.0.1", "work").Dial(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Dial() error = %v, want containing %q", err, tt.want)
			}
			if tr != nil {
				t.Fatalf("Dial() transport = %T, want nil", tr)
			}
			if proc.killed != tt.killed {
				t.Fatalf("killed=%v, want %v", proc.killed, tt.killed)
			}
		})
	}
}

func TestRemoteDialerProbeFailureCleansUpWithoutStdioFallback(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	proc := &fakeBootstrapProcess{stdout: io.NopCloser(strings.NewReader("VEV-UDP 4444 " + key + "\n"))}
	withBootstrapStarter(t, func(_ context.Context, target, session string, _ io.Writer) bootstrapProcess {
		if target != "127.0.0.1" || session != "work" {
			t.Fatalf("bootstrap target/session = %q/%q", target, session)
		}
		return proc
	})
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	withListenUDP(t, func(context.Context) (net.PacketConn, error) {
		return closeTrackPacketConn{PacketConn: pc, closed: &closed}, nil
	})
	d := NewRemoteDialer("127.0.0.1", "work")
	d.ProbeTimeout = time.Nanosecond
	tr, err := d.Dial(context.Background())
	if err == nil || !strings.Contains(err.Error(), "vev: remote UDP transport unavailable: probe UDP transport") {
		t.Fatalf("Dial() error = %v, want probe unavailable", err)
	}
	if tr != nil {
		t.Fatalf("transport=%T, want nil", tr)
	}
	if !closed {
		t.Fatal("packet conn not closed after probe failure")
	}
	if !proc.killed {
		t.Fatal("bootstrap process not killed after probe failure")
	}
}

type closeTrackPacketConn struct {
	net.PacketConn
	closed *bool
}

func (c closeTrackPacketConn) Close() error { *c.closed = true; return c.PacketConn.Close() }

func TestReadUDPReady(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	tests := []struct {
		name, line string
		wantPort   int
		wantErr    string
	}{
		{name: "valid", line: "VEV-UDP 1234 " + key + "\n", wantPort: 1234},
		{name: "bad prefix", line: "NOPE 1234 " + key + "\n", wantErr: "malformed readiness"},
		{name: "bad port", line: "VEV-UDP 99999 " + key + "\n", wantErr: "invalid UDP port"},
		{name: "bad key", line: "VEV-UDP 1234 !!!\n", wantErr: "invalid bootstrap key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readUDPReady(strings.NewReader(tt.line))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err=%v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.port != tt.wantPort || len(got.key) != pdgram.KeySize {
				t.Fatalf("ready=%+v keylen=%d", got, len(got.key))
			}
		})
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
