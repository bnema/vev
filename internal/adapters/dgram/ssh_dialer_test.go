package dgram

import (
	"context"
	"encoding/base64"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"time"

	pdgram "github.com/bnema/vev/pkg/dgram"
)

type fakeBootstrapProcess struct {
	stdout   io.ReadCloser
	startErr error
	waitErr  error
	killed   bool
	waited   bool
}

type closeTrackReadCloser struct {
	io.Reader
	closed *bool
}

func (r closeTrackReadCloser) Close() error { *r.closed = true; return nil }

func (p *fakeBootstrapProcess) StdoutPipe() (io.ReadCloser, error) { return p.stdout, nil }
func (p *fakeBootstrapProcess) Start() error                       { return p.startErr }
func (p *fakeBootstrapProcess) Kill() error                        { p.killed = true; return nil }
func (p *fakeBootstrapProcess) Wait() error                        { p.waited = true; return p.waitErr }

func withBootstrapStarter(t *testing.T, fn func(context.Context, string, io.Writer) bootstrapProcess) {
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

func withUDPIPLookup(t *testing.T, fn func(context.Context, string) ([]net.IPAddr, error)) {
	t.Helper()
	old := lookupUDPIPAddrs
	lookupUDPIPAddrs = fn
	t.Cleanup(func() { lookupUDPIPAddrs = old })
}

func withUDPProbe(t *testing.T, fn func(context.Context, *Transport) error) {
	t.Helper()
	old := probeUDPTransport
	probeUDPTransport = fn
	t.Cleanup(func() { probeUDPTransport = old })
}

func TestRemoteDialerUDPBootstrapFailures(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	tests := []struct {
		name             string
		stdout           string
		start            error
		listen           error
		want             string
		killed           bool
		waited           bool
		wantStdoutClosed bool
	}{
		{name: "start failure", start: errors.New("ssh missing"), want: "remote UDP transport unavailable: start bootstrap: ssh missing", wantStdoutClosed: true},
		{name: "malformed readiness", stdout: "hello\n", want: "malformed UDP readiness line", killed: true, waited: true, wantStdoutClosed: true},
		{name: "bootstrap wait failure", stdout: "VEV-UDP 4444 " + key + "\n", want: "wait bootstrap: exit status 255", waited: true, wantStdoutClosed: true},
		{name: "packet listen failure", stdout: "VEV-UDP 4444 " + key + "\n", listen: errors.New("bind denied"), want: "listen UDP: bind denied", waited: true, wantStdoutClosed: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			stdoutClosed := false
			proc := &fakeBootstrapProcess{stdout: closeTrackReadCloser{Reader: strings.NewReader(tt.stdout), closed: &stdoutClosed}, startErr: tt.start}
			if tt.name == "bootstrap wait failure" {
				proc.waitErr = errors.New("exit status 255")
			}
			withBootstrapStarter(t, func(context.Context, string, io.Writer) bootstrapProcess { return proc })
			if tt.listen != nil {
				withListenUDP(t, func(context.Context) (net.PacketConn, error) { return nil, tt.listen })
			}
			tr, err := NewRemoteDialer("127.0.0.1", "work").Dial(context.Background())
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("Dial() error = %v, want containing %q", err, tt.want)
			}
			var dialErr *RemoteDialError
			if !errors.As(err, &dialErr) {
				t.Fatalf("Dial() error = %T, want *RemoteDialError", err)
			}
			if dialErr.Kind != RemoteDialBootstrapUnavailable {
				t.Fatalf("Dial() error kind = %v, want bootstrap unavailable", dialErr.Kind)
			}
			if tr != nil {
				t.Fatalf("Dial() transport = %T, want nil", tr)
			}
			if proc.killed != tt.killed {
				t.Fatalf("killed=%v, want %v", proc.killed, tt.killed)
			}
			if proc.waited != tt.waited {
				t.Fatalf("waited=%v, want %v", proc.waited, tt.waited)
			}
			if stdoutClosed != tt.wantStdoutClosed {
				t.Fatalf("stdout closed=%v, want %v", stdoutClosed, tt.wantStdoutClosed)
			}
		})
	}
}

func TestListenUDPPacketUsesDualStackWildcard(t *testing.T) {
	pc, err := listenUDPPacket(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = pc.Close() }()
	addr, ok := pc.LocalAddr().(*net.UDPAddr)
	if !ok {
		t.Fatalf("local addr = %T, want *net.UDPAddr", pc.LocalAddr())
	}
	if !addr.IP.IsUnspecified() {
		t.Fatalf("listen IP = %v, want unspecified wildcard", addr.IP)
	}
}

func TestResolveUDPPeers(t *testing.T) {
	tests := []struct {
		name        string
		target      string
		addrs       []net.IPAddr
		wantPeers   []string
		wantErr     string
		wantLookups int
	}{
		{
			name:   "hostname preserves order and removes duplicates",
			target: "remote.example",
			addrs: []net.IPAddr{
				{IP: net.ParseIP("192.0.2.1")},
				{IP: net.ParseIP("2001:db8::1")},
				{IP: net.ParseIP("192.0.2.1")},
			},
			wantPeers:   []string{"192.0.2.1:4444", "[2001:db8::1]:4444"},
			wantLookups: 1,
		},
		{
			name:        "explicit IP bypasses lookup",
			target:      "[2001:db8::1]",
			wantPeers:   []string{"[2001:db8::1]:4444"},
			wantLookups: 0,
		},
		{
			name:        "empty lookup result",
			target:      "remote.example",
			wantErr:     `no UDP peer addresses for "remote.example"`,
			wantLookups: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lookups := 0
			withUDPIPLookup(t, func(_ context.Context, host string) ([]net.IPAddr, error) {
				lookups++
				if host != "remote.example" {
					t.Fatalf("lookup host = %q, want remote.example", host)
				}
				return tt.addrs, nil
			})

			peers, err := resolveUDPPeers(context.Background(), tt.target, 4444)
			if tt.wantErr != "" {
				if err == nil || err.Error() != tt.wantErr {
					t.Fatalf("resolveUDPPeers() error = %v, want %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatal(err)
			} else {
				got := make([]string, 0, len(peers))
				for _, peer := range peers {
					got = append(got, peer.String())
				}
				if !slices.Equal(got, tt.wantPeers) {
					t.Fatalf("peers = %v, want %v", got, tt.wantPeers)
				}
			}
			if got := lookups; got != tt.wantLookups {
				t.Fatalf("lookup calls = %d, want %d", got, tt.wantLookups)
			}
		})
	}
}

func TestCandidateProbeContext(t *testing.T) {
	tests := []struct {
		name              string
		remaining         int
		newContext        func() (context.Context, context.CancelFunc)
		wantDeadline      bool
		wantDone          bool
		minDeadlineRemain time.Duration
		maxDeadlineRemain time.Duration
	}{
		{
			name:      "one candidate retains selection deadline",
			remaining: 1,
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Hour)
			},
			wantDeadline:      true,
			minDeadlineRemain: 59 * time.Minute,
			maxDeadlineRemain: time.Hour,
		},
		{
			name:       "no selection deadline",
			remaining:  2,
			newContext: func() (context.Context, context.CancelFunc) { return context.WithCancel(context.Background()) },
		},
		{
			name:      "expired selection deadline",
			remaining: 2,
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			},
			wantDeadline: true,
			wantDone:     true,
		},
		{
			name:      "splits remaining budget",
			remaining: 4,
			newContext: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), time.Hour)
			},
			wantDeadline:      true,
			minDeadlineRemain: 14 * time.Minute,
			maxDeadlineRemain: 16 * time.Minute,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			selectionCtx, selectionCancel := tt.newContext()
			defer selectionCancel()
			probeCtx, probeCancel := candidateProbeContext(selectionCtx, tt.remaining)
			defer probeCancel()

			deadline, hasDeadline := probeCtx.Deadline()
			if hasDeadline != tt.wantDeadline {
				t.Fatalf("probe context has deadline = %v, want %v", hasDeadline, tt.wantDeadline)
			}
			if hasDeadline && tt.minDeadlineRemain != 0 {
				remaining := time.Until(deadline)
				if remaining < tt.minDeadlineRemain || remaining > tt.maxDeadlineRemain {
					t.Fatalf("probe deadline remaining = %v, want [%v, %v]", remaining, tt.minDeadlineRemain, tt.maxDeadlineRemain)
				}
			}
			if tt.wantDone {
				select {
				case <-probeCtx.Done():
				default:
					t.Fatal("probe context is not done")
				}
			}
		})
	}
}

func TestRemoteDialerFallsBackAcrossResolvedUDPPeers(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	proc := &fakeBootstrapProcess{stdout: io.NopCloser(strings.NewReader("VEV-UDP 4444 " + key + "\n"))}
	withBootstrapStarter(t, func(context.Context, string, io.Writer) bootstrapProcess { return proc })
	withUDPIPLookup(t, func(_ context.Context, host string) ([]net.IPAddr, error) {
		if host != "remote.example" {
			t.Fatalf("lookup host = %q, want remote.example", host)
		}
		return []net.IPAddr{
			{IP: net.ParseIP("192.0.2.1")},
			{IP: net.ParseIP("192.0.2.2")},
		}, nil
	})

	var (
		firstClosed  bool
		secondClosed bool
		listenCalls  int
	)
	withListenUDP(t, func(context.Context) (net.PacketConn, error) {
		var lc net.ListenConfig
		pc, err := lc.ListenPacket(context.Background(), "udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		listenCalls++
		switch listenCalls {
		case 1:
			return closeTrackPacketConn{PacketConn: pc, closed: &firstClosed}, nil
		case 2:
			return closeTrackPacketConn{PacketConn: pc, closed: &secondClosed}, nil
		default:
			t.Fatalf("listen UDP calls = %d, want at most 2", listenCalls)
			return nil, nil
		}
	})

	var (
		peers          []string
		firstTransport *Transport
	)
	withUDPProbe(t, func(_ context.Context, tr *Transport) error {
		tr.mu.Lock()
		peer := tr.peer.String()
		tr.mu.Unlock()
		peers = append(peers, peer)
		if len(peers) == 1 {
			firstTransport = tr
			return errors.New("first UDP address is unreachable")
		}
		if tr != firstTransport {
			t.Fatal("fallback created a new transport and reset its authenticated state")
		}
		return nil
	})

	tr, err := NewRemoteDialer("remote.example", "work").Dial(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got, want := peers, []string{"192.0.2.1:4444", "192.0.2.2:4444"}; !slices.Equal(got, want) {
		t.Fatalf("probed peers = %v, want %v", got, want)
	}
	if !firstClosed {
		t.Fatal("first candidate socket was not closed")
	}
	if secondClosed {
		t.Fatal("winning candidate socket closed before transport close")
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
	if !secondClosed {
		t.Fatal("winning candidate socket was not closed with transport")
	}
}

func TestRemoteDialerReportsFailureAfterAllResolvedUDPPeers(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	proc := &fakeBootstrapProcess{stdout: io.NopCloser(strings.NewReader("VEV-UDP 4444 " + key + "\n"))}
	withBootstrapStarter(t, func(context.Context, string, io.Writer) bootstrapProcess { return proc })
	withUDPIPLookup(t, func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("192.0.2.1")},
			{IP: net.ParseIP("192.0.2.2")},
		}, nil
	})

	closed := make([]bool, 2)
	listenCalls := 0
	withListenUDP(t, func(context.Context) (net.PacketConn, error) {
		if listenCalls == len(closed) {
			t.Fatal("opened more sockets than resolved candidates")
		}
		var lc net.ListenConfig
		pc, err := lc.ListenPacket(context.Background(), "udp", "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		tracked := closeTrackPacketConn{PacketConn: pc, closed: &closed[listenCalls]}
		listenCalls++
		return tracked, nil
	})

	probeErr := errors.New("unreachable at 192.0.2.2:4444")
	probeCalls := 0
	withUDPProbe(t, func(context.Context, *Transport) error {
		probeCalls++
		return probeErr
	})

	tr, err := NewRemoteDialer("remote.example", "work").Dial(context.Background())
	if err == nil {
		t.Fatal("Dial() error = nil, want all candidates unavailable")
	}
	if tr != nil {
		t.Fatalf("Dial() transport = %T, want nil", tr)
	}
	if got, want := probeCalls, 2; got != want {
		t.Fatalf("probe calls = %d, want %d", got, want)
	}
	for i, wasClosed := range closed {
		if !wasClosed {
			t.Fatalf("candidate socket %d was not closed", i+1)
		}
	}
	if !errors.Is(err, probeErr) {
		t.Fatalf("Dial() error = %v, want final probe cause", err)
	}
	if strings.Contains(err.Error(), "192.0.2.") {
		t.Fatalf("Dial() error leaked candidate address: %q", err)
	}
}

func TestRemoteDialerProbeFailureCleansUpWithoutStdioFallback(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	proc := &fakeBootstrapProcess{stdout: io.NopCloser(strings.NewReader("VEV-UDP 4444 " + key + "\n"))}
	withBootstrapStarter(t, func(_ context.Context, target string, _ io.Writer) bootstrapProcess {
		if target != "127.0.0.1" {
			t.Fatalf("bootstrap target = %q", target)
		}
		return proc
	})
	var lc net.ListenConfig
	pc, err := lc.ListenPacket(context.Background(), "udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	closed := false
	withListenUDP(t, func(context.Context) (net.PacketConn, error) {
		return closeTrackPacketConn{PacketConn: pc, closed: &closed}, nil
	})
	withUDPProbe(t, func(context.Context, *Transport) error { return context.DeadlineExceeded })
	d := NewRemoteDialer("127.0.0.1", "work")
	d.ProbeTimeout = time.Second
	tr, err := d.Dial(context.Background())
	if err == nil || !strings.Contains(err.Error(), "remote UDP transport unavailable: probe UDP transport") {
		t.Fatalf("Dial() error = %v, want probe unavailable", err)
	}
	var dialErr *RemoteDialError
	if !errors.As(err, &dialErr) {
		t.Fatalf("Dial() error = %T, want *RemoteDialError", err)
	}
	if dialErr.Kind != RemoteDialProbeUnreachable {
		t.Fatalf("Dial() error kind = %v, want probe unreachable", dialErr.Kind)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Dial() error = %v, want probe deadline cause", err)
	}
	for _, want := range []string{"context deadline exceeded", "firewall", "VEV_UDP_PORT_RANGE", "VEV_REMOTE_TRANSPORT=stdio vev attach 127.0.0.1"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("Dial() error = %q, want actionable hint %q", err, want)
		}
	}
	if tr != nil {
		t.Fatalf("transport=%T, want nil", tr)
	}
	if !closed {
		t.Fatal("packet conn not closed after probe failure")
	}
	if proc.killed {
		t.Fatal("bootstrap process killed after it was already waited")
	}
	if !proc.waited {
		t.Fatal("bootstrap process not waited after probe failure")
	}
}

type closeTrackPacketConn struct {
	net.PacketConn
	closed *bool
}

func (c closeTrackPacketConn) Close() error { *c.closed = true; return c.PacketConn.Close() }

func TestDeliverUDPReadyErasesParsedKeyAfterCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	key := make([]byte, pdgram.KeySize)
	for i := range key {
		key[i] = byte(i + 1)
	}
	ready, err := readUDPReady(strings.NewReader("VEV-UDP 1234 " + base64.StdEncoding.EncodeToString(key) + "\n"))
	if err != nil {
		t.Fatal(err)
	}

	deliverUDPReady(ctx, make(chan udpReadyResult), udpReadyResult{ready: ready})

	for i, b := range ready.key {
		if b != 0 {
			t.Fatalf("key[%d] = %d, want erased", i, b)
		}
	}
}

func TestReadUDPReady(t *testing.T) {
	key := base64.StdEncoding.EncodeToString(make([]byte, pdgram.KeySize))
	tests := []struct {
		name, line string
		wantPort   int
		wantErr    string
		exactErr   bool
	}{
		{name: "valid", line: "VEV-UDP 1234 " + key + "\n", wantPort: 1234},
		{name: "bad prefix", line: "NOPE 1234 " + key + "\n", wantErr: "malformed UDP readiness line", exactErr: true},
		{name: "bad port", line: "VEV-UDP 99999 " + key + "\n", wantErr: `invalid UDP port "99999"`, exactErr: true},
		{name: "bad key", line: "VEV-UDP 1234 !!!\n", wantErr: "invalid bootstrap key"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readUDPReady(strings.NewReader(tt.line))
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("err=nil, want %q", tt.wantErr)
				}
				if tt.exactErr && err.Error() != tt.wantErr {
					t.Fatalf("err=%q, want exactly %q", err, tt.wantErr)
				}
				if !tt.exactErr && !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err=%v, want containing %q", err, tt.wantErr)
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
