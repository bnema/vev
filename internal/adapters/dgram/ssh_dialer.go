package dgram

import (
	"bufio"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/ports"
)

const (
	maxBootstrapStderr  = 64 * 1024
	defaultProbeTimeout = 3 * time.Second
)

type bootstrapProcess interface {
	StdoutPipe() (io.ReadCloser, error)
	Start() error
	Kill() error
}

type execBootstrapProcess struct{ cmd *exec.Cmd }

func (p execBootstrapProcess) StdoutPipe() (io.ReadCloser, error) { return p.cmd.StdoutPipe() }
func (p execBootstrapProcess) Start() error                       { return p.cmd.Start() }
func (p execBootstrapProcess) Kill() error                        { return p.cmd.Process.Kill() }

var (
	startUDPBootstrap = func(ctx context.Context, target, session string, stderr io.Writer) bootstrapProcess {
		spec := sshstdio.BuildCommandForMode(target, "_udp-bootstrap", session)
		cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
		cmd.Stderr = stderr
		return execBootstrapProcess{cmd: cmd}
	}
	listenUDPPacket = func() (net.PacketConn, error) { return net.ListenPacket("udp", ":0") }
)

type limitedBuffer struct {
	buf []byte
}

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(b.buf) < maxBootstrapStderr {
		keep := min(len(p), maxBootstrapStderr-len(b.buf))
		b.buf = append(b.buf, p[:keep]...)
	}
	return len(p), nil
}

func (b *limitedBuffer) String() string { return string(b.buf) }

// RemoteDialer connects to the authenticated datagram bootstrap for UDP remote
// attach. SSH stdio is selected only through the explicit stdio transport mode.
type RemoteDialer struct {
	Target       string
	Session      string
	ProbeTimeout time.Duration
	Log          *slog.Logger
}

func NewRemoteDialer(target, session string) RemoteDialer {
	return RemoteDialer{Target: target, Session: session, ProbeTimeout: defaultProbeTimeout}
}

func NewRemoteDialerWithLogger(target, session string, log *slog.Logger) RemoteDialer {
	return RemoteDialer{Target: target, Session: session, ProbeTimeout: defaultProbeTimeout, Log: log}
}

func (d RemoteDialer) Dial(ctx context.Context) (ports.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	probeTimeout := d.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	var stderr limitedBuffer
	proc := startUDPBootstrap(ctx, d.Target, d.Session, &stderr)
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, udpUnavailable("bootstrap stdout", err, &stderr)
	}
	if err := proc.Start(); err != nil {
		return nil, udpUnavailable("start bootstrap", err, &stderr)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = proc.Kill()
		}
		_ = stdout.Close()
	}()

	ready, err := readUDPReady(stdout)
	if err != nil {
		return nil, udpUnavailable("read bootstrap readiness", err, &stderr)
	}
	peer, err := net.ResolveUDPAddr("udp", net.JoinHostPort(sshTargetHost(d.Target), strconv.Itoa(ready.port)))
	if err != nil {
		return nil, udpUnavailable("resolve UDP peer", err, &stderr)
	}
	pc, err := listenUDPPacket()
	if err != nil {
		return nil, udpUnavailable("listen UDP", err, &stderr)
	}
	t, err := NewTransport(pc, peer, ready.key, 1, 2)
	if err != nil {
		_ = pc.Close()
		return nil, udpUnavailable("create UDP transport", err, &stderr)
	}
	probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	if err := t.Probe(probeCtx); err != nil {
		_ = t.Close()
		return nil, udpUnavailable("probe UDP transport", err, &stderr)
	}
	cleanup = false
	return t, nil
}

type udpReady struct {
	port int
	key  []byte
}

func readUDPReady(r io.Reader) (udpReady, error) {
	line, err := bufio.NewReader(r).ReadString('\n')
	if err != nil && line == "" {
		return udpReady{}, err
	}
	fields := strings.Fields(line)
	if len(fields) != 3 || fields[0] != "VEV-UDP" {
		return udpReady{}, fmt.Errorf("malformed readiness line %q", strings.TrimSpace(line))
	}
	port, err := strconv.Atoi(fields[1])
	if err != nil || port < 1 || port > 65535 {
		return udpReady{}, fmt.Errorf("invalid UDP port %q", fields[1])
	}
	key, err := base64.StdEncoding.DecodeString(fields[2])
	if err != nil {
		return udpReady{}, fmt.Errorf("invalid bootstrap key: %w", err)
	}
	return udpReady{port: port, key: key}, nil
}

func udpUnavailable(action string, err error, stderr fmt.Stringer) error {
	msg := strings.TrimSpace(stderr.String())
	if msg != "" {
		return fmt.Errorf("vev: remote UDP transport unavailable: %s: %w (stderr: %s)", action, err, msg)
	}
	return fmt.Errorf("vev: remote UDP transport unavailable: %s: %w", action, err)
}

func sshTargetHost(target string) string {
	if at := strings.LastIndexByte(target, '@'); at >= 0 {
		target = target[at+1:]
	}
	if h, _, err := net.SplitHostPort(target); err == nil {
		return h
	}
	return target
}
