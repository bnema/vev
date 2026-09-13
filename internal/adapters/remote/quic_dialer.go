package remote

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/bnema/vev/internal/adapters/quic"
	"github.com/bnema/vev/internal/adapters/sshstdio"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

// quicDialer is the default remote carriage: SSH launches one ephemeral
// QUIC bootstrap server, the readiness line binds the pinned dial, and a
// single authenticated stream carries the session. Command construction
// is structured (no shell interpolation); secret-bearing values never
// reach logs or error text.
type quicDialer struct {
	target   string
	log      *slog.Logger
	observer ports.SerializedRuntimeObserver
	launch   *EndpointLaunch
	// startBootstrap builds the SSH bootstrap child. It is a field
	// (not a method) so tests can stub the child without a network.
	startBootstrap func(ctx context.Context, target string, launch *EndpointLaunch, stderr *limitedBootstrapBuffer) bootstrapProcess
}

var _ wire.Dialer = quicDialer{}

func (d quicDialer) Dial(ctx context.Context) (wire.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bootstrapTimeout := 30 * time.Second
	bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, bootstrapTimeout)
	defer bootstrapCancel()

	var stderr limitedBootstrapBuffer
	start := d.startBootstrap
	if start == nil {
		start = startSSHBootstrap
	}
	proc := start(bootstrapCtx, d.target, d.launch, &stderr)
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, quicUnavailable("bootstrap stdout", err)
	}
	if err := proc.Start(); err != nil {
		_ = stdout.Close()
		return nil, quicUnavailable("start bootstrap", err)
	}
	// Deterministic process cleanup: kill on every path except a
	// transferred success. The bootstrap prints one readiness line
	// from a detached proxy and exits, so Wait returns promptly and
	// the session outlives this SSH child.
	cleanup := true
	waited := false
	defer func() {
		if cleanup {
			_ = proc.Kill()
		}
		if !waited {
			_ = proc.Wait()
		}
		_ = stdout.Close()
	}()

	readiness, err := quic.ReadReadiness(bootstrapCtx, stdout)
	if err != nil {
		_ = proc.Kill()
		_ = proc.Wait()
		waited = true
		cleanup = false
		return nil, quicUnavailable("read bootstrap readiness", err)
	}
	// The bootstrap SSH channel served its purpose: the detached proxy
	// owns the session lifetime, so reap the short-lived child before
	// dialing. Do not kill it: SIGKILL would race the proxy detach.
	if err := proc.Wait(); err != nil {
		waited = true
		cleanup = false
		return nil, quicUnavailable("wait bootstrap", err)
	}
	waited = true
	cleanup = false

	// Compose the dial address from the SSH target host plus the
	// published port: the readiness record is host-independent and a
	// remote loopback address is never dialed.
	addr, err := resolveQUICPeer(d.target, readiness.Port)
	if err != nil {
		return nil, quicUnavailable("resolve QUIC peer", err)
	}
	transport, err := quic.Dial(bootstrapCtx, addr, readiness, quic.Config{}, 15*time.Second, quic.WithRuntimeObserver(d.observer))
	if err != nil {
		return nil, quicUnavailable("dial bootstrap QUIC", err)
	}
	return transport, nil
}

// resolveQUICPeer composes the dial address from the SSH target host
// and the bootstrap-published port. user@ prefixes, explicit ports,
// and bracketed IPv6 literals are stripped to the bare host before
// the published port is attached.
func resolveQUICPeer(target string, port int) (string, error) {
	if port <= 0 || port > 65535 {
		return "", fmt.Errorf("invalid bootstrap port %d", port)
	}
	host := target
	if at := strings.LastIndexByte(host, '@'); at >= 0 {
		host = host[at+1:]
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[")
	if host == "" {
		return "", fmt.Errorf("empty bootstrap host")
	}
	return net.JoinHostPort(host, strconv.Itoa(port)), nil
}

// startSSHBootstrap launches the real `_quic-bootstrap` SSH child.
func startSSHBootstrap(ctx context.Context, target string, launch *EndpointLaunch, stderr *limitedBootstrapBuffer) bootstrapProcess {
	if launch != nil {
		spec := sshstdio.BuildCommandForRemoteLaunchWithRoot(target, launch.Root, launch.OwnerToken, launch.Binary, launch.Environment, "_quic-bootstrap")
		cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
		cmd.Stderr = stderr
		return execBootstrapProcess{cmd: cmd}
	}
	spec := sshstdio.BuildCommandForMode(target, "_quic-bootstrap", "")
	cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
	cmd.Stderr = stderr
	return execBootstrapProcess{cmd: cmd}
}

// bootstrapProcess is the stub seam for the SSH bootstrap child.
type bootstrapProcess interface {
	StdoutPipe() (io.ReadCloser, error)
	Start() error
	Kill() error
	Wait() error
}

type execBootstrapProcess struct{ cmd *exec.Cmd }

func (p execBootstrapProcess) StdoutPipe() (io.ReadCloser, error) { return p.cmd.StdoutPipe() }

func (p execBootstrapProcess) Start() error { return p.cmd.Start() }
func (p execBootstrapProcess) Kill() error  { return p.cmd.Process.Kill() }
func (p execBootstrapProcess) Wait() error  { return p.cmd.Wait() }

// limitedBootstrapBuffer bounds captured stderr; secret-bearing values
// are never logged (the bootstrap prints readiness on stdout only).
type limitedBootstrapBuffer struct {
	buf []byte
}

func (b *limitedBootstrapBuffer) Write(p []byte) (int, error) {
	const maxBootstrapStderr = 4 << 10
	if len(b.buf) < maxBootstrapStderr {
		keep := min(len(p), maxBootstrapStderr-len(b.buf))
		b.buf = append(b.buf, p[:keep]...)
	}
	return len(p), nil
}

func quicUnavailable(action string, err error) error {
	return fmt.Errorf("remote QUIC transport unavailable: %s: %w", action, err)
}
