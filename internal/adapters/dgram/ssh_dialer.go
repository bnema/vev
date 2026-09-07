package dgram

import (
	"bufio"
	"context"
	"encoding/base64"
	"errors"
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
	"github.com/bnema/vev/internal/protocol/wire"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

const (
	maxBootstrapStderr      = 64 * 1024
	defaultBootstrapTimeout = 10 * time.Second
	defaultProbeTimeout     = 3 * time.Second
)

type bootstrapProcess interface {
	StdoutPipe() (io.ReadCloser, error)
	Start() error
	Kill() error
	Wait() error
}

type execBootstrapProcess struct{ cmd *exec.Cmd }

func (p execBootstrapProcess) StdoutPipe() (io.ReadCloser, error) { return p.cmd.StdoutPipe() }
func (p execBootstrapProcess) Start() error                       { return p.cmd.Start() }
func (p execBootstrapProcess) Kill() error                        { return p.cmd.Process.Kill() }
func (p execBootstrapProcess) Wait() error                        { return p.cmd.Wait() }

var (
	startUDPBootstrap = func(ctx context.Context, target string, stderr io.Writer) bootstrapProcess {
		spec := sshstdio.BuildCommandForMode(target, "_udp-bootstrap", "")
		cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
		cmd.Stderr = stderr
		return execBootstrapProcess{cmd: cmd}
	}
	listenUDPPacket = func(ctx context.Context) (net.PacketConn, error) {
		var lc net.ListenConfig
		return lc.ListenPacket(ctx, "udp", ":0")
	}
	lookupUDPIPAddrs = func(ctx context.Context, host string) ([]net.IPAddr, error) {
		return net.DefaultResolver.LookupIPAddr(ctx, host)
	}
	probeUDPTransport = func(ctx context.Context, t *Transport) error { return t.Probe(ctx) }
)

// RemoteDialFailureKind classifies which part of UDP remote bootstrap failed
// so client-side recovery policy and tests do not have to parse human text.
type RemoteDialFailureKind uint8

const (
	RemoteDialBootstrapUnavailable RemoteDialFailureKind = iota + 1
	RemoteDialProbeUnreachable
)

// RemoteDialError wraps a UDP remote bootstrap failure with a stable machine
// classification while preserving the existing human-facing error text.
type RemoteDialError struct {
	Kind   RemoteDialFailureKind
	Action string
	Err    error
	Hint   string
}

func (e *RemoteDialError) Error() string {
	base := fmt.Sprintf("remote UDP transport unavailable: %s: %v", e.Action, e.Err)
	if e.Hint != "" {
		return base + e.Hint
	}
	return base
}

func (e *RemoteDialError) Unwrap() error { return e.Err }

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
	Target           string
	BootstrapTimeout time.Duration
	ProbeTimeout     time.Duration
	Log              *slog.Logger
	RuntimeObserver  ports.SerializedRuntimeObserver
	// BootstrapBinary and BootstrapEnvironment are set only by an explicit
	// launch configuration. Empty values retain the normal remote `vev`
	// bootstrap command.
	BootstrapBinary      string
	BootstrapRoot        string
	BootstrapOwnerToken  string
	BootstrapEnvironment []string
}

func NewRemoteDialer(target, _ string) RemoteDialer {
	return RemoteDialer{Target: target, ProbeTimeout: defaultProbeTimeout}
}

func NewRemoteDialerWithLogger(target, _ string, log *slog.Logger) RemoteDialer {
	return RemoteDialer{Target: target, ProbeTimeout: defaultProbeTimeout, Log: log}
}

func (d RemoteDialer) bootstrapProcess(ctx context.Context, stderr io.Writer) bootstrapProcess {
	if d.BootstrapBinary == "" {
		return startUDPBootstrap(ctx, d.Target, stderr)
	}
	spec := sshstdio.BuildCommandForRemoteLaunchWithRoot(d.Target, d.BootstrapRoot, d.BootstrapOwnerToken, d.BootstrapBinary, d.BootstrapEnvironment, "_udp-bootstrap")
	cmd := exec.CommandContext(ctx, spec.Path, spec.Args...)
	cmd.Stderr = stderr
	return execBootstrapProcess{cmd: cmd}
}

func (d RemoteDialer) Dial(ctx context.Context) (wire.Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	bootstrapTimeout := d.BootstrapTimeout
	if bootstrapTimeout <= 0 {
		bootstrapTimeout = defaultBootstrapTimeout
	}
	probeTimeout := d.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	bootstrapCtx, bootstrapCancel := context.WithTimeout(ctx, bootstrapTimeout)
	defer bootstrapCancel()

	var stderr limitedBuffer
	proc := d.bootstrapProcess(bootstrapCtx, &stderr)
	stdout, err := proc.StdoutPipe()
	if err != nil {
		return nil, udpUnavailable("bootstrap stdout", err, &stderr)
	}
	if err := proc.Start(); err != nil {
		_ = stdout.Close()
		return nil, udpUnavailable("start bootstrap", err, &stderr)
	}
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

	ready, err := readUDPReadyContext(bootstrapCtx, stdout)
	if err != nil {
		_ = proc.Kill()
		_ = proc.Wait()
		waited = true
		cleanup = false
		return nil, udpUnavailable("read bootstrap readiness", err, &stderr)
	}
	defer pdgram.Erase(ready.key)
	if err := waitBootstrapContext(bootstrapCtx, proc); err != nil {
		waited = true
		cleanup = false
		return nil, udpUnavailable("wait bootstrap", err, &stderr)
	}
	waited = true
	cleanup = false
	// Share the existing probe budget across resolution and all candidates so a
	// hostname with many addresses cannot extend remote attach indefinitely.
	selectionCtx, selectionCancel := context.WithTimeout(ctx, probeTimeout)
	defer selectionCancel()
	peers, err := resolveUDPPeers(selectionCtx, d.Target, ready.port)
	if err != nil {
		return nil, udpUnavailable("resolve UDP peers", err, &stderr)
	}

	var (
		t            *Transport
		lastProbeErr error
	)
	for i, peer := range peers {
		if err := selectionCtx.Err(); err != nil {
			if lastProbeErr == nil {
				lastProbeErr = err
			}
			break
		}
		pc, err := listenUDPPacket(selectionCtx)
		if err != nil {
			if t != nil {
				_ = t.Close()
			}
			return nil, udpUnavailable("listen UDP", err, &stderr)
		}
		if t == nil {
			// RebindPacketConn heals NAT rebinds / stale conntrack paths by hopping to a
			// fresh local UDP socket once the link is silent long enough to probe. The
			// bind reuses listenUDPPacket so tests can stub it.
			t, err = NewTransportWithOptions(pc, peer, ready.key, 1, 2, Options{
				Observe: DiagnosticLogObserver(d.Log),
				RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
					return listenUDPPacket(ctx)
				},
			}, WithRuntimeObserver(d.RuntimeObserver))
			pdgram.Erase(ready.key)
			if err != nil {
				_ = pc.Close()
				return nil, udpUnavailable("create UDP transport", err, &stderr)
			}
		} else if err := t.replaceDialCandidate(pc, peer); err != nil {
			_ = pc.Close()
			_ = t.Close()
			return nil, udpUnavailable("replace UDP peer", err, &stderr)
		}

		probeCtx, cancel := candidateProbeContext(selectionCtx, len(peers)-i)
		err = probeUDPTransport(probeCtx, t)
		cancel()
		if err == nil {
			cleanup = false
			return t, nil
		}
		lastProbeErr = err
	}
	if t != nil {
		_ = t.Close()
	}
	if lastProbeErr == nil {
		lastProbeErr = errors.New("all resolved UDP peer candidates failed")
	}
	return nil, udpProbeUnreachable(d.Target, sanitizeUDPProbeError(lastProbeErr), &stderr)
}

func candidateProbeContext(ctx context.Context, remainingCandidates int) (context.Context, context.CancelFunc) {
	if remainingCandidates <= 1 {
		return context.WithCancel(ctx)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		return context.WithCancel(ctx)
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, remaining/time.Duration(remainingCandidates))
}

type udpReadyResult struct {
	ready udpReady
	err   error
}

func deliverUDPReady(ctx context.Context, ch chan<- udpReadyResult, res udpReadyResult) {
	select {
	case ch <- res:
	case <-ctx.Done():
		pdgram.Erase(res.ready.key)
	}
}

func readUDPReadyContext(ctx context.Context, r io.Reader) (udpReady, error) {
	ch := make(chan udpReadyResult)
	go func() {
		ready, err := readUDPReady(r)
		deliverUDPReady(ctx, ch, udpReadyResult{ready: ready, err: err})
	}()
	select {
	case res := <-ch:
		return res.ready, res.err
	case <-ctx.Done():
		return udpReady{}, ctx.Err()
	}
}

func waitBootstrapContext(ctx context.Context, proc bootstrapProcess) error {
	ch := make(chan error, 1)
	go func() { ch <- proc.Wait() }()
	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		_ = proc.Kill()
		return ctx.Err()
	}
}

func resolveUDPPeers(ctx context.Context, target string, port int) ([]*net.UDPAddr, error) {
	host := sshTargetHost(target)
	if ip := net.ParseIP(host); ip != nil {
		return []*net.UDPAddr{{IP: ip, Port: port}}, nil
	}
	addrs, err := lookupUDPIPAddrs(ctx, host)
	if err != nil {
		return nil, err
	}
	peers := make([]*net.UDPAddr, 0, len(addrs))
	seen := make(map[string]struct{}, len(addrs))
	for _, addr := range addrs {
		peer := &net.UDPAddr{IP: addr.IP, Zone: addr.Zone, Port: port}
		if _, ok := seen[peer.String()]; ok {
			continue
		}
		seen[peer.String()] = struct{}{}
		peers = append(peers, peer)
	}
	if len(peers) == 0 {
		return nil, fmt.Errorf("no UDP peer addresses for %q", host)
	}
	return peers, nil
}

type udpReady struct {
	port int
	key  []byte
}

func readUDPReady(r io.Reader) (udpReady, error) {
	out := make([]byte, pdgram.KeySize)
	var (
		port     int
		keyLen   int
		parseErr error
	)
	pdgram.SecretDo(func() {
		line, err := bufio.NewReader(r).ReadString('\n')
		if err != nil && line == "" {
			parseErr = err
			return
		}

		fields := strings.Fields(line)
		if len(fields) != 3 || fields[0] != "VEV-UDP" {
			parseErr = fmt.Errorf("malformed UDP readiness line")
			return
		}
		port, parseErr = strconv.Atoi(fields[1])
		if parseErr != nil || port < 1 || port > 65535 {
			parseErr = fmt.Errorf("invalid UDP port %q", fields[1])
			return
		}
		key, decodeErr := base64.StdEncoding.DecodeString(fields[2])
		defer pdgram.Erase(key)
		if decodeErr != nil {
			parseErr = fmt.Errorf("invalid bootstrap key: %w", decodeErr)
			return
		}
		keyLen = len(key)
		copy(out, key)
	})
	if parseErr != nil {
		pdgram.Erase(out)
		return udpReady{}, parseErr
	}
	if keyLen != pdgram.KeySize {
		pdgram.Erase(out)
		return udpReady{}, fmt.Errorf("invalid bootstrap key length %d", keyLen)
	}
	return udpReady{port: port, key: out}, nil
}

type sanitizedUDPProbeError struct{ cause error }

func (e sanitizedUDPProbeError) Error() string { return "UDP probe failed" }
func (e sanitizedUDPProbeError) Unwrap() error { return e.cause }

func sanitizeUDPProbeError(err error) error {
	switch {
	case errors.Is(err, context.Canceled):
		return context.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		return context.DeadlineExceeded
	default:
		return sanitizedUDPProbeError{cause: err}
	}
}

func udpUnavailable(action string, err error, stderr fmt.Stringer) error {
	return udpDialFailure(RemoteDialBootstrapUnavailable, action, err, stderr, "")
}

func udpDialFailure(kind RemoteDialFailureKind, action string, err error, stderr fmt.Stringer, hint string) error {
	if msg := strings.TrimSpace(stderr.String()); msg != "" {
		hint = fmt.Sprintf(" (stderr: %s)%s", msg, hint)
	}
	return &RemoteDialError{Kind: kind, Action: action, Err: err, Hint: hint}
}

// udpProbeUnreachable wraps a probe failure with actionable guidance without
// assuming a particular VPN or firewall implementation.
func udpProbeUnreachable(target string, err error, stderr fmt.Stringer) error {
	return udpDialFailure(RemoteDialProbeUnreachable, "probe UDP transport", err, stderr, fmt.Sprintf("\n"+
		"  check UDP routing and the remote firewall for VEV_UDP_PORT_RANGE.\n"+
		"  retry over SSH with `VEV_REMOTE_TRANSPORT=stdio vev attach %s`", target))
}

func sshTargetHost(target string) string {
	if at := strings.LastIndexByte(target, '@'); at >= 0 {
		target = target[at+1:]
	}
	if h, _, err := net.SplitHostPort(target); err == nil {
		return h
	}
	if strings.HasPrefix(target, "[") && strings.HasSuffix(target, "]") {
		return strings.TrimPrefix(strings.TrimSuffix(target, "]"), "[")
	}
	return target
}
