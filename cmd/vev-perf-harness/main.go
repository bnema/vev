// vev-perf-harness is a public-CLI performance evidence collector.  It never
// imports daemon packages: process traces are correlated by identifiers only.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bnema/vev/pkg/safedir"
	"golang.org/x/sys/unix"
)

const (
	minimumDuration               = 30 * time.Second
	minimumRepetitions            = 10
	minimumInIntervalEventSamples = 10
	measuredEventCadence          = time.Second
)

type options struct {
	vevBin, manifest, out string
	scenario              string
	warmup, duration      time.Duration
	repetitions           int
}

type manifest struct {
	Schema     uint16      `json:"schema"`
	Topologies []topology  `json:"topologies"`
	Workloads  []string    `json:"workloads"`
	Transports []transport `json:"transports"`
	Scenarios  []scenario  `json:"scenarios"`
}
type topology struct {
	ID          string `json:"id"`
	Geometry    string `json:"geometry"`
	RowsPerPane int    `json:"rows_per_pane"`
}
type transport struct {
	ID          string `json:"id"`
	Kind        string `json:"kind"`
	RTTMS       int    `json:"rtt_ms,omitempty"`
	LossPercent int    `json:"loss_percent,omitempty"`
}
type scenario struct {
	ID                 string   `json:"id"`
	Topology           string   `json:"topology"`
	Workload           string   `json:"workload"`
	Transport          string   `json:"transport"`
	Roles              []string `json:"roles"`
	InapplicableReason string   `json:"inapplicable_reason,omitempty"`
}

type processMapping struct {
	ProcessID   string `json:"process_id"`
	ClockDomain string `json:"clock_domain"`
	TracePath   string `json:"trace_path"`
	Role        string `json:"role"`
	Scenario    string `json:"scenario"`
	Run         int    `json:"run"`
	Identity    string `json:"identity"`
}

type runManifest struct {
	Scenario  string           `json:"scenario"`
	Run       int              `json:"run"`
	Processes []processMapping `json:"processes"`
}
type harnessMark struct {
	Scenario string `json:"scenario"`
	Run      int    `json:"run"`
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"kind"`
	Tick     int64  `json:"tick"`
	Valid    bool   `json:"valid"`
}

// measuredInterval belongs solely to the harness clock domain. A paired event
// is eligible only when both owned boundaries fall inside this interval.
type measuredInterval struct {
	Start int64
	End   int64
}

type eventSample struct {
	Sequence uint64 `json:"sequence"`
	Injected int64  `json:"injected_tick"`
	Flushed  int64  `json:"flushed_tick"`
	Latency  int64  `json:"latency_nanos"`
}
type span struct {
	Component, Name string
	Samples         []int64
}
type runResult struct {
	Spans          []span           `json:"process_local_spans"`
	Scenario       string           `json:"scenario"`
	Run            int              `json:"run"`
	Samples        int              `json:"samples"`
	EndToEnd       []int64          `json:"end_to_end_nanos"`
	Event          distribution     `json:"event_end_to_end"`
	Cadence        distribution     `json:"event_cadence_nanos"`
	CadenceSamples []int64          `json:"-"`
	MaxGap         int64            `json:"event_max_gap_nanos"`
	Processes      []processMapping `json:"processes"`
}
type summary struct {
	Schema           uint16        `json:"schema"`
	GitSHA           string        `json:"git_sha"`
	Warmup           string        `json:"warmup"`
	Duration         string        `json:"duration"`
	Repetitions      int           `json:"repetitions"`
	EndToEnd         distribution  `json:"harness_end_to_end"`
	Cadence          distribution  `json:"harness_event_cadence_nanos"`
	MaxGap           int64         `json:"harness_event_max_gap_nanos"`
	RunP50Dispersion distribution  `json:"run_p50_dispersion"`
	Spans            []spanSummary `json:"process_local_spans"`
	Runs             int           `json:"runs"`
}
type distribution struct {
	Count int   `json:"count"`
	P50   int64 `json:"p50"`
	P95   int64 `json:"p95"`
	P99   int64 `json:"p99"`
	Max   int64 `json:"max"`
}
type spanSummary struct {
	Component    string       `json:"component"`
	Name         string       `json:"name"`
	Distribution distribution `json:"distribution"`
}

type clock interface {
	Now() int64
	Sleep(time.Duration)
}
type systemClock struct{ start time.Time }

func (c systemClock) Now() int64          { return time.Since(c.start).Nanoseconds() }
func (systemClock) Sleep(d time.Duration) { time.Sleep(d) }

type launchedProcess interface {
	Warmup([]byte) error
	Measure([]byte, func() error, func() error) error
	Close() error
}

// roleCommand contains only argv accepted by app.parseArgs. Transport routing
// metadata configures the harness's public-command seam; it is never passed as
// an unrecognised vev flag.
type roleCommand struct {
	Args      []string
	Transport transport
}

type launcher interface {
	Launch(processMapping, roleCommand) (launchedProcess, error)
}

type launchedRole struct {
	mapping processMapping
	process launchedProcess
}

// closeLaunchedRoles closes every role in reverse launch order. errors.Join
// retains every cleanup failure in that stable order.
func closeLaunchedRoles(processes []launchedRole) error {
	var failures []error
	for i := len(processes) - 1; i >= 0; i-- {
		if err := processes[i].process.Close(); err != nil {
			failures = append(failures, fmt.Errorf("close %s role: %w", processes[i].mapping.Role, err))
		}
	}
	return errors.Join(failures...)
}

// cliLauncher is deliberately limited to executing the supplied public vev
// binary. Scenario orchestration never reaches into an in-process daemon.
type cliLauncher struct {
	bin          string
	netemFactory udpNetemFactory
	mu           sync.Mutex
	peers        map[string]peerRoute
	runtimes     map[string]string
}

type peerRoute struct {
	mapping processMapping
	command roleCommand
	pidPath string
	netem   udpNetem
}

type preparedPeer struct {
	route          peerRoute
	cleanupRuntime func() error
}

func (p preparedPeer) Warmup([]byte) error                              { return nil }
func (p preparedPeer) Measure([]byte, func() error, func() error) error { return nil }
func (p preparedPeer) Close() error {
	var errs []error
	if p.route.pidPath != "" {
		b, err := os.ReadFile(p.route.pidPath)
		if err == nil {
			pid, parseErr := strconv.Atoi(strings.TrimSpace(string(b)))
			if parseErr != nil || pid <= 0 {
				errs = append(errs, fmt.Errorf("invalid owned peer pid: %q", b))
			} else if killErr := syscall.Kill(pid, syscall.SIGTERM); killErr != nil && !errors.Is(killErr, syscall.ESRCH) {
				errs = append(errs, killErr)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			errs = append(errs, err)
		}
	}
	if p.route.netem != nil {
		if err := p.route.netem.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	if p.cleanupRuntime != nil {
		if err := p.cleanupRuntime(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type terminalOutput interface {
	io.Writer
	Sync() error
	Close() error
}

type cliProcess struct {
	cmd            *exec.Cmd
	pty            io.ReadWriteCloser
	output         terminalOutput
	chunks         chan []byte
	done           chan struct{}
	closed         sync.Once
	waitErr        chan error
	waitTimeout    func() <-chan time.Time
	forceCleanup   func()
	forceKill      func()
	shutdown       func() error
	cleanupRuntime func() error
	terminalReady  bool
	readyPath      string
}

func (l *cliLauncher) Launch(m processMapping, role roleCommand) (launchedProcess, error) {
	if len(role.Args) == 0 {
		return nil, errors.New("public CLI role has no arguments")
	}
	if info, err := os.Stat(l.bin); err != nil {
		return nil, err
	} else if info.IsDir() {
		return nil, fmt.Errorf("public CLI binary %q is a directory", l.bin)
	}
	if m.Role == "ssh_stdio_peer" || m.Role == "udp_peer" {
		return l.preparePeer(m, role)
	}
	cmd := exec.Command(l.bin, role.Args...)
	runDir := filepath.Dir(m.TracePath)
	runtimeDir, err := l.runtimeDir(runDir)
	if err != nil {
		return nil, err
	}
	// All role inputs are absolute paths resolved at harness startup. Do not set
	// a role working directory: daemon-owned subprocess cleanup may otherwise
	// treat the evidence directory as its working tree and remove a preallocated
	// trace while a later repetition is being merged.
	cmd.Env = append(withoutEnv(os.Environ(), "VEV", "VEV_PERF_TRACE", "VEV_PERF_PROCESS_ID", "VEV_PERF_SCENARIO", "VEV_PERF_RUN", "VEV_REMOTE_TRANSPORT", "XDG_RUNTIME_DIR", "XDG_STATE_HOME"), traceEnvironment(m)...)
	cmd.Env = append(cmd.Env, "XDG_RUNTIME_DIR="+runtimeDir,
		"XDG_STATE_HOME="+filepath.Join(runDir, "state"), "TERM=xterm-256color")
	if m.Role == "client" {
		l.mu.Lock()
		peer, routed := l.peers[runDir]
		l.mu.Unlock()
		if routed {
			mode, err := remoteMode(peer.command.Transport)
			if err != nil {
				return nil, err
			}
			cmd.Env = append(cmd.Env, "PATH="+runDir+":"+os.Getenv("PATH"), "VEV_REMOTE_TRANSPORT="+mode)
		}
	}
	if err := safedir.EnsurePrivate(filepath.Join(runDir, "state")); err != nil {
		return nil, err
	}
	p := &cliProcess{cmd: cmd, waitErr: make(chan error, 1), readyPath: filepath.Join(runtimeDir, "vev", "daemon.sock")}
	if m.Role == "daemon" {
		p.cleanupRuntime = func() error { return l.releaseRuntime(runDir) }
	}
	if m.Role == "client" {
		master, slave, err := openPTY()
		if err != nil {
			return nil, err
		}
		output, err := os.OpenFile(filepath.Join(filepath.Dir(m.TracePath), "terminal-output"), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			_ = master.Close()
			_ = slave.Close()
			return nil, err
		}
		cmd.Stdin, cmd.Stdout, cmd.Stderr = slave, slave, slave
		// Ctty is the child descriptor number after Start remaps slave to stdin.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Setctty: true, Ctty: 0}
		if err := cmd.Start(); err != nil {
			_ = output.Close()
			_ = master.Close()
			_ = slave.Close()
			if p.cleanupRuntime != nil {
				_ = p.cleanupRuntime()
			}
			return nil, err
		}
		_ = slave.Close()
		p.pty, p.output, p.chunks, p.done = master, output, make(chan []byte, 32), make(chan struct{})
		go p.copyTerminal()
	} else {
		// Every launched role owns a dedicated process group, so the bounded
		// cleanup path never signals the harness/test runner's group.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		cmd.Stdin, cmd.Stdout, cmd.Stderr = devNull, devNull, devNull
		if err := cmd.Start(); err != nil {
			_ = devNull.Close()
			if p.cleanupRuntime != nil {
				_ = p.cleanupRuntime()
			}
			return nil, err
		}
		_ = devNull.Close()
	}
	if m.Role == "daemon" {
		// Stop the foreground daemon through its public CLI before its bounded
		// process-group fallback. This lets blocked carriage operations close and
		// serialize their failed end marks instead of being cut off by SIGKILL.
		p.shutdown = func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			shutdown := exec.CommandContext(ctx, l.bin, "kill", "--daemon")
			shutdown.Env = withoutEnv(cmd.Env, "VEV_PERF_TRACE", "VEV_PERF_PROCESS_ID", "VEV_PERF_SCENARIO", "VEV_PERF_RUN")
			return shutdown.Run()
		}
	}
	go func() { p.waitErr <- cmd.Wait() }()
	return p, nil
}

// runtimeDir returns one short, private runtime directory shared by the roles
// of one run. Keeping the Unix socket outside deeply nested evidence output
// avoids the platform socket-path limit without weakening run isolation.
func (l *cliLauncher) runtimeDir(runDir string) (string, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if path := l.runtimes[runDir]; path != "" {
		return path, nil
	}
	path, err := os.MkdirTemp("", "vev-harness-runtime-")
	if err != nil {
		return "", err
	}
	if err := safedir.EnsurePrivate(path); err != nil {
		_ = os.RemoveAll(path)
		return "", err
	}
	if l.runtimes == nil {
		l.runtimes = make(map[string]string)
	}
	l.runtimes[runDir] = path
	return path, nil
}

func (l *cliLauncher) releaseRuntime(runDir string) error {
	l.mu.Lock()
	path := l.runtimes[runDir]
	delete(l.runtimes, runDir)
	l.mu.Unlock()
	if path == "" {
		return nil
	}
	return os.RemoveAll(path)
}

// preparePeer installs a per-run ssh command seam. The ordinary public client
// still executes its documented remote attach command; its ssh child invokes
// only the parsed public _stdio or _udp-proxy command. This makes the peer
// carrying client traffic the declared, exclusively traced role rather than a
// separately launched but unused process.
func (l *cliLauncher) preparePeer(m processMapping, role roleCommand) (launchedProcess, error) {
	runDir := filepath.Dir(m.TracePath)
	l.mu.Lock()
	runtimeDir := l.runtimes[runDir]
	l.mu.Unlock()
	peerOwnsRuntime := runtimeDir == ""
	if peerOwnsRuntime {
		var err error
		runtimeDir, err = l.runtimeDir(runDir)
		if err != nil {
			return nil, err
		}
	}
	if err := safedir.EnsurePrivate(runDir); err != nil {
		return nil, err
	}
	if err := safedir.EnsurePrivate(runtimeDir); err != nil {
		return nil, err
	}
	if err := safedir.EnsurePrivate(filepath.Join(runDir, "state")); err != nil {
		return nil, err
	}
	if len(role.Args) != 2 || (role.Args[0] != "_stdio" && role.Args[0] != "_udp-proxy") {
		return nil, fmt.Errorf("unsupported public peer command %q", role.Args)
	}
	route := peerRoute{mapping: m, command: role}
	if m.Role == "udp_peer" {
		route.pidPath = filepath.Join(runDir, "udp-peer.pid")
		factory := l.netemFactory
		if factory == nil {
			factory = newUDPNetem
		}
		netem, err := factory(udpNetemConfig{
			RTT:         time.Duration(role.Transport.RTTMS) * time.Millisecond,
			LossPercent: role.Transport.LossPercent,
			TargetPath:  filepath.Join(runDir, "udp-peer.target"),
		})
		if err != nil {
			return nil, fmt.Errorf("start harness UDP netem: %w", err)
		}
		route.netem = netem
	}
	shim := filepath.Join(runDir, "ssh")
	ready := filepath.Join(runDir, "udp-peer.ready")
	target := filepath.Join(runDir, "udp-peer.target")
	netemPort := 0
	if route.netem != nil {
		netemPort = route.netem.Port()
	}
	stderr := filepath.Join(runDir, "udp-peer.stderr")
	// The bootstrap protocol requires the SSH command to exit after forwarding
	// the readiness line. The long-lived proxy is owned by pidPath and receives
	// the role's unique trace identity, never the bootstrap/client identity.
	body := fmt.Sprintf(`#!/bin/sh
set -eu
case "$*" in
  *"_stdio"*)
    exec env VEV_PERF_TRACE=%[1]q VEV_PERF_PROCESS_ID=%[2]q VEV_PERF_SCENARIO=%[12]q VEV_PERF_RUN=%[13]d XDG_RUNTIME_DIR=%[3]q XDG_STATE_HOME=%[4]q TERM=xterm-256color %[5]q _stdio %[6]q
    ;;
  *"_udp-bootstrap"*)
    rm -f %[7]q %[8]q %[9]q
    env VEV_PERF_TRACE=%[1]q VEV_PERF_PROCESS_ID=%[2]q VEV_PERF_SCENARIO=%[12]q VEV_PERF_RUN=%[13]d XDG_RUNTIME_DIR=%[3]q XDG_STATE_HOME=%[4]q TERM=xterm-256color %[5]q _udp-proxy %[6]q >%[7]q 2>%[8]q &
    peer=$!
    printf '%%s\n' "$peer" > %[11]q
    i=0
    while [ ! -s %[7]q ] && [ "$i" -lt 1000 ]; do sleep 0.01; i=$((i+1)); done
    test -s %[7]q
    line=$(head -n 1 %[7]q)
    printf '%%s\n' "$line" > %[9]q
    set -- $line
    test "$1" = VEV-UDP
    test -n "$3"
    printf 'VEV-UDP %%s %%s\n' %[10]d "$3"
    exit 0
    ;;
  *) echo 'vev harness ssh seam rejected non-vev command' >&2; exit 64 ;;
esac
`, m.TracePath, m.ProcessID, runtimeDir, filepath.Join(runDir, "state"), l.bin, role.Args[1], ready, stderr, target, netemPort, route.pidPath, m.Scenario, m.Run)
	if err := os.WriteFile(shim, []byte(body), 0o700); err != nil {
		if route.netem != nil {
			_ = route.netem.Close()
		}
		return nil, err
	}
	l.mu.Lock()
	if l.peers == nil {
		l.peers = make(map[string]peerRoute)
	}
	if _, exists := l.peers[runDir]; exists {
		l.mu.Unlock()
		if route.netem != nil {
			_ = route.netem.Close()
		}
		return nil, fmt.Errorf("duplicate transport peer for %s", runDir)
	}
	l.peers[runDir] = route
	l.mu.Unlock()
	peer := preparedPeer{route: route}
	if peerOwnsRuntime {
		peer.cleanupRuntime = func() error { return l.releaseRuntime(runDir) }
	}
	return peer, nil
}

func remoteMode(t transport) (string, error) {
	switch t.Kind {
	case "ssh_stdio":
		return "stdio", nil
	case "udp":
		return "udp", nil
	default:
		return "", fmt.Errorf("transport %q cannot route through a remote peer", t.ID)
	}
}

// traceEnvironment is the complete process-local identity supplied by the
// persisted harness processMapping. App turns Scenario/Run into production
// mark correlation; sequence/request/epoch remain process-local operation IDs.
func traceEnvironment(m processMapping) []string {
	return []string{
		"VEV_PERF_TRACE=" + m.TracePath,
		"VEV_PERF_PROCESS_ID=" + m.ProcessID,
		"VEV_PERF_SCENARIO=" + m.Scenario,
		"VEV_PERF_RUN=" + strconv.Itoa(m.Run),
	}
}

func withoutEnv(env []string, names ...string) []string {
	remove := make(map[string]bool, len(names))
	for _, name := range names {
		remove[name] = true
	}
	out := make([]string, 0, len(env))
	for _, entry := range env {
		name, _, _ := strings.Cut(entry, "=")
		if !remove[name] {
			out = append(out, entry)
		}
	}
	return out
}

func (p *cliProcess) copyTerminal() {
	defer close(p.chunks)
	buf := make([]byte, 4096)
	for {
		n, err := p.pty.Read(buf)
		if n > 0 {
			chunk := append([]byte(nil), buf[:n]...)
			if _, writeErr := p.output.Write(chunk); writeErr != nil {
				return
			}
			select {
			case p.chunks <- chunk:
			case <-p.done:
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// WaitReady confirms the explicitly launched public daemon owns its unique
// socket before a client is launched. Without this barrier, the normal client
// auto-spawn behavior can create an unmanifested daemon process during startup.
func (p *cliProcess) WaitReady() error {
	if p.readyPath == "" {
		return errors.New("daemon readiness path is empty")
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		if info, err := os.Stat(p.readyPath); err == nil {
			if info.Mode()&os.ModeSocket == 0 {
				return fmt.Errorf("daemon readiness path %s is not a socket", p.readyPath)
			}
			return nil
		}
		select {
		case err := <-p.waitErr:
			p.waitErr <- err
			if err == nil {
				return errors.New("daemon exited before readiness")
			}
			return fmt.Errorf("daemon exited before readiness: %w", err)
		case <-ticker.C:
		case <-timer.C:
			return fmt.Errorf("timed out waiting for daemon socket %s", p.readyPath)
		}
	}
}

func (p *cliProcess) Warmup(input []byte) error {
	if p.pty == nil || len(input) == 0 {
		return nil
	}
	// A client cannot write application output until it has received Welcome and
	// entered raw mode. Require that observable output before submitting any
	// warmup input, so the PTY's line discipline cannot echo it locally.
	if !p.terminalReady {
		if err := p.waitForTerminalReady(); err != nil {
			return err
		}
		p.terminalReady = true
	}
	if _, err := p.pty.Write(input); err != nil {
		return err
	}
	// Do not begin measured input until the public client has rendered a real
	// shell response to warmup input. This is intentionally not a measurement.
	return p.waitForTerminalMarker(inputMarker(input), input)
}

// Measure records a boundary only after the harness-owned terminal output file
// has accepted application output and Sync has reported a successful flush.
// The marker is injected through the real client PTY, never fabricated by the
// harness. PTY local echo is discarded and cannot satisfy this boundary.
func (p *cliProcess) Measure(input []byte, injected, flushed func() error) error {
	if p.pty == nil {
		return errors.New("measured process has no terminal PTY")
	}
	if _, err := p.pty.Write(input); err != nil {
		return err
	}
	// The injected callback runs only after the owned PTY accepts the bytes.
	if err := injected(); err != nil {
		return err
	}
	if err := p.waitForTerminalMarker(inputMarker(input), input); err != nil {
		return err
	}
	if err := p.output.Sync(); err != nil {
		return fmt.Errorf("flush terminal output: %w", err)
	}
	return flushed()
}

// waitForTerminalReady waits for public client output produced before any
// harness input. The client enters raw mode before its output loop can render
// this initial prompt/state, so this is an observable readiness boundary.
func (p *cliProcess) waitForTerminalReady() error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for {
		select {
		case chunk, ok := <-p.chunks:
			if !ok {
				return p.terminalClosedError("readiness output")
			}
			if len(chunk) == 0 {
				continue
			}
			if err := p.output.Sync(); err != nil {
				return fmt.Errorf("flush terminal readiness output: %w", err)
			}
			return nil
		case err := <-p.waitErr:
			// Keep the exit result available to Close, which owns reaping.
			p.waitErr <- err
			if err == nil {
				return errors.New("client exited before readiness output")
			}
			return fmt.Errorf("client exited before readiness output: %w", err)
		case <-timer.C:
			return errors.New("timed out waiting for client readiness output")
		}
	}
}

func (p *cliProcess) terminalClosedError(boundary string) error {
	if p.waitErr != nil {
		select {
		case err := <-p.waitErr:
			// Close owns reaping and may need the result after a failed boundary.
			p.waitErr <- err
			if err != nil {
				return fmt.Errorf("client terminal closed before %s: %w", boundary, err)
			}
		case <-time.After(10 * time.Millisecond):
		}
	}
	return fmt.Errorf("client terminal closed before %s", boundary)
}

func (p *cliProcess) waitForTerminalMarker(marker, input []byte) error {
	var received []byte
	echo := newPTYLocalEcho(input)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for !bytes.Contains(received, marker) {
		select {
		case chunk, ok := <-p.chunks:
			if !ok {
				return p.terminalClosedError("application output")
			}
			received = append(received, echo.applicationOutput(chunk)...)
		case err := <-p.waitErr:
			// Keep the exit result available to Close, which owns reaping.
			p.waitErr <- err
			if err == nil {
				return errors.New("client exited before application output")
			}
			return fmt.Errorf("client exited before application output: %w", err)
		case <-timer.C:
			return errors.New("timed out waiting for application terminal output")
		}
	}
	return nil
}

// ptyLocalEcho removes the one ordered input echo that a not-yet-raw slave PTY
// can produce. It normalizes NL to the conventional ONLCR echo spelling; any
// divergence is retained as application output. The line discipline emits the
// echo before the child can read and respond to the same write.
type ptyLocalEcho struct {
	expected []byte
	matched  int
	done     bool
}

func newPTYLocalEcho(input []byte) *ptyLocalEcho {
	expected := make([]byte, 0, len(input))
	for _, b := range input {
		if b == '\n' {
			expected = append(expected, '\r')
		}
		expected = append(expected, b)
	}
	return &ptyLocalEcho{expected: expected}
}

func (e *ptyLocalEcho) applicationOutput(chunk []byte) []byte {
	if e.done || len(e.expected) == 0 {
		return chunk
	}
	for i, b := range chunk {
		if b != e.expected[e.matched] {
			e.done = true
			return append([]byte(nil), chunk[i:]...)
		}
		e.matched++
		if e.matched == len(e.expected) {
			e.done = true
			return append([]byte(nil), chunk[i+1:]...)
		}
	}
	return nil
}

func (p *cliProcess) Close() error {
	var result error
	p.closed.Do(func() {
		if p.done != nil {
			close(p.done)
		}
		// A public terminal client owns a shell session. Ask that shell to exit
		// before closing the PTY so its transport can finish its receive span;
		// this avoids turning ordinary harness teardown into an aborted trace.
		exited := false
		if p.shutdown != nil && p.waitErr != nil {
			select {
			case <-p.waitErr:
				exited = true
			default:
				// The client may have just ended the last session and raced this
				// best-effort public shutdown. The normal bounded reaping path below
				// remains authoritative in that case.
				_ = p.shutdown()
			}
		}
		if p.pty != nil && p.waitErr != nil {
			_, _ = p.pty.Write([]byte("exit\n"))
			select {
			case <-p.waitErr:
				exited = true
			case <-p.closeDeadline():
			}
		}
		if p.pty != nil {
			_ = p.pty.Close()
		}
		if p.waitErr != nil && !exited {
			select {
			case <-p.waitErr:
			case <-p.closeDeadline():
				// Teardown is deliberately bounded: a stuck public CLI or child
				// must not hold a local harness run indefinitely.
				p.forceProcessGroupCleanup()
				select {
				case <-p.waitErr:
				case <-p.closeDeadline():
					p.forceProcessGroupKill()
					select {
					case <-p.waitErr:
					case <-p.closeDeadline():
						result = errors.New("timed out reaping public CLI process")
					}
				}
			}
		}
		if p.output != nil {
			if err := p.output.Close(); result == nil {
				result = err
			}
		}
		if p.cleanupRuntime != nil {
			if err := p.cleanupRuntime(); result == nil {
				result = err
			}
		}
	})
	return result
}

func (p *cliProcess) closeDeadline() <-chan time.Time {
	if p.waitTimeout != nil {
		return p.waitTimeout()
	}
	if p.shutdown != nil {
		// A public kill asks the daemon to finish bounded detached notices and
		// close blocked carriage before it exits; do not turn that graceful
		// protocol into a SIGKILL at the two-second client deadline.
		return time.After(5 * time.Second)
	}
	return time.After(2 * time.Second)
}

func (p *cliProcess) forceProcessGroupCleanup() {
	if p.forceCleanup != nil {
		p.forceCleanup()
		return
	}
	if p.cmd != nil && p.cmd.Process != nil {
		// The client is a session/process-group leader; forced cleanup must
		// include its ssh seam/_stdio descendants.
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM)
	}
}

func (p *cliProcess) forceProcessGroupKill() {
	if p.forceKill != nil {
		p.forceKill()
		return
	}
	if p.cmd != nil && p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
	}
}

// openPTY is the small Linux PTY boundary needed to drive the public terminal
// client. No daemon or adapter implementation is imported by this command.
func openPTY() (*os.File, *os.File, error) {
	masterFD, err := unix.Open("/dev/ptmx", unix.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, nil, err
	}
	closeMaster := func(err error) (*os.File, *os.File, error) { _ = unix.Close(masterFD); return nil, nil, err }
	number, err := unix.IoctlGetInt(masterFD, unix.TIOCGPTN)
	if err != nil {
		return closeMaster(err)
	}
	if err := unix.IoctlSetPointerInt(masterFD, unix.TIOCSPTLCK, 0); err != nil {
		return closeMaster(err)
	}
	slaveFD, err := unix.Open(fmt.Sprintf("/dev/pts/%d", number), unix.O_RDWR|unix.O_NOCTTY, 0)
	if err != nil {
		return closeMaster(err)
	}
	return os.NewFile(uintptr(masterFD), "vev-client-master"), os.NewFile(uintptr(slaveFD), "vev-client-slave"), nil
}

func inputMarker(input []byte) []byte {
	start := bytes.Index(input, []byte("__VEV_HARNESS_"))
	if start < 0 {
		return input
	}
	end := bytes.Index(input[start:], []byte(`\n`))
	if end < 0 {
		return input[start:]
	}
	return input[start : start+end]
}

type harness struct {
	clock           clock
	launcher        launcher
	mkdir           func(string) error
	createRunDir    func(string) error
	create          func(string) (*os.File, error)
	removeAll       func(string) error
	gitSHA          func() string
	selectScenarios func(manifest) []scenario // test-only bounded fixture seam
}

func main() {
	if err := run(os.Args[1:], defaultHarness()); err != nil {
		fmt.Fprintln(os.Stderr, "vev-perf-harness:", err)
		os.Exit(2)
	}
}
func defaultHarness() *harness {
	return &harness{clock: systemClock{time.Now()}, mkdir: safedir.EnsurePrivate, createRunDir: createExclusiveRunDir, create: func(p string) (*os.File, error) { return os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) }, removeAll: os.RemoveAll, gitSHA: recordedGitSHA}
}

// createExclusiveRunDir prevents stale runtime, socket, state, or trace files
// from an earlier invocation being mistaken for the next repetition's evidence.
func createExclusiveRunDir(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil {
		return err
	}
	if err := safedir.EnsurePrivate(path); err != nil {
		_ = os.Remove(path)
		return err
	}
	return nil
}

// recordedGitSHA prefers build provenance so an installed public harness does
// not depend on its launch directory being a checkout. The git fallback keeps
// go run evidence attributable as well.
func recordedGitSHA() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				return setting.Value
			}
		}
	}
	out, err := exec.Command("git", "rev-parse", "HEAD").Output()
	if err == nil && strings.TrimSpace(string(out)) != "" {
		return strings.TrimSpace(string(out))
	}
	return "unknown"
}
func run(args []string, h *harness) error {
	opt, err := parseOptions(args)
	if err != nil {
		return err
	}
	opt, err = resolvePathOptions(opt)
	if err != nil {
		return err
	}
	m, err := readManifest(opt.manifest)
	if err != nil {
		return err
	}
	if err := validateManifest(m); err != nil {
		return err
	}
	if err = h.mkdir(opt.out); err != nil {
		return err
	}
	raw, err := os.OpenFile(filepath.Join(opt.out, "raw-harness.jsonl"), os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer raw.Close()
	results := make([]runResult, 0, len(m.Scenarios)*opt.repetitions)
	var localSpans []span
	scenarios := m.Scenarios
	if opt.scenario != "" {
		scenarios, err = selectScenario(m, opt.scenario)
		if err != nil {
			return err
		}
	}
	if h.selectScenarios != nil {
		scenarios = h.selectScenarios(m)
	}
	for _, s := range scenarios {
		if s.InapplicableReason != "" {
			continue
		}
		for r := 1; r <= opt.repetitions; r++ {
			rr, err := h.runOne(opt, m, s, r, raw)
			if err != nil {
				return fmt.Errorf("scenario %s run %d: %w", s.ID, r, err)
			}
			results = append(results, rr)
			localSpans = append(localSpans, rr.Spans...)
		}
	}
	all, cadence, maxGap, runP50s := aggregateEventResults(results)
	if len(all) == 0 {
		return errors.New("no measured harness input/flush pairs")
	}
	if err := writeJSON(filepath.Join(opt.out, "runs.json"), results); err != nil {
		return err
	}
	return writeJSON(filepath.Join(opt.out, "summary.json"), summary{Schema: 1, GitSHA: h.gitSHA(), Warmup: opt.warmup.String(), Duration: opt.duration.String(), Repetitions: opt.repetitions, EndToEnd: percentiles(all), Cadence: percentiles(cadence), MaxGap: maxGap, RunP50Dispersion: percentiles(runP50s), Spans: summarizeSpans(localSpans), Runs: len(results)})
}

// aggregateEventResults deliberately preserves raw event latencies for
// end-to-end percentiles. Per-run p50s are a separate dispersion series.
func aggregateEventResults(results []runResult) (all, cadence []int64, maxGap int64, runP50s []int64) {
	for _, result := range results {
		all = append(all, result.EndToEnd...)
		cadence = append(cadence, result.CadenceSamples...)
		runP50s = append(runP50s, result.Event.P50)
		if result.MaxGap > maxGap {
			maxGap = result.MaxGap
		}
	}
	return all, cadence, maxGap, runP50s
}

func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("vev-perf-harness", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.vevBin, "vev-bin", "", "public vev binary")
	fs.StringVar(&o.manifest, "manifest", "", "manifest")
	fs.StringVar(&o.out, "out", "", "output directory")
	fs.StringVar(&o.scenario, "scenario", "", "one canonical scenario ID (after full manifest validation)")
	fs.DurationVar(&o.warmup, "warmup", 0, "warmup")
	fs.DurationVar(&o.duration, "duration", 0, "measurement duration")
	fs.IntVar(&o.repetitions, "repetitions", 0, "repetitions")
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	seen := map[string]bool{}
	fs.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	for _, name := range []string{"vev-bin", "manifest", "out", "warmup", "duration", "repetitions"} {
		if !seen[name] {
			return o, fmt.Errorf("--%s is required", name)
		}
	}
	if o.vevBin == "" || o.manifest == "" || o.out == "" {
		return o, errors.New("--vev-bin, --manifest, and --out are required")
	}
	if o.warmup < 0 {
		return o, errors.New("--warmup must not be negative")
	}
	if o.duration < minimumDuration {
		return o, fmt.Errorf("--duration must be at least %s", minimumDuration)
	}
	if o.repetitions < minimumRepetitions {
		return o, fmt.Errorf("--repetitions must be at least %d", minimumRepetitions)
	}
	return o, nil
}

// resolvePathOptions captures the harness cwd once, before role launchers
// assign their isolated working directories. All downstream filesystem access,
// manifests, and process environments consequently refer to the same paths.
func resolvePathOptions(o options) (options, error) {
	cwd, err := os.Getwd()
	if err != nil {
		return o, fmt.Errorf("resolve harness working directory: %w", err)
	}
	absolute := func(path string) string {
		if filepath.IsAbs(path) {
			return filepath.Clean(path)
		}
		return filepath.Clean(filepath.Join(cwd, path))
	}
	o.vevBin = absolute(o.vevBin)
	o.manifest = absolute(o.manifest)
	o.out = absolute(o.out)
	return o, nil
}

func readManifest(path string) (manifest, error) {
	f, e := os.Open(path)
	if e != nil {
		return manifest{}, e
	}
	defer f.Close()
	var m manifest
	e = json.NewDecoder(f).Decode(&m)
	return m, e
}
func validateManifest(m manifest) error {
	if m.Schema != 1 {
		return errors.New("unsupported manifest schema")
	}
	tops := map[string]bool{}
	for _, t := range m.Topologies {
		if t.ID == "" || t.Geometry != "120x40" || t.RowsPerPane != 10000 {
			return fmt.Errorf("invalid topology %q", t.ID)
		}
		tops[t.ID] = true
	}
	wantT := []string{"1x4", "4x1", "4x4", "8x1"}
	for _, v := range wantT {
		if !tops[v] {
			return fmt.Errorf("missing canonical topology %s", v)
		}
	}
	works := set(m.Workloads)
	for _, v := range []string{"idle", "active_output", "all_output", "inactive_output", "interactive_flood", "copy_search", "resize_sweep", "snapshot_output_resize", "attach_restore_tab_switch"} {
		if !works[v] {
			return fmt.Errorf("missing canonical workload %s", v)
		}
	}
	trans := map[string]bool{}
	for _, t := range m.Transports {
		if t.ID == "" {
			return errors.New("transport missing id")
		}
		if err := validateTransportFixture(t); err != nil {
			return err
		}
		trans[t.ID] = true
	}
	for _, v := range []string{"local", "ssh_stdio", "udp_baseline", "udp_25ms", "udp_100ms", "udp_loss_0pct", "udp_loss_1pct"} {
		if !trans[v] {
			return fmt.Errorf("missing canonical transport %s", v)
		}
	}
	seen := map[string]bool{}
	covered := map[string]bool{}
	for _, s := range m.Scenarios {
		if s.ID == "" || seen[s.ID] {
			return fmt.Errorf("duplicate or empty scenario id %q", s.ID)
		}
		seen[s.ID] = true
		if !tops[s.Topology] || !works[s.Workload] || !trans[s.Transport] {
			return fmt.Errorf("scenario %s references unknown matrix entry", s.ID)
		}
		if s.InapplicableReason == "" && len(s.Roles) == 0 {
			return fmt.Errorf("scenario %s has no process roles", s.ID)
		}
		if s.InapplicableReason != "" && len(s.Roles) != 0 {
			return fmt.Errorf("inapplicable scenario %s must not launch roles", s.ID)
		}
		if s.InapplicableReason == "" && !equalRoleSet(s.Roles, requiredRoles(s.Transport)) {
			return fmt.Errorf("scenario %s roles do not route transport %s", s.ID, s.Transport)
		}
		covered[s.Topology+"\x00"+s.Workload+"\x00"+s.Transport] = true
	}
	for topologyID := range tops {
		for workload := range works {
			for transportID := range trans {
				key := topologyID + "\x00" + workload + "\x00" + transportID
				if !covered[key] {
					return fmt.Errorf("missing scenario or inapplicable reason for %s/%s/%s", topologyID, workload, transportID)
				}
			}
		}
	}
	return nil
}

// selectScenario is intentionally called only after validateManifest. A bounded
// local evidence run therefore cannot mask an incomplete canonical matrix.
func selectScenario(m manifest, id string) ([]scenario, error) {
	for _, s := range m.Scenarios {
		if s.ID != id {
			continue
		}
		if s.InapplicableReason != "" {
			return nil, fmt.Errorf("scenario %s is inapplicable: %s", id, s.InapplicableReason)
		}
		return []scenario{s}, nil
	}
	return nil, fmt.Errorf("unknown canonical scenario %s", id)
}

func requiredRoles(transportID string) []string {
	switch transportID {
	case "local":
		return []string{"daemon", "client"}
	case "ssh_stdio":
		return []string{"daemon", "client", "ssh_stdio_peer"}
	default:
		return []string{"daemon", "client", "udp_peer"}
	}
}

func equalRoleSet(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	seen := make(map[string]bool, len(got))
	for _, role := range got {
		seen[role] = true
	}
	for _, role := range want {
		if !seen[role] {
			return false
		}
	}
	return true
}

func validateTransportFixture(t transport) error {
	want := map[string]transport{
		"local":         {ID: "local", Kind: "local"},
		"ssh_stdio":     {ID: "ssh_stdio", Kind: "ssh_stdio"},
		"udp_baseline":  {ID: "udp_baseline", Kind: "udp"},
		"udp_25ms":      {ID: "udp_25ms", Kind: "udp", RTTMS: 25},
		"udp_100ms":     {ID: "udp_100ms", Kind: "udp", RTTMS: 100},
		"udp_loss_0pct": {ID: "udp_loss_0pct", Kind: "udp", LossPercent: 0},
		"udp_loss_1pct": {ID: "udp_loss_1pct", Kind: "udp", LossPercent: 1},
	}
	w, ok := want[t.ID]
	if !ok || t.Kind != w.Kind || t.RTTMS != w.RTTMS || t.LossPercent != w.LossPercent {
		return fmt.Errorf("invalid canonical transport fixture %+v", t)
	}
	return nil
}

func set(v []string) map[string]bool {
	r := map[string]bool{}
	for _, x := range v {
		r[x] = true
	}
	return r
}
func (h *harness) runOne(o options, mat manifest, s scenario, run int, raw io.Writer) (res runResult, err error) {
	dir := filepath.Join(o.out, fmt.Sprintf("%s-run-%03d", safeName(s.ID), run))
	createRunDir := h.createRunDir
	if createRunDir == nil {
		createRunDir = createExclusiveRunDir
	}
	if err = createRunDir(dir); err != nil {
		return res, fmt.Errorf("create isolated run directory: %w", err)
	}
	success := false
	defer func() {
		if !success {
			_ = h.removeAll(dir)
		}
	}()
	maps := make([]processMapping, 0, len(s.Roles))
	for i, role := range s.Roles {
		pid := fmt.Sprintf("%s-r%03d-%s-%02d", safeName(s.ID), run, safeName(role), i+1)
		path := filepath.Join(dir, pid+".jsonl")
		f, e := h.create(path)
		if e != nil {
			return res, fmt.Errorf("allocate exclusive trace %s: %w", role, e)
		}
		if e = f.Close(); e != nil {
			return res, e
		}
		maps = append(maps, processMapping{ProcessID: pid, ClockDomain: pid, TracePath: path, Role: role, Scenario: s.ID, Run: run, Identity: "harness-assigned:" + pid})
	}
	// Persist the complete mapping before any process can be launched. This is
	// both crash evidence and the only legal source for later ID correlation.
	if err = writeJSON(filepath.Join(dir, "manifest.json"), runManifest{Scenario: s.ID, Run: run, Processes: maps}); err != nil {
		return res, err
	}
	if h.launcher == nil {
		h.launcher = &cliLauncher{bin: o.vevBin}
	}
	processes := make([]launchedRole, 0, len(maps))
	closed := false
	closeRoles := func() error {
		if closed {
			return nil
		}
		closed = true
		// Clients own SSH-stdio descendants; close them before the deferred UDP
		// peer and daemon so no role survives a failed or completed run.
		return closeLaunchedRoles(processes)
	}
	defer func() {
		if cleanupErr := closeRoles(); cleanupErr != nil {
			if err == nil {
				err = cleanupErr
			} else {
				// Keep the primary workload failure first while retaining every
				// cleanup failure in reverse launch order.
				err = errors.Join(err, cleanupErr)
			}
		}
	}()
	selectedTransport, e := manifestTransport(mat, s.Transport)
	if e != nil {
		return res, e
	}
	for _, pm := range launchOrder(maps) {
		p, e := h.launcher.Launch(pm, routeRoleArgs(s, pm, selectedTransport))
		if e != nil {
			return res, e
		}
		processes = append(processes, launchedRole{mapping: pm, process: p})
		if pm.Role == "daemon" {
			if ready, ok := p.(interface{ WaitReady() error }); ok {
				if err = ready.WaitReady(); err != nil {
					return res, err
				}
			}
		}
	}
	for _, p := range processes {
		if p.mapping.Role != "client" {
			continue
		}
		if err = p.process.Warmup(workloadInput(s, run, "warmup")); err != nil {
			return res, err
		}
	}
	// The harness clock, rather than a process clock, owns the explicit warmup
	// and measured intervals. Warmup boundaries are intentionally never added to
	// marks, and each measured event must complete before interval.End to count.
	h.clock.Sleep(o.warmup)
	interval := measuredInterval{Start: h.clock.Now()}
	interval.End = interval.Start + o.duration.Nanoseconds()
	marks := []harnessMark{}
	seq := uint64(0)
	nextInjection := interval.Start
	for {
		now := h.clock.Now()
		if now < nextInjection {
			h.clock.Sleep(time.Duration(nextInjection - now))
			now = h.clock.Now()
		}
		if now >= interval.End {
			break
		}
		for _, p := range processes {
			if p.mapping.Role != "client" {
				continue
			}
			seq++
			sequence := seq
			injected := func() error {
				marks = append(marks, harnessMark{s.ID, run, sequence, "input_injected", h.clock.Now(), true})
				return nil
			}
			flushed := func() error {
				marks = append(marks, harnessMark{s.ID, run, sequence, "terminal_flushed", h.clock.Now(), true})
				return nil
			}
			if err = p.process.Measure(workloadInput(s, run, fmt.Sprintf("measured-%d", sequence)), injected, flushed); err != nil {
				return res, err
			}
		}
		// Cap injection rate rather than bursting events after a slow terminal
		// flush. Any resulting spacing is retained as raw cadence evidence.
		nextInjection = h.clock.Now() + measuredEventCadence.Nanoseconds()
	}
	if now := h.clock.Now(); now < interval.End {
		h.clock.Sleep(time.Duration(interval.End - now))
	}
	events, e := measuredEventSamples(marks, interval)
	if e != nil {
		return res, e
	}
	if err := requireMinimumEventSamples(events); err != nil {
		return res, err
	}
	samples := eventLatencies(events)
	cadenceSamples, maxGap := eventCadenceSamples(events)
	// Process teardown can unblock a receive and cause its failed end mark to
	// be serialized. Merge only the complete, post-cleanup per-process traces.
	if cleanupErr := closeRoles(); cleanupErr != nil {
		return res, cleanupErr
	}
	spans, e := mergeProcessTraces(maps)
	if e != nil {
		return res, e
	}
	inInterval := make(map[uint64]bool, len(events))
	for _, event := range events {
		inInterval[event.Sequence] = true
	}
	for _, m := range marks {
		if !inInterval[m.Sequence] {
			continue
		}
		if _, e := fmt.Fprintf(raw, "%s\n", mustJSON(m)); e != nil {
			return res, e
		}
	}
	success = true
	return runResult{Spans: spans, Scenario: s.ID, Run: run, Samples: len(samples), EndToEnd: samples, Event: percentiles(samples), Cadence: percentiles(cadenceSamples), CadenceSamples: cadenceSamples, MaxGap: maxGap, Processes: maps}, nil
}

func roleArgs(s scenario, m processMapping) roleCommand {
	return routeRoleArgs(s, m, scenarioTransport(s))
}

func routeRoleArgs(s scenario, m processMapping, selected transport) roleCommand {
	session := "perf-" + safeName(s.ID) + fmt.Sprintf("-%03d", m.Run)
	remote := []string{"attach", "harness@127.0.0.1:" + session}
	switch m.Role {
	case "daemon":
		return roleCommand{Args: []string{"--daemon"}}
	case "client":
		switch s.Transport {
		case "local":
			return roleCommand{Args: []string{"new", session}}
		case "ssh_stdio", "udp_baseline", "udp_25ms", "udp_100ms", "udp_loss_0pct", "udp_loss_1pct":
			return roleCommand{Args: remote}
		}
	case "ssh_stdio_peer":
		return roleCommand{Args: []string{"_stdio", session}, Transport: transport{ID: s.Transport, Kind: "ssh_stdio"}}
	case "udp_peer":
		return roleCommand{Args: []string{"_udp-proxy", session}, Transport: selected}
	}
	return roleCommand{}
}

func manifestTransport(m manifest, id string) (transport, error) {
	for _, t := range m.Transports {
		if t.ID == id {
			return t, nil
		}
	}
	// Focused unit tests may call runOne with a minimal manifest; production
	// already validates every scenario reference before this point.
	if m.Schema == 0 {
		return scenarioTransport(scenario{Transport: id}), nil
	}
	return transport{}, fmt.Errorf("scenario references missing transport %q", id)
}

func scenarioTransport(s scenario) transport {
	switch s.Transport {
	case "udp_baseline":
		return transport{ID: s.Transport, Kind: "udp"}
	case "udp_25ms":
		return transport{ID: s.Transport, Kind: "udp", RTTMS: 25}
	case "udp_100ms":
		return transport{ID: s.Transport, Kind: "udp", RTTMS: 100}
	case "udp_loss_0pct":
		return transport{ID: s.Transport, Kind: "udp", LossPercent: 0}
	case "udp_loss_1pct":
		return transport{ID: s.Transport, Kind: "udp", LossPercent: 1}
	default:
		return transport{ID: s.Transport}
	}
}

func launchOrder(mappings []processMapping) []processMapping {
	out := append([]processMapping(nil), mappings...)
	priority := func(role string) int {
		switch role {
		case "daemon":
			return 0
		case "ssh_stdio_peer", "udp_peer":
			return 1
		case "client":
			return 2
		default:
			return 3
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return priority(out[i].Role) < priority(out[j].Role) })
	return out
}

// workloadInput is shell input delivered through the client PTY. It avoids any
// private daemon API and makes every topology/workload/transport manifest row
// execute a real terminal workload. The marker is the output boundary awaited
// by cliProcess; it is not a synthetic timestamp.
func workloadInput(s scenario, run int, phase string) []byte {
	marker := fmt.Sprintf("__VEV_HARNESS_%s_r%d_%s__", safeName(s.ID), run, safeName(phase))
	body := "printf 'vev perf %s\\n'"
	switch s.Workload {
	case "active_output", "all_output", "inactive_output", "interactive_flood":
		body = "i=0; while [ $i -lt 128 ]; do printf 'vev perf output %s\\n'; i=$((i+1)); done"
	case "resize_sweep":
		body = "printf 'vev perf resize-sweep %s\\n'"
	case "copy_search":
		body = "printf 'vev perf copy-search %s\\n'"
	case "snapshot_output_resize":
		body = "printf 'vev perf snapshot-output-resize %s\\n'"
	case "attach_restore_tab_switch":
		body = "printf 'vev perf attach-restore-tab-switch %s\\n'"
	}
	return []byte(fmt.Sprintf(body+"; printf '%s\\n'\n", s.ID, marker))
}

// measuredEventSamples validates every owned pair, then retains only events
// whose injection and successful terminal flush are both in the interval.
// Straddling pairs remain raw diagnostic facts but are never percentile input.
func measuredEventSamples(marks []harnessMark, interval measuredInterval) ([]eventSample, error) {
	if interval.End < interval.Start {
		return nil, errors.New("negative measured interval")
	}
	starts := map[uint64]int64{}
	events := make([]eventSample, 0, len(marks)/2)
	for _, m := range marks {
		if !m.Valid {
			return nil, errors.New("invalid harness boundary")
		}
		switch m.Kind {
		case "input_injected":
			if _, exists := starts[m.Sequence]; exists {
				return nil, errors.New("duplicate input boundary")
			}
			starts[m.Sequence] = m.Tick
		case "terminal_flushed":
			start, ok := starts[m.Sequence]
			if !ok {
				return nil, errors.New("terminal flush without input pair")
			}
			if m.Tick < start {
				return nil, errors.New("negative harness duration")
			}
			delete(starts, m.Sequence)
			if start >= interval.Start && m.Tick <= interval.End {
				events = append(events, eventSample{Sequence: m.Sequence, Injected: start, Flushed: m.Tick, Latency: m.Tick - start})
			}
		default:
			return nil, errors.New("unknown harness mark")
		}
	}
	if len(starts) != 0 {
		return nil, errors.New("missing terminal flush pair")
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Injected == events[j].Injected {
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].Injected < events[j].Injected
	})
	return events, nil
}

func requireMinimumEventSamples(events []eventSample) error {
	if len(events) < minimumInIntervalEventSamples {
		return fmt.Errorf("insufficient in-interval event samples: got %d, need at least %d", len(events), minimumInIntervalEventSamples)
	}
	return nil
}

func eventLatencies(events []eventSample) []int64 {
	out := make([]int64, len(events))
	for i, event := range events {
		out[i] = event.Latency
	}
	return out
}

func eventCadenceSamples(events []eventSample) ([]int64, int64) {
	if len(events) < 2 {
		return nil, 0
	}
	gaps := make([]int64, 0, len(events)-1)
	var maxGap int64
	for i := 1; i < len(events); i++ {
		gap := events[i].Injected - events[i-1].Injected
		gaps = append(gaps, gap)
		if gap > maxGap {
			maxGap = gap
		}
	}
	return gaps, maxGap
}

func eventCadence(events []eventSample) (distribution, int64) {
	gaps, maxGap := eventCadenceSamples(events)
	if len(gaps) == 0 {
		return distribution{}, maxGap
	}
	return percentiles(gaps), maxGap
}

func pairedSamples(marks []harnessMark) ([]int64, error) {
	starts := map[uint64]int64{}
	var out []int64
	for _, m := range marks {
		if !m.Valid {
			return nil, errors.New("invalid harness boundary")
		}
		switch m.Kind {
		case "input_injected":
			if _, ok := starts[m.Sequence]; ok {
				return nil, errors.New("duplicate input boundary")
			}
			starts[m.Sequence] = m.Tick
		case "terminal_flushed":
			start, ok := starts[m.Sequence]
			if !ok {
				return nil, errors.New("terminal flush without input pair")
			}
			if m.Tick < start {
				return nil, errors.New("negative harness duration")
			}
			out = append(out, m.Tick-start)
			delete(starts, m.Sequence)
		default:
			return nil, errors.New("unknown harness mark")
		}
	}
	if len(starts) != 0 {
		return nil, errors.New("missing terminal flush pair")
	}
	if len(out) == 0 {
		return nil, errors.New("insufficient harness samples")
	}
	return out, nil
}

// traceRecord is intentionally local to the command. It mirrors JSONL fields
// needed for correlation but never exposes a clock to any production component.
type traceRecord struct {
	ProcessID string `json:"process_id"`
	Component string `json:"component"`
	Scenario  string `json:"scenario"`
	Run       uint64 `json:"run"`
	Sequence  uint64 `json:"sequence"`
	RequestID uint64 `json:"request_id"`
	Epoch     uint64 `json:"epoch"`
	Kind      string `json:"kind"`
	Tick      int64  `json:"tick"`
	Valid     bool   `json:"valid"`
}
type spanPair struct{ start, end, name string }

var spanPairs = []spanPair{{"capture_start", "capture_end", "capture_duration"}, {"compose_start", "compose_end", "compose_duration"}, {"diff_start", "diff_end", "diff_duration"}, {"queue_enqueued", "queue_dequeued", "queue_wait"}, {"ack_blocked_start", "ack_blocked_end", "ack_blocked_interval"}, {"emit_start", "emit_end", "emit_duration"}, {"adapter_send_start", "adapter_send_end", "adapter_send_duration"}, {"adapter_receive_start", "adapter_receive_end", "adapter_receive_duration"}}

// mergeProcessTraces permits only records from one manifest process in a span.
// In particular, the correlation key includes process_id before ticks are read.
func mergeProcessTraces(mappings []processMapping) ([]span, error) {
	known := map[string]processMapping{}
	for _, m := range mappings {
		if m.ProcessID == "" || m.ClockDomain == "" || m.TracePath == "" || known[m.ProcessID].ProcessID != "" {
			return nil, errors.New("duplicate or incomplete manifest process mapping")
		}
		known[m.ProcessID] = m
	}
	out := []span{}
	for _, m := range mappings {
		f, err := os.Open(m.TracePath)
		if err != nil {
			return nil, fmt.Errorf("open manifest trace for %s role: %w", m.Role, err)
		}
		var records []traceRecord
		scan := bufio.NewScanner(f)
		for scan.Scan() {
			var r traceRecord
			if err := json.Unmarshal(scan.Bytes(), &r); err != nil {
				_ = f.Close()
				return nil, err
			}
			if r.ProcessID != m.ProcessID || r.Scenario != m.Scenario || r.Run != uint64(m.Run) {
				_ = f.Close()
				return nil, errors.New("trace record identity does not match manifest")
			}
			if r.Scenario == "" || r.Run == 0 || r.Component == "" || r.Sequence == 0 || r.RequestID == 0 || r.Epoch == 0 {
				_ = f.Close()
				return nil, errors.New("trace record has invalid correlation fields")
			}
			if _, ok := known[r.ProcessID]; !ok {
				_ = f.Close()
				return nil, errors.New("unknown trace process_id")
			}
			records = append(records, r)
		}
		if err := scan.Err(); err != nil {
			_ = f.Close()
			return nil, err
		}
		_ = f.Close()
		// A process shares one observer across concurrent components. Correlation
		// IDs are allocated before a goroutine reaches its first mark, so a later
		// ID can be serialized before an earlier one. Their first appearance is
		// consequently not an ordering contract. Only marks in the same component
		// and exact correlation domain may pair, and each pair's ticks must retain
		// its own start-before-end ordering.
		starts := map[string]traceRecord{}
		for _, r := range records {
			for _, pair := range spanPairs {
				key := fmt.Sprintf("%s/%s/%s/%d/%d/%d/%d", r.ProcessID, r.Component, m.Scenario, m.Run, r.Sequence, r.RequestID, r.Epoch)
				if r.Kind == pair.start {
					k := pair.name + "/" + key
					if _, exists := starts[k]; exists {
						return nil, errors.New("duplicate process-local span start")
					}
					starts[k] = r
				}
				if r.Kind == pair.end {
					k := pair.name + "/" + key
					start, ok := starts[k]
					if !ok {
						return nil, errors.New("span end without same-process start")
					}
					if r.Tick < start.Tick {
						return nil, errors.New("negative same-process span")
					}
					// Validity affects only measurement eligibility, never structural
					// pairing: every start/end must still match in its exact process,
					// component, and correlation domain. A failed start, failed end, or
					// both is a paired diagnostic fact but contributes no latency sample.
					if start.Valid && r.Valid {
						out = append(out, span{Component: r.Component, Name: pair.name, Samples: []int64{r.Tick - start.Tick}})
					}
					delete(starts, k)
				}
			}
		}
		if len(starts) != 0 {
			keys := make([]string, 0, len(starts))
			for key := range starts {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			return nil, fmt.Errorf("missing process-local span pair for %s: %s", m.ProcessID, strings.Join(keys, ", "))
		}
	}
	return out, nil
}
func summarizeSpans(all []span) []spanSummary {
	grouped := map[string]*span{}
	for _, s := range all {
		k := s.Component + "\x00" + s.Name
		if grouped[k] == nil {
			grouped[k] = &span{Component: s.Component, Name: s.Name}
		}
		grouped[k].Samples = append(grouped[k].Samples, s.Samples...)
	}
	keys := make([]string, 0, len(grouped))
	for k := range grouped {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]spanSummary, 0, len(keys))
	for _, k := range keys {
		s := grouped[k]
		if len(s.Samples) < minimumRepetitions {
			out = append(out, spanSummary{Component: s.Component, Name: s.Name, Distribution: distribution{Count: len(s.Samples)}})
			continue
		}
		out = append(out, spanSummary{Component: s.Component, Name: s.Name, Distribution: percentiles(s.Samples)})
	}
	return out
}

func percentiles(samples []int64) distribution {
	v := append([]int64(nil), samples...)
	sort.Slice(v, func(i, j int) bool { return v[i] < v[j] })
	at := func(p int) int64 { return v[(len(v)-1)*p/100] }
	return distribution{len(v), at(50), at(95), at(99), v[len(v)-1]}
}
func writeJSON(path string, v any) error {
	f, e := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if e != nil {
		return e
	}
	defer f.Close()
	e = json.NewEncoder(f).Encode(v)
	return e
}
func mustJSON(v any) string { b, _ := json.Marshal(v); return string(b) }
func safeName(s string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			return r
		}
		return '-'
	}, s)
}
func readJSONL(path string) ([]map[string]json.RawMessage, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, e
	}
	defer f.Close()
	var r []map[string]json.RawMessage
	scan := bufio.NewScanner(f)
	for scan.Scan() {
		var v map[string]json.RawMessage
		if e := json.Unmarshal(scan.Bytes(), &v); e != nil {
			return nil, e
		}
		r = append(r, v)
	}
	return r, scan.Err()
}
