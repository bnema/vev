//go:build linux

package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/bnema/vev/pkg/rawterm"
	"github.com/bnema/vev/pkg/safedir"
)

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

func (p preparedPeer) Warmup([]byte) error { return nil }

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
	closeResult    error
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
	fail := func(err error) error {
		if peerOwnsRuntime {
			return errors.Join(err, l.releaseRuntime(runDir))
		}
		return err
	}
	if peerOwnsRuntime {
		var err error
		runtimeDir, err = l.runtimeDir(runDir)
		if err != nil {
			return nil, fail(err)
		}
	}
	if err := safedir.EnsurePrivate(runDir); err != nil {
		return nil, fail(err)
	}
	if err := safedir.EnsurePrivate(runtimeDir); err != nil {
		return nil, fail(err)
	}
	if err := safedir.EnsurePrivate(filepath.Join(runDir, "state")); err != nil {
		return nil, fail(err)
	}
	if len(role.Args) != 2 || (role.Args[0] != "_stdio" && role.Args[0] != "_udp-proxy") {
		return nil, fail(fmt.Errorf("unsupported public peer command %q", role.Args))
	}
	route := peerRoute{mapping: m, command: role}
	closeNetem := func(err error) error {
		if route.netem != nil {
			err = errors.Join(err, route.netem.Close())
		}
		return fail(err)
	}
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
			return nil, fail(fmt.Errorf("start harness UDP netem: %w", err))
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
		return nil, closeNetem(err)
	}
	l.mu.Lock()
	if l.peers == nil {
		l.peers = make(map[string]peerRoute)
	}
	if _, exists := l.peers[runDir]; exists {
		l.mu.Unlock()
		return nil, closeNetem(fmt.Errorf("duplicate transport peer for %s", runDir))
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

// Close tears down a launched role along a bounded escalation ladder. Each rung
// performs one action, then waits a single closeDeadline for the process to be
// reaped before the next rung escalates. The graceful rungs — the public daemon
// stop, the client shell exit, the controlling-terminal hangup, and SIGTERM —
// let a role close its blocked transport operations and serialize their end
// marks; only a SIGKILL severs a process before it can, so only that rung fails
// teardown. Every path closes the PTY exactly once and finalizes output and
// runtime state.
func (p *cliProcess) Close() error {
	p.closed.Do(func() {
		// 1. Drain terminal output for the whole teardown. Without an active
		// consumer a full chunks channel stalls copyTerminal, which then stops
		// reading the PTY master and blocks the client mid-restore before it can
		// close its observed receive operation. The drain stops with p.done.
		drainDone := p.drainTerminalOutput()

		exited := false
		// waitForExit blocks up to one bounded deadline for the reap.
		waitForExit := func() {
			if p.waitErr == nil || exited {
				return
			}
			select {
			case <-p.waitErr:
				exited = true
			case <-p.closeDeadline():
			}
		}
		// reaped reports whether the process is already gone; a role with no
		// process to reap needs no escalation at all.
		reaped := func() bool {
			if p.waitErr == nil || exited {
				return true
			}
			select {
			case <-p.waitErr:
				exited = true
				return true
			default:
				return false
			}
		}
		ptyClosed := false
		closePTY := func() {
			if p.pty != nil && !ptyClosed {
				_ = p.pty.Close()
				ptyClosed = true
			}
		}

		// 2. Daemon role: ask the public daemon to stop before any signal so its
		// blocked carriage can close and serialize failed end marks.
		if p.shutdown != nil && !reaped() {
			_ = p.shutdown()
			waitForExit()
		}
		// 3. Client role: a public terminal client owns a shell session. Ask that
		// shell to exit so the client detaches and its transport finishes its
		// receive span cleanly.
		if p.pty != nil && !reaped() {
			_, _ = p.pty.Write([]byte("exit\n"))
			waitForExit()
		}
		// 4. Hang up the controlling terminal. The attach path installs a signal
		// handler, so this SIGHUP is a graceful detach — not an abrupt kill — and
		// the client still flushes its in-flight receive end mark.
		if p.pty != nil && !reaped() {
			closePTY()
			waitForExit()
		}
		// 5. SIGTERM the role's own process group. The daemon and, through its
		// attach signal handler, the client both treat this as a graceful stop.
		if !reaped() {
			p.forceProcessGroupCleanup()
			waitForExit()
		}
		// 6. SIGKILL the role's process group. This severs the process; an
		// in-flight observed operation cannot be closed, so its trace may truncate.
		killed := false
		if !reaped() {
			killed = true
			p.forceProcessGroupKill()
			waitForExit()
		}
		// 7. A SIGKILL escalation cannot guarantee a complete trace. Fail teardown
		// with a clear cause rather than letting the truncation surface later as a
		// downstream merge mystery. The graceful rungs never error on their own.
		if killed {
			if exited {
				p.closeResult = errors.New("public CLI process did not exit after hangup and SIGTERM; killed")
			} else {
				p.closeResult = errors.New("timed out reaping public CLI process")
			}
		}

		// Close the PTY exactly once on every path: an early graceful exit skips
		// the hangup rung, so close it here before the drain stops.
		closePTY()
		if p.done != nil {
			close(p.done)
		}
		if drainDone != nil {
			<-drainDone
		}
		if p.output != nil {
			if err := p.output.Close(); p.closeResult == nil {
				p.closeResult = err
			}
		}
		if p.cleanupRuntime != nil {
			if err := p.cleanupRuntime(); p.closeResult == nil {
				p.closeResult = err
			}
		}
	})
	return p.closeResult
}

// drainTerminalOutput consumes copyTerminal's chunks for the duration of a
// teardown so a full channel can never wedge copyTerminal (and thus the client's
// PTY). It returns nil when there is no terminal to drain, and otherwise a
// channel closed once the consumer has stopped on p.done.
func (p *cliProcess) drainTerminalOutput() <-chan struct{} {
	if p.chunks == nil || p.done == nil {
		return nil
	}
	done := make(chan struct{})
	chunks := p.chunks
	go func() {
		defer close(done)
		for {
			select {
			case _, ok := <-chunks:
				if !ok {
					// copyTerminal closed the channel; a closed channel is always
					// ready, so stop selecting it and wait only for the stop signal.
					chunks = nil
				}
			case <-p.done:
				return
			}
		}
	}()
	return done
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
	// O_CLOEXEC keeps these harness-owned descriptors out of the launched client:
	// exec dup2s the slave onto the child's stdio, but an inherited master (or a
	// second slave) would otherwise leak in and hold the terminal open, so closing
	// the harness master could never hang up the client. cmd.Start still remaps the
	// slave onto fds 0-2, which are exempt from close-on-exec.
	masterFD, err := syscall.Open("/dev/ptmx", syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, nil, err
	}
	closeMaster := func(err error) (*os.File, *os.File, error) { _ = syscall.Close(masterFD); return nil, nil, err }
	number, err := rawterm.PtsNumber(masterFD)
	if err != nil {
		return closeMaster(err)
	}
	if err := rawterm.UnlockPt(masterFD); err != nil {
		return closeMaster(err)
	}
	slaveFD, err := syscall.Open(fmt.Sprintf("/dev/pts/%d", number), syscall.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
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
