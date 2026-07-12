// vev-perf-harness is a public-CLI performance evidence collector.  It never
// imports daemon packages: process traces are correlated by identifiers only.
package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
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
	minimumDuration    = 30 * time.Second
	minimumRepetitions = 10
)

type options struct {
	vevBin, manifest, out string
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
type span struct {
	Component, Name string
	Samples         []int64
}
type runResult struct {
	Spans     []span           `json:"process_local_spans"`
	Scenario  string           `json:"scenario"`
	Run       int              `json:"run"`
	Samples   int              `json:"samples"`
	EndToEnd  []int64          `json:"end_to_end_nanos"`
	Processes []processMapping `json:"processes"`
}
type summary struct {
	Schema      uint16        `json:"schema"`
	GitSHA      string        `json:"git_sha"`
	Warmup      string        `json:"warmup"`
	Duration    string        `json:"duration"`
	Repetitions int           `json:"repetitions"`
	EndToEnd    distribution  `json:"harness_end_to_end"`
	Spans       []spanSummary `json:"process_local_spans"`
	Runs        int           `json:"runs"`
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
			failures = append(failures, err)
		}
	}
	return errors.Join(failures...)
}

// cliLauncher is deliberately limited to executing the supplied public vev
// binary. Scenario orchestration never reaches into an in-process daemon.
type cliLauncher struct {
	bin   string
	mu    sync.Mutex
	peers map[string]peerRoute
}

type peerRoute struct {
	mapping processMapping
	command roleCommand
	pidPath string
}

type preparedPeer struct{ route peerRoute }

func (p preparedPeer) Warmup([]byte) error                              { return nil }
func (p preparedPeer) Measure([]byte, func() error, func() error) error { return nil }
func (p preparedPeer) Close() error {
	if p.route.pidPath == "" {
		return nil
	}
	b, err := os.ReadFile(p.route.pidPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || pid <= 0 {
		return fmt.Errorf("invalid owned peer pid: %q", b)
	}
	if err := syscall.Kill(pid, syscall.SIGTERM); err != nil && !errors.Is(err, syscall.ESRCH) {
		return err
	}
	return nil
}

type terminalOutput interface {
	io.Writer
	Sync() error
	Close() error
}

type cliProcess struct {
	cmd           *exec.Cmd
	pty           io.ReadWriteCloser
	output        terminalOutput
	chunks        chan []byte
	done          chan struct{}
	closed        sync.Once
	waitErr       chan error
	waitTimeout   func() <-chan time.Time
	forceCleanup  func()
	terminalReady bool
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
	cmd.Env = append(withoutEnv(os.Environ(), "VEV", "VEV_PERF_TRACE", "VEV_PERF_PROCESS_ID", "VEV_REMOTE_TRANSPORT"), "VEV_PERF_TRACE="+m.TracePath, "VEV_PERF_PROCESS_ID="+m.ProcessID,
		"XDG_RUNTIME_DIR="+filepath.Join(runDir, "runtime"),
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
	if err := safedir.EnsurePrivate(filepath.Join(runDir, "runtime")); err != nil {
		return nil, err
	}
	if err := safedir.EnsurePrivate(filepath.Join(runDir, "state")); err != nil {
		return nil, err
	}
	p := &cliProcess{cmd: cmd, waitErr: make(chan error, 1)}
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
			return nil, err
		}
		_ = slave.Close()
		p.pty, p.output, p.chunks, p.done = master, output, make(chan []byte, 32), make(chan struct{})
		go p.copyTerminal()
	} else {
		devNull, err := os.OpenFile(os.DevNull, os.O_RDWR, 0)
		if err != nil {
			return nil, err
		}
		cmd.Stdin, cmd.Stdout, cmd.Stderr = devNull, devNull, devNull
		if err := cmd.Start(); err != nil {
			_ = devNull.Close()
			return nil, err
		}
		_ = devNull.Close()
	}
	go func() { p.waitErr <- cmd.Wait() }()
	return p, nil
}

// preparePeer installs a per-run ssh command seam. The ordinary public client
// still executes its documented remote attach command; its ssh child invokes
// only the parsed public _stdio or _udp-proxy command. This makes the peer
// carrying client traffic the declared, exclusively traced role rather than a
// separately launched but unused process.
func (l *cliLauncher) preparePeer(m processMapping, role roleCommand) (launchedProcess, error) {
	runDir := filepath.Dir(m.TracePath)
	if err := safedir.EnsurePrivate(runDir); err != nil {
		return nil, err
	}
	if err := safedir.EnsurePrivate(filepath.Join(runDir, "runtime")); err != nil {
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
	}
	shim := filepath.Join(runDir, "ssh")
	ready := filepath.Join(runDir, "udp-peer.ready")
	stderr := filepath.Join(runDir, "udp-peer.stderr")
	// The bootstrap protocol requires the SSH command to exit after forwarding
	// the readiness line. The long-lived proxy is owned by pidPath and receives
	// the role's unique trace identity, never the bootstrap/client identity.
	body := fmt.Sprintf(`#!/bin/sh
set -eu
case "$*" in
  *"_stdio"*)
    exec env VEV_PERF_TRACE=%[1]q VEV_PERF_PROCESS_ID=%[2]q XDG_RUNTIME_DIR=%[3]q XDG_STATE_HOME=%[4]q TERM=xterm-256color %[5]q _stdio %[6]q
    ;;
  *"_udp-bootstrap"*)
    rm -f %[7]q %[8]q
    env VEV_PERF_TRACE=%[1]q VEV_PERF_PROCESS_ID=%[2]q XDG_RUNTIME_DIR=%[3]q XDG_STATE_HOME=%[4]q TERM=xterm-256color VEV_PERF_UDP_RTT_MS=%[9]q VEV_PERF_UDP_LOSS_PERCENT=%[10]q %[5]q _udp-proxy %[6]q >%[7]q 2>%[8]q &
    peer=$!
    printf '%%s\\n' "$peer" > %[11]q
    i=0
    while [ ! -s %[7]q ] && [ "$i" -lt 1000 ]; do sleep 0.01; i=$((i+1)); done
    test -s %[7]q
    head -n 1 %[7]q
    exit 0
    ;;
  *) echo 'vev harness ssh seam rejected non-vev command' >&2; exit 64 ;;
esac
`, m.TracePath, m.ProcessID, filepath.Join(runDir, "runtime"), filepath.Join(runDir, "state"), l.bin, role.Args[1], ready, stderr, strconv.Itoa(role.Transport.RTTMS), strconv.Itoa(role.Transport.LossPercent), route.pidPath)
	if err := os.WriteFile(shim, []byte(body), 0o700); err != nil {
		return nil, err
	}
	l.mu.Lock()
	if l.peers == nil {
		l.peers = make(map[string]peerRoute)
	}
	if _, exists := l.peers[runDir]; exists {
		l.mu.Unlock()
		return nil, fmt.Errorf("duplicate transport peer for %s", runDir)
	}
	l.peers[runDir] = route
	l.mu.Unlock()
	return preparedPeer{route: route}, nil
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
				return errors.New("client terminal closed before readiness output")
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

func (p *cliProcess) waitForTerminalMarker(marker, input []byte) error {
	var received []byte
	echo := newPTYLocalEcho(input)
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()
	for !bytes.Contains(received, marker) {
		select {
		case chunk, ok := <-p.chunks:
			if !ok {
				return errors.New("client terminal closed before application output")
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
		// Closing the client PTY causes its stdin pump to detach, then close the
		// transport and wait for its adapter receive end mark. Do this before a
		// signal so process-local spans are complete in the trace.
		if p.pty != nil {
			_ = p.pty.Close()
		}
		if p.waitErr != nil {
			select {
			case <-p.waitErr:
				// The client exited through its normal detach path.
			case <-p.closeDeadline():
				p.forceProcessGroupCleanup()
				<-p.waitErr
			}
		}
		if p.output != nil {
			result = p.output.Close()
		}
	})
	return result
}

func (p *cliProcess) closeDeadline() <-chan time.Time {
	if p.waitTimeout != nil {
		return p.waitTimeout()
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
	clock     clock
	launcher  launcher
	mkdir     func(string) error
	create    func(string) (*os.File, error)
	removeAll func(string) error
	gitSHA    func() string
}

func main() {
	if err := run(os.Args[1:], defaultHarness()); err != nil {
		fmt.Fprintln(os.Stderr, "vev-perf-harness:", err)
		os.Exit(2)
	}
}
func defaultHarness() *harness {
	return &harness{clock: systemClock{time.Now()}, mkdir: safedir.EnsurePrivate, create: func(p string) (*os.File, error) { return os.OpenFile(p, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) }, removeAll: os.RemoveAll, gitSHA: func() string { return "unknown" }}
}
func run(args []string, h *harness) error {
	opt, err := parseOptions(args)
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
	var all []int64
	var localSpans []span
	for _, s := range m.Scenarios {
		if s.InapplicableReason != "" {
			continue
		}
		for r := 1; r <= opt.repetitions; r++ {
			rr, err := h.runOne(opt, m, s, r, raw)
			if err != nil {
				return err
			}
			results = append(results, rr)
			all = append(all, rr.EndToEnd...)
			localSpans = append(localSpans, rr.Spans...)
		}
	}
	if len(all) == 0 {
		return errors.New("no measured harness input/flush pairs")
	}
	if err := writeJSON(filepath.Join(opt.out, "runs.json"), results); err != nil {
		return err
	}
	return writeJSON(filepath.Join(opt.out, "summary.json"), summary{Schema: 1, GitSHA: h.gitSHA(), Warmup: opt.warmup.String(), Duration: opt.duration.String(), Repetitions: opt.repetitions, EndToEnd: percentiles(all), Spans: summarizeSpans(localSpans), Runs: len(results)})
}
func parseOptions(args []string) (options, error) {
	var o options
	fs := flag.NewFlagSet("vev-perf-harness", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&o.vevBin, "vev-bin", "", "public vev binary")
	fs.StringVar(&o.manifest, "manifest", "", "manifest")
	fs.StringVar(&o.out, "out", "", "output directory")
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
	if err = h.mkdir(dir); err != nil {
		return res, err
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
	// interval. Fake clocks make this path instant in unit tests.
	h.clock.Sleep(o.warmup)
	marks := []harnessMark{}
	seq := uint64(0)
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
	// A measured run is never shorter than the requested duration in the
	// production clock domain. The terminal boundary stays inside that interval.
	h.clock.Sleep(o.duration)
	samples, e := pairedSamples(marks)
	if e != nil {
		return res, e
	}
	// Process teardown can unblock a receive and cause its failed end mark to
	// be serialized. Merge only the complete, post-cleanup per-process traces.
	if cleanupErr := closeRoles(); cleanupErr != nil {
		return res, cleanupErr
	}
	spans, e := mergeProcessTraces(maps)
	if e != nil {
		return res, e
	}
	for _, m := range marks {
		if _, e := fmt.Fprintf(raw, "%s\n", mustJSON(m)); e != nil {
			return res, e
		}
	}
	success = true
	return runResult{Spans: spans, Scenario: s.ID, Run: run, Samples: len(samples), EndToEnd: samples, Processes: maps}, nil
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
}
type spanPair struct{ start, end, name string }

var spanPairs = []spanPair{{"diff_start", "diff_end", "diff_duration"}, {"queue_enqueued", "queue_dequeued", "queue_wait"}, {"ack_blocked_start", "ack_blocked_end", "ack_blocked_interval"}, {"adapter_send_start", "adapter_send_end", "adapter_send_duration"}, {"adapter_receive_start", "adapter_receive_end", "adapter_receive_duration"}}

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
			return nil, err
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
		// Spans may overlap: an end for an older operation can legitimately be
		// serialized after a newer operation's start. New correlation IDs still
		// must be allocated monotonically within the process.
		lastSequence := uint64(0)
		seenSequences := map[uint64]bool{}
		starts := map[string]traceRecord{}
		for _, r := range records {
			if !seenSequences[r.Sequence] {
				if r.Sequence < lastSequence {
					return nil, errors.New("nonmonotonic same-process sequence")
				}
				seenSequences[r.Sequence] = true
				lastSequence = r.Sequence
			}
			for _, pair := range spanPairs {
				key := fmt.Sprintf("%s/%s/%d/%d/%d/%d", r.ProcessID, m.Scenario, m.Run, r.Sequence, r.RequestID, r.Epoch)
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
					out = append(out, span{Component: r.Component, Name: pair.name, Samples: []int64{r.Tick - start.Tick}})
					delete(starts, k)
				}
			}
		}
		if len(starts) != 0 {
			return nil, errors.New("missing process-local span pair")
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
