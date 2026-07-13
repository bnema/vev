package main

import (
	"bytes"
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
	"strings"
	"syscall"
	"time"

	"github.com/bnema/vev/pkg/safedir"
	"golang.org/x/sys/unix"
)

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
