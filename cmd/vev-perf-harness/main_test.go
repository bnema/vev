package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ tick int64 }

func (c *fakeClock) Now() int64 { c.tick += 10; return c.tick }
func (c *fakeClock) Sleep(d time.Duration) {
	c.tick += d.Nanoseconds()
}

type fakeUDPNetem struct {
	port   int
	closed bool
}

func (n *fakeUDPNetem) Port() int    { return n.port }
func (n *fakeUDPNetem) Close() error { n.closed = true; return nil }

type fakeProcess struct {
	fail       bool
	measureErr error
	closeErr   error
	warmups    [][]byte
	measures   [][]byte
	closed     bool
}

func (p *fakeProcess) Warmup(input []byte) error {
	p.warmups = append(p.warmups, append([]byte(nil), input...))
	return nil
}
func (p *fakeProcess) Measure(input []byte, injected, flush func() error) error {
	p.measures = append(p.measures, append([]byte(nil), input...))
	if p.measureErr != nil {
		return p.measureErr
	}
	if p.fail {
		return errors.New("measure failure")
	}
	if err := injected(); err != nil {
		return err
	}
	return flush()
}
func (p *fakeProcess) Close() error { p.closed = true; return p.closeErr }

type fakeLauncher struct {
	mappings        []processMapping
	args            [][]string
	commands        []roleCommand
	process         []*fakeProcess
	measureErr      map[string]error
	closeErr        map[string]error
	manifestPresent bool
}

func (l *fakeLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	l.mappings = append(l.mappings, m)
	l.args = append(l.args, append([]string(nil), command.Args...))
	l.commands = append(l.commands, command)
	_, e := os.Stat(filepath.Join(filepath.Dir(m.TracePath), "manifest.json"))
	l.manifestPresent = e == nil
	p := &fakeProcess{measureErr: l.measureErr[m.Role], closeErr: l.closeErr[m.Role]}
	l.process = append(l.process, p)
	return p, nil
}

type lateFlushProcess struct{ clock *fakeClock }

func (*lateFlushProcess) Warmup([]byte) error { return nil }
func (p *lateFlushProcess) Measure(_ []byte, injected, flushed func() error) error {
	if err := injected(); err != nil {
		return err
	}
	p.clock.tick += minimumDuration.Nanoseconds()
	return flushed()
}
func (*lateFlushProcess) Close() error { return nil }

type insufficientEventLauncher struct{ clock *fakeClock }

func (l insufficientEventLauncher) Launch(m processMapping, _ roleCommand) (launchedProcess, error) {
	if m.Role == "client" {
		return &lateFlushProcess{clock: l.clock}, nil
	}
	return &fakeProcess{}, nil
}

func TestCLIProcessCloseIsBoundedWhenPublicRoleDoesNotExit(t *testing.T) {
	deadline := make(chan time.Time)
	close(deadline)
	var term, kill int
	p := &cliProcess{
		waitErr:      make(chan error),
		waitTimeout:  func() <-chan time.Time { return deadline },
		forceCleanup: func() { term++ },
		forceKill:    func() { kill++ },
	}
	if err := p.Close(); err == nil {
		t.Fatal("stuck public role close succeeded")
	}
	if term != 1 || kill != 1 {
		t.Fatalf("TERM/KILL calls = %d/%d, want 1/1", term, kill)
	}
}

func TestCLIProcessWaitReadyRequiresDaemonSocket(t *testing.T) {
	path := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := (&cliProcess{readyPath: path, waitErr: make(chan error, 1)}).WaitReady(); err != nil {
		t.Fatal(err)
	}
}

func TestCLIProcessCloseUsesPublicDaemonShutdown(t *testing.T) {
	wait := make(chan error, 1)
	wait <- nil
	var shutdown int
	p := &cliProcess{waitErr: wait, shutdown: func() error { shutdown++; return nil }}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if shutdown != 0 {
		t.Fatalf("shutdown calls = %d, want 0 for an exited daemon", shutdown)
	}

	wait = make(chan error, 1)
	p = &cliProcess{waitErr: wait, shutdown: func() error { shutdown++; wait <- nil; return nil }}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if shutdown != 1 {
		t.Fatalf("shutdown calls = %d, want 1", shutdown)
	}
}

func TestHarnessCanonicalLocalSmokeRealRoles(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI smoke")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "vev")
	build := exec.Command("/usr/local/go/bin/go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}
	m, err := readManifest(filepath.Join(root, "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManifest(m); err != nil {
		t.Fatal(err)
	}
	var local scenario
	for _, s := range m.Scenarios {
		if s.Topology == "1x4" && s.Workload == "idle" && s.Transport == "local" {
			local = s
			break
		}
	}
	if local.ID == "" {
		t.Fatal("canonical local scenario missing")
	}
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	h := defaultHarness()
	h.clock = &fakeClock{} // injected duration keeps the public 30s contract bounded in test
	h.launcher = &cliLauncher{bin: bin}
	result, err := h.runOne(options{vevBin: bin, out: dir, duration: minimumDuration}, m, local, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.Samples < minimumInIntervalEventSamples || len(result.Processes) != 2 {
		t.Fatalf("canonical local smoke result=%+v", result)
	}
}

func TestHarnessCanonicalLocalRolesAreIsolatedAcrossRepetitions(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI repetition lifecycle")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "vev")
	build := exec.Command("/usr/local/go/bin/go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}
	m, err := readManifest(filepath.Join(root, "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var local scenario
	for _, candidate := range m.Scenarios {
		if candidate.ID == "1x4-idle-local" {
			local = candidate
			break
		}
	}
	if local.ID == "" {
		t.Fatal("canonical local scenario missing")
	}
	out, err := os.MkdirTemp("", "vev-ob-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(out)
	raw, err := os.OpenFile(filepath.Join(out, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	h := defaultHarness()
	h.clock = &fakeClock{}
	h.launcher = &cliLauncher{bin: bin}
	for run := 1; run <= 3; run++ {
		result, err := h.runOne(options{vevBin: bin, out: out, duration: minimumDuration}, m, local, run, raw)
		if err != nil {
			t.Fatalf("run %d: %v", run, err)
		}
		if len(result.Processes) != 2 {
			t.Fatalf("run %d process mappings = %+v", run, result.Processes)
		}
		runDir := filepath.Join(out, fmt.Sprintf("%s-run-%03d", safeName(local.ID), run))
		manifestBytes, err := os.ReadFile(filepath.Join(runDir, "manifest.json"))
		if err != nil {
			t.Fatalf("run %d manifest: %v", run, err)
		}
		var persisted runManifest
		if err := json.Unmarshal(manifestBytes, &persisted); err != nil {
			t.Fatalf("run %d manifest JSON: %v", run, err)
		}
		if persisted.Run != run || len(persisted.Processes) != len(result.Processes) {
			t.Fatalf("run %d manifest = %+v", run, persisted)
		}
		for _, process := range result.Processes {
			if process.Role != "daemon" && process.Role != "client" {
				t.Fatalf("run %d unexpected role %q", run, process.Role)
			}
			if filepath.Dir(process.TracePath) != runDir {
				t.Fatalf("run %d %s trace outside its isolated run directory: %q", run, process.Role, process.TracePath)
			}
			trace, err := os.ReadFile(process.TracePath)
			if err != nil {
				t.Fatalf("run %d %s trace %q: %v", run, process.Role, process.TracePath, err)
			}
			if len(bytes.TrimSpace(trace)) == 0 {
				t.Fatalf("run %d %s did not emit its declared trace", run, process.Role)
			}
			for _, line := range bytes.Split(bytes.TrimSpace(trace), []byte{'\n'}) {
				var record traceRecord
				if err := json.Unmarshal(line, &record); err != nil {
					t.Fatalf("run %d %s trace JSON: %v", run, process.Role, err)
				}
				if record.ProcessID != process.ProcessID || record.Scenario != local.ID || record.Run != uint64(run) {
					t.Fatalf("run %d %s trace escaped its manifest identity: %+v", run, process.Role, record)
				}
			}
		}
		if _, err := os.Stat(filepath.Join(runDir, "runtime", "vev", "daemon.sock")); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("run %d left daemon socket for the next repetition: %v", run, err)
		}
	}
}

func TestHarnessScenarioSelectionKeepsCanonicalManifestValidation(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	manifestPath := filepath.Join(root, "testdata", "perf", "manifest.json")
	m, err := readManifest(manifestPath)
	if err != nil {
		t.Fatal(err)
	}
	selected := m.Scenarios[0]
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, &fakeLauncher{}
	out := filepath.Join(t.TempDir(), "out")
	if err := run([]string{"--vev-bin", "ignored", "--manifest", manifestPath, "--out", out, "--scenario", selected.ID, "--warmup", "0s", "--duration", "30s", "--repetitions", "10"}, h); err != nil {
		t.Fatal(err)
	}
	var runs []runResult
	b, err := os.ReadFile(filepath.Join(out, "runs.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &runs); err != nil {
		t.Fatal(err)
	}
	if len(runs) != 10 {
		t.Fatalf("selected canonical runs=%d, want 10", len(runs))
	}
	for _, result := range runs {
		if result.Scenario != selected.ID {
			t.Fatalf("selected scenario = %q, want %q", result.Scenario, selected.ID)
		}
	}
}

func TestHarnessScenarioSelectionRejectsIncompleteCanonicalManifest(t *testing.T) {
	m, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	selected := m.Scenarios[0]
	m.Scenarios = m.Scenarios[:len(m.Scenarios)-1]
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(mustJSON(m)), 0o600); err != nil {
		t.Fatal(err)
	}
	launcher := &fakeLauncher{}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, launcher
	out := filepath.Join(dir, "out")
	err = run([]string{"--vev-bin", "ignored", "--manifest", manifestPath, "--out", out, "--scenario", selected.ID, "--warmup", "0s", "--duration", "30s", "--repetitions", "10"}, h)
	if err == nil || !strings.Contains(err.Error(), "missing scenario or inapplicable reason") {
		t.Fatalf("incomplete manifest selection error = %v", err)
	}
	if len(launcher.mappings) != 0 {
		t.Fatalf("launched with incomplete canonical manifest: %+v", launcher.mappings)
	}
	if _, statErr := os.Stat(out); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("output created before manifest validation: %v", statErr)
	}
}

func TestTraceEnvironmentMatchesProcessMapping(t *testing.T) {
	mapping := processMapping{TracePath: "/tmp/trace.jsonl", ProcessID: "p-01", Scenario: "1x4-idle-local", Run: 3}
	got := strings.Join(traceEnvironment(mapping), "\n")
	for _, want := range []string{"VEV_PERF_TRACE=/tmp/trace.jsonl", "VEV_PERF_PROCESS_ID=p-01", "VEV_PERF_SCENARIO=1x4-idle-local", "VEV_PERF_RUN=3"} {
		if !strings.Contains(got, want) {
			t.Errorf("trace environment missing %q: %s", want, got)
		}
	}
}

func TestHarnessFlagsRejectShortMeasurements(t *testing.T) {
	for _, args := range [][]string{
		{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--duration", "29s", "--repetitions", "10"},
		{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--duration", "30s", "--repetitions", "9"},
		{"--manifest", "m", "--out", "o", "--duration", "30s", "--repetitions", "10"},
	} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("parseOptions(%v) succeeded", args)
		}
	}
	if _, err := parseOptions([]string{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--duration", "30s", "--repetitions", "10"}); err == nil {
		t.Fatal("missing --warmup accepted")
	}
	if _, err := parseOptions([]string{"--vev-bin", "vev", "--manifest", "m", "--out", "o", "--warmup", "1s", "--duration", "30s", "--repetitions", "10"}); err != nil {
		t.Fatal(err)
	}
}

func TestHarnessManifestCoversCanonicalMatrix(t *testing.T) {
	m, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := validateManifest(m); err != nil {
		t.Fatal(err)
	}
	if len(m.Scenarios) != 4*9*7 {
		t.Fatalf("scenarios=%d", len(m.Scenarios))
	}
	m.Scenarios = m.Scenarios[:len(m.Scenarios)-1]
	if err := validateManifest(m); err == nil {
		t.Fatal("missing combination accepted")
	}
}

func TestHarnessRejectsExistingRunDirectoryBeforeLaunchingRoles(t *testing.T) {
	out := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(out, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	s := scenario{ID: "isolated", Roles: []string{"daemon", "client"}}
	stale := filepath.Join(out, "isolated-run-001")
	if err := os.Mkdir(stale, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(stale, "preserve"), []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	launcher := &fakeLauncher{}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, launcher
	_, err = h.runOne(options{out: out, duration: minimumDuration}, manifest{}, s, 1, raw)
	if err == nil || !strings.Contains(err.Error(), "create isolated run directory") {
		t.Fatalf("existing run directory error = %v", err)
	}
	if len(launcher.mappings) != 0 {
		t.Fatalf("roles launched against stale run directory: %+v", launcher.mappings)
	}
	if contents, readErr := os.ReadFile(filepath.Join(stale, "preserve")); readErr != nil || string(contents) != "stale" {
		t.Fatalf("stale directory changed: contents=%q err=%v", contents, readErr)
	}
}

func TestHarnessCreatesExclusiveTraceManifestAndEvidence(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw-harness.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	launcher := &fakeLauncher{}
	h := defaultHarness()
	h.clock = &fakeClock{}
	h.launcher = launcher
	o := options{vevBin: "ignored", out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}
	r, err := h.runOne(o, manifest{}, scenario{ID: "4x4-local", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if !launcher.manifestPresent {
		t.Fatal("launched before complete run manifest")
	}
	if len(r.EndToEnd) < minimumInIntervalEventSamples || r.Samples < minimumInIntervalEventSamples {
		t.Fatalf("samples=%+v", r)
	}
	seen := map[string]bool{}
	for _, m := range r.Processes {
		if m.ProcessID == "" || m.ClockDomain != m.ProcessID || m.TracePath == "" || seen[m.TracePath] {
			t.Fatalf("bad mapping %+v", m)
		}
		seen[m.TracePath] = true
		if _, err := os.Stat(m.TracePath); err != nil {
			t.Fatal(err)
		}
	}
	b, err := os.ReadFile(filepath.Join(dir, "4x4-local-run-001", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	var got runManifest
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if len(got.Processes) != 2 {
		t.Fatalf("manifest=%+v", got)
	}
	if len(launcher.process) != 2 || !launcher.process[0].closed || len(launcher.process[0].warmups) != 0 || len(launcher.process[1].warmups) != 1 || len(launcher.process[1].measures) < minimumInIntervalEventSamples {
		t.Fatalf("process lifecycle not recorded: %+v", launcher.process)
	}
}

type fakePTY struct{ writes [][]byte }

func (p *fakePTY) Read([]byte) (int, error) { return 0, os.ErrClosed }
func (p *fakePTY) Write(b []byte) (int, error) {
	p.writes = append(p.writes, append([]byte(nil), b...))
	return len(b), nil
}
func (*fakePTY) Close() error { return nil }

type stagedPTY struct {
	allow  <-chan struct{}
	writes chan []byte
}

func (*stagedPTY) Read([]byte) (int, error) { return 0, os.ErrClosed }
func (p *stagedPTY) Write(b []byte) (int, error) {
	if p.allow != nil {
		<-p.allow
	}
	p.writes <- append([]byte(nil), b...)
	return len(b), nil
}
func (*stagedPTY) Close() error { return nil }

type fakeOutput struct {
	bytes.Buffer
	syncs int
}

func (o *fakeOutput) Sync() error { o.syncs++; return nil }
func (*fakeOutput) Close() error  { return nil }

type orderedPTY struct {
	write func([]byte)
	close func()
}

func (*orderedPTY) Read([]byte) (int, error) { return 0, os.ErrClosed }
func (p *orderedPTY) Write(b []byte) (int, error) {
	if p.write != nil {
		p.write(b)
	}
	return len(b), nil
}
func (p *orderedPTY) Close() error {
	p.close()
	return nil
}

type orderedOutput struct{ close func() }

func (*orderedOutput) Write(b []byte) (int, error) { return len(b), nil }
func (*orderedOutput) Sync() error                 { return nil }
func (o *orderedOutput) Close() error {
	o.close()
	return nil
}

func TestCLIProcessCloseGracefulDetachCompletesSpanBeforeForcedCleanup(t *testing.T) {
	var order []string
	waitErr := make(chan error, 1)
	timeout := make(chan time.Time)
	p := &cliProcess{
		pty: &orderedPTY{
			write: func(input []byte) {
				if string(input) != "exit\n" {
					t.Fatalf("graceful exit input=%q", input)
				}
				order = append(order, "exit_written")
				// The graceful client exit has closed its transport and emitted
				// its adapter receive-end mark before cmd.Wait completes.
				order = append(order, "adapter_receive_end")
				waitErr <- nil
			},
			close: func() { order = append(order, "pty_closed") },
		},
		output:      &orderedOutput{close: func() { order = append(order, "output_closed") }},
		waitErr:     waitErr,
		waitTimeout: func() <-chan time.Time { return timeout },
		forceCleanup: func() {
			order = append(order, "forced_process_group_cleanup")
		},
	}

	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if !equalStrings(order, []string{"exit_written", "adapter_receive_end", "pty_closed", "output_closed"}) {
		t.Fatalf("cleanup order=%q", order)
	}
}

func TestHarnessDoesNotInheritNestedSession(t *testing.T) {
	env := withoutEnv([]string{"VEV=session=old", "PATH=/bin", "VEV_PERF_TRACE=old"}, "VEV")
	if !equalStrings(env, []string{"PATH=/bin", "VEV_PERF_TRACE=old"}) {
		t.Fatalf("environment=%q", env)
	}
}

func TestCLIProcessWarmupWaitsForDelayedTerminalReadiness(t *testing.T) {
	allowWrite := make(chan struct{})
	pty, output := &stagedPTY{allow: allowWrite, writes: make(chan []byte, 1)}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte)}
	input := workloadInput(scenario{ID: "s"}, 1, "warmup")
	done := make(chan error, 1)
	go func() { done <- p.Warmup(input) }()

	// This is public client output from the initial application prompt/state,
	// emitted only after the client has entered raw mode. A premature PTY write
	// blocks on allowWrite and therefore cannot consume this delayed readiness.
	go func() {
		p.chunks <- []byte("shell prompt$ ")
		close(allowWrite)
	}()
	select {
	case got := <-pty.writes:
		if !bytes.Equal(got, input) {
			t.Fatalf("warmup write=%q want %q", got, input)
		}
	case <-time.After(time.Second):
		t.Fatal("warmup did not wait for and then proceed from client readiness")
	}
	go func() { p.chunks <- append([]byte("application "), inputMarker(input)...); close(p.chunks) }()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if output.syncs != 1 {
		t.Fatalf("readiness output was not flushed: syncs=%d", output.syncs)
	}
}

func TestCLIProcessRejectsPTYLocalEchoAsFlushEvidence(t *testing.T) {
	pty, output := &fakePTY{}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte, 1)}
	input := workloadInput(scenario{ID: "s"}, 1, "measured-1")
	p.chunks <- newPTYLocalEcho(input).expected
	close(p.chunks)
	flushed := false
	if err := p.Measure(input, func() error { return nil }, func() error { flushed = true; return nil }); err == nil {
		t.Fatal("local PTY echo satisfied terminal flush boundary")
	}
	if flushed || output.syncs != 0 {
		t.Fatalf("local echo stamped a flush: flushed=%t syncs=%d", flushed, output.syncs)
	}
}

func TestCLIProcessPairsApplicationOutputWithSuccessfulFlush(t *testing.T) {
	pty, output := &fakePTY{}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte, 1)}
	input := workloadInput(scenario{ID: "s"}, 1, "measured-1")
	echo := newPTYLocalEcho(input).expected
	p.chunks <- append(append([]byte(nil), echo...), append([]byte("application "), inputMarker(input)...)...)
	order := []string{}
	if err := p.Measure(input, func() error { order = append(order, "injected"); return nil }, func() error { order = append(order, "flushed"); return nil }); err != nil {
		t.Fatal(err)
	}
	if len(pty.writes) != 1 || output.syncs != 1 || !equalStrings(order, []string{"injected", "flushed"}) {
		t.Fatalf("writes=%q syncs=%d boundary order=%q", pty.writes, output.syncs, order)
	}
}

func TestHarnessUsesPublicRoleCommandsAndPTYWorkloads(t *testing.T) {
	for _, tc := range []struct {
		role string
		want []string
	}{
		{"daemon", []string{"--daemon"}},
		{"client", []string{"new", "perf-s-001"}},
		{"ssh_stdio_peer", []string{"_stdio", "perf-s-001"}},
		{"udp_peer", []string{"_udp-proxy", "perf-s-001"}},
	} {
		t.Run(tc.role, func(t *testing.T) {
			got := roleArgs(scenario{ID: "s", Transport: "local"}, processMapping{Role: tc.role, Run: 1})
			if !equalStrings(got.Args, tc.want) {
				t.Fatalf("args=%q want %q", got.Args, tc.want)
			}
		})
	}
	input := string(workloadInput(scenario{ID: "s", Workload: "interactive_flood"}, 1, "measured-1"))
	if string(inputMarker([]byte(input))) != "__VEV_HARNESS_s_r1_measured-1__" || !bytes.Contains([]byte(input), []byte("while [ $i -lt 128 ]")) || !strings.HasSuffix(input, "printf '__VEV_HARNESS_s_r1_measured-1__\\n'\n") {
		t.Fatalf("workload is not real PTY shell input with an observable marker: %q", input)
	}
}

func TestHarnessRoutesEveryRemoteFixtureThroughItsDeclaredPeer(t *testing.T) {
	cases := []struct {
		name      string
		transport transport
		peer      string
		wantRTT   int
		wantLoss  int
	}{
		{"ssh", transport{ID: "ssh_stdio", Kind: "ssh_stdio"}, "ssh_stdio_peer", 0, 0},
		{"udp baseline", transport{ID: "udp_baseline", Kind: "udp"}, "udp_peer", 0, 0},
		{"udp 25ms", transport{ID: "udp_25ms", Kind: "udp", RTTMS: 25}, "udp_peer", 25, 0},
		{"udp 100ms", transport{ID: "udp_100ms", Kind: "udp", RTTMS: 100}, "udp_peer", 100, 0},
		{"udp loss zero", transport{ID: "udp_loss_0pct", Kind: "udp", LossPercent: 0}, "udp_peer", 0, 0},
		{"udp loss", transport{ID: "udp_loss_1pct", Kind: "udp", LossPercent: 1}, "udp_peer", 0, 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := scenario{ID: "route", Transport: tc.transport.ID}
			client := routeRoleArgs(s, processMapping{Role: "client", Run: 2}, tc.transport)
			peer := routeRoleArgs(s, processMapping{Role: tc.peer, Run: 2}, tc.transport)
			if !equalStrings(client.Args, []string{"attach", "harness@127.0.0.1:perf-route-002"}) {
				t.Fatalf("client did not use public remote attach: %q", client.Args)
			}
			if tc.peer == "ssh_stdio_peer" && !equalStrings(peer.Args, []string{"_stdio", "perf-route-002"}) {
				t.Fatalf("ssh peer command=%q", peer.Args)
			}
			if tc.peer == "udp_peer" && !equalStrings(peer.Args, []string{"_udp-proxy", "perf-route-002"}) {
				t.Fatalf("udp peer command=%q", peer.Args)
			}
			if peer.Transport.RTTMS != tc.wantRTT || peer.Transport.LossPercent != tc.wantLoss {
				t.Fatalf("peer lost manifest network settings: %+v", peer.Transport)
			}
		})
	}
}

func TestHarnessFakeRunnerRoutesClientToPeerAndCleansEveryRole(t *testing.T) {
	for _, tc := range []struct {
		name, transport, peer, peerCommand string
	}{
		{"ssh", "ssh_stdio", "ssh_stdio_peer", "_stdio"},
		{"udp baseline", "udp_baseline", "udp_peer", "_udp-proxy"},
		{"udp 25ms", "udp_25ms", "udp_peer", "_udp-proxy"},
		{"udp 100ms", "udp_100ms", "udp_peer", "_udp-proxy"},
		{"udp loss zero", "udp_loss_0pct", "udp_peer", "_udp-proxy"},
		{"udp loss", "udp_loss_1pct", "udp_peer", "_udp-proxy"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			l := &fakeLauncher{}
			h := defaultHarness()
			h.clock, h.launcher = &fakeClock{}, l
			s := scenario{ID: "routed", Transport: tc.transport, Roles: []string{"daemon", "client", tc.peer}}
			if _, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}, manifest{}, s, 1, raw); err != nil {
				t.Fatal(err)
			}
			if len(l.mappings) != 3 || l.mappings[1].Role != tc.peer || l.mappings[2].Role != "client" {
				t.Fatalf("dependency launch order=%+v", l.mappings)
			}
			if l.commands[1].Args[0] != tc.peerCommand || l.commands[2].Args[0] != "attach" {
				t.Fatalf("commands do not connect client through declared peer: %+v", l.commands)
			}
			for i, p := range l.process {
				if !p.closed {
					t.Fatalf("role %s was not cleaned up", l.mappings[i].Role)
				}
			}
		})
	}
}

func TestCLITransportSeamOwnsExclusivePeerTraceAndCleanup(t *testing.T) {
	fixtures := []struct {
		name string
		tr   transport
		rtt  time.Duration
		loss int
	}{
		{"baseline", transport{ID: "udp_baseline", Kind: "udp"}, 0, 0},
		{"25ms", transport{ID: "udp_25ms", Kind: "udp", RTTMS: 25}, 25 * time.Millisecond, 0},
		{"100ms", transport{ID: "udp_100ms", Kind: "udp", RTTMS: 100}, 100 * time.Millisecond, 0},
		{"loss0", transport{ID: "udp_loss_0pct", Kind: "udp"}, 0, 0},
		{"loss1", transport{ID: "udp_loss_1pct", Kind: "udp", LossPercent: 1}, 0, 1},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			if err := os.Chmod(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			var configs []udpNetemConfig
			netem := &fakeUDPNetem{port: 45678}
			l := &cliLauncher{bin: "/bin/true", netemFactory: func(c udpNetemConfig) (udpNetem, error) {
				configs = append(configs, c)
				return netem, nil
			}}
			m := processMapping{ProcessID: "udp-peer", TracePath: filepath.Join(dir, "udp.jsonl"), Role: "udp_peer"}
			if err := os.WriteFile(m.TracePath, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			p, err := l.preparePeer(m, roleCommand{Args: []string{"_udp-proxy", "work"}, Transport: tc.tr})
			if err != nil {
				t.Fatal(err)
			}
			if len(configs) != 1 || configs[0].RTT != tc.rtt || configs[0].LossPercent != tc.loss || configs[0].TargetPath != filepath.Join(dir, "udp-peer.target") {
				t.Fatalf("fixture did not reach netem seam: %+v", configs)
			}
			shim, err := os.ReadFile(filepath.Join(dir, "ssh"))
			if err != nil {
				t.Fatal(err)
			}
			text := string(shim)
			for _, want := range []string{"_udp-proxy", m.TracePath, m.ProcessID, "udp-peer.target", "VEV-UDP %s %s\\n' 45678", "udp-peer.pid"} {
				if !strings.Contains(text, want) {
					t.Errorf("seam does not retain %q:\n%s", want, text)
				}
			}
			if strings.Contains(text, "VEV_PERF_UDP_") {
				t.Fatalf("ignored vev UDP environment was used instead of netem: %s", text)
			}
			// A nonexistent pid is already-cleaned-up; Close must still close the
			// harness-owned emulator.
			if err := p.Close(); err != nil {
				t.Fatal(err)
			}
			if !netem.closed {
				t.Fatal("harness netem was not cleaned up")
			}
		})
	}
}

func TestUDPNetemExecutesRTTAndLossFixtures(t *testing.T) {
	fixtures := []struct {
		name string
		rtt  time.Duration
		loss int
		sent int
		want int
	}{
		{"25ms RTT", 25 * time.Millisecond, 0, 1, 1},
		{"100ms RTT", 100 * time.Millisecond, 0, 1, 1},
		{"zero loss", 0, 0, 10, 10},
		{"one percent loss", 0, 1, 100, 99},
	}
	for _, tc := range fixtures {
		t.Run(tc.name, func(t *testing.T) {
			target, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer target.Close()
			path := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(path, []byte("VEV-UDP "+strconv.Itoa(target.LocalAddr().(*net.UDPAddr).Port)+" key\\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			netem, err := newUDPNetem(udpNetemConfig{RTT: tc.rtt, LossPercent: tc.loss, TargetPath: path})
			if err != nil {
				t.Fatal(err)
			}
			defer netem.Close()
			client, err := net.ListenPacket("udp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()
			started := time.Now()
			for i := 0; i < tc.sent; i++ {
				if _, err := client.WriteTo([]byte{byte(i)}, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: netem.Port()}); err != nil {
					t.Fatal(err)
				}
			}
			if err := target.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			got := 0
			buf := make([]byte, 32)
			for got < tc.want {
				if _, _, err := target.ReadFrom(buf); err != nil {
					t.Fatalf("received %d packets, want %d: %v", got, tc.want, err)
				}
				got++
			}
			if tc.rtt > 0 {
				if elapsed := time.Since(started); elapsed < tc.rtt/2 {
					t.Fatalf("one-way netem delay=%s, want at least %s", elapsed, tc.rtt/2)
				}
			}
			// Keep the deadline short: extra carriage would prove the exact 1%%
			// loss cycle was not enforced.
			if err := target.SetReadDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
				t.Fatal(err)
			}
			if _, _, err := target.ReadFrom(buf); tc.want != tc.sent && err == nil {
				t.Fatalf("received more than %d packets with %d%% loss", tc.want, tc.loss)
			}
		})
	}
}

func TestHarnessRejectsInvalidBoundaryPairs(t *testing.T) {
	for _, marks := range [][]harnessMark{
		{{Sequence: 1, Kind: "terminal_flushed", Tick: 1, Valid: true}},
		{{Sequence: 1, Kind: "input_injected", Tick: 2, Valid: true}, {Sequence: 1, Kind: "terminal_flushed", Tick: 1, Valid: true}},
		{{Sequence: 1, Kind: "input_injected", Tick: 1, Valid: true}},
		{{Sequence: 1, Kind: "input_injected", Tick: 1, Valid: true}, {Sequence: 1, Kind: "input_injected", Tick: 2, Valid: true}},
	} {
		if _, err := pairedSamples(marks); err == nil {
			t.Fatalf("invalid marks accepted: %+v", marks)
		}
	}
	got, err := pairedSamples([]harnessMark{{Sequence: 9, Kind: "input_injected", Tick: 1, Valid: true}, {Sequence: 9, Kind: "terminal_flushed", Tick: 5, Valid: true}})
	if err != nil || len(got) != 1 || got[0] != 4 {
		t.Fatalf("got %v %v", got, err)
	}
}

func TestHarnessCollectsOnlyCompleteMeasuredIntervalEvents(t *testing.T) {
	interval := measuredInterval{Start: 100, End: 200}
	marks := []harnessMark{
		{Sequence: 1, Kind: "input_injected", Tick: 10, Valid: true},
		{Sequence: 1, Kind: "terminal_flushed", Tick: 20, Valid: true}, // warmup
		{Sequence: 2, Kind: "input_injected", Tick: 100, Valid: true},
		{Sequence: 2, Kind: "terminal_flushed", Tick: 120, Valid: true},
		{Sequence: 3, Kind: "input_injected", Tick: 150, Valid: true},
		{Sequence: 3, Kind: "terminal_flushed", Tick: 201, Valid: true}, // exits interval
		{Sequence: 4, Kind: "input_injected", Tick: 99, Valid: true},
		{Sequence: 4, Kind: "terminal_flushed", Tick: 110, Valid: true}, // enters interval
	}
	events, err := measuredEventSamples(marks, interval)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Sequence != 2 || events[0].Latency != 20 {
		t.Fatalf("measured events=%+v, want only complete sequence 2", events)
	}
}

func TestHarnessRejectsInsufficientMeasuredEventSamples(t *testing.T) {
	marks := make([]harnessMark, 0, 2*(minimumInIntervalEventSamples-1))
	for i := 0; i < minimumInIntervalEventSamples-1; i++ {
		marks = append(marks,
			harnessMark{Sequence: uint64(i + 1), Kind: "input_injected", Tick: int64(i * 100), Valid: true},
			harnessMark{Sequence: uint64(i + 1), Kind: "terminal_flushed", Tick: int64(i*100 + 10), Valid: true},
		)
	}
	events, err := measuredEventSamples(marks, measuredInterval{Start: 0, End: int64(minimumInIntervalEventSamples * 100)})
	if err != nil {
		t.Fatal(err)
	}
	if err := requireMinimumEventSamples(events); err == nil {
		t.Fatal("insufficient measured event samples accepted")
	}
}

func TestHarnessRunRejectsStraddlingEventsAsInsufficient(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	clock := &fakeClock{}
	h := defaultHarness()
	h.clock, h.launcher = clock, insufficientEventLauncher{clock: clock}
	_, err = h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration}, manifest{}, scenario{ID: "late", Roles: []string{"daemon", "client"}}, 1, raw)
	if err == nil || !strings.Contains(err.Error(), "insufficient in-interval event samples") {
		t.Fatalf("straddling run error=%v, want insufficient in-interval sample rejection", err)
	}
}

func TestHarnessCalculatesEventCadenceAndMaxGapFromRawPairs(t *testing.T) {
	events := []eventSample{
		{Sequence: 1, Injected: 100, Flushed: 110, Latency: 10},
		{Sequence: 2, Injected: 130, Flushed: 140, Latency: 10},
		{Sequence: 3, Injected: 180, Flushed: 190, Latency: 10},
	}
	cadence, maxGap := eventCadence(events)
	if cadence.Count != 2 || cadence.P50 != 30 || cadence.P95 != 30 || cadence.P99 != 30 || maxGap != 50 {
		t.Fatalf("cadence=%+v maxGap=%d", cadence, maxGap)
	}
}

func TestHarnessAggregatesRawEventsSeparatelyFromRunDispersion(t *testing.T) {
	first := append([]int64(nil), make([]int64, minimumInIntervalEventSamples+5)...)
	for i := range first {
		first[i] = 1
	}
	second := []int64{100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 100, 1_000, 1_000}
	all, _, _, runP50s := aggregateEventResults([]runResult{
		{EndToEnd: first, Event: distribution{P50: 1}},
		{EndToEnd: second, Event: distribution{P50: 100}},
	})
	if got, dispersion := percentiles(all), percentiles(runP50s); got.Count != 30 || got.P99 != 1_000 || dispersion.Count != 2 || dispersion.P99 != 1 {
		t.Fatalf("event aggregation=%+v run dispersion=%+v", got, dispersion)
	}
}

func TestHarnessSchedulesMeasuredEventsAcrossFullInterval(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, &fakeLauncher{}
	result, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration}, manifest{}, scenario{ID: "schedule", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if result.Samples < minimumInIntervalEventSamples || result.Cadence.Count != result.Samples-1 || result.MaxGap <= 0 {
		t.Fatalf("result did not include repeated measured events: %+v", result)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	records, err := readJSONL(filepath.Join(dir, "raw.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != result.Samples*2 {
		t.Fatalf("raw records=%d, want %d complete measured boundaries", len(records), result.Samples*2)
	}
}

func TestHarnessPreservesWorkloadAndCleanupErrors(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	primary := errors.New("primary workload failure")
	cleanup := errors.New("daemon cleanup failure")
	launcher := &fakeLauncher{
		measureErr: map[string]error{"client": primary},
		closeErr:   map[string]error{"daemon": cleanup},
	}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, launcher
	_, err = h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}, manifest{}, scenario{ID: "errors", Roles: []string{"daemon", "client"}}, 1, raw)
	if !errors.Is(err, primary) || !errors.Is(err, cleanup) {
		t.Fatalf("workload and cleanup errors were not both preserved: %v", err)
	}
	if got, want := err.Error(), primary.Error()+"\nclose daemon role: "+cleanup.Error(); got != want {
		t.Fatalf("error ordering=%q want %q", got, want)
	}
	for i, p := range launcher.process {
		if !p.closed {
			t.Fatalf("role %s was not closed", launcher.mappings[i].Role)
		}
	}
}

func TestHarnessCleansRunDirectoryOnProcessFailure(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw-harness.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	launcher := &fakeLauncher{}
	h := defaultHarness()
	h.clock = &fakeClock{}
	// The client is the second role and fails after the daemon has started.
	h.launcher = &failingLauncher{fakeLauncher: launcher}
	o := options{vevBin: "ignored", out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}
	if _, err := h.runOne(o, manifest{}, scenario{ID: "cleanup", Roles: []string{"daemon", "client"}}, 1, raw); err == nil {
		t.Fatal("failed process accepted")
	}
	if _, err := os.Stat(filepath.Join(dir, "cleanup-run-001")); !os.IsNotExist(err) {
		t.Fatalf("failed run directory remains: %v", err)
	}
}

type failingLauncher struct{ *fakeLauncher }

func (l *failingLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	if m.Role == "client" {
		p.(*fakeProcess).fail = true
	}
	return p, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestHarnessIncludesAllRequiredProcessLocalSpans(t *testing.T) {
	got := make([]string, 0, len(spanPairs))
	for _, pair := range spanPairs {
		got = append(got, pair.name)
	}
	want := []string{"capture_duration", "compose_duration", "diff_duration", "queue_wait", "ack_blocked_interval", "emit_duration", "adapter_send_duration", "adapter_receive_duration"}
	if !equalStrings(got, want) {
		t.Fatalf("span pairs = %v, want %v", got, want)
	}
}

func TestHarnessRawRecordsAreDeterministicJSONL(t *testing.T) {
	var b bytes.Buffer
	for _, m := range []harnessMark{{Scenario: "s", Run: 1, Sequence: 1, Kind: "input_injected", Tick: 1, Valid: true}, {Scenario: "s", Run: 1, Sequence: 1, Kind: "terminal_flushed", Tick: 2, Valid: true}} {
		b.WriteString(mustJSON(m))
		b.WriteByte('\n')
	}
	path := filepath.Join(t.TempDir(), "raw.jsonl")
	if err := os.WriteFile(path, b.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if got, err := readJSONL(path); err != nil || len(got) != 2 {
		t.Fatalf("records=%d err=%v", len(got), err)
	}
}

type traceLauncher struct{ fakeLauncher }

func (l *traceLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	f, err := os.OpenFile(m.TracePath, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	for i, pair := range spanPairs {
		for n, kind := range []string{pair.start, pair.end} {
			b, _ := json.Marshal(traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: uint64(i + 1), RequestID: 1, Epoch: 1, Kind: kind, Tick: int64(i*20 + n*5)})
			if _, err := f.Write(append(b, '\n')); err != nil {
				return nil, err
			}
		}
	}
	return p, nil
}

type closeTraceProcess struct {
	*fakeProcess
	traceEnd func() error
}

func (p *closeTraceProcess) Close() error {
	p.closed = true
	return errors.Join(p.closeErr, p.traceEnd())
}

type closeTraceLauncher struct{ fakeLauncher }

func (l *closeTraceLauncher) Launch(m processMapping, command roleCommand) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, command)
	if err != nil {
		return nil, err
	}
	start := traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_start", Tick: 10}
	if err := appendTraceRecord(m.TracePath, start); err != nil {
		return nil, err
	}
	return &closeTraceProcess{fakeProcess: p.(*fakeProcess), traceEnd: func() error {
		return appendTraceRecord(m.TracePath, traceRecord{ProcessID: m.ProcessID, Component: m.Role, Scenario: m.Scenario, Run: uint64(m.Run), Sequence: 1, RequestID: 1, Epoch: 1, Kind: "adapter_receive_end", Tick: 20})
	}}, nil
}

func appendTraceRecord(path string, record traceRecord) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer f.Close()
	b, err := json.Marshal(record)
	if err != nil {
		return err
	}
	_, err = f.Write(append(b, '\n'))
	return err
}

func TestHarnessClosesRolesBeforeMergingReceiveSpans(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	launcher := &closeTraceLauncher{}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, launcher
	result, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration, repetitions: minimumRepetitions}, manifest{}, scenario{ID: "receive-cleanup", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Spans) != 2 {
		t.Fatalf("post-cleanup receive spans=%+v", result.Spans)
	}
	for _, s := range result.Spans {
		if s.Name != "adapter_receive_duration" || len(s.Samples) != 1 || s.Samples[0] != 10 {
			t.Fatalf("unmatched or wrong receive span: %+v", s)
		}
	}
	for i, p := range launcher.process {
		if !p.closed {
			t.Fatalf("role %s was not closed before trace merge", launcher.mappings[i].Role)
		}
	}
}

func TestHarnessWritesRequiredEvidenceWithSufficientSpans(t *testing.T) {
	base, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range base.Scenarios {
		if i > 0 {
			base.Scenarios[i].InapplicableReason = "fixture limits this test to one public topology"
			base.Scenarios[i].Roles = nil
		}
	}
	dir := t.TempDir()
	manifestPath := filepath.Join(dir, "manifest.json")
	if err := os.WriteFile(manifestPath, []byte(mustJSON(base)), 0o600); err != nil {
		t.Fatal(err)
	}
	l := &traceLauncher{}
	h := defaultHarness()
	h.clock = &fakeClock{}
	h.launcher = l
	h.gitSHA = func() string { return "test-sha" }
	if err := run([]string{"--vev-bin", "ignored", "--manifest", manifestPath, "--out", filepath.Join(dir, "out"), "--warmup", "1s", "--duration", "30s", "--repetitions", "10"}, h); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"raw-harness.jsonl", "runs.json", "summary.json"} {
		if _, err := os.Stat(filepath.Join(dir, "out", name)); err != nil {
			t.Errorf("%s: %v", name, err)
		}
	}
	var got summary
	b, err := os.ReadFile(filepath.Join(dir, "out", "summary.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatal(err)
	}
	if got.EndToEnd.Count < 10*minimumInIntervalEventSamples || got.Repetitions != 10 || got.RunP50Dispersion.Count != 10 || got.Cadence.Count == 0 || got.MaxGap == 0 || len(got.Spans) == 0 {
		t.Fatalf("summary=%+v", got)
	}
	for _, s := range got.Spans {
		if s.Distribution.Count < 10 {
			t.Fatalf("insufficient span summary: %+v", s)
		}
	}
}

// boundedLocalConcurrentTrace is an excerpt from the second run of the
// bounded local public-CLI smoke. Receive sequence 3 is in flight while the
// same process serializes sequence 4; shared-process records therefore cannot
// have a global sequence ordering requirement.
const boundedLocalConcurrentTrace = `{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":3,"request_id":3,"epoch":3,"kind":"adapter_receive_start","tick":1783902621495284782,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":4,"request_id":4,"epoch":4,"kind":"adapter_send_start","tick":1783902621495364552,"bytes":96,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":4,"request_id":4,"epoch":4,"kind":"adapter_send_end","tick":1783902621495386452,"bytes":96,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
{"schema":1,"process_id":"1x4-idle-local-r002-client-02","component":"ipc","scenario":"1x4-idle-local","run":2,"sequence":3,"request_id":3,"epoch":3,"kind":"adapter_receive_end","tick":1783902621495463332,"bytes":0,"fragments":0,"retransmits":0,"pending":0,"ack_rtt_nanos":0,"valid":true}
`

func TestHarnessAcceptsConcurrentProductionTraceInterleaving(t *testing.T) {
	path := filepath.Join(t.TempDir(), "local-client.jsonl")
	if err := os.WriteFile(path, []byte(boundedLocalConcurrentTrace), 0o600); err != nil {
		t.Fatal(err)
	}
	spans, err := mergeProcessTraces([]processMapping{{
		ProcessID: "1x4-idle-local-r002-client-02", ClockDomain: "1x4-idle-local-r002-client-02", TracePath: path,
		Scenario: "1x4-idle-local", Run: 2,
	}})
	if err != nil {
		t.Fatalf("valid concurrent production trace rejected: %v", err)
	}
	if len(spans) != 2 || spans[0].Component != "ipc" || spans[0].Name != "adapter_send_duration" || spans[0].Samples[0] != 21900 || spans[1].Component != "ipc" || spans[1].Name != "adapter_receive_duration" || spans[1].Samples[0] != 178550 {
		t.Fatalf("spans=%+v", spans)
	}
}

func TestHarnessRejectsMissingManifestRoleTrace(t *testing.T) {
	dir := t.TempDir()
	missing := filepath.Join(dir, "daemon.jsonl")
	client := filepath.Join(dir, "client.jsonl")
	if err := os.WriteFile(client, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := mergeProcessTraces([]processMapping{
		{ProcessID: "daemon", ClockDomain: "daemon", TracePath: missing, Role: "daemon", Scenario: "s", Run: 1},
		{ProcessID: "client", ClockDomain: "client", TracePath: client, Role: "client", Scenario: "s", Run: 1},
	})
	if !errors.Is(err, os.ErrNotExist) || !strings.Contains(err.Error(), "daemon role") {
		t.Fatalf("missing daemon trace error = %v, want daemon role not-exist error", err)
	}
}

func TestHarnessRejectsCrossProcessAndBadTraceSpans(t *testing.T) {
	write := func(t *testing.T, records ...traceRecord) processMapping {
		t.Helper()
		path := filepath.Join(t.TempDir(), "trace.jsonl")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			b, _ := json.Marshal(r)
			_, _ = f.Write(append(b, '\n'))
		}
		_ = f.Close()
		return processMapping{ProcessID: "one", ClockDomain: "one", TracePath: path, Scenario: "s", Run: 1}
	}
	base := func(kind string, tick int64) traceRecord {
		return traceRecord{ProcessID: "one", Component: "daemon", Scenario: "s", Run: 1, Sequence: 1, RequestID: 1, Epoch: 1, Kind: kind, Tick: tick}
	}
	for _, records := range [][]traceRecord{
		{base("diff_end", 1)},
		{base("diff_start", 2), base("diff_end", 1)},
		{base("diff_start", 1)},
		{base("diff_start", 1), base("diff_start", 2)},
		{base("diff_start", 1), func() traceRecord { r := base("diff_end", 2); r.Component = "ipc"; return r }()},
		{func() traceRecord { r := base("diff_start", 1); r.ProcessID = "other"; return r }()},
		{func() traceRecord { r := base("diff_start", 1); r.Scenario = "other-scenario"; return r }()},
		{func() traceRecord { r := base("diff_start", 1); r.Run = 2; return r }()},
	} {
		if _, err := mergeProcessTraces([]processMapping{write(t, records...)}); err == nil {
			t.Fatalf("invalid trace accepted: %+v", records)
		}
	}
	m := write(t, base("diff_start", 1), base("diff_end", 3))
	spans, err := mergeProcessTraces([]processMapping{m})
	if err != nil || len(spans) != 1 || spans[0].Samples[0] != 2 {
		t.Fatalf("spans=%+v err=%v", spans, err)
	}
}
