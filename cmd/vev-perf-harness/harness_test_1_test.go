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

func TestCLILauncherIsolatesRoleXDGEnvironmentAcrossRepetitions(t *testing.T) {
	captureDir := t.TempDir()
	t.Setenv("CAPTURE_DIR", captureDir)
	t.Setenv("XDG_RUNTIME_DIR", filepath.Join(t.TempDir(), "inherited-runtime"))
	t.Setenv("XDG_STATE_HOME", filepath.Join(t.TempDir(), "inherited-state"))

	bin := filepath.Join(t.TempDir(), "capture-env")
	if err := os.WriteFile(bin, []byte("#!/bin/sh\nenv > \"$CAPTURE_DIR/$VEV_PERF_PROCESS_ID\"\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	launcher := &cliLauncher{bin: bin}
	for run := 1; run <= 3; run++ {
		runDir := filepath.Join(t.TempDir(), fmt.Sprintf("run-%d", run))
		if err := os.MkdirAll(runDir, 0o700); err != nil {
			t.Fatal(err)
		}
		roles := []string{"daemon", "client"}
		for _, role := range roles {
			id := fmt.Sprintf("run-%d-%s", run, role)
			p, err := launcher.Launch(processMapping{
				Role: role, TracePath: filepath.Join(runDir, role+".jsonl"), ProcessID: id,
			}, roleCommand{Args: []string{"test"}})
			if err != nil {
				t.Fatalf("run %d %s launch: %v", run, role, err)
			}
			runtimeDir := launcher.runtimes[runDir]
			if err := p.Close(); err != nil {
				t.Fatalf("run %d %s close: %v", run, role, err)
			}

			env, err := os.ReadFile(filepath.Join(captureDir, id))
			if err != nil {
				t.Fatalf("run %d %s capture: %v", run, role, err)
			}
			for _, want := range []string{
				"XDG_RUNTIME_DIR=" + runtimeDir,
				"XDG_STATE_HOME=" + filepath.Join(runDir, "state"),
			} {
				name := strings.SplitN(want, "=", 2)[0]
				if count := bytes.Count(env, []byte(name+"=")); count != 1 {
					t.Fatalf("run %d %s %s entries = %d, want exactly one; env:\n%s", run, role, name, count, env)
				}
				if !bytes.Contains(env, []byte(want+"\n")) {
					t.Fatalf("run %d %s missing %q; env:\n%s", run, role, want, env)
				}
			}
		}
		if err := launcher.releaseRuntime(runDir); err != nil {
			t.Fatalf("run %d release runtime: %v", run, err)
		}
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

func TestHarnessResolvesCanonicalRelativePathsBeforeRoleCWDChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI relative-path lifecycle")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	// Keep the real Unix-socket path below its platform limit while testing the
	// exact documented relative output hierarchy.
	workspace, err := os.MkdirTemp("", "vev-rel-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(workspace) })
	bin := filepath.Join(workspace, "vev")
	build := exec.Command("/usr/local/go/bin/go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}
	manifestPath := filepath.Join(workspace, "testdata", "perf", "manifest.json")
	if err := os.MkdirAll(filepath.Dir(manifestPath), 0o700); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(filepath.Join(root, "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manifestPath, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(workspace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Errorf("restore cwd: %v", err)
		}
	})

	// These are the documented relative locations. cliLauncher changes every
	// role to its run directory, so a failure to resolve them before launch
	// would make VEV_PERF_TRACE point at a nonexistent nested directory.
	h := defaultHarness()
	h.clock = &fakeClock{}
	if err := run([]string{
		"--vev-bin", "./vev",
		"--manifest", "testdata/perf/manifest.json",
		"--out", "testdata/perf/results",
		"--scenario", "1x4-idle-local",
		"--warmup", "0s",
		"--duration", "30s",
		"--repetitions", "10",
	}, h); err != nil {
		t.Fatal(err)
	}

	out := filepath.Join(workspace, "testdata", "perf", "results")
	for run := 1; run <= 10; run++ {
		runDir := filepath.Join(out, fmt.Sprintf("1x4-idle-local-run-%03d", run))
		manifestBytes, err := os.ReadFile(filepath.Join(runDir, "manifest.json"))
		if err != nil {
			t.Fatalf("run %d manifest: %v", run, err)
		}
		var persisted runManifest
		if err := json.Unmarshal(manifestBytes, &persisted); err != nil {
			t.Fatalf("run %d manifest JSON: %v", run, err)
		}
		if persisted.Run != run || len(persisted.Processes) != 2 {
			t.Fatalf("run %d manifest=%+v", run, persisted)
		}
		for _, process := range persisted.Processes {
			if !filepath.IsAbs(process.TracePath) || filepath.Dir(process.TracePath) != runDir {
				t.Fatalf("run %d %s trace path=%q, want absolute path in %q", run, process.Role, process.TracePath, runDir)
			}
			trace, err := os.ReadFile(process.TracePath)
			if err != nil || len(bytes.TrimSpace(trace)) == 0 {
				t.Fatalf("run %d %s trace=%q err=%v", run, process.Role, process.TracePath, err)
			}
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

func TestResolvePathOptionsMakesRelativeAndAbsoluteInputsIdentical(t *testing.T) {
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	absolute := options{
		vevBin:   filepath.Join(cwd, "vev"),
		manifest: filepath.Join(cwd, "testdata", "perf", "manifest.json"),
		out:      filepath.Join(cwd, "testdata", "perf", "results"),
	}
	relative := options{
		vevBin:   "vev",
		manifest: "testdata/perf/manifest.json",
		out:      "testdata/perf/results",
	}
	gotRelative, err := resolvePathOptions(relative)
	if err != nil {
		t.Fatal(err)
	}
	gotAbsolute, err := resolvePathOptions(absolute)
	if err != nil {
		t.Fatal(err)
	}
	if gotRelative != gotAbsolute {
		t.Fatalf("relative paths resolved to %+v, absolute paths to %+v", gotRelative, gotAbsolute)
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
