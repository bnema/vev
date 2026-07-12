package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeClock struct{ tick int64 }

func (c *fakeClock) Now() int64        { c.tick += 10; return c.tick }
func (*fakeClock) Sleep(time.Duration) {}

type fakeProcess struct {
	fail     bool
	warmups  [][]byte
	measures [][]byte
	closed   bool
}

func (p *fakeProcess) Warmup(input []byte) error {
	p.warmups = append(p.warmups, append([]byte(nil), input...))
	return nil
}
func (p *fakeProcess) Measure(input []byte, injected, flush func() error) error {
	p.measures = append(p.measures, append([]byte(nil), input...))
	if p.fail {
		return errors.New("measure failure")
	}
	if err := injected(); err != nil {
		return err
	}
	return flush()
}
func (p *fakeProcess) Close() error { p.closed = true; return nil }

type fakeLauncher struct {
	mappings        []processMapping
	args            [][]string
	process         []*fakeProcess
	manifestPresent bool
}

func (l *fakeLauncher) Launch(m processMapping, args []string) (launchedProcess, error) {
	l.mappings = append(l.mappings, m)
	l.args = append(l.args, append([]string(nil), args...))
	_, e := os.Stat(filepath.Join(filepath.Dir(m.TracePath), "manifest.json"))
	l.manifestPresent = e == nil
	p := &fakeProcess{}
	l.process = append(l.process, p)
	return p, nil
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
	if len(m.Scenarios) != 4*9*6 {
		t.Fatalf("scenarios=%d", len(m.Scenarios))
	}
	m.Scenarios = m.Scenarios[:len(m.Scenarios)-1]
	if err := validateManifest(m); err == nil {
		t.Fatal("missing combination accepted")
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
	if len(r.EndToEnd) != 1 || r.Samples != 1 {
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
	if len(launcher.process) != 2 || !launcher.process[0].closed || len(launcher.process[0].warmups) != 0 || len(launcher.process[1].warmups) != 1 || len(launcher.process[1].measures) != 1 {
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

type fakeOutput struct {
	bytes.Buffer
	syncs int
}

func (o *fakeOutput) Sync() error { o.syncs++; return nil }
func (*fakeOutput) Close() error  { return nil }

func TestHarnessDoesNotInheritNestedSession(t *testing.T) {
	env := withoutEnv([]string{"VEV=session=old", "PATH=/bin", "VEV_PERF_TRACE=old"}, "VEV")
	if !equalStrings(env, []string{"PATH=/bin", "VEV_PERF_TRACE=old"}) {
		t.Fatalf("environment=%q", env)
	}
}

func TestCLIProcessOwnsPTYInjectionAndSuccessfulFlushBoundary(t *testing.T) {
	pty, output := &fakePTY{}, &fakeOutput{}
	p := &cliProcess{pty: pty, output: output, chunks: make(chan []byte, 1)}
	input := workloadInput(scenario{ID: "s"}, 1, "measured-1")
	p.chunks <- append([]byte("terminal "), inputMarker(input)...)
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
			got := roleArgs(scenario{ID: "s"}, processMapping{Role: tc.role, Run: 1})
			if !equalStrings(got, tc.want) {
				t.Fatalf("args=%q want %q", got, tc.want)
			}
		})
	}
	input := string(workloadInput(scenario{ID: "s", Workload: "interactive_flood"}, 1, "measured-1"))
	if string(inputMarker([]byte(input))) != "__VEV_HARNESS_s_r1_measured-1__" || !bytes.Contains([]byte(input), []byte("while [ $i -lt 128 ]")) || !strings.HasSuffix(input, "printf '__VEV_HARNESS_s_r1_measured-1__\\n'\n") {
		t.Fatalf("workload is not real PTY shell input with an observable marker: %q", input)
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

func (l *failingLauncher) Launch(m processMapping, args []string) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, args)
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

func (l *traceLauncher) Launch(m processMapping, args []string) (launchedProcess, error) {
	p, err := l.fakeLauncher.Launch(m, args)
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
	if got.EndToEnd.Count != 10 || got.Repetitions != 10 || len(got.Spans) == 0 {
		t.Fatalf("summary=%+v", got)
	}
	for _, s := range got.Spans {
		if s.Distribution.Count < 10 {
			t.Fatalf("insufficient span summary: %+v", s)
		}
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
	for _, records := range [][]traceRecord{{base("diff_end", 1)}, {base("diff_start", 2), base("diff_end", 1)}, {base("diff_start", 1)}, {func() traceRecord { r := base("diff_start", 1); r.ProcessID = "other"; return r }()}} {
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
