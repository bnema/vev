//go:build linux

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/pkg/safedir"
)

func TestHarnessCanonicalLocalRolesAreIsolatedAcrossRepetitions(t *testing.T) {
	if testing.Short() {
		t.Skip("public CLI repetition lifecycle")
	}
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(t.TempDir(), "vev")
	build := exec.Command("go", "build", "-o", bin, "./")
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
	removeTestTree(t, out)
	raw, err := os.OpenFile(filepath.Join(out, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	closeTestFile(t, raw)
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
	removeTestTree(t, workspace)
	bin := filepath.Join(workspace, "vev")
	build := exec.Command("go", "build", "-o", bin, "./")
	build.Dir = root
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build public CLI: %v\n%s", err, output)
	}
	manifestPath := filepath.Join(workspace, "testdata", "perf", "manifest.json")
	if err := safedir.EnsurePrivate(filepath.Dir(manifestPath)); err != nil {
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

func TestHarnessRejectsExistingRunDirectoryBeforeLaunchingRoles(t *testing.T) {
	out := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(out, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	closeTestFile(t, raw)
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

func TestHarnessDoesNotInheritNestedSession(t *testing.T) {
	env := withoutEnv([]string{"VEV=session=old", "PATH=/bin", "VEV_PERF_TRACE=old"}, "VEV")
	if !equalStrings(env, []string{"PATH=/bin", "VEV_PERF_TRACE=old"}) {
		t.Fatalf("environment=%q", env)
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
			if !equalStrings(client.Args, []string{"attach", "harness@127.0.0.1"}) {
				t.Fatalf("client did not use ephemeral public remote attach: %q", client.Args)
			}
			if tc.peer == "ssh_stdio_peer" && !equalStrings(peer.Args, []string{"_stdio"}) {
				t.Fatalf("ssh peer command=%q", peer.Args)
			}
			if tc.peer == "udp_peer" && !equalStrings(peer.Args, []string{"_udp-proxy"}) {
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
			closeTestFile(t, raw)
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

func TestWorkloadInputShapes(t *testing.T) {
	tests := []struct {
		workload string
		contains []string
	}{
		{"active_output", []string{`printf '\033[10;20H1'`}},
		{"all_output", []string{"120", "vev perf line"}},
		{"inactive_output", []string{
			`printf '\033[38;2;20;120;220mred'`,
			`printf '\033[38;2;220;80;40mgreen'`,
			`printf '\033[38;2;80;220;120ms'`,
		}},
		{"interactive_flood", []string{"printf 'vev perf output\\n'"}},
		{"snapshot_output_resize", []string{`\033[2J`, "vev perf snapshot-output-resize"}},
		{"attach_restore_tab_switch", []string{`"$VEV_PERF_BIN" cmd next-tab`, `"$VEV_PERF_BIN" cmd previous-tab`}},
	}
	for _, tt := range tests {
		t.Run(tt.workload, func(t *testing.T) {
			got := string(workloadInput(scenario{ID: "s", Workload: tt.workload}, 1, "measured-1"))
			for _, want := range tt.contains {
				if !strings.Contains(got, want) {
					t.Errorf("workload input %q does not contain %q", got, want)
				}
			}
			if !strings.Contains(got, "__VEV_HARNESS_s_r1_measured-1__") {
				t.Errorf("workload input has no terminal marker: %q", got)
			}
		})
	}

	activeWarmup := string(workloadInput(scenario{ID: "s", Workload: "active_output"}, 1, "warmup"))
	activeOne := string(workloadInput(scenario{ID: "s", Workload: "active_output"}, 1, "measured-1"))
	activeTwo := string(workloadInput(scenario{ID: "s", Workload: "active_output"}, 1, "measured-2"))
	for phase, got := range map[string]string{"warmup": activeWarmup, "measured-1": activeOne, "measured-2": activeTwo} {
		marker := fmt.Sprintf("__VEV_HARNESS_s_r1_%s__", phase)
		wantByte := map[string]byte{"warmup": 'X', "measured-1": '1', "measured-2": '0'}[phase]
		want := fmt.Sprintf("printf '\\033[10;20H%c'; : 's'; printf '%s\\n'\n", wantByte, marker)
		if got != want {
			t.Errorf("active %s input = %q, want exactly one changing cell write %q", phase, got, want)
		}
	}
	if activeWarmup == activeOne || activeOne == activeTwo {
		t.Fatalf("active one-cell payload did not change by sequence: %q %q %q", activeWarmup, activeOne, activeTwo)
	}
	inactive := string(workloadInput(scenario{ID: "s", Workload: "inactive_output"}, 1, "measured-1"))
	if count := strings.Count(inactive, `\033[38;2;`); count != 3 {
		t.Fatalf("inactive output truecolor runs = %d, want 3: %q", count, inactive)
	}

	interactive := string(workloadInput(scenario{ID: "s", Workload: "interactive_flood"}, 1, "measured-1"))
	if count := strings.Count(interactive, `printf 'vev perf output\n'`); count != 128 {
		t.Fatalf("interactive flood printf commands = %d, want 128: %q", count, interactive)
	}
	if strings.Contains(interactive, "i=0") || strings.Contains(interactive, "while [") {
		t.Fatalf("interactive flood uses shell-specific loop syntax: %q", interactive)
	}

	warmup := string(workloadInput(scenario{ID: "s", Workload: "attach_restore_tab_switch"}, 1, "warmup"))
	if count := strings.Count(warmup, `"$VEV_PERF_BIN" cmd new-tab`); count != 1 {
		t.Fatalf("attach warmup new-tab commands = %d, want 1: %q", count, warmup)
	}
	wantWarmup := `"$VEV_PERF_BIN" cmd new-tab && "$VEV_PERF_BIN" cmd previous-tab && printf 'vev perf attach-restore-tab-switch s\n' && printf '__VEV_HARNESS_s_r1_warmup__\n'` + "\n"
	if warmup != wantWarmup {
		t.Fatalf("attach warmup success marker is not gated by every command:\n got %q\nwant %q", warmup, wantWarmup)
	}
	measured := string(workloadInput(scenario{ID: "s", Workload: "attach_restore_tab_switch"}, 1, "measured-1"))
	wantMeasured := `"$VEV_PERF_BIN" cmd next-tab && "$VEV_PERF_BIN" cmd previous-tab && printf 'vev perf attach-restore-tab-switch s\n' && printf '__VEV_HARNESS_s_r1_measured-1__\n'` + "\n"
	if measured != wantMeasured {
		t.Fatalf("attach measured success marker is not gated by every command:\n got %q\nwant %q", measured, wantMeasured)
	}
}

func TestWorkloadInputResizeCommandsAreComplete(t *testing.T) {
	for _, tt := range []struct {
		workload string
		want     string
	}{
		{"resize_sweep", `printf 'vev perf resize-sweep s\n'; printf '__VEV_HARNESS_s_r1_measured-1__\n'` + "\n"},
		{"snapshot_output_resize", `printf '\033[2J\033[Hvev perf snapshot-output-resize s\n'; printf '__VEV_HARNESS_s_r1_measured-1__\n'` + "\n"},
	} {
		t.Run(tt.workload, func(t *testing.T) {
			got := string(workloadInput(scenario{ID: "s", Workload: tt.workload}, 1, "measured-1"))
			if got != tt.want {
				t.Fatalf("workload input = %q, want complete command %q", got, tt.want)
			}
			if strings.Contains(got, "%!") {
				t.Fatalf("workload input contains fmt diagnostic: %q", got)
			}
		})
	}
}

func TestWorkloadResize(t *testing.T) {
	for _, tt := range []struct {
		workload string
		sequence uint64
		wantCols uint16
		wantOK   bool
	}{
		{"active_output", 1, 0, false},
		{"resize_sweep", 1, 119, true},
		{"resize_sweep", 2, 120, true},
		{"snapshot_output_resize", 1, 119, true},
	} {
		t.Run(fmt.Sprintf("%s-%d", tt.workload, tt.sequence), func(t *testing.T) {
			cols, rows, ok := workloadResize(scenario{Workload: tt.workload}, tt.sequence)
			if ok != tt.wantOK || cols != tt.wantCols {
				t.Fatalf("workloadResize() = (%d, %d, %t), want cols=%d ok=%t", cols, rows, ok, tt.wantCols, tt.wantOK)
			}
			if ok && rows != 40 {
				t.Fatalf("workloadResize() rows = %d, want 40", rows)
			}
		})
	}
}

type resizeOrderingWriter struct {
	bytes.Buffer
	order *[]string
}

func (w *resizeOrderingWriter) Write(p []byte) (int, error) {
	if bytes.Contains(p, []byte(`"sequence":2`)) {
		switch {
		case bytes.Contains(p, []byte(`"kind":"input_injected"`)):
			*w.order = append(*w.order, "injected")
		case bytes.Contains(p, []byte(`"kind":"terminal_flushed"`)):
			*w.order = append(*w.order, "terminal_flushed")
		}
	}
	return w.Buffer.Write(p)
}

type resizeOrderingProcess struct {
	order    *[]string
	inputs   [][]byte
	resizes  int
	measures int
}

func (*resizeOrderingProcess) Warmup([]byte) error { return nil }

func (p *resizeOrderingProcess) Resize(cols, rows uint16) error {
	p.resizes++
	if p.resizes == 1 {
		*p.order = append(*p.order, fmt.Sprintf("resize:%dx%d", cols, rows))
	}
	return nil
}

func (p *resizeOrderingProcess) Measure(input []byte, injected, flushed func() error) error {
	p.measures++
	p.inputs = append(p.inputs, append([]byte(nil), input...))
	if p.measures == 1 {
		*p.order = append(*p.order, "workload_write")
	}
	if err := injected(); err != nil {
		return err
	}
	if p.measures == 1 {
		*p.order = append(*p.order, "workload_flush")
	}
	return flushed()
}

func (*resizeOrderingProcess) Close() error { return nil }

type resizeOrderingLauncher struct{ client *resizeOrderingProcess }

func (l resizeOrderingLauncher) Launch(m processMapping, _ roleCommand) (launchedProcess, error) {
	if m.Role == "client" {
		return l.client, nil
	}
	return &fakeProcess{}, nil
}

func TestResizeBoundaryPrecedesWorkloadWriteAndMarker(t *testing.T) {
	dir := t.TempDir()
	order := []string{}
	client := &resizeOrderingProcess{order: &order}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, resizeOrderingLauncher{client: client}
	raw := &resizeOrderingWriter{order: &order}
	_, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration}, manifest{}, scenario{ID: "resize", Workload: "resize_sweep", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	wantOrder := []string{"injected", "resize:120x40", "workload_write", "workload_flush", "terminal_flushed"}
	if len(order) < len(wantOrder) {
		t.Fatalf("resize event order = %q, want prefix %q", order, wantOrder)
	}
	if !equalStrings(order[:len(wantOrder)], wantOrder) {
		t.Fatalf("first resize event order = %q, want %q", order[:len(wantOrder)], wantOrder)
	}
	first := string(client.inputs[0])
	for _, want := range []string{`printf 'vev perf resize-sweep resize\n'`, `printf '__VEV_HARNESS_resize_r1_measured-2__\n'`} {
		if !strings.Contains(first, want) {
			t.Errorf("resize input has no observable workload or marker %q: %q", want, first)
		}
	}
	if strings.Contains(first, "while [") || strings.Contains(first, "%!") {
		t.Fatalf("resize input contains unsupported loop or fmt diagnostic: %q", first)
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

func TestHarnessRawRecordsContainEveryMarkWhileSamplesRemainFiltered(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, &fakeLauncher{}
	result, err := h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration}, manifest{}, scenario{ID: "raw-complete", Roles: []string{"daemon", "client"}}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	marks := readHarnessMarks(t, filepath.Join(dir, "raw.jsonl"))
	if got, want := len(marks), 2*(result.Samples+1); got != want {
		t.Fatalf("raw marks=%d, want warmup plus every measured boundary=%d", got, want)
	}
	for i, mark := range marks[:2] {
		if mark.Sequence != 1 || !mark.Valid || []string{"input_injected", "terminal_flushed"}[i] != mark.Kind {
			t.Fatalf("warmup mark %d=%+v", i, mark)
		}
	}
	for sequence := uint64(2); sequence <= uint64(result.Samples+1); sequence++ {
		if marks[2*(sequence-1)].Sequence != sequence || marks[2*(sequence-1)+1].Sequence != sequence {
			t.Fatalf("raw marks did not retain both boundaries for sequence %d: %+v", sequence, marks)
		}
	}
}

func TestHarnessPersistsFailedFlushDiagnostics(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	failure := errors.New("flush failed")
	h := defaultHarness()
	h.clock, h.launcher = &fakeClock{}, failedFlushLauncher{err: failure}
	_, err = h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration}, manifest{}, scenario{ID: "failed-flush", Roles: []string{"daemon", "client"}}, 1, raw)
	if !errors.Is(err, failure) {
		t.Fatalf("run error=%v, want failed flush", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	marks := readHarnessMarks(t, filepath.Join(dir, "raw.jsonl"))
	if got, want := len(marks), 4; got != want {
		t.Fatalf("raw marks=%d, want warmup and failed measured pair=%d: %+v", got, want, marks)
	}
	failed := marks[len(marks)-1]
	if failed.Sequence != 2 || failed.Kind != "terminal_flushed" || failed.Valid {
		t.Fatalf("failed flush diagnostic=%+v", failed)
	}
}

func TestHarnessSchedulesMeasuredEventsAcrossFullInterval(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	closeTestFile(t, raw)
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
	if want := result.Samples*2 + 2; len(records) != want {
		t.Fatalf("raw records=%d, want %d warmup and measured boundaries", len(records), want)
	}
}

func TestHarnessPreservesWorkloadAndCleanupErrors(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	closeTestFile(t, raw)
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
	closeTestFile(t, raw)
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
