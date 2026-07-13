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
