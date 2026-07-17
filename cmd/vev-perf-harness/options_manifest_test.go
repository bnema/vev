//go:build linux

package main

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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

func TestValidateManifestRejectsEmptyAndDuplicateMatrixIDs(t *testing.T) {
	canonical, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*manifest)
		want   string
	}{
		{"empty topology", func(m *manifest) { m.Topologies[0].ID = "" }, "empty topology id"},
		{"duplicate topology", func(m *manifest) { m.Topologies[1].ID = m.Topologies[0].ID }, "duplicate topology id"},
		{"empty workload", func(m *manifest) { m.Workloads[0] = "" }, "empty workload id"},
		{"duplicate workload", func(m *manifest) { m.Workloads[1] = m.Workloads[0] }, "duplicate workload id"},
		{"empty transport", func(m *manifest) { m.Transports[0].ID = "" }, "empty transport id"},
		{"duplicate transport", func(m *manifest) { m.Transports[1].ID = m.Transports[0].ID }, "duplicate transport id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := canonical
			m.Topologies = append([]topology(nil), canonical.Topologies...)
			m.Workloads = append([]string(nil), canonical.Workloads...)
			m.Transports = append([]transport(nil), canonical.Transports...)
			tc.mutate(&m)
			if err := validateManifest(m); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateManifest() error = %v, want %q", err, tc.want)
			}
		})
	}
}

func TestValidateManifestRejectsFullyCoveredUnknownMatrixIDs(t *testing.T) {
	canonical, err := readManifest(filepath.Join("..", "..", "testdata", "perf", "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		mutate func(*manifest)
		want   string
	}{
		{
			name: "topology",
			mutate: func(m *manifest) {
				m.Topologies = append(m.Topologies, topology{ID: "9x9", Geometry: "120x40", RowsPerPane: 10000})
				for _, s := range canonical.Scenarios {
					s.ID = "9x9-" + s.ID
					s.Topology = "9x9"
					m.Scenarios = append(m.Scenarios, s)
				}
			},
			want: "unknown topology id \"9x9\"",
		},
		{
			name: "workload",
			mutate: func(m *manifest) {
				m.Workloads = append(m.Workloads, "unknown_workload")
				for _, s := range canonical.Scenarios {
					s.ID = "unknown-workload-" + s.ID
					s.Workload = "unknown_workload"
					m.Scenarios = append(m.Scenarios, s)
				}
			},
			want: "unknown workload id \"unknown_workload\"",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := canonical
			m.Topologies = append([]topology(nil), canonical.Topologies...)
			m.Workloads = append([]string(nil), canonical.Workloads...)
			m.Scenarios = append([]scenario(nil), canonical.Scenarios...)
			tc.mutate(&m)
			if err := validateManifest(m); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("validateManifest() error = %v, want %q", err, tc.want)
			}
		})
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

func TestHarnessCreatesExclusiveTraceManifestAndEvidence(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw-harness.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	closeTestFile(t, raw)
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
