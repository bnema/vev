//go:build linux

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
		{Sequence: 5, Kind: "input_injected", Tick: 180, Valid: true},
		{Sequence: 5, Kind: "terminal_flushed", Tick: 200, Valid: true}, // end is excluded
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
	t.Cleanup(func() {
		if err := raw.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	})
	clock := &fakeClock{}
	h := defaultHarness()
	h.clock, h.launcher = clock, insufficientEventLauncher{clock: clock}
	_, err = h.runOne(options{out: dir, warmup: time.Second, duration: minimumDuration}, manifest{}, scenario{ID: "late", Roles: []string{"daemon", "client"}}, 1, raw)
	if err == nil || !strings.Contains(err.Error(), "insufficient in-interval event samples") {
		t.Fatalf("straddling run error=%v, want insufficient in-interval sample rejection", err)
	}
	if err := raw.Close(); err != nil {
		t.Fatal(err)
	}
	marks := readHarnessMarks(t, filepath.Join(dir, "raw.jsonl"))
	if got, want := len(marks), 4; got != want {
		t.Fatalf("raw marks=%d, want warmup and straddling pair=%d: %+v", got, want, marks)
	}
	if straddling := marks[len(marks)-1]; straddling.Sequence != 2 || straddling.Kind != "terminal_flushed" || !straddling.Valid {
		t.Fatalf("straddling boundary=%+v", straddling)
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

func TestHarnessClosesRolesBeforeMergingReceiveSpans(t *testing.T) {
	dir := t.TempDir()
	raw, err := os.OpenFile(filepath.Join(dir, "raw.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := raw.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
			t.Error(err)
		}
	})
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

func TestHarnessRejectsCrossProcessAndBadTraceSpans(t *testing.T) {
	write := func(t *testing.T, records ...traceRecord) processMapping {
		t.Helper()
		path := filepath.Join(t.TempDir(), "trace.jsonl")
		f, err := os.Create(path)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range records {
			b, err := json.Marshal(r)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(append(b, '\n')); err != nil {
				t.Fatal(err)
			}
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		return processMapping{ProcessID: "one", ClockDomain: "one", TracePath: path, Scenario: "s", Run: 1}
	}
	base := func(kind string, tick int64) traceRecord {
		return traceRecord{ProcessID: "one", Component: "daemon", Scenario: "s", Run: 1, Sequence: 1, RequestID: 1, Epoch: 1, Kind: kind, Tick: tick, Valid: true}
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

func TestHarnessExcludesFailedSpanDurationsWhileValidatingPairing(t *testing.T) {
	base := func(kind string, tick int64, valid bool) traceRecord {
		return traceRecord{ProcessID: "one", Component: "adapter", Scenario: "s", Run: 1, Sequence: 1, RequestID: 1, Epoch: 1, Kind: kind, Tick: tick, Valid: valid}
	}
	for _, tc := range []struct {
		name      string
		records   []traceRecord
		wantSpans int
		wantErr   bool
	}{
		{"successful adapter send is sampled", []traceRecord{base("adapter_send_start", 10, true), base("adapter_send_end", 20, true)}, 1, false},
		{"failed adapter send end is excluded", []traceRecord{base("adapter_send_start", 10, true), base("adapter_send_end", 20, false)}, 0, false},
		{"failed adapter receive end is excluded", []traceRecord{base("adapter_receive_start", 10, true), base("adapter_receive_end", 20, false)}, 0, false},
		{"failed adapter send start is excluded", []traceRecord{base("adapter_send_start", 10, false), base("adapter_send_end", 20, true)}, 0, false},
		{"failed adapter receive boundaries are excluded", []traceRecord{base("adapter_receive_start", 10, false), base("adapter_receive_end", 20, false)}, 0, false},
		{"failed end without start is rejected", []traceRecord{base("adapter_send_end", 20, false)}, 0, true},
		{"duplicate failed start is rejected", []traceRecord{base("adapter_send_start", 10, false), base("adapter_send_start", 15, false)}, 0, true},
		{"negative failed span is rejected", []traceRecord{base("adapter_receive_start", 20, false), base("adapter_receive_end", 10, false)}, 0, true},
		{"cross-component failed end is rejected", []traceRecord{base("adapter_send_start", 10, true), func() traceRecord { r := base("adapter_send_end", 20, false); r.Component = "other"; return r }()}, 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "trace.jsonl")
			var lines []string
			for _, record := range tc.records {
				lines = append(lines, mustJSON(record))
			}
			if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			spans, err := mergeProcessTraces([]processMapping{{ProcessID: "one", ClockDomain: "one", TracePath: path, Scenario: "s", Run: 1}})
			if tc.wantErr {
				if err == nil {
					t.Fatalf("invalid trace accepted: %+v", tc.records)
				}
				return
			}
			if err != nil || len(spans) != tc.wantSpans {
				t.Fatalf("spans=%+v err=%v, want %d spans", spans, err, tc.wantSpans)
			}
			if tc.wantSpans == 1 && spans[0].Samples[0] != 10 {
				t.Fatalf("successful span=%+v, want 10ns", spans[0])
			}
		})
	}
}
