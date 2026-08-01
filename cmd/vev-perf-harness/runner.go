//go:build linux

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/bnema/vev/pkg/safedir"
)

type harnessMark struct {
	Scenario string `json:"scenario"`
	Run      int    `json:"run"`
	Sequence uint64 `json:"sequence"`
	Kind     string `json:"kind"`
	Tick     int64  `json:"tick"`
	Valid    bool   `json:"valid"`
}

func (c systemClock) Now() int64 { return time.Since(c.start).Nanoseconds() }

func (systemClock) Sleep(d time.Duration) { time.Sleep(d) }

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

// createExclusiveRunDir prevents stale runtime, socket, state, or trace files
// from an earlier invocation being mistaken for the next repetition's evidence.
func createExclusiveRunDir(path string) error {
	if err := os.Mkdir(path, 0o700); err != nil {
		return err
	}
	if err := safedir.EnsurePrivate(path); err != nil {
		return errors.Join(err, os.Remove(path))
	}
	return nil
}

// recordedGitSHA prefers build provenance so an installed public harness does
// not depend on its launch directory being a checkout. The git fallback keeps
// go run evidence attributable as well.
const gitSHACommandTimeout = time.Second

func recordedGitSHA() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			if setting.Key == "vcs.revision" && setting.Value != "" {
				return setting.Value
			}
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), gitSHACommandTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "git", "rev-parse", "HEAD").Output()
	if err == nil && strings.TrimSpace(string(out)) != "" {
		return strings.TrimSpace(string(out))
	}
	return "unknown"
}

func run(args []string, h *harness) (err error) {
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
	defer func() {
		if closeErr := raw.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()
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
			if cleanupErr := h.removeAll(dir); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("remove failed run directory: %w", cleanupErr))
			}
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
	marks := []harnessMark{}
	recordMark := func(mark harnessMark) error {
		marks = append(marks, mark)
		if err := json.NewEncoder(raw).Encode(mark); err != nil {
			return fmt.Errorf("write raw harness mark: %w", err)
		}
		return nil
	}
	seq := uint64(0)
	for _, p := range processes {
		if p.mapping.Role != "client" {
			continue
		}
		seq++
		sequence := seq
		if err = recordMark(harnessMark{Scenario: s.ID, Run: run, Sequence: sequence, Kind: "input_injected", Tick: h.clock.Now(), Valid: true}); err != nil {
			return res, err
		}
		warmupErr := p.process.Warmup(workloadInput(s, run, "warmup"))
		if markErr := recordMark(harnessMark{Scenario: s.ID, Run: run, Sequence: sequence, Kind: "terminal_flushed", Tick: h.clock.Now(), Valid: warmupErr == nil}); markErr != nil {
			return res, errors.Join(warmupErr, markErr)
		}
		if warmupErr != nil {
			return res, warmupErr
		}
	}
	// The harness clock, rather than a process clock, owns the explicit warmup
	// and measured intervals. Raw evidence retains every boundary, while only
	// complete valid pairs within [Start, End) contribute measurement samples.
	h.clock.Sleep(o.warmup)
	interval := measuredInterval{Start: h.clock.Now()}
	interval.End = interval.Start + o.duration.Nanoseconds()
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
			injectedRecorded := false
			flushedRecorded := false
			injected := func() error {
				if markErr := recordMark(harnessMark{Scenario: s.ID, Run: run, Sequence: sequence, Kind: "input_injected", Tick: h.clock.Now(), Valid: true}); markErr != nil {
					return markErr
				}
				injectedRecorded = true
				return nil
			}
			flushed := func() error {
				if markErr := recordMark(harnessMark{Scenario: s.ID, Run: run, Sequence: sequence, Kind: "terminal_flushed", Tick: h.clock.Now(), Valid: true}); markErr != nil {
					return markErr
				}
				flushedRecorded = true
				return nil
			}
			if cols, rows, resize := workloadResize(s, sequence); resize {
				resizer, ok := p.process.(processResizer)
				if !ok {
					return res, errors.New("client process does not support resize workload")
				}
				if resizeErr := resizer.Resize(cols, rows); resizeErr != nil {
					return res, fmt.Errorf("resize client PTY: %w", resizeErr)
				}
			}
			if measureErr := p.process.Measure(workloadInput(s, run, fmt.Sprintf("measured-%d", sequence)), injected, flushed); measureErr != nil {
				if injectedRecorded && !flushedRecorded {
					diagnosticErr := recordMark(harnessMark{Scenario: s.ID, Run: run, Sequence: sequence, Kind: "terminal_flushed", Tick: h.clock.Now(), Valid: false})
					return res, errors.Join(measureErr, diagnosticErr)
				}
				return res, measureErr
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

type processResizer interface {
	Resize(cols, rows uint16) error
}

func workloadResize(s scenario, sequence uint64) (uint16, uint16, bool) {
	if s.Workload != "resize_sweep" && s.Workload != "snapshot_output_resize" {
		return 0, 0, false
	}
	if sequence%2 == 1 {
		return 119, 40, true
	}
	return 120, 40, true
}

// workloadInput is shell input delivered through the client PTY. It avoids any
// private daemon API and makes every topology/workload/transport manifest row
// execute a real terminal workload. The marker is the output boundary awaited
// by cliProcess; it is not a synthetic timestamp.
func workloadInput(s scenario, run int, phase string) []byte {
	marker := fmt.Sprintf("__VEV_HARNESS_%s_r%d_%s__", safeName(s.ID), run, safeName(phase))
	body := "printf 'vev perf %s\\n'"
	switch s.Workload {
	case "active_output":
		body = `printf '\033[10;20HX'; : '%s'`
	case "all_output":
		body = `printf '%%-120s' 'vev perf line %s'`
	case "inactive_output":
		body = `printf '\033[38;2;20;120;220mred'; printf '\033[38;2;220;80;40mgreen'; printf '\033[38;2;80;220;120m%s'; printf '\033[0m'`
	case "interactive_flood":
		body = "i=0; while [ $i -lt 128 ]; do printf 'vev perf output %s\\n'; i=$((i+1)); done"
	case "resize_sweep":
		body = "printf 'vev perf resize-sweep %s\\n'"
	case "copy_search":
		body = "printf 'vev perf copy-search %s\\n'"
	case "snapshot_output_resize":
		body = `printf '\033[2J\033[Hvev perf snapshot-output-resize %s\n'`
	case "attach_restore_tab_switch":
		body = `"$VEV_PERF_BIN" cmd next-tab; "$VEV_PERF_BIN" cmd previous-tab; printf 'vev perf attach-restore-tab-switch %s\n'`
		if phase == "warmup" {
			body = `"$VEV_PERF_BIN" cmd new-tab; "$VEV_PERF_BIN" cmd previous-tab; printf 'vev perf attach-restore-tab-switch %s\n'`
		}
	}
	return []byte(fmt.Sprintf(body+"; printf '%s\\n'\n", s.ID, marker))
}
