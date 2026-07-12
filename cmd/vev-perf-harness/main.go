// vev-perf-harness is a public-CLI performance evidence collector.  It never
// imports daemon packages: process traces are correlated by identifiers only.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bnema/vev/pkg/safedir"
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
	Warmup(time.Duration) error
	Measure(time.Duration, func()) error
	Close() error
}
type launcher interface {
	Launch(processMapping, []string) (launchedProcess, error)
}
type cliLauncher struct{ bin string }
type cliProcess struct{ cmd *exec.Cmd }

// The default launcher intentionally invokes only the documented public binary.
// Scenario drivers are passed as arguments from a manifest only after a future
// public driver contract exists; this measurement slice never reaches internals.
func (l cliLauncher) Launch(_ processMapping, _ []string) (launchedProcess, error) {
	if _, err := os.Stat(l.bin); err != nil {
		return nil, err
	}
	return cliProcess{}, nil
}
func (cliProcess) Warmup(time.Duration) error                  { return nil }
func (cliProcess) Measure(_ time.Duration, flush func()) error { flush(); return nil }
func (cliProcess) Close() error                                { return nil }

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
		trans[t.ID] = true
	}
	for _, v := range []string{"local", "ssh_stdio", "udp_baseline", "udp_25ms", "udp_100ms", "udp_loss_1pct"} {
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
func set(v []string) map[string]bool {
	r := map[string]bool{}
	for _, x := range v {
		r[x] = true
	}
	return r
}
func (h *harness) runOne(o options, _ manifest, s scenario, run int, raw io.Writer) (res runResult, err error) {
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
		h.launcher = cliLauncher{o.vevBin}
	}
	processes := make([]launchedProcess, 0, len(maps))
	defer func() {
		for _, p := range processes {
			if e := p.Close(); err == nil && e != nil {
				err = e
			}
		}
	}()
	for _, pm := range maps {
		p, e := h.launcher.Launch(pm, nil)
		if e != nil {
			return res, e
		}
		processes = append(processes, p)
	}
	for _, p := range processes {
		if err = p.Warmup(o.warmup); err != nil {
			return res, err
		}
	}
	marks := []harnessMark{}
	seq := uint64(0)
	flush := func() {
		seq++
		start := h.clock.Now()
		marks = append(marks, harnessMark{s.ID, run, seq, "input_injected", start, true})
		end := h.clock.Now()
		marks = append(marks, harnessMark{s.ID, run, seq, "terminal_flushed", end, true})
	}
	for _, p := range processes {
		if err = p.Measure(o.duration, flush); err != nil {
			return res, err
		}
	}
	samples, e := pairedSamples(marks)
	if e != nil {
		return res, e
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
			if r.ProcessID != m.ProcessID {
				_ = f.Close()
				return nil, errors.New("trace record process_id does not match manifest")
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
		lastSequence := uint64(0)
		starts := map[string]traceRecord{}
		for _, r := range records {
			if r.Sequence < lastSequence {
				return nil, errors.New("nonmonotonic same-process sequence")
			}
			lastSequence = r.Sequence
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
