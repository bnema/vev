package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strings"
)

func measuredEventSamples(marks []harnessMark, interval measuredInterval) ([]eventSample, error) {
	if interval.End < interval.Start {
		return nil, errors.New("negative measured interval")
	}
	starts := map[uint64]int64{}
	events := make([]eventSample, 0, len(marks)/2)
	for _, m := range marks {
		if !m.Valid {
			return nil, errors.New("invalid harness boundary")
		}
		switch m.Kind {
		case "input_injected":
			if _, exists := starts[m.Sequence]; exists {
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
			delete(starts, m.Sequence)
			if start >= interval.Start && m.Tick <= interval.End {
				events = append(events, eventSample{Sequence: m.Sequence, Injected: start, Flushed: m.Tick, Latency: m.Tick - start})
			}
		default:
			return nil, errors.New("unknown harness mark")
		}
	}
	if len(starts) != 0 {
		return nil, errors.New("missing terminal flush pair")
	}
	sort.Slice(events, func(i, j int) bool {
		if events[i].Injected == events[j].Injected {
			return events[i].Sequence < events[j].Sequence
		}
		return events[i].Injected < events[j].Injected
	})
	return events, nil
}

func requireMinimumEventSamples(events []eventSample) error {
	if len(events) < minimumInIntervalEventSamples {
		return fmt.Errorf("insufficient in-interval event samples: got %d, need at least %d", len(events), minimumInIntervalEventSamples)
	}
	return nil
}

func eventLatencies(events []eventSample) []int64 {
	out := make([]int64, len(events))
	for i, event := range events {
		out[i] = event.Latency
	}
	return out
}

func eventCadenceSamples(events []eventSample) ([]int64, int64) {
	if len(events) < 2 {
		return nil, 0
	}
	gaps := make([]int64, 0, len(events)-1)
	var maxGap int64
	for i := 1; i < len(events); i++ {
		gap := events[i].Injected - events[i-1].Injected
		gaps = append(gaps, gap)
		if gap > maxGap {
			maxGap = gap
		}
	}
	return gaps, maxGap
}

func eventCadence(events []eventSample) (distribution, int64) {
	gaps, maxGap := eventCadenceSamples(events)
	if len(gaps) == 0 {
		return distribution{}, maxGap
	}
	return percentiles(gaps), maxGap
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
	Valid     bool   `json:"valid"`
}
type spanPair struct{ start, end, name string }

var spanPairs = []spanPair{{"capture_start", "capture_end", "capture_duration"}, {"compose_start", "compose_end", "compose_duration"}, {"diff_start", "diff_end", "diff_duration"}, {"queue_enqueued", "queue_dequeued", "queue_wait"}, {"ack_blocked_start", "ack_blocked_end", "ack_blocked_interval"}, {"emit_start", "emit_end", "emit_duration"}, {"adapter_send_start", "adapter_send_end", "adapter_send_duration"}, {"adapter_receive_start", "adapter_receive_end", "adapter_receive_duration"}}

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
			return nil, fmt.Errorf("open manifest trace for %s role: %w", m.Role, err)
		}
		var records []traceRecord
		scan := bufio.NewScanner(f)
		for scan.Scan() {
			var r traceRecord
			if err := json.Unmarshal(scan.Bytes(), &r); err != nil {
				_ = f.Close()
				return nil, err
			}
			if r.ProcessID != m.ProcessID || r.Scenario != m.Scenario || r.Run != uint64(m.Run) {
				_ = f.Close()
				return nil, errors.New("trace record identity does not match manifest")
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
		// A process shares one observer across concurrent components. Correlation
		// IDs are allocated before a goroutine reaches its first mark, so a later
		// ID can be serialized before an earlier one. Their first appearance is
		// consequently not an ordering contract. Only marks in the same component
		// and exact correlation domain may pair, and each pair's ticks must retain
		// its own start-before-end ordering.
		starts := map[string]traceRecord{}
		for _, r := range records {
			for _, pair := range spanPairs {
				key := fmt.Sprintf("%s/%s/%s/%d/%d/%d/%d", r.ProcessID, r.Component, m.Scenario, m.Run, r.Sequence, r.RequestID, r.Epoch)
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
					// Validity affects only measurement eligibility, never structural
					// pairing: every start/end must still match in its exact process,
					// component, and correlation domain. A failed start, failed end, or
					// both is a paired diagnostic fact but contributes no latency sample.
					if start.Valid && r.Valid {
						out = append(out, span{Component: r.Component, Name: pair.name, Samples: []int64{r.Tick - start.Tick}})
					}
					delete(starts, k)
				}
			}
		}
		if len(starts) != 0 {
			keys := make([]string, 0, len(starts))
			for key := range starts {
				keys = append(keys, key)
			}
			sort.Strings(keys)
			return nil, fmt.Errorf("missing process-local span pair for %s: %s", m.ProcessID, strings.Join(keys, ", "))
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
