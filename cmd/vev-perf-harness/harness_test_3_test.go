package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
