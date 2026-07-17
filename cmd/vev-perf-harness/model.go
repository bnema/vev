//go:build linux

package main

import "time"

const (
	minimumDuration               = 30 * time.Second
	minimumRepetitions            = 10
	minimumInIntervalEventSamples = 10
	measuredEventCadence          = time.Second
)

type options struct {
	vevBin, manifest, out string
	scenario              string
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

// measuredInterval belongs solely to the harness clock domain. A paired event
// is eligible only when both owned boundaries fall inside this interval.
type measuredInterval struct {
	Start int64
	End   int64
}

type eventSample struct {
	Sequence uint64 `json:"sequence"`
	Injected int64  `json:"injected_tick"`
	Flushed  int64  `json:"flushed_tick"`
	Latency  int64  `json:"latency_nanos"`
}

type span struct {
	Component, Name string
	Samples         []int64
}

type runResult struct {
	Spans          []span           `json:"process_local_spans"`
	Scenario       string           `json:"scenario"`
	Run            int              `json:"run"`
	Samples        int              `json:"samples"`
	EndToEnd       []int64          `json:"end_to_end_nanos"`
	Event          distribution     `json:"event_end_to_end"`
	Cadence        distribution     `json:"event_cadence_nanos"`
	CadenceSamples []int64          `json:"-"`
	MaxGap         int64            `json:"event_max_gap_nanos"`
	Processes      []processMapping `json:"processes"`
}

type summary struct {
	Schema           uint16        `json:"schema"`
	GitSHA           string        `json:"git_sha"`
	Warmup           string        `json:"warmup"`
	Duration         string        `json:"duration"`
	Repetitions      int           `json:"repetitions"`
	EndToEnd         distribution  `json:"harness_end_to_end"`
	Cadence          distribution  `json:"harness_event_cadence_nanos"`
	MaxGap           int64         `json:"harness_event_max_gap_nanos"`
	RunP50Dispersion distribution  `json:"run_p50_dispersion"`
	Spans            []spanSummary `json:"process_local_spans"`
	Runs             int           `json:"runs"`
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

var spanPairs = []spanPair{{"capture_start", "capture_end", "capture_duration"}, {"compose_start", "compose_end", "compose_duration"}, {"diff_start", "diff_end", "diff_duration"}, {"queue_enqueued", "queue_dequeued", "queue_wait"}, {"ack_blocked_start", "ack_blocked_end", "ack_blocked_interval"}, {"emit_start", "emit_end", "emit_duration"}, {"adapter_send_start", "adapter_send_end", "adapter_send_duration"}, {"adapter_receive_start", "adapter_receive_end", "adapter_receive_duration"}}
