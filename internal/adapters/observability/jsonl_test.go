package observability

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

func TestTransportObservabilityJSONLOwnsTicksAndSchema(t *testing.T) {
	path := t.TempDir() + "/daemon.jsonl"
	observer, closer, err := NewJSONL(path, fixedRuntimeClock{now: time.Unix(0, 41)}, "daemon-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}

	observer.ObserveRuntime(validRuntimeMark(ports.RuntimeComposeStart))
	// A producer's clock domain must never enter a JSONL record. Rejection is
	// observable as no second record, rather than a silently rewritten mark.
	observer.ObserveRuntime(ports.RuntimeMark{Schema: 1, ProcessID: "daemon-process", Component: "daemon", Scenario: "resize", Run: 7, Sequence: 9, Kind: ports.RuntimeComposeEnd, Tick: 99, Valid: true})
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}

	records := readJSONLRecords(t, path)
	if len(records) != 1 {
		t.Fatalf("JSONL records = %d, want 1 (caller Tick must be rejected)", len(records))
	}
	wantKeys := map[string]bool{
		"schema": true, "process_id": true, "component": true, "scenario": true,
		"run": true, "sequence": true, "request_id": true, "epoch": true,
		"kind": true, "tick": true, "bytes": true, "fragments": true,
		"retransmits": true, "pending": true, "ack_rtt_nanos": true, "valid": true,
	}
	if len(records[0]) != len(wantKeys) {
		t.Fatalf("JSONL fields = %#v, want exactly canonical schema", records[0])
	}
	for key := range wantKeys {
		if _, ok := records[0][key]; !ok {
			t.Errorf("JSONL missing %q", key)
		}
	}
	if got := records[0]["tick"]; got != float64(41) {
		t.Errorf("JSONL tick = %v, want observer clock tick 41", got)
	}
	if got := records[0]["process_id"]; got != "daemon-process" {
		t.Errorf("JSONL process_id = %v, want observer process", got)
	}
}

func TestTransportObservabilityJSONLRejectsInvalidRequiredFields(t *testing.T) {
	for _, mark := range []ports.RuntimeMark{
		{},
		{Schema: 1, ProcessID: "p", Component: "daemon", Scenario: "s", Run: 1, Sequence: 1, Kind: ports.RuntimeEmitStart},
		{Schema: 2, ProcessID: "p", Component: "daemon", Scenario: "s", Run: 1, Sequence: 1, Kind: ports.RuntimeEmitStart, Valid: true},
	} {
		path := t.TempDir() + "/invalid.jsonl"
		observer, closer, err := NewJSONL(path, fixedRuntimeClock{now: time.Unix(0, 5)}, "p")
		if err != nil {
			t.Fatalf("NewJSONL() error = %v", err)
		}
		observer.ObserveRuntime(mark)
		if err := closer.Close(); err != nil {
			t.Fatalf("Close() error = %v", err)
		}
		if records := readJSONLRecords(t, path); len(records) != 0 {
			t.Errorf("invalid mark %#v wrote %#v", mark, records)
		}
	}
}

func TestTransportObservabilityJSONLWriteErrorSurfacesOnClose(t *testing.T) {
	// /dev/full accepts Open but rejects every write, exercising the concrete
	// observer lifecycle without changing the one-way RuntimeObserver port.
	observer, closer, err := NewJSONL("/dev/full", fixedRuntimeClock{now: time.Unix(0, 1)}, "p")
	if err != nil {
		t.Skipf("/dev/full unavailable: %v", err)
	}
	observer.ObserveRuntime(validRuntimeMark(ports.RuntimeEmitStart))
	if err := closer.Close(); err == nil {
		t.Fatal("Close() error = nil, want surfaced JSONL write error")
	}
}

func TestTransportObservabilitySameProcessSpanClockDomains(t *testing.T) {
	for _, tc := range []struct {
		name  string
		start ports.RuntimeMarkKind
		end   ports.RuntimeMarkKind
		field string
	}{
		{"diff", ports.RuntimeDiffStart, ports.RuntimeDiffEnd, "diff_duration"},
		{"queue", ports.RuntimeQueueEnqueued, ports.RuntimeQueueDequeued, "queue_wait"},
		{"ack", ports.RuntimeACKBlockedStart, ports.RuntimeACKBlockedEnd, "ack_blocked_interval"},
		{"send", ports.RuntimeAdapterSendStart, ports.RuntimeAdapterSendEnd, "adapter_send_duration"},
		{"receive", ports.RuntimeAdapterReceiveStart, ports.RuntimeAdapterReceiveEnd, "adapter_receive_duration"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start := validRuntimeMark(tc.start)
			start.Tick = 11
			end := start
			end.Kind, end.Tick = tc.end, 19
			if got, err := sameProcessDuration(start, end); err != nil || got != 8 {
				t.Fatalf("%s = %d, %v; want 8, nil", tc.field, got, err)
			}

			end.ProcessID = "other-process"
			if _, err := sameProcessDuration(start, end); err == nil {
				t.Fatalf("%s accepted cross-process ticks", tc.field)
			}
		})
	}
}

func TestTransportObservabilityJSONLSerializesConcurrentMarks(t *testing.T) {
	path := t.TempDir() + "/concurrent.jsonl"
	observer, closer, err := NewJSONL(path, fixedRuntimeClock{now: time.Unix(0, 73)}, "ipc-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	var wg sync.WaitGroup
	for i := uint64(0); i < 32; i++ {
		wg.Add(1)
		go func(sequence uint64) {
			defer wg.Done()
			mark := validRuntimeMark(ports.RuntimeAdapterSendEnd)
			mark.Sequence = sequence + 1
			observer.ObserveRuntime(mark)
		}(i)
	}
	wg.Wait()
	if err := closer.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if records := readJSONLRecords(t, path); len(records) != 32 {
		t.Fatalf("concurrently serialized records = %d, want 32", len(records))
	}
}

func sameProcessDuration(start, end ports.RuntimeMark) (int64, error) {
	if start.ProcessID == "" || start.ProcessID != end.ProcessID {
		return 0, os.ErrInvalid
	}
	if start.Scenario != end.Scenario || start.Run != end.Run || start.Sequence != end.Sequence || start.RequestID != end.RequestID || start.Epoch != end.Epoch || end.Tick < start.Tick {
		return 0, os.ErrInvalid
	}
	return end.Tick - start.Tick, nil
}

func validRuntimeMark(kind ports.RuntimeMarkKind) ports.RuntimeMark {
	return ports.RuntimeMark{Schema: 1, ProcessID: "daemon-process", Component: "daemon", Scenario: "resize", Run: 7, Sequence: 9, RequestID: 11, Epoch: 13, Kind: kind, Valid: true}
}

func readJSONLRecords(t *testing.T, path string) []map[string]any {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatalf("Open(%q): %v", path, err)
	}
	defer func() { _ = f.Close() }()
	var records []map[string]any
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("invalid JSONL record %q: %v", scanner.Text(), err)
		}
		records = append(records, record)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("reading JSONL: %v", err)
	}
	return records
}

type fixedRuntimeClock struct{ now time.Time }

func (c fixedRuntimeClock) Now() time.Time { return c.now }
func (c fixedRuntimeClock) NewTimer(d time.Duration) ports.Timer {
	return fixedRuntimeTimer{timer: time.NewTimer(d)}
}

type fixedRuntimeTimer struct{ timer *time.Timer }

func (t fixedRuntimeTimer) C() <-chan time.Time        { return t.timer.C }
func (t fixedRuntimeTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t fixedRuntimeTimer) Stop() bool                 { return t.timer.Stop() }
