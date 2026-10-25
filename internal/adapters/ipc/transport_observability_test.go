package ipc

import (
	"encoding/json"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/observability"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol/wire"
)

// real I/O: writes and reads JSONL trace files.
func TestTransportObservabilityIPCEOFAndCloseEndFailedSpans(t *testing.T) {
	trace := t.TempDir() + "/ipc-shutdown.jsonl"
	observer, closer, err := observability.NewJSONL(trace, newRuntimeClock(t, time.Unix(0, 223)), "ipc-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()

	// EOF from the peer and a locally closed blocked receive are distinct
	// shutdown paths; both must record a failed, correlated end.
	for _, closeLocal := range []bool{false, true} {
		left, right := net.Pipe()
		var started <-chan struct{}
		conn := left
		if closeLocal {
			observedConn := &ipcReadStartedConn{Conn: left, started: make(chan struct{})}
			conn, started = observedConn, observedConn.started
		}
		transport := NewTransport(conn, WithRuntimeObserver(reporter))
		recvDone := make(chan error, 1)
		go func() { _, err := transport.Recv(); recvDone <- err }()
		if closeLocal {
			<-started
			if err := transport.Close(); err != nil {
				t.Fatalf("Close() error = %v", err)
			}
		} else if err := right.Close(); err != nil {
			t.Fatalf("peer Close() error = %v", err)
		}
		if err := <-recvDone; err == nil {
			t.Fatal("Recv() error = nil after transport shutdown")
		}
		_ = left.Close()
		_ = right.Close()
	}

	// A peer shutdown also makes Send fail after its start mark.
	left, right := net.Pipe()
	transport := NewTransport(left, WithRuntimeObserver(reporter))
	_ = right.Close()
	if err := transport.Send(wire.Envelope{Payload: []byte("failed")}); err == nil {
		t.Fatal("Send() error = nil after peer close")
	}
	_ = transport.Close()
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	assertIPCFailedSpanPairs(t, trace)
}

type ipcReadStartedConn struct {
	net.Conn
	started chan struct{}
	once    sync.Once
}

func (c *ipcReadStartedConn) Read(p []byte) (int, error) {
	c.once.Do(func() { close(c.started) })
	return c.Conn.Read(p)
}

func assertIPCFailedSpanPairs(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(trace): %v", err)
	}
	type mark struct {
		ProcessID string `json:"process_id"`
		Component string `json:"component"`
		Scenario  string `json:"scenario"`
		Run       uint64 `json:"run"`
		Sequence  uint64 `json:"sequence"`
		RequestID uint64 `json:"request_id"`
		Epoch     uint64 `json:"epoch"`
		Kind      string `json:"kind"`
		Valid     bool   `json:"valid"`
	}
	type spanKey struct {
		processID, scenario             string
		run, sequence, requestID, epoch uint64
	}
	starts := make(map[spanKey]mark)
	startCounts := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var m mark
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("Unmarshal(trace): %v", err)
		}
		key := spanKey{m.ProcessID, m.Scenario, m.Run, m.Sequence, m.RequestID, m.Epoch}
		switch m.Kind {
		case "adapter_receive_start", "adapter_send_start":
			if m.ProcessID != "ipc-process" || m.Component != "ipc" || m.Scenario != "runtime" || m.Run != 1 || m.Sequence == 0 || m.RequestID != m.Sequence || m.Epoch != m.Sequence || !m.Valid {
				t.Fatalf("adapter start has invalid identity: %#v", m)
			}
			if _, exists := starts[key]; exists {
				t.Fatalf("duplicate adapter start for %#v", key)
			}
			starts[key] = m
			startCounts[m.Kind]++
		case "adapter_receive_end", "adapter_send_end":
			start, ok := starts[key]
			if !ok || strings.TrimSuffix(start.Kind, "_start") != strings.TrimSuffix(m.Kind, "_end") {
				t.Fatalf("unmatched adapter end %#v", m)
			}
			if m.Valid {
				t.Fatalf("failed adapter end is valid: %#v", m)
			}
			delete(starts, key)
		}
	}
	if len(starts) != 0 {
		t.Fatalf("unmatched adapter starts: %#v", starts)
	}
	if got := startCounts["adapter_receive_start"]; got != 2 {
		t.Errorf("adapter receive starts = %d, want 2", got)
	}
	if got := startCounts["adapter_send_start"]; got != 1 {
		t.Errorf("adapter send starts = %d, want 1", got)
	}
}

// real I/O: writes and reads JSONL trace files.
func TestTransportObservabilityIPCMarksCarriageWithoutChangingBytes(t *testing.T) {
	trace := t.TempDir() + "/ipc.jsonl"
	observer, closer, err := observability.NewJSONL(trace, newRuntimeClock(t, time.Unix(0, 211)), "ipc-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()

	left, right := net.Pipe()
	defer func() { _ = left.Close() }()
	defer func() { _ = right.Close() }()
	client := NewTransport(left, WithRuntimeObserver(reporter))
	server := NewTransport(right, WithRuntimeObserver(reporter))
	want := wire.Envelope{Payload: []byte("wire bytes stay exact")}

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := client.Send(want); err != nil {
			t.Errorf("Send() error = %v", err)
		}
	})
	got, err := server.Recv()
	wg.Wait()
	if err != nil {
		t.Fatalf("Recv() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("carried frame = %#v, want %#v", got, want)
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	traceBytes, err := os.ReadFile(trace)
	if err != nil {
		t.Fatalf("ReadFile(trace): %v", err)
	}
	for _, kind := range []string{"adapter_send_start", "adapter_send_end", "adapter_receive_start", "adapter_receive_end"} {
		if !strings.Contains(string(traceBytes), `"kind":"`+kind+`"`) {
			t.Errorf("trace missing %s", kind)
		}
	}
}

func newRuntimeClock(t *testing.T, now time.Time) *portsmocks.MockClock {
	t.Helper()
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(now).Maybe()
	return clock
}
