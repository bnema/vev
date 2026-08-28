package dgram

import (
	"encoding/json"
	"errors"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

func TestTransportObservabilityDgramFailedCloseEndsReceiveAndSend(t *testing.T) {
	behaviorClock := newManualClock(time.Unix(0, 417))
	link := newSimulatedLink(behaviorClock, packetPolicy{})
	aPC, _ := newPairWithCapacity(link, 64)
	closeErr := errors.New("packet connection close failed")
	tracePath := t.TempDir() + "/dgram-close.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, dgramObserverClock{now: time.Unix(0, 521)}, "dgram-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	receiveStarted := make(chan struct{})
	signalingObserver := &dgramStartObserver{RuntimeObserver: observer, receiveStarted: receiveStarted}
	reporter := observability.NewSerialized(signalingObserver, 64)
	defer reporter.Close()
	transport, err := NewTransportWithOptions(&dgramCloseFailPC{fakePC: aPC, err: closeErr}, testAddr("b"), key(), 1, 2, Options{Clock: behaviorClock, ResendAfter: time.Hour, Heartbeat: time.Hour}, WithRuntimeObserver(reporter))
	if err != nil {
		t.Fatalf("NewTransportWithOptions() error = %v", err)
	}
	if err := transport.Send(wire.Frame{Type: wire.MsgOutput, Payload: []byte("write failure")}); err == nil {
		t.Fatal("Send() error = nil on packet write failure")
	}
	recvDone := make(chan error, 1)
	go func() { _, err := transport.Recv(); recvDone <- err }()
	<-receiveStarted
	if err := transport.Close(); !errors.Is(err, closeErr) {
		t.Fatalf("Close() error = %v, want %v", err, closeErr)
	}
	if err := <-recvDone; err == nil {
		t.Fatal("Recv() error = nil after failed Close()")
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	assertDgramFailedSpanPairs(t, tracePath)
}

type dgramStartObserver struct {
	ports.RuntimeObserver
	receiveStarted chan struct{}
	once           sync.Once
}

func (o *dgramStartObserver) ObserveRuntime(mark ports.RuntimeMark) {
	if mark.Kind == ports.RuntimeAdapterReceiveStart {
		o.once.Do(func() { close(o.receiveStarted) })
	}
	o.RuntimeObserver.ObserveRuntime(mark)
}

type dgramCloseFailPC struct {
	*fakePC
	err error
}

func (p *dgramCloseFailPC) WriteTo([]byte, net.Addr) (int, error) {
	return 0, errors.New("packet write failed")
}

func (p *dgramCloseFailPC) Close() error {
	_ = p.fakePC.Close()
	return p.err
}

func assertDgramFailedSpanPairs(t *testing.T, path string) {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile(trace): %v", err)
	}
	type mark struct {
		ProcessID string `json:"process_id"`
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
		kind                            string
	}
	starts := make(map[spanKey]mark)
	pairs := map[string]int{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		var m mark
		if err := json.Unmarshal([]byte(line), &m); err != nil {
			t.Fatalf("Unmarshal(trace): %v", err)
		}
		key := spanKey{m.ProcessID, m.Scenario, m.Run, m.Sequence, m.RequestID, m.Epoch, m.Kind}
		switch m.Kind {
		case "adapter_receive_start", "adapter_send_start":
			if _, exists := starts[key]; exists {
				t.Fatalf("duplicate adapter start: %#v", m)
			}
			starts[key] = m
		case "adapter_receive_end", "adapter_send_end":
			key.kind = strings.TrimSuffix(m.Kind, "_end") + "_start"
			start, ok := starts[key]
			if !ok || m.Valid {
				t.Fatalf("unmatched or valid failed end: start=%#v end=%#v", start, m)
			}
			pairs[strings.TrimSuffix(m.Kind, "_end")]++
			delete(starts, key)
		}
	}
	if len(starts) != 0 {
		t.Fatalf("unmatched adapter starts: %#v", starts)
	}
	if pairs["adapter_send"] != 1 || pairs["adapter_receive"] != 1 || len(pairs) != 2 {
		t.Fatalf("failed adapter span pairs=%v, want exactly one send and one receive", pairs)
	}
}

func TestTransportObservabilityDgramKeepsBehaviorClockSeparate(t *testing.T) {
	behaviorClock := newManualClock(time.Unix(0, 401))
	link := newSimulatedLink(behaviorClock, packetPolicy{})
	aPC, bPC := newPairWithCapacity(link, 64)
	tracePath := t.TempDir() + "/dgram.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, dgramObserverClock{now: time.Unix(0, 509)}, "dgram-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}
	reporter := observability.NewSerialized(observer, 64)
	defer reporter.Close()

	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: behaviorClock, ResendAfter: time.Hour, Heartbeat: time.Hour}, WithRuntimeObserver(reporter))
	if err != nil {
		t.Fatalf("NewTransportWithOptions(a) error = %v", err)
	}
	defer func() { _ = a.Close() }()
	b, err := NewTransportWithOptions(bPC, testAddr("a"), key(), 2, 1, Options{Clock: behaviorClock, ResendAfter: time.Hour, Heartbeat: time.Hour}, WithRuntimeObserver(reporter))
	if err != nil {
		t.Fatalf("NewTransportWithOptions(b) error = %v", err)
	}
	defer func() { _ = b.Close() }()

	want := wire.Frame{Type: wire.MsgOutput, Payload: []byte("datagram carriage remains exact")}
	if err := a.Send(want); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	gotCh := make(chan wire.Frame, 1)
	errCh := make(chan error, 1)
	go func() {
		frame, recvErr := b.Recv()
		if recvErr != nil {
			errCh <- recvErr
			return
		}
		gotCh <- frame
	}()
	select {
	case err := <-errCh:
		t.Fatalf("Recv() error = %v", err)
	case got := <-gotCh:
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("carried frame = %#v, want %#v", got, want)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv() did not receive deterministic simulated datagram")
	}
	reporter.Close()
	if err := closer.Close(); err != nil {
		t.Fatalf("trace Close() error = %v", err)
	}
	trace, err := os.ReadFile(tracePath)
	if err != nil {
		t.Fatalf("ReadFile(trace): %v", err)
	}
	for _, kind := range []string{"adapter_send_start", "adapter_send_end", "adapter_receive_start", "adapter_receive_end"} {
		if !strings.Contains(string(trace), `"kind":"`+kind+`"`) {
			t.Errorf("trace missing %s", kind)
		}
	}
}

type dgramObserverClock struct{ now time.Time }

func (c dgramObserverClock) Now() time.Time { return c.now }
func (c dgramObserverClock) NewTimer(d time.Duration) ports.Timer {
	return dgramObserverTimer{timer: time.NewTimer(d)}
}

type dgramObserverTimer struct{ timer *time.Timer }

func (t dgramObserverTimer) C() <-chan time.Time        { return t.timer.C }
func (t dgramObserverTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t dgramObserverTimer) Stop() bool                 { return t.timer.Stop() }
