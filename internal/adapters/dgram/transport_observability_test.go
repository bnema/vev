package dgram

import (
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/ports"
)

func TestTransportObservabilityDgramKeepsBehaviorClockSeparate(t *testing.T) {
	behaviorClock := newManualClock(time.Unix(0, 401))
	link := newSimulatedLink(behaviorClock, packetPolicy{})
	aPC, bPC := newPairWithCapacity(link, 64)
	tracePath := t.TempDir() + "/dgram.jsonl"
	observer, closer, err := observability.NewJSONL(tracePath, dgramObserverClock{now: time.Unix(0, 509)}, "dgram-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}

	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: behaviorClock, ResendAfter: time.Hour, Heartbeat: time.Hour}, WithRuntimeObserver(observer))
	if err != nil {
		t.Fatalf("NewTransportWithOptions(a) error = %v", err)
	}
	defer func() { _ = a.Close() }()
	b, err := NewTransportWithOptions(bPC, testAddr("a"), key(), 2, 1, Options{Clock: behaviorClock, ResendAfter: time.Hour, Heartbeat: time.Hour}, WithRuntimeObserver(observer))
	if err != nil {
		t.Fatalf("NewTransportWithOptions(b) error = %v", err)
	}
	defer func() { _ = b.Close() }()

	want := ports.Frame{Type: ports.MsgOutput, Payload: []byte("datagram carriage remains exact")}
	if err := a.Send(want); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	gotCh := make(chan ports.Frame, 1)
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
