package ipc

import (
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/observability"
	"github.com/bnema/vev/internal/ports"
)

func TestTransportObservabilityIPCMarksCarriageWithoutChangingBytes(t *testing.T) {
	trace := t.TempDir() + "/ipc.jsonl"
	observer, closer, err := observability.NewJSONL(trace, ipcRuntimeClock{now: time.Unix(0, 211)}, "ipc-process")
	if err != nil {
		t.Fatalf("NewJSONL() error = %v", err)
	}

	left, right := net.Pipe()
	defer func() { _ = left.Close() }()
	defer func() { _ = right.Close() }()
	client := NewTransport(left, WithRuntimeObserver(observer))
	server := NewTransport(right, WithRuntimeObserver(observer))
	want := ports.Frame{Type: ports.MsgOutput, Payload: []byte("wire bytes stay exact")}

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

type ipcRuntimeClock struct{ now time.Time }

func (c ipcRuntimeClock) Now() time.Time { return c.now }
func (c ipcRuntimeClock) NewTimer(d time.Duration) ports.Timer {
	return ipcRuntimeTimer{timer: time.NewTimer(d)}
}

type ipcRuntimeTimer struct{ timer *time.Timer }

func (t ipcRuntimeTimer) C() <-chan time.Time        { return t.timer.C }
func (t ipcRuntimeTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t ipcRuntimeTimer) Stop() bool                 { return t.timer.Stop() }
