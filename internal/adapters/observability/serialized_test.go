package observability

import (
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

type runtimeObserverFunc func(ports.RuntimeMark)

func (f runtimeObserverFunc) ObserveRuntime(mark ports.RuntimeMark) { f(mark) }

func TestSerializedFlushesInOrderAndCloses(t *testing.T) {
	var mu sync.Mutex
	var got []ports.RuntimeMark
	observer := NewSerialized(runtimeObserverFunc(func(mark ports.RuntimeMark) {
		mu.Lock()
		got = append(got, mark)
		mu.Unlock()
	}), 2)
	observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeEmitStart, 1, true))
	observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeEmitEnd, 1, true))
	observer.Flush()
	observer.Close()
	observer.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 || got[0].Kind != ports.RuntimeEmitStart || got[1].Kind != ports.RuntimeEmitEnd {
		t.Fatalf("serialized marks = %#v", got)
	}
}

func TestEnsureSerializedReusesExistingOwner(t *testing.T) {
	existing := NewSerialized(runtimeObserverFunc(func(ports.RuntimeMark) {}), 1)
	reporter, owned := EnsureSerialized(existing, 1)
	if owned || reporter != existing {
		t.Fatalf("EnsureSerialized() = (%T, %t), want existing unowned reporter", reporter, owned)
	}
	existing.Close()
}

func TestSerializedReportsBoundedQueueLoss(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	var mu sync.Mutex
	var got []ports.RuntimeMark
	observer := NewSerialized(runtimeObserverFunc(func(mark ports.RuntimeMark) {
		if mark.Kind == ports.RuntimeEmitStart {
			close(entered)
			<-release
		}
		mu.Lock()
		got = append(got, mark)
		mu.Unlock()
	}), 2)
	observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeEmitStart, 1, true))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("observer did not start")
	}
	observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeComposeStart, 1, true))
	observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeComposeEnd, 1, true))
	observer.ObserveRuntime(ports.NewRuntimeMark("client", ports.RuntimeEmitEnd, 1, true))
	close(release)
	observer.Flush()
	observer.Close()

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 4 || got[0].Kind != ports.RuntimeEmitStart || got[1].Kind != ports.RuntimeComposeStart || got[2].Kind != ports.RuntimeComposeEnd || got[3].Kind != ports.RuntimeTransportDiagnostic || got[3].Bytes != 1 || got[3].Valid {
		t.Fatalf("bounded-loss marks = %#v", got)
	}
}
