package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// This acceptance boundary deliberately configures the daemon with an
// observer only. The clock remains the daemon's existing behavior clock;
// timestamp ownership belongs to the concrete process observer.
func TestTransportObservabilityDaemonBoundaries(t *testing.T) {
	observer := &daemonRuntimeObserver{}
	d := New(nil, daemonRuntimeClock{now: time.Unix(0, 101)}, nil, WithRuntimeObserver(observer))
	if d == nil {
		t.Fatal("New() returned nil")
	}

	want := []ports.RuntimeMarkKind{
		ports.RuntimeResizeRequested, ports.RuntimeResizeCommitted,
		ports.RuntimeCaptureStart, ports.RuntimeCaptureEnd,
		ports.RuntimeComposeStart, ports.RuntimeComposeEnd,
		ports.RuntimeDiffStart, ports.RuntimeDiffEnd,
		ports.RuntimeQueueEnqueued, ports.RuntimeQueueDequeued,
		ports.RuntimeACKBlockedStart, ports.RuntimeACKBlockedEnd,
		ports.RuntimeEmitStart, ports.RuntimeEmitEnd,
	}
	if len(want) != 14 {
		t.Fatalf("daemon boundary mark inventory changed: %d", len(want))
	}
}

type daemonRuntimeObserver struct{ marks []ports.RuntimeMark }

func (o *daemonRuntimeObserver) ObserveRuntime(mark ports.RuntimeMark) {
	o.marks = append(o.marks, mark)
}

type daemonRuntimeClock struct{ now time.Time }

func (c daemonRuntimeClock) Now() time.Time { return c.now }
func (c daemonRuntimeClock) NewTimer(d time.Duration) ports.Timer {
	return daemonRuntimeTimer{timer: time.NewTimer(d)}
}

type daemonRuntimeTimer struct{ timer *time.Timer }

func (t daemonRuntimeTimer) C() <-chan time.Time        { return t.timer.C }
func (t daemonRuntimeTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
func (t daemonRuntimeTimer) Stop() bool                 { return t.timer.Stop() }
