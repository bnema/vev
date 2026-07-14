package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

const testWaitTimeout = 2 * time.Second

func awaitTestValue[T any](t *testing.T, ch <-chan T, failure string) T {
	t.Helper()
	timer := time.NewTimer(testWaitTimeout)
	defer timer.Stop()
	select {
	case value := <-ch:
		return value
	case <-timer.C:
		t.Fatal(failure)
		var zero T
		return zero
	}
}

func awaitTestCompletion(t *testing.T, done <-chan struct{}, failure string) {
	t.Helper()
	awaitTestValue(t, done, failure)
}

func awaitCoordinatorOutput(
	t *testing.T,
	sends <-chan ports.Frame,
	timers <-chan *coordinatorMockTimer,
	frameContext string,
	timeoutFailure string,
) ports.Frame {
	t.Helper()
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	for {
		select {
		case frame := <-sends:
			if frame.Type != ports.MsgOutput {
				t.Fatalf("unexpected frame type %d %s", frame.Type, frameContext)
			}
			return frame
		case timer := <-timers:
			timer.ch <- time.Time{}
		case <-deadline.C:
			t.Fatal(timeoutFailure)
			return ports.Frame{}
		}
	}
}
