package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

const testWaitTimeout = 2 * time.Second

func testFramesOfType(frames []wire.Frame, typ wire.MsgType) []wire.Frame {
	matched := make([]wire.Frame, 0, len(frames))
	for _, frame := range frames {
		if frame.Type == typ {
			matched = append(matched, frame)
		}
	}
	return matched
}

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
	sends <-chan wire.Frame,
	timers <-chan *coordinatorMockTimer,
	frameContext string,
	timeoutFailure string,
) wire.Frame {
	t.Helper()
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	for {
		select {
		case frame := <-sends:
			if frame.Type == wire.MsgRoutePosition {
				continue
			}
			if frame.Type != wire.MsgOutput {
				t.Fatalf("unexpected frame type %d %s", frame.Type, frameContext)
			}
			return frame
		case timer := <-timers:
			timer.ch <- time.Time{}
		case <-deadline.C:
			t.Fatal(timeoutFailure)
			return wire.Frame{}
		}
	}
}
