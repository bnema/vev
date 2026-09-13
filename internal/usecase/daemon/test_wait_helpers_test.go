package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

const testWaitTimeout = 2 * time.Second

func testFramesOfType(frames []wire.Envelope, name string) []wire.Envelope {
	matched := make([]wire.Envelope, 0, len(frames))
	for _, frame := range frames {
		if envelopeMessageName(nil, frame.Payload) == name {
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
	sends <-chan wire.Envelope,
	timers <-chan *coordinatorMockTimer,
	frameContext string,
	timeoutFailure string,
) wire.Envelope {
	t.Helper()
	deadline := time.NewTimer(testWaitTimeout)
	defer deadline.Stop()
	for {
		select {
		case frame := <-sends:
			name := envelopeMessageName(t, frame.Payload)
			if name == "RoutePosition" {
				continue
			}
			if name != "Output" {
				t.Fatalf("unexpected frame type %s %s", name, frameContext)
			}
			return frame
		case timer := <-timers:
			timer.ch <- time.Time{}
		case <-deadline.C:
			t.Fatal(timeoutFailure)
			return wire.Envelope{}
		}
	}
}
