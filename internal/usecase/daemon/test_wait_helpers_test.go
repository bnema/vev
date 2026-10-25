package daemon

import (
	"testing"
	"testing/synctest"
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

// requireSnapshotIdle is the synctest-bubble form of awaitSnapshotIdle: it
// waits for quiescence, then asserts no snapshot is pending.
func requireSnapshotIdle(t testing.TB, sess *session) {
	t.Helper()
	synctest.Wait()
	sess.snapshotMu.Lock()
	pending := sess.snapshotPending
	sess.snapshotMu.Unlock()
	if pending {
		t.Fatal("snapshot is still pending at quiescence")
	}
}

// requireSnapshotClean is the synctest-bubble form of awaitSnapshotClean: it
// waits for quiescence, then asserts the session has no dirty snapshot state.
func requireSnapshotClean(t testing.TB, sess *session) {
	t.Helper()
	synctest.Wait()
	if sess.snapDirty.Load() {
		t.Fatal("snapshot is still dirty at quiescence")
	}
}

// requireAttachmentCleanup is the synctest-bubble form of
// waitForAttachmentCleanup: after quiescence, every attachment cleanup
// goroutine must have been joined.
func requireAttachmentCleanup(t testing.TB, d *Daemon) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		d.attachmentCleanupWg.Wait()
		close(done)
	}()
	synctest.Wait()
	select {
	case <-done:
	default:
		t.Fatal("attachment cleanup was not joined at quiescence")
	}
}

// requireReceived asserts a channel result after bubble goroutines are quiescent.
func requireReceived[T any](t *testing.T, ch <-chan T, failure string) T {
	t.Helper()
	synctest.Wait()
	select {
	case value := <-ch:
		return value
	default:
		t.Fatal(failure)
		var zero T
		return zero
	}
}
