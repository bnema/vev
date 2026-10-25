package daemon

import (
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/protocol/wire"
)

// requireNoOutputFrame asserts no Output frame was sent. Inside a synctest
// bubble it first waits for quiescence; outside a bubble it only inspects what
// is already queued (the callers there are synchronous).
func requireNoOutputFrame(t *testing.T, sends chan wire.Envelope) {
	t.Helper()
	waitIfInBubble()
	for {
		select {
		case f := <-sends:
			if envelopeMessageName(t, f.Payload) == "Output" {
				t.Fatalf("unexpected output frame: %+v", f)
			}
		default:
			return
		}
	}
}

// waitIfInBubble calls synctest.Wait when the caller is inside a bubble and
// reports whether it was. Shared coordinator helpers are used both inside
// bubbles and by files that still run outside one.
func waitIfInBubble() (inBubble bool) {
	defer func() {
		if recover() != nil { // Wait panics outside a bubble
			inBubble = false
		}
	}()
	synctest.Wait()
	return true
}

// receiveEither receives from ch after bubble quiescence, or, outside a bubble,
// through the transitional real-time wait.
func receiveEither[T any](t *testing.T, ch <-chan T, failure string) T {
	t.Helper()
	if waitIfInBubble() {
		return requireReceived(t, ch, failure)
	}
	return awaitTestValue(t, ch, failure)
}

// frameEither is receiveEither for a named envelope.
func frameEither(t *testing.T, ch chan wire.Envelope, want string) wire.Envelope {
	t.Helper()
	if waitIfInBubble() {
		return requireFrame(t, ch, want)
	}
	return awaitFrame(t, ch, want)
}
