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

// waitIfInBubble calls synctest.Wait when the caller is inside a bubble.
func waitIfInBubble() {
	defer func() { _ = recover() }() // Wait panics outside a bubble
	synctest.Wait()
}
