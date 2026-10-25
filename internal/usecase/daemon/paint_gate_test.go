package daemon

import (
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/protocol/wire"
)

// requireNoOutputFrame asserts no Output frame was sent. It first waits for
// bubble quiescence.
func requireNoOutputFrame(t *testing.T, sends chan wire.Envelope) {
	t.Helper()
	synctest.Wait()
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
