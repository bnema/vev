package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol/wire"
)

func requireNoOutputFrame(t *testing.T, sends chan wire.Envelope) {
	t.Helper()
	deadline := time.After(50 * time.Millisecond)
	for {
		select {
		case f := <-sends:
			if envelopeMessageName(t, f.Payload) == "Output" {
				t.Fatalf("unexpected output frame: %+v", f)
			}
		case <-deadline:
			return
		}
	}
}
