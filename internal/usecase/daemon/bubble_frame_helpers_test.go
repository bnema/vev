package daemon

import (
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/protocol/wire"
)

// requireFrame is the synctest-bubble form of awaitFrame: after quiescence it
// drains already-sent envelopes until one named want is found.
func requireFrame(t *testing.T, ch <-chan wire.Envelope, want string) wire.Envelope {
	t.Helper()
	synctest.Wait()
	for {
		select {
		case f := <-ch:
			if envelopeMessageName(t, f.Payload) == want {
				return f
			}
		default:
			t.Fatalf("envelope %s was not sent at quiescence", want)
			return wire.Envelope{}
		}
	}
}
