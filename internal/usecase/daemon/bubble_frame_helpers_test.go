package daemon

import (
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/domain"
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

// requireToastCount is the synctest-bubble form of awaitToastCount: after
// quiescence the visible toast count must equal want.
func requireToastCount(t *testing.T, ac *attachedClient, want int) []domain.Notification {
	t.Helper()
	synctest.Wait()
	got, _ := visibleToasts(ac)
	if len(got) != want {
		t.Fatalf("toast count = %d, want %d", len(got), want)
	}
	return got
}
