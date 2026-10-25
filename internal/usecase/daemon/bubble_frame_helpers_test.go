package daemon

import (
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol/wire"
)

// requireFrame (the single shared frame helper) waits for quiescence, then
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

// stopOverlayTimersOnCleanup stops every retained overlay/echo/toast timer
// worker of ac at test cleanup. Stub-clock timers never fire, so without this a
// synctest bubble would end with those workers still parked.
func stopOverlayTimersOnCleanup(t testing.TB, ac *attachedClient) {
	t.Helper()
	t.Cleanup(func() {
		ac.echo.mu.Lock()
		ac.echo.timer.stop()
		ac.echo.mu.Unlock()
		if rt := ac.overlays; rt != nil {
			rt.noticeMu.Lock()
			rt.noticeTimer.stop()
			rt.noticesESC.stop()
			rt.noticeMu.Unlock()
			rt.copyMu.Lock()
			rt.copyESC.stop()
			rt.copyScroll.timer.stop()
			rt.copyMu.Unlock()
			rt.resizeMu.Lock()
			rt.resizeESC.stop()
			rt.resizeMu.Unlock()
		}
	})
}
