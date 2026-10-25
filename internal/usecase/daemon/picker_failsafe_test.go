package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// pickerGraceClock holds a settable time behind a generated MockClock. Render and
// attention workers read Now concurrently with the test's advance, so the time
// is mutex-guarded state read from the mock's RunAndReturn closure.
type pickerGraceClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *pickerGraceClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

func newPickerGraceClock(t testing.TB, now time.Time) (*portsmocks.MockClock, *pickerGraceClock) {
	t.Helper()
	state := &pickerGraceClock{now: now}
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().RunAndReturn(func() time.Time {
		state.mu.Lock()
		defer state.mu.Unlock()
		return state.now
	}).Maybe()
	clock.EXPECT().NewTimer(mock.Anything).Return(newStubTimer(t)).Maybe()
	return clock, state
}

// TestMovePickerInputFailSafe pins that raw input is never swallowed without a
// live offer: bytes that arrive inside the acquisition grace were in flight
// before the client saw the offer and are dropped, while raw input after the
// grace proves the client never took the interaction and releases it.
func TestMovePickerInputFailSafe(t *testing.T) {
	tests := []struct {
		name         string
		elapsed      time.Duration
		wantOpen     bool
		wantReleased bool
	}{
		{name: "in-flight input inside the grace is dropped", elapsed: 0, wantOpen: true},
		{name: "input just before the grace is still dropped", elapsed: pickerAcquisitionGrace - time.Millisecond, wantOpen: true},
		{name: "input after the grace releases the interaction", elapsed: pickerAcquisitionGrace, wantReleased: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock, graceTime := newPickerGraceClock(t, time.Unix(100, 0))
			d, source, ac, _, _, releases, sends := setupMovePickerSessionsCore(t, clock, 0)
			defer releaseAll(releases)

			openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, ac, "source-tab", "source-pane"))
			ac.overlays.pickerMu.Lock()
			interaction := ac.overlays.pickerInteraction
			ac.overlays.pickerMu.Unlock()
			graceTime.advance(tt.elapsed)

			effect := pickActionEffectForTest(t, source, ac)
			d.handleInputForAttachment(effect, []byte("a"))
			effect.End()

			require.Equal(t, tt.wantOpen, ac.overlays.pickerClientActive())
			if !tt.wantReleased {
				return
			}
			closed := awaitSentServerMessage(t, sends, "PickerClosed", func(message protocol.ServerMessage) bool {
				_, ok := message.(protocol.PickerClosed)
				return ok
			}).(protocol.PickerClosed)
			require.Equal(t, interaction, closed.InteractionID)
		})
	}
}
