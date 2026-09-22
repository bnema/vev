package daemon

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// awaitSentServerMessage drains captured frames until one satisfies match.
func awaitSentServerMessage(t *testing.T, sends <-chan wire.Envelope, what string, match func(protocol.ServerMessage) bool) protocol.ServerMessage {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case frame := <-sends:
			message := decodeServerMessage(t, frame)
			if match(message) {
				return message
			}
		case <-deadline:
			t.Fatalf("daemon sent no %s", what)
			return nil
		}
	}
}

// TestSessionPickerCommandOffersClientPickerOverLiveAttachment pins PA1 on the
// daemon side: SSP keeps the attachment, sends a navigation offer the client
// composes its own picker for, and opens no daemon interaction that could
// swallow input.
func TestSessionPickerCommandOffersClientPickerOverLiveAttachment(t *testing.T) {
	d, source, ac, _, _, releases, sends := setupMovePickerSessionsCore(t, stubClock{}, 0)
	defer releaseAll(releases)

	d.handleInput(source, ac, []byte("\x1b "))
	require.True(t, ac.overlays.paletteActive())
	d.handleInput(source, ac, []byte("SSP\r"))

	offer := awaitSentServerMessage(t, sends, "navigation PickerOffer", func(message protocol.ServerMessage) bool {
		_, ok := message.(protocol.PickerOffer)
		return ok
	}).(protocol.PickerOffer)
	require.Equal(t, protocol.PickerIntentNavigation, offer.Intent)
	require.NotZero(t, offer.InteractionID)
	require.False(t, ac.overlays.pickerClientActive(), "a navigation offer must not open a daemon interaction")
	require.Same(t, source, ac.currentAttachmentSession(), "SSP must keep the attachment live")
	assertNoFurtherFrame(t, sends, "Detached", "SSP must not detach the attachment")
}
