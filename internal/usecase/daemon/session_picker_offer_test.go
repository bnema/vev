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

// TestPaletteClientPickerFollowsPaletteErasingFrame pins the ordering the
// client overlay depends on: it suppresses attachment output once the picker
// opens, so the frame erasing the palette must reach it before the offer (SSP)
// or the first move snapshot (MFP), even while output is ACK-blocked.
func TestPaletteClientPickerFollowsPaletteErasingFrame(t *testing.T) {
	tests := []struct {
		name      string
		command   string
		saturated bool
		opens     func(protocol.ServerMessage) bool
	}{
		{name: "session picker", command: "SSP", opens: isPickerOffer},
		{name: "session picker with full output window", command: "SSP", saturated: true, opens: isPickerOffer},
		{name: "move picker", command: "MFP", opens: isPickerSnapshot},
		{name: "move picker with full output window", command: "MFP", saturated: true, opens: isPickerSnapshot},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, ac, _, _, releases, sends := setupMovePickerSessionsCore(t, stubClock{}, 0)
			defer releaseAll(releases)
			// Production paints through the coordinator, which retries an
			// ACK-blocked paint once the client acknowledges.
			rc := d.attachCoordinator(source, nil, ac, true)
			t.Cleanup(func() { rc.beginSessionTeardown().finish(); rc.waitForTimerWorkers() })
			d.handleInput(source, ac, []byte("\x1b "))
			d.handleInput(source, ac, []byte(tt.command))
			awaitFrame(t, sends, "Output")
			var blocking protocol.Output
			for _, frame := range drainAllFrames(sends) {
				if output, ok := decodeServerMessage(t, frame).(protocol.Output); ok {
					blocking = output
				}
			}
			if tt.saturated {
				// The palette frame is still unacknowledged: a one-frame window
				// blocks every further paint until the client ACKs it.
				require.NotZero(t, blocking.New)
				ac.output.setWindow(1)
				require.True(t, ac.output.atCapacity())
			}

			d.handleInput(source, ac, []byte("\r"))
			require.False(t, ac.overlays.paletteActive())
			if tt.saturated {
				for _, frame := range drainAllFrames(sends) {
					require.False(t, tt.opens(decodeServerMessage(t, frame)), "picker opened before the palette-erasing frame could be sent")
				}
				d.handleAttachmentClientMessage(captureAttachmentCapability(source, ac, ac.transport()), protocol.Ack{Epoch: blocking.Epoch, State: blocking.New})
			}

			first := awaitSentServerMessage(t, sends, "Output or picker", func(message protocol.ServerMessage) bool {
				_, output := message.(protocol.Output)
				return output || tt.opens(message)
			})
			require.IsType(t, protocol.Output{}, first, "the palette-erasing frame must precede the picker")
			awaitSentServerMessage(t, sends, "picker open", tt.opens)
		})
	}
}

func isPickerOffer(message protocol.ServerMessage) bool {
	_, ok := message.(protocol.PickerOffer)
	return ok
}

func isPickerSnapshot(message protocol.ServerMessage) bool {
	_, ok := message.(protocol.PickerSnapshot)
	return ok
}
