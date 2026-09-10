package client

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
)

// TestTerminalInputPumpClipboardReadBlockedDoesNotHoldForegroundSwitch pins
// the P2.1 foreground-ownership boundary: a slow workstation clipboard read
// (e.g. a hung wl-paste) triggered by Ctrl+V must abort when the foreground
// is stopped for a route switch, so the pump cannot hold the switch waiting
// and a late result cannot reach a replacement owner.
func TestTerminalInputPumpClipboardReadBlockedDoesNotHoldForegroundSwitch(t *testing.T) {
	input := newTerminalInputPump(nil)
	input.enqueue(terminalReadResult{data: []byte{ctrlV}}, 1)

	started := make(chan struct{})
	reader := portsmocks.NewMockClipboardReader(t)
	reader.EXPECT().ReadImage(mock.Anything).RunAndReturn(func(ctx context.Context) (string, []byte, error) {
		close(started)
		<-ctx.Done()
		// Adapters map cancellation onto ErrNoClipboardImage so a cancelled
		// read silently forwards the keystroke; mirror that contract here.
		return "", nil, ports.ErrNoClipboardImage
	}).Once()

	var activeGeneration atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan protocol.ClientMessage, 16)
	done := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: ctx, cancel: cancel, input: input,
			out: out, clock: newAttachPaletteClock(),
			clipboard: reader,
			logger:    slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent, 32),
			activeGeneration: &activeGeneration,
			sendLease:        newForegroundSendLease(),
		}).run()
		close(done)
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("clipboard read did not start")
	}
	// Route switch: stopForeground cancels the foreground context, then
	// waits for the pump. A read bound to the foreground must release it.
	cancel()
	requireSignal(t, done, "cancelled stdin pump stayed blocked in a clipboard read after foreground stop")

	// The cancelled read resolves through the standard ErrNoClipboardImage
	// contract. Delivery races the foreground cancellation: either the
	// Ctrl+V won the send race and sits in out as ordinary input, or the
	// pump surrendered it into the input residual for the replacement
	// foreground. Both outcomes preserve the keystroke; neither may
	// produce an image or a notice to a replacement owner.
	forwarded := false
	for {
		select {
		case message := <-out:
			switch got := message.(type) {
			case protocol.Input:
				require.Equal(t, []byte{ctrlV}, got.Data, "cancelled Ctrl+V must be forwarded verbatim as ordinary input")
				forwarded = true
			case protocol.ImagePush:
				t.Fatal("cancelled clipboard read delivered an image to the replacement owner")
			case protocol.ClientNotice:
				t.Fatal("cancelled clipboard read delivered a notice to the replacement owner")
			default:
				t.Fatalf("unexpected message after cancelled clipboard read: %T", message)
			}
			if forwarded {
				goto drained
			}
		default:
			goto drained
		}
	}
drained:
	if !forwarded {
		// Send lost the race to cancellation: the byte must wait in the
		// input residual for the replacement foreground, not vanish.
		replacement := input.claim()
		defer input.revoke(replacement)
		got, ok := input.take(context.Background(), replacement)
		require.True(t, ok, "cancelled Ctrl+V was neither forwarded nor preserved: keystroke lost")
		require.Equal(t, []byte{ctrlV}, got.data)
		input.ack(replacement)
	}
	select {
	case message := <-out:
		t.Fatalf("cancelled clipboard read delivered a second frame: %T", message)
	default:
	}
}
