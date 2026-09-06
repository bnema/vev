package client

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestUIInputPumpBatchFenceOrdering(t *testing.T) {
	for _, text := range []string{"hello", "\x1b", "\x1b[A"} {
		t.Run(text, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			input := newTerminalInputPump(nil)
			consumer := input.claim()
			out := make(chan protocol.ClientMessage, 32)
			events := make(chan paletteGenerationEvent, 32)
			var palette atomic.Uint64
			done := make(chan struct{})
			go func() {
				defer close(done)
				(&stdinPump{ctx: ctx, cancel: cancel, input: input, consumer: consumer, uiGeneration: 7, out: out, clock: newAttachPaletteClock(), logger: slog.New(slog.DiscardHandler), paletteEvents: events, activeGeneration: &palette}).run()
			}()
			t.Cleanup(func() { cancel(); requireSignal(t, done, "scanner did not stop"); input.revoke(consumer) })
			req := terminalAutomationRequest{ctx: ctx, consumer: consumer, record: terminalReadResult{source: terminalInputAutomation, generation: 7, actionID: 9, endBatch: true, data: []byte(text)}, admitted: make(chan bool, 1), dispatched: make(chan bool, 1)}
			select {
			case input.automation <- req:
			case <-time.After(time.Second):
				t.Fatal("admission stalled")
			}
			require.True(t, <-req.admitted)
			var boundary paletteGenerationEvent
			select {
			case boundary = <-events:
			case <-time.After(time.Second):
				t.Fatal("missing FIFO boundary")
			}
			require.Equal(t, paletteEventUIBatchEnd, boundary.kind)
			var got string
			for len(out) > 0 {
				message := <-out
				in, ok := message.(protocol.Input)
				require.True(t, ok, "fence passed the local barrier")
				require.Equal(t, uint64(9), in.ActionID)
				got += string(in.Data)
			}
			require.Equal(t, text, got)
			// A physical read can queue while the action waits, but cannot interleave.
			require.True(t, input.enqueue(terminalReadResult{data: []byte("human")}, 1))
			close(boundary.batchApplied)
			select {
			case message := <-out:
				require.Equal(t, protocol.UIFence{ActionID: 9}, message)
			case <-time.After(time.Second):
				t.Fatal("missing fence")
			}
			require.True(t, <-req.dispatched)
			var human string
			deadline := time.After(time.Second)
			for len(human) < len("human") {
				select {
				case message := <-out:
					in, ok := message.(protocol.Input)
					require.True(t, ok)
					require.Zero(t, in.ActionID)
					human += string(in.Data)
				case <-deadline:
					t.Fatal("physical input stalled")
				}
			}
			require.Equal(t, "human", human)
		})
	}
}

func TestUIInputPumpCancellationDiscardsAutomatedSuffix(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	input := newTerminalInputPump(nil)
	consumer := input.claim()
	defer input.revoke(consumer)
	out := make(chan protocol.ClientMessage, 32)
	var palette atomic.Uint64
	done := make(chan struct{})
	go func() {
		defer close(done)
		(&stdinPump{ctx: ctx, cancel: cancel, input: input, consumer: consumer, uiGeneration: 7, out: out, clock: newAttachPaletteClock(), logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent, 32), activeGeneration: &palette, afterInputTake: cancel}).run()
	}()
	req := terminalAutomationRequest{ctx: ctx, consumer: consumer, record: terminalReadResult{source: terminalInputAutomation, generation: 7, actionID: 9, endBatch: true, data: []byte("suffix\x1b")}, admitted: make(chan bool, 1), dispatched: make(chan bool, 1)}
	select {
	case input.automation <- req:
	case <-time.After(time.Second):
		t.Fatal("admission stalled")
	}
	requireSignal(t, done, "cancelled scanner did not stop")
	require.True(t, <-req.admitted)
	require.False(t, <-req.dispatched)
	require.Empty(t, out)
	input.mu.Lock()
	defer input.mu.Unlock()
	require.Empty(t, input.residual, "automated suffix cannot move to a replacement")
}

func TestUIInputPumpRejectsHumanPartialBatch(t *testing.T) {
	for _, prefix := range []string{"\x1b]10;rgb:", "\x1b[?2031;", "\x1b[200~human", "\x1b]10;rgb:1111/2222/3333\x07"} {
		t.Run(prefix, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			input := newTerminalInputPump(nil)
			consumer := input.claim()
			out := make(chan protocol.ClientMessage, 32)
			events := make(chan paletteGenerationEvent, 32)
			var palette atomic.Uint64
			done := make(chan struct{})
			go func() {
				defer close(done)
				(&stdinPump{ctx: ctx, cancel: cancel, input: input, consumer: consumer, uiGeneration: 7, out: out, clock: newAttachPaletteClock(), logger: slog.New(slog.DiscardHandler), paletteEvents: events, activeGeneration: &palette}).run()
			}()
			t.Cleanup(func() { cancel(); requireSignal(t, done, "scanner did not stop"); input.revoke(consumer) })
			require.True(t, input.enqueue(terminalReadResult{data: []byte(prefix)}, 1))
			// The unbuffered request runs at the next owner select, after the pending
			// physical read or before it; either order must reject without flushing it.
			req := terminalAutomationRequest{ctx: ctx, consumer: consumer, record: terminalReadResult{source: terminalInputAutomation, generation: 7, actionID: 9, endBatch: true, data: []byte("automated")}, admitted: make(chan bool, 1), dispatched: make(chan bool, 1)}
			select {
			case input.automation <- req:
			case <-time.After(time.Second):
				t.Fatal("admission stalled")
			}
			select {
			case admitted := <-req.admitted:
				require.False(t, admitted)
			case <-time.After(time.Second):
				t.Fatal("missing refusal")
			}
			select {
			case message := <-out:
				t.Fatalf("refusal flushed human input: %#v", message)
			default:
			}
		})
	}
}
