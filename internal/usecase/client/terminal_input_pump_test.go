package client

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

type lifecycleReader struct {
	chunks  chan []byte
	started chan struct{}

	mu     sync.Mutex
	active int
	max    int
}

func newLifecycleReader() *lifecycleReader {
	return &lifecycleReader{chunks: make(chan []byte), started: make(chan struct{}, 4)}
}

func (r *lifecycleReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	r.active++
	if r.active > r.max {
		r.max = r.active
	}
	r.mu.Unlock()
	r.started <- struct{}{}
	data, ok := <-r.chunks
	r.mu.Lock()
	r.active--
	r.mu.Unlock()
	if !ok {
		return 0, io.EOF
	}
	return copy(p, data), nil
}

func (r *lifecycleReader) maxActive() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.max
}

func TestTerminalInputPumpCancellationHandoffPreservesQueuedBytes(t *testing.T) {
	input := newTerminalInputPump(nil)
	// Both cancellation and a terminal read are ready before the old scanner
	// tries to dequeue. It must surrender the read to its replacement.
	want := []byte("first\x00second\xff")
	input.enqueue(terminalReadResult{data: append([]byte(nil), want...)})

	var activeGeneration atomic.Uint64
	cancelledCtx, cancelCancelled := context.WithCancel(context.Background())
	cancelCancelled()
	cancelledDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: cancelledCtx, cancel: cancelCancelled, input: input,
			out: make(chan ports.Frame, 1), clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(cancelledDone)
	}()
	<-cancelledDone

	replacementCtx, cancelReplacement := context.WithCancel(context.Background())
	defer cancelReplacement()
	out := make(chan ports.Frame, len(want))
	replacementDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: replacementCtx, cancel: cancelReplacement, input: input,
			out: out, clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(replacementDone)
	}()

	var got []byte
	for len(got) < len(want) {
		frame := <-out
		input, err := ports.UnmarshalInput(frame.Payload)
		require.NoError(t, err)
		got = append(got, input.Data...)
	}
	require.Equal(t, want, got, "handoff must preserve every queued byte in order")

	cancelReplacement()
	<-replacementDone
}

func TestTerminalInputPumpReusesOneReaderAcrossCancelledAttempts(t *testing.T) {
	reader := newLifecycleReader()
	input := newTerminalInputPump(reader)
	input.start()
	t.Cleanup(input.stop)
	<-reader.started // The lifecycle pump owns the sole blocking Read.

	var activeGeneration atomic.Uint64
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	firstDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: firstCtx, cancel: cancelFirst, input: input,
			out: make(chan ports.Frame), clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(firstDone)
	}()
	cancelFirst()
	<-firstDone

	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	out := make(chan ports.Frame, 1)
	secondDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: secondCtx, cancel: cancelSecond, input: input,
			out: out, clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(secondDone)
	}()

	reader.chunks <- []byte("x")
	frame := <-out
	got, err := ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), got.Data)
	require.Equal(t, 1, reader.maxActive(), "cancelled attempts must not leave competing reads")

	cancelSecond()
	<-secondDone
}
