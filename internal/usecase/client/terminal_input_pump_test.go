package client

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
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

type lateStopReader struct {
	started chan struct{}
	release chan terminalReadResult
	reads   atomic.Int32
}

func newLateStopReader() *lateStopReader {
	return &lateStopReader{
		started: make(chan struct{}, 2),
		release: make(chan terminalReadResult, 1),
	}
}

func (r *lateStopReader) Read(p []byte) (int, error) {
	r.reads.Add(1)
	r.started <- struct{}{}
	result := <-r.release
	return copy(p, result.data), result.err
}

func requireSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal(message)
	}
}

func requireFrame(t *testing.T, ch <-chan ports.Frame, message string) ports.Frame {
	t.Helper()
	select {
	case frame := <-ch:
		return frame
	case <-time.After(time.Second):
		t.Fatal(message)
		return ports.Frame{}
	}
}

func requireTimer(t *testing.T, ch <-chan *attachPaletteTimer, message string) *attachPaletteTimer {
	t.Helper()
	select {
	case timer := <-ch:
		return timer
	case <-time.After(time.Second):
		t.Fatal(message)
		return nil
	}
}

func TestPaletteSlotRejectsOutOfRangeIntBeforeNarrowing(t *testing.T) {
	for _, tc := range []struct {
		slot int
		want uint8
		ok   bool
	}{
		{slot: -1},
		{slot: 0, want: 0, ok: true},
		{slot: 15, want: 15, ok: true},
		{slot: 16},
		{slot: 256},
	} {
		got, ok := paletteSlot(tc.slot)
		require.Equal(t, tc.ok, ok, "slot %d", tc.slot)
		if ok {
			require.Equal(t, tc.want, got, "slot %d", tc.slot)
		}
	}
}

func TestTerminalInputPumpStopDropsLateReadWithoutStartingAnother(t *testing.T) {
	reader := newLateStopReader()
	input := newTerminalInputPump(reader)
	input.start()
	<-reader.started // The sole lifecycle read is blocked in the caller-owned reader.

	input.stop()
	reader.release <- terminalReadResult{data: []byte("late")}

	// The late read must be discarded rather than published. Both possible
	// outcomes are channel-synchronized: a correct pump exits; the historical
	// race published and immediately issued a second Read.
	select {
	case <-input.exited:
	case <-reader.started:
		require.Fail(t, "pump issued another Read after stop")
	}
	require.Equal(t, int32(1), reader.reads.Load())

	consumer := input.claim()
	_, ok := input.take(context.Background(), consumer)
	require.False(t, ok, "late read must not be available after stop")
	input.revoke(consumer)
	input.stop() // stop is deliberately idempotent.
}

func TestRunnerReusesBlockedTerminalInputPumpAcrossRuns(t *testing.T) {
	reader := newLateStopReader()
	runner := &Runner{term: &attachPaletteTerminalHarness{
		in: reader, out: newAttachPaletteWriter(), resize: make(chan domain.Geometry),
	}}

	first := runner.terminalInput()
	<-reader.started
	first.suspend()
	second := runner.terminalInput()
	require.Same(t, first, second, "a later Run must reuse a blocked caller-owned reader")
	require.Equal(t, int32(1), reader.reads.Load())

	first.stop()
	reader.release <- terminalReadResult{}
	<-first.exited
}

func TestTerminalInputPumpCancellationHandoffPreservesQueuedBytes(t *testing.T) {
	input := newTerminalInputPump(nil)
	// Both cancellation and a terminal read are ready before the old scanner
	// tries to dequeue. It must surrender the read to its replacement.
	want := []byte("first\x00second\xff")
	input.enqueue(terminalReadResult{data: append([]byte(nil), want...)}, 1)

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

func TestTerminalInputPumpCancellationAfterTakeRequeuesRawRead(t *testing.T) {
	input := newTerminalInputPump(nil)
	want := []byte("first\x00second\xff")
	input.enqueue(terminalReadResult{data: append([]byte(nil), want...)}, 1)

	ctx, cancel := context.WithCancel(context.Background())
	consumer := input.claim()
	taken := make(chan struct{})
	release := make(chan struct{})
	done := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: ctx, cancel: cancel, input: input, consumer: consumer,
			out: make(chan ports.Frame, 1), clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			afterInputTake: func() {
				close(taken)
				<-release
			},
		}).run()
		close(done)
	}()

	<-taken
	cancel()
	close(release)
	<-done
	input.revoke(consumer)

	replacement := input.claim()
	got, ok := input.take(context.Background(), replacement)
	require.True(t, ok, "cancelled delivery must leave the read for the replacement scanner")
	require.Equal(t, want, got.data)
	input.ack(replacement)
	input.revoke(replacement)
}

func TestTerminalInputPumpCancellationPreservesStandaloneEscapeForReplacement(t *testing.T) {
	input := newTerminalInputPump(nil)
	input.enqueue(terminalReadResult{data: []byte("\x1b")}, 1)
	var activeGeneration atomic.Uint64

	oldCtx, cancelOld := context.WithCancel(context.Background())
	oldClock := newAttachPaletteClock()
	oldDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: oldCtx, cancel: cancelOld, input: input, out: make(chan ports.Frame, 1),
			clock: oldClock, logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(oldDone)
	}()
	// The ambiguity timer proves the ESC was scanned and its raw read was
	// committed; only the marker scanner's undecided suffix remains.
	<-oldClock.timers
	cancelOld()
	<-oldDone

	newCtx, cancelNew := context.WithCancel(context.Background())
	defer cancelNew()
	newClock := newAttachPaletteClock()
	out := make(chan ports.Frame, 1)
	newDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: newCtx, cancel: cancelNew, input: input, out: out,
			clock: newClock, logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(newDone)
	}()
	(<-newClock.timers).fire() // marker ambiguity deadline
	(<-newClock.timers).fire() // paste coalescer's lone-ESC deadline
	frame := <-out
	got, err := ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("\x1b"), got.Data)

	cancelNew()
	<-newDone
}

func TestTerminalInputPumpCancellationAfterInputBeforeOSCPreservesOnlyUndeliveredSuffix(t *testing.T) {
	input := newTerminalInputPump(nil)
	input.enqueue(terminalReadResult{data: []byte("x\x1b]10;rgb:ffff/0000/0000\x07y")}, 1)
	var activeGeneration atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan struct{})
	done := make(chan struct{})
	out := make(chan ports.Frame, 1)
	go func() {
		(&stdinPump{
			ctx: ctx, cancel: cancel, input: input, out: out, clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
			afterInputDelivered: func() {
				cancel()
				close(delivered)
			},
		}).run()
		close(done)
	}()
	frame := requireFrame(t, out, "timed out waiting for delivered input prefix")
	got, err := ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), got.Data)
	requireSignal(t, delivered, "timed out waiting for cancellation after input delivery")
	requireSignal(t, done, "timed out waiting for cancelled stdin pump")

	replacementCtx, cancelReplacement := context.WithCancel(context.Background())
	replacementOut := make(chan ports.Frame, 1)
	replacementDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: replacementCtx, cancel: cancelReplacement, input: input,
			out: replacementOut, clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(replacementDone)
	}()
	frame = requireFrame(t, replacementOut, "timed out waiting for undelivered input suffix")
	got, err = ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("y"), got.Data, "replacement must receive only the undelivered ordinary suffix")

	cancelReplacement()
	requireSignal(t, replacementDone, "timed out waiting for replacement stdin pump shutdown")
}

func TestTerminalInputPumpCancellationPreservesCoalescerHeldSuffix(t *testing.T) {
	input := newTerminalInputPump(nil)
	held := ports.BracketedPasteOpenMarker[:3]
	raw := append([]byte("x\x1b]10;rgb:ffff/0000/0000\x07"), held...)
	input.enqueue(terminalReadResult{data: raw}, 1)
	var activeGeneration atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan ports.Frame, 1)
	done := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: ctx, cancel: cancel, input: input, out: out, clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration:    &activeGeneration,
			afterInputDelivered: cancel,
		}).run()
		close(done)
	}()
	frame := requireFrame(t, out, "timed out waiting for delivered input prefix")
	got, err := ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), got.Data)
	requireSignal(t, done, "timed out waiting for cancelled stdin pump")

	replacementCtx, cancelReplacement := context.WithCancel(context.Background())
	replacementClock := newAttachPaletteClock()
	replacementOut := make(chan ports.Frame, 1)
	replacementDone := make(chan struct{})
	go func() {
		(&stdinPump{
			ctx: replacementCtx, cancel: cancelReplacement, input: input,
			out: replacementOut, clock: replacementClock,
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
		}).run()
		close(replacementDone)
	}()
	requireTimer(t, replacementClock.timers, "timed out waiting for paste-prefix ambiguity timer").fire()
	frame = requireFrame(t, replacementOut, "timed out waiting for coalescer-held suffix")
	got, err = ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, held, got.Data, "replacement must receive the coalescer-held suffix exactly once")

	cancelReplacement()
	requireSignal(t, replacementDone, "timed out waiting for replacement stdin pump shutdown")
}

func TestTerminalInputPumpRejectsReadFromPriorActivation(t *testing.T) {
	reader := newLateStopReader()
	input := newTerminalInputPump(reader)
	input.start()
	requireSignal(t, reader.started, "timed out waiting for initial activation Read")

	input.suspend()
	input.resume()
	reader.release <- terminalReadResult{data: []byte("stale")}

	// The old read must be discarded after resume, then the reader may begin a
	// new Read for the current activation.
	requireSignal(t, reader.started, "timed out waiting for current activation Read")
	consumer := input.claim()
	_, ok := input.take(context.Background(), consumer)
	require.False(t, ok, "a Read authorized before suspend/resume must not reach the new activation")
	input.revoke(consumer)

	input.stop()
	reader.release <- terminalReadResult{}
	requireSignal(t, input.exited, "timed out waiting for terminal input pump shutdown")
}

func TestTerminalInputPumpCancellationAfterDeliveredCallbackDoesNotReplay(t *testing.T) {
	input := newTerminalInputPump(nil)
	input.enqueue(terminalReadResult{data: []byte("x")}, 1)
	var activeGeneration atomic.Uint64
	ctx, cancel := context.WithCancel(context.Background())
	delivered := make(chan struct{})
	done := make(chan struct{})
	out := make(chan ports.Frame, 1)
	go func() {
		(&stdinPump{
			ctx: ctx, cancel: cancel, input: input, out: out, clock: newAttachPaletteClock(),
			logger: slog.New(slog.DiscardHandler), paletteEvents: make(chan paletteGenerationEvent),
			activeGeneration: &activeGeneration,
			afterInputDelivered: func() {
				cancel()
				close(delivered)
			},
		}).run()
		close(done)
	}()
	frame := <-out
	got, err := ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), got.Data)
	<-delivered
	<-done

	consumer := input.claim()
	_, ok := input.take(context.Background(), consumer)
	require.False(t, ok, "a callback already delivered to the sender must not replay after reconnect")
	input.revoke(consumer)
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
