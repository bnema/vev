package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

// attachPaletteClock deliberately exposes each independently-created timer.
// Tests fire only the deadline they intend to exercise; no wall clock is used.
type attachPaletteClock struct{ timers chan *attachPaletteTimer }

type attachPaletteTimer struct {
	ch       chan time.Time
	stopped  chan struct{}
	stopOnce sync.Once
}

func newAttachPaletteClock() *attachPaletteClock {
	return &attachPaletteClock{timers: make(chan *attachPaletteTimer, 16)}
}

func (*attachPaletteClock) Now() time.Time { return time.Time{} }
func (c *attachPaletteClock) NewTimer(time.Duration) ports.Timer {
	timer := &attachPaletteTimer{ch: make(chan time.Time, 1), stopped: make(chan struct{})}
	c.timers <- timer
	return timer
}
func (t *attachPaletteTimer) C() <-chan time.Time    { return t.ch }
func (*attachPaletteTimer) Reset(time.Duration) bool { return false }
func (t *attachPaletteTimer) Stop() bool {
	t.stopOnce.Do(func() { close(t.stopped) })
	return true
}
func (t *attachPaletteTimer) fire() { t.ch <- time.Time{} }

type attachPaletteReader struct{ chunks chan []byte }

func newAttachPaletteReader() *attachPaletteReader {
	return &attachPaletteReader{chunks: make(chan []byte, 16)}
}
func (r *attachPaletteReader) Read(p []byte) (int, error) {
	data := <-r.chunks
	return copy(p, data), nil
}
func (r *attachPaletteReader) send(data string) { r.chunks <- []byte(data) }

type attachPaletteWriter struct {
	mu     sync.Mutex
	writes []string
	wrote  chan string
	err    error
}

func newAttachPaletteWriter() *attachPaletteWriter {
	return &attachPaletteWriter{wrote: make(chan string, 16)}
}
func (w *attachPaletteWriter) Write(data []byte) (int, error) {
	copyData := string(data)
	w.mu.Lock()
	w.writes = append(w.writes, copyData)
	err := w.err
	w.mu.Unlock()
	if err != nil {
		return 0, err
	}
	w.wrote <- copyData
	return len(data), nil
}

type attachPaletteTerminalHarness struct {
	in     io.Reader
	out    *attachPaletteWriter
	resize chan domain.Size
}

func (t *attachPaletteTerminalHarness) EnterRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (*attachPaletteTerminalHarness) Size() (domain.Size, error) {
	return domain.Size{Cols: 80, Rows: 24}, nil
}
func (t *attachPaletteTerminalHarness) ResizeEvents() <-chan domain.Size { return t.resize }
func (t *attachPaletteTerminalHarness) In() io.Reader                    { return t.in }
func (t *attachPaletteTerminalHarness) Out() io.Writer                   { return t.out }
func (*attachPaletteTerminalHarness) Flush() error                       { return nil }

type attachPaletteTransport struct {
	mu           sync.Mutex
	frames       []ports.Frame
	themes       []ports.Theme
	themeCh      chan ports.Theme
	inputCh      chan ports.Input
	detached     chan ports.Frame
	themeSendErr error
	welcomed     bool
}

func newAttachPaletteTransport() *attachPaletteTransport {
	return &attachPaletteTransport{
		themeCh:  make(chan ports.Theme, 16),
		inputCh:  make(chan ports.Input, 16),
		detached: make(chan ports.Frame, 1),
	}
}
func (t *attachPaletteTransport) Send(frame ports.Frame) error {
	t.mu.Lock()
	t.frames = append(t.frames, frame)
	themeSendErr := t.themeSendErr
	t.mu.Unlock()
	switch frame.Type {
	case ports.MsgTheme:
		if themeSendErr != nil {
			return themeSendErr
		}
		got, err := ports.UnmarshalTheme(frame.Payload)
		if err != nil {
			return err
		}
		t.mu.Lock()
		t.themes = append(t.themes, got)
		t.mu.Unlock()
		t.themeCh <- got
	case ports.MsgInput:
		got, err := ports.UnmarshalInput(frame.Payload)
		if err != nil {
			return err
		}
		t.inputCh <- got
	}
	return nil
}
func (t *attachPaletteTransport) Recv() (ports.Frame, error) {
	t.mu.Lock()
	if !t.welcomed {
		t.welcomed = true
		t.mu.Unlock()
		return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s"})}, nil
	}
	t.mu.Unlock()
	return <-t.detached, nil
}
func (*attachPaletteTransport) Close() error { return nil }
func (t *attachPaletteTransport) snapshots() []ports.Theme {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ports.Theme(nil), t.themes...)
}

type attachPaletteHarness struct {
	clock     *attachPaletteClock
	reader    *attachPaletteReader
	writer    *attachPaletteWriter
	transport *attachPaletteTransport
	done      chan attachResult
}

func startAttachPaletteHarness(state *terminalThemeState) *attachPaletteHarness {
	return startAttachPaletteHarnessWithTransport(state, newAttachPaletteTransport())
}

func startAttachPaletteHarnessWithTransport(state *terminalThemeState, transport *attachPaletteTransport) *attachPaletteHarness {
	return startAttachPaletteHarnessWithInput(state, transport, nil)
}

func startAttachPaletteHarnessWithInput(state *terminalThemeState, transport *attachPaletteTransport, input *terminalInputPump) *attachPaletteHarness {
	return startAttachPaletteHarnessWithInputAndWriteError(state, transport, input, nil)
}

func startAttachPaletteHarnessWithInputAndWriteError(state *terminalThemeState, transport *attachPaletteTransport, input *terminalInputPump, writeErr error) *attachPaletteHarness {
	clock := newAttachPaletteClock()
	reader := newAttachPaletteReader()
	writer := newAttachPaletteWriter()
	writer.err = writeErr
	term := &attachPaletteTerminalHarness{in: reader, out: writer, resize: make(chan domain.Size)}
	ms := &milestones{}
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler)}
	attempt := &attachAttempt{
		runner: runner, transport: transport, request: AttachRequest{Intent: ports.IntentAttach},
		milestones: ms, themeState: state,
		enterRaw:  func() error { ms.rawEntered = true; return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
		terminalInput: func() *terminalInputPump {
			return input
		},
	}
	h := &attachPaletteHarness{clock: clock, reader: reader, writer: writer, transport: transport, done: make(chan attachResult, 1)}
	go func() { h.done <- attempt.run(context.Background()) }()
	// The handshake's two bounded-pre-welcome timers are unrelated to palette
	// acquisition. Remove them in this harness so every exposed timer is a
	// generation deadline (or a timer deliberately created by input handling).
	<-clock.timers
	<-clock.timers
	return h
}

func (h *attachPaletteHarness) nextWrite(t *testing.T) string {
	t.Helper()
	return <-h.writer.wrote
}
func (h *attachPaletteHarness) nextTheme(t *testing.T) ports.Theme {
	t.Helper()
	return <-h.transport.themeCh
}
func (h *attachPaletteHarness) nextTimer(t *testing.T) *attachPaletteTimer {
	t.Helper()
	return <-h.clock.timers
}
func (h *attachPaletteHarness) detach(t *testing.T) {
	t.Helper()
	h.transport.detached <- ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach})}
	require.NoError(t, (<-h.done).err)
}

func paletteReply(foreground, background string, slot uint8, color string) string {
	return "\x1b]10;" + foreground + "\a\x1b]11;" + background + "\a\x1b]4;" + string(rune('0'+slot)) + ";" + color + "\a\x1b[?2031;1$y"
}

func TestAttachReconnectClearsRetainedPaletteInOnePublication(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	state := &terminalThemeState{}
	first := startAttachPaletteHarness(state)
	require.Equal(t, paletteColorBatch, first.nextWrite(t))
	first.reader.send(paletteReply("#010203", "#040506", 2, "#102030"))
	_ = first.nextTheme(t) // initial cleared snapshot
	final := first.nextTheme(t)
	require.True(t, final.HasForeground)
	first.detach(t)

	reconnected := startAttachPaletteHarness(state)
	require.Equal(t, paletteColorBatch, reconnected.nextWrite(t))
	cleared := reconnected.nextTheme(t)
	require.True(t, cleared.HasForeground)
	require.Equal(t, final.Foreground, cleared.Foreground)
	require.True(t, cleared.HasBackground)
	require.Equal(t, final.Background, cleared.Background)
	require.Zero(t, cleared.PaletteKnown)
	require.Equal(t, [16]renderer.RGB{}, cleared.Palette)

	reconnected.reader.send(paletteReply("#111213", "#141516", 2, "#202122"))
	_ = reconnected.nextTheme(t)
	reconnected.detach(t)
	require.Len(t, reconnected.transport.snapshots(), 2, "reconnect must not send a restore Theme before its cleared publication")
}

func TestAttachReconnectPreservesStandaloneEscapeBeforeClaimRevocation(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	reader := newAttachPaletteReader()
	input := newTerminalInputPump(reader)
	input.start()
	t.Cleanup(input.stop)

	first := startAttachPaletteHarnessWithInput(&terminalThemeState{}, newAttachPaletteTransport(), input)
	require.Equal(t, paletteColorBatch, first.nextWrite(t))
	_ = first.nextTheme(t)
	_ = first.nextTimer(t) // initial palette completion deadline

	// A standalone Escape is withheld as a possible DECRQM marker prefix. Its
	// raw read is committed, leaving only the scanner-local residual to hand
	// over when this attach is replaced.
	reader.send("\x1b")
	_ = first.nextTimer(t) // standalone Escape ambiguity deadline

	revoked := make(chan []byte, 1)
	allowRevocationToReturn := make(chan struct{})
	var releaseRevocation sync.Once
	release := func() { releaseRevocation.Do(func() { close(allowRevocationToReturn) }) }
	defer release()
	input.afterRevoke = func() {
		input.mu.Lock()
		residual := append([]byte(nil), input.residual...)
		input.mu.Unlock()
		revoked <- residual
		<-allowRevocationToReturn
	}

	// Detach starts reconnect teardown. The residual must already be owned by
	// the lifecycle reader at the precise point the old claim is revoked.
	first.transport.detached <- ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach})}
	require.Equal(t, []byte("\x1b"), <-revoked)
	release()
	require.NoError(t, (<-first.done).err)
	input.afterRevoke = nil

	// The replacement attach receives the retained byte exactly once.
	second := startAttachPaletteHarnessWithInput(&terminalThemeState{}, newAttachPaletteTransport(), input)
	require.Equal(t, paletteColorBatch, second.nextWrite(t))
	_ = second.nextTheme(t)
	_ = second.nextTimer(t) // initial palette completion deadline
	second.nextTimer(t).fire()
	second.nextTimer(t).fire()
	got := <-second.transport.inputCh
	require.Equal(t, []byte("\x1b"), got.Data)
	select {
	case duplicate := <-second.transport.inputCh:
		require.Failf(t, "duplicate input", "unexpected duplicate %q", duplicate.Data)
	default:
	}
	second.detach(t)
}

func TestAttachSchemeReplacementAndDeadlinesProgressWhileReadBlocks(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	h := startAttachPaletteHarness(&terminalThemeState{})
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	initialDeadline := h.nextTimer(t)
	h.reader.send(paletteReply("#010203", "#040506", 2, "#102030"))
	_ = h.nextTheme(t)
	_ = h.nextTheme(t)
	// The initial completion timer was cancelled by the marker; it is distinct
	// from both replacement deadlines.
	_ = initialDeadline

	h.reader.send("\x1b[?997;2n")
	cleared := h.nextTheme(t)
	require.True(t, cleared.SchemeKnown)
	require.True(t, cleared.Light)
	require.Zero(t, cleared.PaletteKnown)
	require.Equal(t, paletteBoundaryQuery, h.nextWrite(t))
	drainDeadline := h.nextTimer(t)

	// stdin is now blocked in Read. Firing the independently-created drain
	// deadline still starts the color batch, and its completion deadline still
	// finalizes the partial generation.
	drainDeadline.fire()
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	completionDeadline := h.nextTimer(t)
	completionDeadline.fire()
	final := h.nextTheme(t)
	require.True(t, final.SchemeKnown)
	require.True(t, final.Light)
	require.False(t, final.HasForeground)
	require.False(t, final.HasBackground)
	h.detach(t)
	require.Len(t, h.transport.snapshots(), 4, "initial and replacement generations each publish only cleared then definitive Theme snapshots")
}

func TestAttachInitialPaletteFailureRevokesInputClaim(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	for _, tc := range []struct {
		name     string
		start    func(*terminalInputPump) *attachPaletteHarness
		contains string
	}{
		{
			name: "publish",
			start: func(input *terminalInputPump) *attachPaletteHarness {
				want := errors.New("initial theme send failed")
				transport := newAttachPaletteTransport()
				transport.themeSendErr = want
				return startAttachPaletteHarnessWithInput(&terminalThemeState{}, transport, input)
			},
			contains: "publishing theme",
		},
		{
			name: "write",
			start: func(input *terminalInputPump) *attachPaletteHarness {
				return startAttachPaletteHarnessWithInputAndWriteError(&terminalThemeState{}, newAttachPaletteTransport(), input, errors.New("palette write failed"))
			},
			contains: "writing palette query",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			input := newTerminalInputPump(nil)
			failed := tc.start(input)
			err := (<-failed.done).err
			require.ErrorContains(t, err, tc.contains)

			// The next attach must be able to claim the lifecycle-owned reader.
			// Before revocation was registered at claim time this panicked above.
			reconnected := startAttachPaletteHarnessWithInput(&terminalThemeState{}, newAttachPaletteTransport(), input)
			require.Equal(t, paletteColorBatch, reconnected.nextWrite(t))
			reconnected.detach(t)
		})
	}
}

func TestStdinPumpSameReadSchemeAndOldMarkerCannotAdvanceReplacement(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	coordinator := newPaletteGenerationCoordinator()
	coordinator.start(ports.Theme{}, false)
	var activeGeneration atomic.Uint64
	activeGeneration.Store(uint64(coordinator.current.id))

	// An unbuffered event channel forces the attach loop to process the scheme
	// notification and publish its replacement generation before the scanner
	// can deliver the old marker from the same terminal Read.
	events := make(chan paletteGenerationEvent)
	out := make(chan ports.Frame, 1)
	schemeHandled := make(chan struct{})
	pump := &stdinPump{
		ctx:              ctx,
		cancel:           cancel,
		in:               strings.NewReader("\x1b[?997;2n\x1b[?2031;1$y"),
		out:              out,
		clock:            newAttachPaletteClock(),
		logger:           slog.New(slog.DiscardHandler),
		paletteEvents:    events,
		activeGeneration: &activeGeneration,
		afterPaletteEvent: func(event paletteGenerationEvent) {
			if event.kind == paletteEventScheme {
				<-schemeHandled
			}
		},
	}
	pumpDone := make(chan struct{})
	go func() {
		pump.run()
		close(pumpDone)
	}()

	scheme := <-events
	require.Equal(t, paletteEventScheme, scheme.kind)
	oldID := scheme.id

	// This models attachAttempt handling the scheme event. The replacement is
	// draining, so a marker tagged with its ID would incorrectly start its
	// color batch before the real drain boundary arrives.
	coordinator.start(ports.Theme{SchemeKnown: true, Light: scheme.light}, true)
	newID := coordinator.current.id
	activeGeneration.Store(uint64(newID))
	close(schemeHandled)
	require.NotEqual(t, oldID, newID)
	require.Equal(t, generationDraining, coordinator.current.phase)

	oldMarker := <-events
	require.Equal(t, paletteEventMarker, oldMarker.kind)
	require.Equal(t, oldID, oldMarker.id, "one read must retain its generation tag")
	require.Empty(t, coordinator.handle(oldMarker))
	require.True(t, coordinator.active)
	require.Equal(t, generationDraining, coordinator.current.phase)
	require.Equal(t, newID, coordinator.current.id)

	<-pumpDone
}

func TestAttachLateDrainCannotFinalizeReplacementEarly(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	h := startAttachPaletteHarness(&terminalThemeState{})
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	_ = h.nextTimer(t)
	h.reader.send(paletteReply("#010203", "#040506", 2, "#102030"))
	_ = h.nextTheme(t)
	_ = h.nextTheme(t)

	h.reader.send("\x1b[?997;1n")
	_ = h.nextTheme(t)
	require.Equal(t, paletteBoundaryQuery, h.nextWrite(t))
	drainDeadline := h.nextTimer(t)
	drainDeadline.fire()
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	completionDeadline := h.nextTimer(t)

	// The drain's stale reports arrive before its late boundary and must not
	// contaminate the replacement accumulator. Reports after that boundary are
	// the batch's reports and are collected through the second marker.
	h.reader.send("\x1b]10;#a1a2a3\a\x1b]11;#a4a5a6\a\x1b]4;2;#a7a8a9\a\x1b[?2031;1$y\x1b]10;#111213\a\x1b]11;#141516\a\x1b]4;3;#212223\a\x1b[?2031;1$y")
	final := h.nextTheme(t)
	require.Equal(t, renderer.RGB{R: 17, G: 18, B: 19}, final.Foreground)
	require.Equal(t, renderer.RGB{R: 20, G: 21, B: 22}, final.Background)
	require.Equal(t, uint16(1<<3), final.PaletteKnown)
	// The marker finalization cancels, rather than fires, the independently
	// armed completion deadline.
	_ = completionDeadline
	h.detach(t)
}

func TestAttachForwardsStandaloneEscapeAfterAmbiguityDeadline(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	h := startAttachPaletteHarness(&terminalThemeState{})
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	_ = h.nextTimer(t) // initial palette completion deadline
	_ = h.nextTheme(t) // initial cleared publication

	// A bare Escape is ambiguous with the prefix of the DECRQM completion
	// response. It must not require another Read or EOF before reaching the
	// daemon.
	h.reader.send("\x1b")
	h.nextTimer(t).fire() // DECRQM ambiguity deadline
	h.nextTimer(t).fire() // bracketed-paste ambiguity deadline
	input := <-h.transport.inputCh
	require.Equal(t, []byte("\x1b"), input.Data)
	h.detach(t)
}

func TestAttachConsumesSplitMarkerAndCancelsAmbiguityDeadline(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	h := startAttachPaletteHarness(&terminalThemeState{})
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	_ = h.nextTimer(t) // initial palette completion deadline
	_ = h.nextTheme(t) // initial cleared publication

	h.reader.send("before\x1b[?2031;")
	markerDeadline := h.nextTimer(t)
	h.reader.send("1$yafter")

	final := h.nextTheme(t)
	require.False(t, final.HasForeground)
	var got []byte
	for len(got) < len("beforeafter") {
		got = append(got, (<-h.transport.inputCh).Data...)
	}
	require.Equal(t, []byte("beforeafter"), got)
	<-markerDeadline.stopped
	h.detach(t)
}

func TestAttachForwardsInputOnceAndPartialOSCDoesNotPublishTheme(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")
	h := startAttachPaletteHarness(&terminalThemeState{})
	require.Equal(t, paletteColorBatch, h.nextWrite(t))
	_ = h.nextTimer(t)
	cleared := h.nextTheme(t)
	require.Zero(t, cleared.PaletteKnown)

	h.reader.send("\x1b]10;#0102")
	h.reader.send("03\az")
	input := <-h.transport.inputCh
	require.Equal(t, []byte("z"), input.Data)
	require.Len(t, h.transport.snapshots(), 1, "partial and incomplete OSC replies must not publish Theme")

	h.reader.send("\x1b[?2031;1$y")
	final := h.nextTheme(t)
	require.True(t, final.HasForeground)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, final.Foreground)
	require.Len(t, h.transport.snapshots(), 2)
	h.detach(t)
}
