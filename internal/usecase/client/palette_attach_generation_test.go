package client

import (
	"context"
	"io"
	"log/slog"
	"sync"
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

type attachPaletteTimer struct{ ch chan time.Time }

func newAttachPaletteClock() *attachPaletteClock {
	return &attachPaletteClock{timers: make(chan *attachPaletteTimer, 16)}
}

func (*attachPaletteClock) Now() time.Time { return time.Time{} }
func (c *attachPaletteClock) NewTimer(time.Duration) ports.Timer {
	timer := &attachPaletteTimer{ch: make(chan time.Time, 1)}
	c.timers <- timer
	return timer
}
func (t *attachPaletteTimer) C() <-chan time.Time    { return t.ch }
func (*attachPaletteTimer) Reset(time.Duration) bool { return false }
func (*attachPaletteTimer) Stop() bool               { return true }
func (t *attachPaletteTimer) fire()                  { t.ch <- time.Time{} }

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
}

func newAttachPaletteWriter() *attachPaletteWriter {
	return &attachPaletteWriter{wrote: make(chan string, 16)}
}
func (w *attachPaletteWriter) Write(data []byte) (int, error) {
	copyData := string(data)
	w.mu.Lock()
	w.writes = append(w.writes, copyData)
	w.mu.Unlock()
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
	mu       sync.Mutex
	frames   []ports.Frame
	themes   []ports.Theme
	themeCh  chan ports.Theme
	inputCh  chan ports.Input
	detached chan ports.Frame
	welcomed bool
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
	t.mu.Unlock()
	switch frame.Type {
	case ports.MsgTheme:
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
	clock := newAttachPaletteClock()
	reader := newAttachPaletteReader()
	writer := newAttachPaletteWriter()
	term := &attachPaletteTerminalHarness{in: reader, out: writer, resize: make(chan domain.Size)}
	transport := newAttachPaletteTransport()
	ms := &milestones{}
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler)}
	attempt := &attachAttempt{
		runner: runner, transport: transport, request: AttachRequest{Intent: ports.IntentAttach},
		milestones: ms, themeState: state,
		enterRaw:  func() error { ms.rawEntered = true; return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
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

	// The first marker is the late drain response. The second is the batch
	// completion marker. Colors between them prove the first cannot finalize.
	h.reader.send("\x1b[?2031;1$y\x1b]10;#111213\a\x1b]11;#141516\a\x1b]4;3;#212223\a\x1b[?2031;1$y")
	final := h.nextTheme(t)
	require.Equal(t, renderer.RGB{R: 17, G: 18, B: 19}, final.Foreground)
	require.Equal(t, renderer.RGB{R: 20, G: 21, B: 22}, final.Background)
	require.Equal(t, uint16(1<<3), final.PaletteKnown)
	// The marker finalization cancels, rather than fires, the independently
	// armed completion deadline.
	_ = completionDeadline
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
