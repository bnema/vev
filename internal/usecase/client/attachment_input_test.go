package client

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/protocol"
)

// Ported from the old client's palette_attach_generation_test.go and paste
// tests: the attached worker now owns terminal input demultiplexing.

// inputTestReader hands each queued chunk to exactly one terminal Read.
type inputTestReader struct{ chunks chan []byte }

func (r *inputTestReader) Read(p []byte) (int, error) {
	data, ok := <-r.chunks
	if !ok {
		return 0, io.EOF
	}
	return copy(p, data), nil
}

type inputHarness struct {
	stream *sessionTestStream
	term   *workerTestTerminal
	clock  *supervisorTestClock
	reader *inputTestReader
	events chan AttachmentEvent
}

// startInputHarness runs one real worker through the real foreground host and
// terminal input pump until its initial publication is committed. The worker
// clock is separate from the host clock, so every timer on it belongs to the
// input path.
func startInputHarness(t *testing.T, themes *terminalThemeState) *inputHarness {
	t.Helper()
	reader := &inputTestReader{chunks: make(chan []byte, 16)}
	pump := newTerminalInputPump(reader)
	pump.start()
	t.Cleanup(func() {
		pump.stop()
		close(reader.chunks)
	})
	h := &inputHarness{
		stream: newSessionTestStream(),
		term:   newWorkerTestTerminal(),
		clock:  newSupervisorTestClock(),
		reader: reader,
		events: make(chan AttachmentEvent, 1),
	}
	host := newAttachmentHost(attachmentHostConfig{Terminal: h.term, Clock: newSupervisorTestClock(), Input: pump})
	cfg := sessionTestWorkerConfig(sessionTestRequest(true))
	cfg.Clock = h.clock
	cfg.Theme = themes
	cfg.TrueColor = true
	worker, err := newSessionAttachmentWorker(cfg)
	require.NoError(t, err)
	go func() {
		event, _ := host.Run(context.Background(), AttachmentToken{Generation: 4, Attempt: 1}, worker, h.stream)
		h.events <- event
	}()
	awaitHello(t, h.stream)
	h.stream.deliver(protocol.Welcome{SessionName: "alpha"})
	h.stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
	return h
}

func (h *inputHarness) send(data string) { h.reader.chunks <- []byte(data) }

func (h *inputHarness) end(t *testing.T) {
	t.Helper()
	h.stream.deliver(protocol.Detached{Reason: protocol.ReasonDetach})
	select {
	case event := <-h.events:
		require.Equal(t, AttachmentEventEnded, event.Kind)
	case <-time.After(5 * time.Second):
		t.Fatal("the worker never settled")
	}
}

func (h *inputHarness) themes() []protocol.Theme {
	var themes []protocol.Theme
	for _, message := range h.stream.messages() {
		if theme, ok := message.(protocol.Theme); ok {
			themes = append(themes, theme)
		}
	}
	return themes
}

func (h *inputHarness) inputs() []string {
	var inputs []string
	for _, message := range h.stream.messages() {
		if input, ok := message.(protocol.Input); ok {
			inputs = append(inputs, string(input.Data))
		}
	}
	return inputs
}

// awaitThemes waits for the nth Theme and returns every Theme sent so far.
func (h *inputHarness) awaitThemes(t *testing.T, n int) []protocol.Theme {
	t.Helper()
	require.Eventually(t, func() bool { return len(h.themes()) >= n }, 5*time.Second, time.Millisecond, "worker never sent Theme #%d", n)
	return h.themes()
}

func (h *inputHarness) awaitInputs(t *testing.T, n int) []string {
	t.Helper()
	require.Eventually(t, func() bool { return len(h.inputs()) >= n }, 5*time.Second, time.Millisecond, "worker never sent Input #%d", n)
	return h.inputs()
}

// awaitQueries waits until query was written n times after the frame.
func (h *inputHarness) awaitQueries(t *testing.T, query string, n int) {
	t.Helper()
	require.Eventually(t, func() bool { return strings.Count(h.term.written(), query) >= n }, 5*time.Second, time.Millisecond, "worker never wrote query #%d %q", n, query)
}

func paletteReply(foreground, background string, slot uint8, color string) string {
	return "\x1b]10;" + foreground + "\a\x1b]11;" + background + "\a\x1b]4;" + string(rune('0'+slot)) + ";" + color + "\a\x1b[?2031;1$y"
}

func TestAttachmentInputPublishesClearedThenDefinitivePalette(t *testing.T) {
	themes := &terminalThemeState{}
	first := startInputHarness(t, themes)
	first.awaitQueries(t, paletteColorBatch, 1)
	require.True(t, strings.HasPrefix(first.term.written(), "\x1b[Hready"), "the query follows the committed frame")
	cleared := first.awaitThemes(t, 1)[0]
	require.True(t, cleared.TrueColor)
	require.Zero(t, cleared.PaletteKnown)
	require.False(t, cleared.HasForeground)

	first.send(paletteReply("#010203", "#040506", 2, "#102030"))
	final := first.awaitThemes(t, 2)[1]
	require.True(t, final.HasForeground)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, final.Foreground)
	require.True(t, final.HasBackground)
	require.Equal(t, renderer.RGB{R: 4, G: 5, B: 6}, final.Background)
	require.Equal(t, uint16(1<<2), final.PaletteKnown)
	require.Equal(t, renderer.RGB{R: 16, G: 32, B: 48}, final.Palette[2])
	first.end(t)
	require.Empty(t, first.inputs(), "terminal replies never reach the session")

	// A replacement attachment clears only the palette: the retained default
	// colors avoid a neutral flash, and no restore Theme precedes it.
	second := startInputHarness(t, themes)
	second.awaitQueries(t, paletteColorBatch, 1)
	cleared = second.awaitThemes(t, 1)[0]
	require.True(t, cleared.HasForeground)
	require.Equal(t, final.Foreground, cleared.Foreground)
	require.Equal(t, final.Background, cleared.Background)
	require.Zero(t, cleared.PaletteKnown)
	require.Equal(t, [16]renderer.RGB{}, cleared.Palette)
	second.send(paletteReply("#111213", "#141516", 2, "#202122"))
	second.awaitThemes(t, 2)
	second.end(t)
	require.Len(t, second.themes(), 2)
}

func TestAttachmentInputSchemeNotificationReplacesPalette(t *testing.T) {
	h := startInputHarness(t, &terminalThemeState{})
	h.awaitQueries(t, paletteColorBatch, 1)
	_ = h.clock.awaitTimer(t) // initial completion deadline, cancelled by the marker
	h.send(paletteReply("#010203", "#040506", 2, "#102030"))
	h.awaitThemes(t, 2)

	h.send("\x1b[?997;2n")
	cleared := h.awaitThemes(t, 3)[2]
	require.True(t, cleared.SchemeKnown)
	require.True(t, cleared.Light)
	require.Zero(t, cleared.PaletteKnown)
	h.awaitQueries(t, paletteBoundaryQuery, 2) // one inside the first batch, then the drain
	drain := h.clock.awaitTimer(t)
	require.Equal(t, paletteGenerationDeadline, drain.delay)

	// The drain deadline starts the color batch even though no reply arrives,
	// and the completion deadline finalizes the partial generation.
	drain.fire()
	h.awaitQueries(t, paletteColorBatch, 2)
	h.clock.awaitTimer(t).fire()
	final := h.awaitThemes(t, 4)[3]
	require.True(t, final.SchemeKnown)
	require.True(t, final.Light)
	require.False(t, final.HasForeground)
	require.False(t, final.HasBackground)
	h.end(t)
	require.Len(t, h.themes(), 4, "each generation publishes only a cleared then a definitive Theme")
	require.Empty(t, h.inputs(), "the scheme notification never reaches the session")
}

func TestAttachmentInputLateDrainCannotFinalizeReplacementEarly(t *testing.T) {
	h := startInputHarness(t, &terminalThemeState{})
	h.awaitQueries(t, paletteColorBatch, 1)
	_ = h.clock.awaitTimer(t)
	h.send(paletteReply("#010203", "#040506", 2, "#102030"))
	h.awaitThemes(t, 2)

	h.send("\x1b[?997;1n")
	h.awaitThemes(t, 3)
	h.clock.awaitTimer(t).fire() // drain deadline
	h.awaitQueries(t, paletteColorBatch, 2)
	_ = h.clock.awaitTimer(t) // completion deadline, cancelled by the second marker

	// The drain's stale reports arrive before its late boundary and must not
	// contaminate the replacement. Reports after it belong to the batch.
	h.send("\x1b]10;#a1a2a3\a\x1b]11;#a4a5a6\a\x1b]4;2;#a7a8a9\a\x1b[?2031;1$y\x1b]10;#111213\a\x1b]11;#141516\a\x1b]4;3;#212223\a\x1b[?2031;1$y")
	final := h.awaitThemes(t, 4)[3]
	require.Equal(t, renderer.RGB{R: 17, G: 18, B: 19}, final.Foreground)
	require.Equal(t, renderer.RGB{R: 20, G: 21, B: 22}, final.Background)
	require.Equal(t, uint16(1<<3), final.PaletteKnown)
	h.end(t)
}

func TestAttachmentInputForwardsStandaloneEscapeAfterAmbiguityDeadlines(t *testing.T) {
	h := startInputHarness(t, &terminalThemeState{})
	h.awaitQueries(t, paletteColorBatch, 1)
	_ = h.clock.awaitTimer(t) // initial completion deadline

	// A bare Escape may start a DECRQM reply, then a bracketed-paste marker.
	// Each window elapses on the clock; no later read is needed.
	h.send("\x1b")
	marker := h.clock.awaitTimer(t)
	require.Equal(t, paletteMarkerAmbiguityDeadline, marker.delay)
	require.Empty(t, h.inputs())
	marker.fire()
	paste := h.clock.awaitTimer(t)
	require.Equal(t, pasteFlushDelay, paste.delay)
	require.Empty(t, h.inputs())
	paste.fire()
	require.Equal(t, []string{"\x1b"}, h.awaitInputs(t, 1))
	h.end(t)
}

func TestAttachmentInputConsumesSplitMarkerAndCancelsAmbiguityDeadline(t *testing.T) {
	h := startInputHarness(t, &terminalThemeState{})
	h.awaitQueries(t, paletteColorBatch, 1)
	_ = h.clock.awaitTimer(t)
	h.awaitThemes(t, 1)

	h.send("before\x1b[?2031;")
	marker := h.clock.awaitTimer(t)
	h.send("1$yafter")
	final := h.awaitThemes(t, 2)[1]
	require.False(t, final.HasForeground)
	require.Equal(t, "beforeafter", strings.Join(h.awaitInputs(t, 2), ""))
	require.True(t, marker.stopped(), "the completed marker cancels its ambiguity deadline")
	h.end(t)
}

func TestAttachmentInputPartialOSCPublishesNothingUntilComplete(t *testing.T) {
	h := startInputHarness(t, &terminalThemeState{})
	h.awaitQueries(t, paletteColorBatch, 1)
	h.awaitThemes(t, 1)

	h.send("\x1b]10;#0102")
	h.send("03\az")
	require.Equal(t, []string{"z"}, h.awaitInputs(t, 1))
	require.Len(t, h.themes(), 1, "a partial OSC reply never publishes Theme")

	h.send("\x1b[?2031;1$y")
	final := h.awaitThemes(t, 2)[1]
	require.True(t, final.HasForeground)
	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, final.Foreground)
	h.end(t)
	require.Equal(t, []string{"z"}, h.inputs())
}

// TestAttachmentInputFraming covers the session-bound framing without palette
// detection: replies are stripped, and a bracketed paste split across reads
// or markers reaches the daemon as one Input.
func TestAttachmentInputFraming(t *testing.T) {
	tests := []struct {
		name  string
		reads []string
		want  []string
	}{
		{name: "plain keys", reads: []string{"abc"}, want: []string{"abc"}},
		{name: "mouse report stays intact", reads: []string{"\x1b[<0;1;1M"}, want: []string{"\x1b[<0;1;1M"}},
		{name: "scheme notification and color reply are stripped", reads: []string{"a\x1b[?997;1nb\x1b]11;rgb:0000/0000/0000\ac"}, want: []string{"abc"}},
		{name: "paste split across reads", reads: []string{"\x1b[200~hello\n", "world\x1b[201~"}, want: []string{"\x1b[200~hello\nworld\x1b[201~"}},
		{name: "paste split inside the opening marker", reads: []string{"ab\x1b[20", "0~x\x1b[201~z"}, want: []string{"ab", "\x1b[200~x\x1b[201~", "z"}},
		{name: "paste split after its escape", reads: []string{"ab\x1b", "[200~x\x1b[201~"}, want: []string{"ab", "\x1b[200~x\x1b[201~"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := startInputHarness(t, nil)
			for _, read := range tt.reads {
				h.send(read)
			}
			require.Equal(t, tt.want, h.awaitInputs(t, len(tt.want)))
			h.end(t)
			require.Equal(t, tt.want, h.inputs())
			require.Empty(t, h.themes(), "no Theme without a retained theme state")
			require.Equal(t, "\x1b[Hready", h.term.written(), "no palette query without a retained theme state")
		})
	}
}
