package client

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

var echoTestStart = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// echoHarness drives one predictor against a 20x3 mirror whose prompt "$ "
// leaves the cursor at row 0, column 2.
type echoHarness struct {
	t   *testing.T
	p   *echoPredictor
	now time.Time
	seq uint64
}

func newEchoHarness(t *testing.T, mode domain.EchoPredictMode) *echoHarness {
	t.Helper()
	h := &echoHarness{t: t, p: newEchoPredictor(mode), now: echoTestStart}
	h.p.applyOutput(protocol.Output{Epoch: 1, New: 1, Full: true, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("\x1b[H\x1b[2J$ ")}, h.now)
	return h
}

// warm sends one empty input and acknowledges it after rtt of network time.
func (h *echoHarness) warm(rtt time.Duration) {
	h.seq++
	h.p.input(h.seq, nil, h.now)
	h.now = h.now.Add(rtt + echoServerDelay)
	h.p.applyOutput(protocol.Output{Epoch: 1, Echo: h.seq}, h.now)
}

func (h *echoHarness) typed(data string) []byte {
	h.seq++
	h.p.input(h.seq, []byte(data), h.now)
	return h.p.render()
}

// daemon applies a state frame that writes data and acknowledges echo.
func (h *echoHarness) daemon(data string, echo uint64) []byte {
	h.now = h.now.Add(time.Millisecond)
	h.p.applyOutput(protocol.Output{Epoch: 1, New: 2, Echo: echo, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte(data)}, h.now)
	return h.p.render()
}

// confirm types a and lets the daemon echo it, which confirms the first epoch.
func (h *echoHarness) confirm() {
	h.t.Helper()
	if got := h.typed("a"); len(got) != 0 {
		h.t.Fatalf("first guess is tentative, rendered %q", got)
	}
	h.daemon("\x1b[1;3Ha", h.seq)
	if h.p.confEpoch == 0 {
		h.t.Fatal("echoed guess did not confirm its epoch")
	}
}

func TestEchoPredictorDisplay(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		mode domain.EchoPredictMode
		rtt  time.Duration
		want string
	}{
		{name: "fast link hides guesses", mode: domain.EchoPredictAdaptive, rtt: 10 * time.Millisecond, want: ""},
		{name: "slow link shows guesses", mode: domain.EchoPredictAdaptive, rtt: 100 * time.Millisecond, want: "\x1b[1;4H\x1b[0mb\x1b[0m\x1b[1;5H"},
		{name: "very slow link underlines guesses", mode: domain.EchoPredictAdaptive, rtt: 200 * time.Millisecond, want: "\x1b[1;4H\x1b[0;4mb\x1b[0m\x1b[1;5H"},
		{name: "always shows on a fast link", mode: domain.EchoPredictAlways, rtt: 10 * time.Millisecond, want: "\x1b[1;4H\x1b[0mb\x1b[0m\x1b[1;5H"},
		{name: "never predicts", mode: domain.EchoPredictNever, rtt: 200 * time.Millisecond, want: ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newEchoHarness(t, tt.mode)
			h.warm(tt.rtt)
			if tt.mode != domain.EchoPredictNever {
				h.confirm()
			}
			if got := string(h.typed("b")); got != tt.want {
				t.Fatalf("render = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestEchoPredictorNeverDrawsStaleGuesses covers daemon frames that arrive
// before the echo acknowledgement and move or replace the guessed row.
func TestEchoPredictorNeverDrawsStaleGuesses(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
		frame protocol.Output
	}{
		{
			name:  "enter then a scrolled prompt before the ack",
			input: "b\r",
			frame: protocol.Output{Epoch: 1, New: 2, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("b\r\nfoo\r\nbar\r\n$ ")},
		},
		{
			name:  "full frame before the ack",
			input: "b",
			frame: protocol.Output{Epoch: 1, New: 2, Full: true, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("\x1b[H\x1b[2Jother")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newEchoHarness(t, domain.EchoPredictAlways)
			h.warm(100 * time.Millisecond)
			h.confirm()
			h.typed(tt.input)
			h.p.undraw()
			h.p.applyOutput(tt.frame, h.now)
			if got := h.p.render(); bytes.Contains(got, []byte("b")) {
				t.Fatalf("render = %q redraws a guess over the new frame", got)
			}
		})
	}
}

func TestEchoPredictorEnterKeepsDrawnGuessesUntilAFrame(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.warm(100 * time.Millisecond)
	h.confirm()
	if got := h.typed("b"); !bytes.Contains(got, []byte("b")) {
		t.Fatalf("render = %q, want the guess drawn", got)
	}
	if got := h.typed("\r"); bytes.Contains(got, []byte(" ")) {
		t.Fatalf("render after Enter = %q erases the typed guess before any frame", got)
	}
}

func TestEchoPredictorUndrawRestoresMirror(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name, typed, want string
	}{
		{name: "drawn guess", typed: "b", want: "\x1b[1;4H\x1b[0m \x1b[0m\x1b[1;4H"},
		// A blank guess matches the mirror cell, so only the cursor moved.
		{name: "cursor-only guess", typed: " ", want: "\x1b[0m\x1b[1;4H"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newEchoHarness(t, domain.EchoPredictAlways)
			h.warm(100 * time.Millisecond)
			h.confirm()
			if got := h.typed(tt.typed); len(got) == 0 {
				t.Fatal("guess rendered nothing")
			}
			if got := string(h.p.undraw()); got != tt.want {
				t.Fatalf("undraw = %q, want %q", got, tt.want)
			}
			if got := h.p.undraw(); len(got) != 0 {
				t.Fatalf("second undraw = %q, want nothing", got)
			}
		})
	}
}

func TestEchoPredictorConfirmationRepaintsFromMirror(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(200 * time.Millisecond)
	h.confirm()
	if got := string(h.typed("b")); !strings.Contains(got, "\x1b[0;4mb") {
		t.Fatalf("pending guess is not underlined: %q", got)
	}
	got := string(h.daemon("\x1b[1;4Hb", h.seq))
	if want := "\x1b[1;4H\x1b[0mb\x1b[0m\x1b[1;5H"; got != want {
		t.Fatalf("confirmed render = %q, want %q", got, want)
	}
	if h.p.active() {
		t.Fatal("confirmed guess is still active")
	}
}

func TestEchoPredictorWrongEchoErasesGuess(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(100 * time.Millisecond)
	h.confirm()
	if got := string(h.typed("p")); !strings.Contains(got, "p") {
		t.Fatalf("guess not drawn: %q", got)
	}
	// A password prompt echoes nothing.
	got := string(h.daemon("", h.seq))
	if want := "\x1b[1;4H\x1b[0m \x1b[0m\x1b[1;4H"; got != want {
		t.Fatalf("render after wrong echo = %q, want %q", got, want)
	}
	if h.p.active() {
		t.Fatal("wrong guess survived")
	}
	// The reset makes the next guess tentative again.
	if got := h.typed("q"); len(got) != 0 {
		t.Fatalf("guess after reset rendered %q", got)
	}
}

func TestEchoPredictorBackspace(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(100 * time.Millisecond)
	h.confirm()
	got := string(h.typed("\x7f"))
	if want := "\x1b[1;3H\x1b[0m \x1b[0m\x1b[1;3H"; got != want {
		t.Fatalf("backspace render = %q, want %q", got, want)
	}
	got = string(h.daemon("\x1b[1;3H \x1b[1;3H", h.seq))
	if want := "\x1b[1;3H\x1b[0m \x1b[0m\x1b[1;3H"; got != want {
		t.Fatalf("confirmed backspace render = %q, want %q", got, want)
	}
	if h.p.active() {
		t.Fatal("confirmed backspace is still active")
	}
}

func TestEchoPredictorInsertShiftsText(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.warm(10 * time.Millisecond)
	// "$ ac" with the cursor on c.
	h.p.applyOutput(protocol.Output{Epoch: 1, New: 2, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("ac\x1b[1;4H")}, h.now)
	h.p.confEpoch = h.p.predEpoch
	got := string(h.typed("b"))
	if want := "\x1b[1;4H\x1b[0mb\x1b[1;5H\x1b[0mc\x1b[0m\x1b[1;5H"; got != want {
		t.Fatalf("insert render = %q, want %q", got, want)
	}
}

func TestEchoPredictorUnknownInputIsTentative(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		input string
	}{
		{name: "control byte", input: "\x03"},
		{name: "escape sequence", input: "\x1b[A"},
		{name: "kitty CSI-u key", input: "\x1b[97;5u"},
		{name: "carriage return", input: "\r"},
		{name: "wide rune", input: "界"},
		{name: "bracketed paste", input: "\x1b[200~text\x1b[201~"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newEchoHarness(t, domain.EchoPredictAdaptive)
			h.warm(100 * time.Millisecond)
			h.confirm()
			h.typed(tt.input)
			if got := h.typed("z"); len(got) != 0 {
				t.Fatalf("guess after %s rendered %q", tt.name, got)
			}
		})
	}
}

func TestEchoPredictorWrongTentativeGuessKillsItsEpoch(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(100 * time.Millisecond)
	h.confirm()
	confirmed := h.p.confEpoch
	h.typed("\x03z")
	// The daemon cleared the line instead of echoing z.
	h.daemon("\x1b[1;1H\x1b[2K$ ", h.seq)
	if h.p.active() {
		t.Fatal("killed epoch left guesses")
	}
	if h.p.confEpoch != confirmed {
		t.Fatalf("confirmed epoch = %d, want %d", h.p.confEpoch, confirmed)
	}
}

func TestEchoPredictorHiddenCursorDisables(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.p.applyOutput(protocol.Output{Epoch: 1, New: 2, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("\x1b[?25l")}, h.now)
	h.p.confEpoch = h.p.predEpoch
	if got := h.typed("b"); len(got) != 0 {
		t.Fatalf("hidden cursor rendered %q", got)
	}
	if h.p.active() {
		t.Fatal("hidden cursor kept guesses")
	}
}

func TestEchoPredictorSRTTHysteresis(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(200 * time.Millisecond)
	if !h.p.srttOn || !h.p.flagging {
		t.Fatalf("slow link: srttOn=%v flagging=%v, want both on", h.p.srttOn, h.p.flagging)
	}
	sawMiddle := false
	for range 64 {
		h.warm(0)
		interval := h.p.sendInterval()
		if interval > echoSRTTTriggerLow && interval <= echoSRTTTriggerHigh {
			sawMiddle = true
			if !h.p.srttOn {
				t.Fatalf("display turned off at send interval %v", interval)
			}
		}
		if interval <= echoFlagTriggerLow && h.p.flagging {
			t.Fatalf("still flagging at send interval %v", interval)
		}
		if interval <= echoSRTTTriggerLow {
			break
		}
	}
	if !sawMiddle {
		t.Fatal("never sampled the hysteresis band")
	}
	if h.p.srttOn {
		t.Fatal("display still on after the link recovered")
	}
}

func TestEchoPredictorSRTTStaysOnWhileGuessesShow(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(100 * time.Millisecond)
	h.confirm()
	h.typed("b")
	// Fast acknowledgements of other input keep the b guess pending.
	h.p.srtt = 0
	h.p.cull(h.now)
	if !h.p.srttOn {
		t.Fatal("display turned off while a guess is shown")
	}
}

func TestEchoPredictorGlitchShowsAndFlags(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAdaptive)
	h.warm(0)
	h.confirm()
	if got := h.typed("b"); len(got) != 0 {
		t.Fatalf("fast link rendered %q", got)
	}
	if !h.p.needsTick() {
		t.Fatal("pending guess needs no timing tick")
	}
	h.now = h.now.Add(echoGlitchThreshold)
	h.p.cull(h.now)
	if got := string(h.p.render()); got != "\x1b[1;4H\x1b[0mb\x1b[0m\x1b[1;5H" {
		t.Fatalf("glitch render = %q", got)
	}
	h.now = h.now.Add(echoGlitchFlagThreshold)
	h.p.cull(h.now)
	h.p.cull(h.now)
	if !h.p.flagging {
		t.Fatal("long glitch does not underline")
	}
	if h.p.needsTick() {
		t.Fatal("timing tick still armed once every trigger fired")
	}
}

func TestEchoPredictorResizeForgetsGuesses(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.warm(100 * time.Millisecond)
	h.confirm()
	h.typed("b")
	h.p.applyOutput(protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 30, Rows: 3}, Data: []byte("\x1b[H\x1b[2J$ ")}, h.now)
	if h.p.active() || len(h.p.drawn) != 0 {
		t.Fatal("resize kept guesses")
	}
	if got := h.p.render(); len(got) != 0 {
		t.Fatalf("render after resize = %q", got)
	}
}

func TestEchoSGR(t *testing.T) {
	t.Parallel()
	style := newEchoHarness(t, domain.EchoPredictAlways).p.screen.Cell(0, 0).Style
	style.Bold = true
	style.Foreground = 9
	style.Background = 200
	if got, want := echoSGR(style, true), "\x1b[0;1;4;91;48;5;200m"; got != want {
		t.Fatalf("echoSGR = %q, want %q", got, want)
	}
	style.HasForegroundRGB = true
	style.ForegroundRGB.R, style.ForegroundRGB.G, style.ForegroundRGB.B = 1, 2, 3
	if got, want := echoSGR(style, false), "\x1b[0;1;38;2;1;2;3;48;5;200m"; got != want {
		t.Fatalf("echoSGR rgb = %q, want %q", got, want)
	}
}

func TestEchoPredictorWaitsForFirstAcknowledgement(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.p.confEpoch = h.p.predEpoch
	if got := h.typed("b"); len(got) != 0 || h.p.active() {
		t.Fatalf("predicted before any echo acknowledgement: %q", got)
	}
}

func TestEchoPredictorRefusesUnshiftableText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name  string
		frame string
	}{
		{name: "wide rune after the cursor", frame: "a界\x1b[1;3H"},
		{name: "pane border after the cursor", frame: "ab│\x1b[1;3H"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newEchoHarness(t, domain.EchoPredictAlways)
			h.warm(10 * time.Millisecond)
			h.p.applyOutput(protocol.Output{Epoch: 1, New: 2, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte(tt.frame)}, h.now)
			h.p.confEpoch = h.p.predEpoch
			if got := h.typed("x"); len(got) != 0 || h.p.active() {
				t.Fatalf("shifted unshiftable text: %q", got)
			}
		})
	}
}

func TestEchoPredictorRestoresWideCellWhole(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.p.applyOutput(protocol.Output{Epoch: 1, New: 2, Size: domain.Size{Cols: 20, Rows: 3}, Data: []byte("\x1b[1;1H\x1b[4:3m界\x1b[0m")}, h.now)
	var out bytes.Buffer
	h.p.restoreCell(&out, 0, 1)
	if got, want := out.String(), "\x1b[1;1H\x1b[0;4:3m界"; got != want {
		t.Fatalf("restoreCell = %q, want %q", got, want)
	}
}

func TestEchoPredictorSameSizeResizeErasesGuesses(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.warm(10 * time.Millisecond)
	h.confirm()
	if got := h.typed("b"); len(got) == 0 {
		t.Fatal("guess not drawn")
	}
	h.p.reset()
	if got, want := string(h.p.render()), "\x1b[1;4H\x1b[0m \x1b[0m\x1b[1;4H"; got != want {
		t.Fatalf("render after reset = %q, want %q", got, want)
	}
}

// TestEchoPredictorTypesAheadInModalInput covers daemon modals (command
// palette, prompt, copy search): the daemon places the real cursor on their
// input line inside a bordered box, and guesses stay inside the box.
func TestEchoPredictorTypesAheadInModalInput(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	// Row 2: "│> " then the cursor, with the box's right border at column 15.
	h.daemon("\x1b[2;1H│> \x1b[2;16H│\x1b[2;4H", 0)
	h.warm(10 * time.Millisecond)
	if got := h.typed("s"); len(got) != 0 {
		t.Fatalf("first guess is tentative, rendered %q", got)
	}
	h.daemon("\x1b[2;4Hs", h.seq)
	got := string(h.typed("p"))
	if want := "\x1b[2;5H\x1b[0mp\x1b[0m\x1b[2;6H"; got != want {
		t.Fatalf("render = %q, want %q", got, want)
	}
	if cell := h.p.screen.Cell(15, 1); cell.Rune != '│' {
		t.Fatalf("box border moved: %q", cell.Rune)
	}
}

// TestEchoPredictorBackspaceStopsAtBorder holds backspace past an empty
// modal query: the input's left border is never predicted away.
func TestEchoPredictorBackspaceStopsAtBorder(t *testing.T) {
	t.Parallel()
	h := newEchoHarness(t, domain.EchoPredictAlways)
	h.daemon("\x1b[2;1H│\x1b[2;16H│\x1b[2;2H", 0)
	h.warm(10 * time.Millisecond)
	h.typed("s")
	h.daemon("\x1b[2;2Hs", h.seq)
	h.typed("\x7f")
	got := string(h.typed("\x7f"))
	if strings.Contains(got, "\x1b[2;1H") {
		t.Fatalf("backspace predicted over the border: %q", got)
	}
	if h.p.cursor.active && h.p.cursor.col < 1 {
		t.Fatalf("predicted cursor crossed the border: col %d", h.p.cursor.col)
	}
}

func TestApcFilterStripsGraphicsAcrossOutputs(t *testing.T) {
	t.Parallel()
	var f apcFilter
	chunks := []string{"a\x1b_Gf=100;AAA", "A\x1b", "\\b\x1b[1mc\x1b"}
	var got []byte
	for _, chunk := range chunks {
		got = append(got, f.strip([]byte(chunk))...)
	}
	got = append(got, f.strip([]byte("[0m"))...)
	if want := "ab\x1b[1mc\x1b[0m"; string(got) != want {
		t.Fatalf("strip = %q, want %q", got, want)
	}
}

func TestEscapeLength(t *testing.T) {
	t.Parallel()
	tests := []struct {
		in   string
		want int
	}{
		{in: "\x1b[A", want: 3},
		{in: "\x1bOA", want: 3},
		{in: "\x1b]11;?\x07x", want: 7},
		{in: "\x1b]11;?\x1b\\x", want: 8},
		{in: "\x1b]11;", want: 5},
		{in: "\x1bx", want: 2},
	}
	for _, tt := range tests {
		if got := escapeLength([]byte(tt.in)); got != tt.want {
			t.Errorf("escapeLength(%q) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
