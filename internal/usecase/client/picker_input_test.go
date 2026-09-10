package client

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// keyEvents builds one key batch for applyPickerBatch.
func keyEvents(keys ...string) []pickerEvent {
	events := make([]pickerEvent, 0, len(keys))
	for _, key := range keys {
		events = append(events, pickerEvent{kind: pickerEventKey, key: key})
	}
	return events
}

// TestPickerPhysicalInputDecoding pins the byte-level decoder: recognized
// keys, fragmented sequences, fragmented UTF-8, paste state, and the
// deliberate consumption of everything the picker does not understand.
// Nothing is left for the session pipeline.
func TestPickerPhysicalInputDecoding(t *testing.T) {
	tests := []struct {
		name        string
		pending     []byte
		tail        string
		pasting     bool
		data        []byte
		wantKeys    []string
		wantText    string
		wantPending string
		wantTail    string
		wantPasting bool
	}{
		{name: "up arrow csi", data: []byte("\x1b[A"), wantKeys: []string{"Up"}},
		{name: "up arrow ss3", data: []byte("\x1bOA"), wantKeys: []string{"Up"}},
		{name: "down arrow csi", data: []byte("\x1b[B"), wantKeys: []string{"Down"}},
		{name: "lone escape is withheld", data: []byte("\x1b"), wantPending: "\x1b"},
		{name: "csi prefix is withheld", data: []byte("\x1b["), wantPending: "\x1b["},
		{name: "fragmented arrow", pending: []byte("\x1b["), data: []byte("A"), wantKeys: []string{"Up"}},
		{name: "enter", data: []byte("\r"), wantKeys: []string{"Enter"}},
		{name: "backspace", data: []byte("\x7f"), wantKeys: []string{"Backspace"}},
		{name: "ctrl-c", data: []byte("\x03"), wantKeys: []string{"Ctrl+C"}},
		{name: "printable ascii", data: []byte("jkq"), wantKeys: []string{"j", "k", "q"}},
		{name: "utf8 rune is text", data: []byte("é"), wantText: "é"},
		{name: "fragmented utf8 first byte", data: []byte{0xc3}, wantPending: string([]byte{0xc3})},
		{name: "fragmented utf8 completed", pending: []byte{0xc3}, data: []byte{0xa9}, wantText: "é"},
		{name: "ordered ascii then rune", data: []byte("aé"), wantKeys: []string{"a"}, wantText: "é"},
		{name: "mouse report is consumed", data: []byte("\x1b[<0;5;5M")},
		{name: "unknown csi is consumed", data: []byte("\x1b[Z")},
		{name: "oversized incomplete sequence is dropped", data: []byte("\x1b[1234567890"), wantPending: ""},
		{name: "escape before ordinary byte", data: []byte("\x1bq"), wantKeys: []string{"Escape", "q"}},
		// A paste is a state, not a heuristic: the content between the
		// markers is consumed even when it arrives across reads, and even
		// when it spells a command.
		{name: "paste start alone", data: []byte("\x1b[200~"), wantPasting: true},
		{name: "paste content and end", pasting: true, data: []byte("q\x1b[201~"), wantPasting: false},
		// Only the marker-relevant suffix of a paste is retained: the rest is
		// dropped, and a split end marker still resolves on the next read.
		{name: "paste content split across reads", pasting: true, data: []byte("hidden\x1b["), wantPasting: true, wantTail: "den\x1b["},
		{name: "paste end split across reads", pasting: true, tail: "\x1b[", data: []byte("201~"), wantPasting: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var batch pickerInputBatch
			d := decoder{pending: test.pending, pasting: test.pasting, tail: []byte(test.tail)}
			d.decode(test.data, &batch)
			var keys []string
			var text string
			for _, event := range batch.events {
				if event.kind == pickerEventKey {
					keys = append(keys, event.key)
					continue
				}
				text += string(event.r)
			}
			require.Equal(t, test.wantKeys, keys)
			require.Equal(t, test.wantText, text)
			require.Equal(t, test.wantPending, string(d.pending))
			require.Equal(t, test.wantTail, string(d.tail))
			require.Equal(t, test.wantPasting, d.pasting)
		})
	}
}

// TestPickerInputConsumerOwnership pins who owns terminal input and what
// the release window does with it: consume and drop, never replay.
func TestPickerInputConsumerOwnership(t *testing.T) {
	var consumer pickerConsumer
	_, consumed := consumer.consume(terminalReadResult{data: []byte("x")})
	require.False(t, consumed, "the session owns input while no interaction is open")

	consumer.setOwned(7, 3)
	outcome, consumed := consumer.consume(terminalReadResult{data: []byte("\x1b[B")})
	require.True(t, consumed)
	require.Equal(t, uint64(7), outcome.interaction)
	require.Equal(t, uint64(3), outcome.generation)
	require.Equal(t, keyEvents("Down"), outcome.events)

	// The release window consumes and drops, and purges any withheld prefix.
	consumer.setDrain(7, 3)
	outcome, consumed = consumer.consume(terminalReadResult{data: []byte("y")})
	require.True(t, consumed)
	require.Empty(t, outcome.events)

	// Clearing hands input back to the session.
	consumer.clear()
	_, consumed = consumer.consume(terminalReadResult{data: []byte("z")})
	require.False(t, consumed)
}

// TestPickerApplyBatchAppliesEveryEvent pins that a terminal read carrying
// several legitimate keystrokes is applied in order: batch size never
// decides whether input is a command. Paste protection is the decoder's
// paste state, which is covered by the decoding table.
func TestPickerApplyBatchAppliesEveryEvent(t *testing.T) {
	open := func() *pickerLoop { return openPickerLoop(pickerSnapshot()) }

	loop := open()
	commit, closeOp, changed := applyPickerBatch(loop, keyEvents("Down"))
	require.False(t, commit)
	require.False(t, closeOp)
	require.True(t, changed, "a cursor move repaints")
	key, ok := loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "bb/second", key)

	// Two keystrokes in one read both act, in order.
	loop = open()
	_, _, changed = applyPickerBatch(loop, keyEvents("Down", "Down"))
	require.True(t, changed)
	key, _ = loop.commitKey()
	require.Equal(t, "bb/second", key, "the second Down is clamped at the last row")
	loop.up()
	key, _ = loop.commitKey()
	require.Equal(t, "aa/first", key, "both Downs moved the cursor")

	loop = open()
	_, closeOp, _ = applyPickerBatch(loop, keyEvents("Escape"))
	require.True(t, closeOp)

	// A close inside a batch ends it: events after it do not act.
	loop = open()
	_, closeOp, _ = applyPickerBatch(loop, keyEvents("Escape", "Down"))
	require.True(t, closeOp)

	// Search inserts runes and printable keys in arrival order.
	loop = open()
	loop.enterSearch()
	_, _, changed = applyPickerBatch(loop, []pickerEvent{
		{kind: pickerEventRune, r: 'é'},
		{kind: pickerEventKey, key: "a"},
	})
	require.True(t, changed)
	_, closeOp, _ = applyPickerBatch(loop, keyEvents("Escape"))
	require.False(t, closeOp, "Escape leaves search first")
	require.False(t, loop.model.SearchActive())
}

// TestPickerOutcomeScope pins that a decoded operation only applies to the
// interaction and attachment generation that admitted it: this is what makes
// a generation change (reconnect) and a superseded interaction safe without
// forwarding their input anywhere.
func TestPickerOutcomeScope(t *testing.T) {
	outcome := pickerConsumeOutcome{consumed: true, interaction: 7, generation: 3}
	require.True(t, outcome.acceptOutcome(7, 3))
	require.False(t, outcome.acceptOutcome(7, 4), "a new generation must not apply the old operation")
	require.False(t, outcome.acceptOutcome(8, 3), "a superseding interaction must not apply it either")
	require.False(t, pickerConsumeOutcome{interaction: 7, generation: 3}.acceptOutcome(7, 3), "an unconsumed outcome never applies")
}

// TestPickerRendererTogglesBracketedPaste pins that the picker enables the
// terminal's bracketed-paste mode with its first frame and resets it once:
// a marker-wrapped paste then arrives as one consumable unit instead of
// ordinary bytes that could look like fast typing.
func TestPickerRendererTogglesBracketedPaste(t *testing.T) {
	renderer := newPickerRenderer()
	loop := openPickerLoop(pickerSnapshot())

	first := renderer.render(loop, domain.Size{Cols: 80, Rows: 24})
	require.Contains(t, string(first), bracketedPasteEnable)
	require.True(t, renderer.pasteMode, "the mode is enabled with the first frame")

	// The reset is written exactly once, and the next frame re-enables mode.
	require.Equal(t, []byte(bracketedPasteDisable), renderer.disableBracketedPaste())
	require.False(t, renderer.pasteMode)
	require.Nil(t, renderer.disableBracketedPaste(), "the reset is written once")

	third := renderer.render(loop, domain.Size{Cols: 80, Rows: 24})
	require.Contains(t, string(third), bracketedPasteEnable)
}

// TestPickerPhysicalInputWithoutUIDriver pins that the picker owns and
// completes physical input in the ordinary CLI composition, where the
// ui-driver (and its automation channel) does not exist: the picker frame
// is observed on the terminal itself, then real bytes drive it.
func TestPickerPhysicalInputWithoutUIDriver(t *testing.T) {
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	harness := startPickerE2EHeadless(t, reader)
	require.Nil(t, harness.ui, "this harness has no ui-driver")
	transport := harness.transport

	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(pickerSnapshot())}
	// The drawn picker is the barrier: write only once it owns the screen.
	awaitTerminalText(t, harness.terminal, "second")

	// Down then Enter commits the row the physical keys selected.
	writeTerminal(t, writer, "\x1b[B")
	writeTerminal(t, writer, "\r")
	selection := awaitPickerSelection(t, transport)
	require.Equal(t, "bb/second", selection.Key)
	requireNoPickerInput(t, transport)
}

// TestPickerPhysicalInputOwnsSession drives real terminal bytes through the
// pump: the picker reacts by committing the row the physical keys selected,
// the close crosses the wire, and no byte ever reaches the PTY pipeline.
func TestPickerPhysicalInputOwnsSession(t *testing.T) {
	ctx := context.Background()
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	harness := startPickerE2EWithInput(t, reader)
	ui, transport := harness.ui, harness.transport

	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(pickerSnapshot())}
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	// A physical Down arrow then Enter commits the row the cursor reached:
	// both rows are on screen from the start, so the committed key is the
	// only proof the movement happened.
	writeTerminal(t, writer, "\x1b[B")
	writeTerminal(t, writer, "\r")
	selection := awaitPickerSelection(t, transport)
	require.Equal(t, "bb/second", selection.Key)
	requireNoPickerInput(t, transport)

	// A physical Escape closes the picker once the disambiguation window
	// expires: the client sends the typed close and never leaks the byte.
	writeTerminal(t, writer, "\x1b")
	harness.awaitAfterAmbiguityDeadlines(t, transport, wire.MsgPickerCloseClient)
	requireNoPickerInput(t, transport)
}

// TestPickerPhysicalPasteNeverActs drives a bracketed paste through the pump
// in fragments: its markers and content are consumed, and the command it
// spells never fires.
func TestPickerPhysicalPasteNeverActs(t *testing.T) {
	ctx := context.Background()
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	harness := startPickerE2EWithInput(t, reader)
	ui, transport := harness.ui, harness.transport

	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(pickerSnapshot())}
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	before := transport.frameCount()
	writeTerminal(t, writer, "\x1b[200~")
	writeTerminal(t, writer, "q\r")
	writeTerminal(t, writer, "\x1b[201~")
	time.Sleep(200 * time.Millisecond)
	require.Equal(t, before, transport.frameCount(), "paste produced a client frame")
	requireNoPickerInput(t, transport)

	// The picker is still open: a real Escape closes it.
	writeTerminal(t, writer, "\x1b")
	harness.awaitAfterAmbiguityDeadlines(t, transport, wire.MsgPickerCloseClient)
}

// TestPickerReleaseDropsInputWithoutReplay pins the release window: bytes
// arriving after the close are consumed and dropped, never replayed into
// the session, and routing resumes only after the authoritative paint.
func TestPickerReleaseDropsInputWithoutReplay(t *testing.T) {
	ctx := context.Background()
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	harness := startPickerE2EWithInput(t, reader)
	ui, transport := harness.ui, harness.transport

	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(pickerSnapshot())}
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, utilWait(ui, &attached, &first))
	require.NoError(t, err)

	// Close the interaction from the daemon side, then keep typing: the
	// release paint has not landed, so those bytes belong to the picker.
	transport.detached <- wire.Frame{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: 7, Revision: 1})}
	writeTerminal(t, writer, "leaked")
	requireNoPickerInput(t, transport)

	// The authoritative full paint releases the terminal; later input is
	// session input again, and only the new bytes are delivered.
	require.NotNil(t, pickerReleasePaint(transport, "released"))
	released := "released"
	_, err = ui.Wait(ctx, utilWait(ui, &attached, &released))
	require.NoError(t, err, "the release paint must be displayed")

	writeTerminal(t, writer, "ok")
	var delivered []byte
	deadline := time.After(2 * time.Second)
	for len(delivered) < len("ok") {
		select {
		case frame := <-transport.inputCh:
			delivered = append(delivered, frame.Data...)
		case <-deadline:
			t.Fatalf("session input did not resume after the release paint: got %q", delivered)
		}
	}
	require.Equal(t, []byte("ok"), delivered)
	select {
	case frame := <-transport.inputCh:
		t.Fatalf("released input was replayed: %q", frame.Data)
	case <-time.After(100 * time.Millisecond):
	}
}

// writeTerminal feeds raw bytes without blocking the test when the pump is
// momentarily inactive.
func writeTerminal(t *testing.T, writer *io.PipeWriter, data string) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = writer.Write([]byte(data))
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("terminal write %q never reached the pump", data)
	}
}

// utilWait builds a UI wait request for one expected fragment.
func utilWait(ui *UI, status *ports.UIPresentationStatus, text *string) ports.UIWaitRequest {
	return ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: status, TextContains: text}}
}
