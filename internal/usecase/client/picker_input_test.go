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

// TestPickerPhysicalInputDecoding pins the byte-level decoder: recognized
// keys, fragmented sequences, UTF-8, and the deliberate consumption of
// everything the picker does not understand. Nothing is left for the
// session pipeline.
func TestPickerPhysicalInputDecoding(t *testing.T) {
	tests := []struct {
		name        string
		pending     []byte
		data        []byte
		wantKeys    []string
		wantText    string
		wantPending string
	}{
		{name: "up arrow csi", data: []byte("\x1b[A"), wantKeys: []string{"Up"}},
		{name: "up arrow ss3", data: []byte("\x1bOA"), wantKeys: []string{"Up"}},
		{name: "down arrow csi", data: []byte("\x1b[B"), wantKeys: []string{"Down"}},
		{name: "down arrow ss3", data: []byte("\x1bOB"), wantKeys: []string{"Down"}},
		{name: "lone escape is withheld", data: []byte("\x1b"), wantPending: "\x1b"},
		{name: "csi prefix is withheld", data: []byte("\x1b["), wantPending: "\x1b["},
		{name: "fragmented arrow", pending: []byte("\x1b["), data: []byte("A"), wantKeys: []string{"Up"}},
		{name: "enter", data: []byte("\r"), wantKeys: []string{"Enter"}},
		{name: "newline", data: []byte("\n"), wantKeys: []string{"Enter"}},
		{name: "backspace", data: []byte("\x7f"), wantKeys: []string{"Backspace"}},
		{name: "ctrl-c", data: []byte("\x03"), wantKeys: []string{"Ctrl+C"}},
		{name: "printable ascii", data: []byte("jkq"), wantKeys: []string{"j", "k", "q"}},
		{name: "utf8 rune is text", data: []byte("é"), wantText: "é"},
		{name: "mouse report is consumed", data: []byte("\x1b[<0;5;5M")},
		{name: "unknown csi is consumed", data: []byte("\x1b[Z")},
		{name: "bracketed paste markers are consumed", data: []byte("\x1b[200~"), wantPending: ""},
		{name: "oversized incomplete sequence is dropped", data: []byte("\x1b[1234567890"), wantPending: ""},
		{name: "escape before ordinary byte", data: []byte("\x1bq"), wantKeys: []string{"Escape", "q"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var batch pickerInputBatch
			pending := decodePickerInto(test.pending, test.data, &batch)
			require.Equal(t, test.wantKeys, batch.keys)
			require.Equal(t, test.wantText, batch.text)
			require.Equal(t, test.wantPending, string(pending))
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
	require.Equal(t, []string{"Down"}, outcome.keys)

	// The release window consumes and drops, and purges any withheld prefix.
	consumer.setDrain(7, 3)
	outcome, consumed = consumer.consume(terminalReadResult{data: []byte("y")})
	require.True(t, consumed)
	require.Empty(t, outcome.keys)
	require.Empty(t, outcome.text)

	// Clearing hands input back to the session.
	consumer.clear()
	_, consumed = consumer.consume(terminalReadResult{data: []byte("z")})
	require.False(t, consumed)
}

// TestPickerApplyBatchAntiPaste pins the batch rules: a command needs a
// single key token, and a paste can never become a run of modal commands.
func TestPickerApplyBatchAntiPaste(t *testing.T) {
	open := func() *pickerLoop { return openPickerLoop(pickerSnapshot()) }

	loop := open()
	commit, closeOp := applyPickerBatch(loop, []string{"Down"}, "")
	require.False(t, commit)
	require.False(t, closeOp)
	key, ok := loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "bb/second", key, "single key moves the cursor")

	loop = open()
	_, _ = applyPickerBatch(loop, []string{"Down", "Down"}, "")
	key, ok = loop.commitKey()
	require.True(t, ok)
	require.Equal(t, "aa/first", key, "a multi-key batch is not applied in normal mode")

	loop = open()
	_, _ = applyPickerBatch(loop, nil, "abc")
	key, _ = loop.commitKey()
	require.Equal(t, "aa/first", key, "text does not act in normal mode")

	// Escape alone closes; a paste containing Enter does not commit.
	loop = open()
	_, closeOp = applyPickerBatch(loop, []string{"Escape"}, "")
	require.True(t, closeOp)

	loop = open()
	commit, closeOp = applyPickerBatch(loop, []string{"a", "Enter"}, "")
	require.False(t, commit, "a pasted Enter must not commit")
	require.False(t, closeOp)

	// Search inserts printable runes and still honours a lone control token.
	loop = open()
	loop.enterSearch()
	_, closeOp = applyPickerBatch(loop, []string{"a", "b"}, "")
	require.False(t, closeOp)
	_, closeOp = applyPickerBatch(loop, []string{"Escape"}, "")
	require.False(t, closeOp, "Escape leaves search first")
	require.False(t, loop.model.SearchActive())

	loop = open()
	loop.enterSearch()
	_, _ = applyPickerBatch(loop, []string{"Enter", "x"}, "")
	require.True(t, loop.model.SearchActive(), "a control token riding with other input is paste noise")
}

// TestPickerPhysicalInputOwnsSession drives real terminal bytes through the
// pump: the picker reacts, the close crosses the wire, and no byte ever
// reaches the PTY pipeline.
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

	// A physical Down arrow moves the cursor without any ui-driver action.
	writeTerminal(t, writer, "\x1b[B")
	second := "second"
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &second}})
	require.NoError(t, err)
	requireNoPickerInput(t, transport)

	// A physical Escape closes the picker once the disambiguation window
	// expires: the client sends the typed close and never leaks the byte.
	writeTerminal(t, writer, "\x1b")
	harness.awaitAfterAmbiguityDeadlines(t, transport, wire.MsgPickerCloseClient)
	requireNoPickerInput(t, transport)
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
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	// Close the interaction from the daemon side, then keep typing: the
	// release paint has not landed, so those bytes belong to the picker.
	transport.detached <- wire.Frame{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: 7, Revision: 1})}
	writeTerminal(t, writer, "leaked")
	requireNoPickerInput(t, transport)

	// The authoritative full paint releases the terminal; later input is
	// session input again.
	view := protocol.ViewContext{
		Publication: 2,
		Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{
			LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture",
		}},
		TabID: "t_abc123", FocusedPaneID: "p_def456",
	}
	full, err := wire.MarshalOutput(protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view, Data: []byte("\x1b[2J\x1b[Hready")})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgOutput, Payload: full}
	ready := "ready"
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &ready}})
	require.NoError(t, err, "the release paint must be displayed")

	writeTerminal(t, writer, "ok")
	select {
	case <-transport.inputCh:
	case <-time.After(2 * time.Second):
		t.Fatal("session input did not resume after the release paint")
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
