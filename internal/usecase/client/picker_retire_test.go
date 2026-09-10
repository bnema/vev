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

// pickerReleasePaint sends the daemon's authoritative repaint that releases
// the client's presentation lease.
func pickerReleasePaint(transport *attachPaletteTransport, text string) []byte {
	view := protocol.ViewContext{
		Publication: 2,
		Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{
			LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture",
		}},
		TabID: "t_abc123", FocusedPaneID: "p_def456",
	}
	payload, err := wire.MarshalOutput(protocol.Output{
		Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24},
		Context: &view, Data: []byte("\x1b[2J\x1b[H" + text),
	})
	if err != nil {
		return nil
	}
	transport.detached <- wire.Frame{Type: wire.MsgOutput, Payload: payload}
	return payload
}

// TestPickerRetireRejectsLateSnapshot pins that a closed interaction is
// never reopened by an in-flight snapshot for the same ID.
func TestPickerRetireRejectsLateSnapshot(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	snapshot := pickerSnapshot()
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)}
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	// The daemon retires the interaction and repaints authoritatively.
	transport.detached <- wire.Frame{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision})}
	require.NotNil(t, pickerReleasePaint(transport, "released"))
	released := "released"
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &released}})
	require.NoError(t, err)

	// A late snapshot for the retired interaction must not restore the modal.
	late := snapshot
	late.Revision = snapshot.Revision + 1
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(late)}
	time.Sleep(100 * time.Millisecond)
	captured, err := ui.Capture(ui.Handle())
	require.NoError(t, err)
	text := uiSnapshotText(captured)
	require.Contains(t, text, "released")
	require.NotContains(t, text, "second", "a retired interaction must not be reopened")
}

// TestPickerReplaceAbortsSupersededLease pins that a superseding
// interaction takes presentation over and commits under its own ID, and
// that the replaced lease is resolved rather than left installed.
func TestPickerReplaceAbortsSupersededLease(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	first := pickerSnapshot()
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(first)}
	attached := ports.UIStatusAttached
	firstText := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &firstText}})
	require.NoError(t, err)

	// A new interaction arrives without a close for the previous one.
	superseding := protocol.PickerSnapshot{
		InteractionID: first.InteractionID + 1, Revision: 1, Title: " Sessions ",
		Rows:         []protocol.PickerRow{{Key: "cc/third", Display: "third"}},
		Cursor:       protocol.PickerCursor{Key: "cc/third", Index: 0},
		BarrierEpoch: 1, BarrierState: 1, SizeEpoch: 1,
	}
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(superseding)}
	third := "third"
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &third}})
	require.NoError(t, err)

	// A commit crosses under the superseding interaction, never the retired one.
	committed := make(chan ports.UIActionResult, 1)
	go func() {
		action, actionErr := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Enter"}})
		if actionErr == nil {
			committed <- action
		}
	}()
	selection := awaitPickerSelection(t, transport)
	require.Equal(t, superseding.InteractionID, selection.InteractionID)
	require.Equal(t, "cc/third", selection.Key)
}

// TestPickerStaleRevisionClosesInteraction pins that a terminal rejection
// closes the daemon-side interaction instead of leaving it open, and that
// input routing resumes only after the authoritative repaint.
func TestPickerStaleRevisionClosesInteraction(t *testing.T) {
	ctx := context.Background()
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = writer.Close() })
	harness := startPickerE2EWithInput(t, reader)
	ui, transport := harness.ui, harness.transport

	snapshot := pickerSnapshot()
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)}
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	// The daemon refuses the committed revision: the client must close the
	// interaction so it stops owning input on the daemon side.
	failure := wire.MarshalPickerFailure(protocol.PickerFailure{
		CauseActionID: 0, InteractionID: snapshot.InteractionID, Key: "aa/first", Code: protocol.PickerStaleRevision,
	})
	require.NotNil(t, failure)
	transport.detached <- wire.Frame{Type: wire.MsgPickerFailure, Payload: failure}

	closePayload := awaitWireFrame(t, transport, wire.MsgPickerCloseClient)
	closed, err := wire.UnmarshalPickerClose(closePayload)
	require.NoError(t, err)
	require.Equal(t, snapshot.InteractionID, closed.InteractionID)

	// Input during the release window is dropped, then routing resumes once
	// the daemon confirms the close and its authoritative repaint is shown.
	writeTerminal(t, writer, "leaked")
	requireNoPickerInput(t, transport)
	transport.detached <- wire.Frame{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision})}
	require.NotNil(t, pickerReleasePaint(transport, "released"))
	released := "released"
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &released}})
	require.NoError(t, err, "the release paint must be displayed")

	writeTerminal(t, writer, "ok")
	select {
	case <-transport.inputCh:
	case <-time.After(2 * time.Second):
		t.Fatal("session input did not resume after the release paint")
	}
}
