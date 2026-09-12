package client

import (
	"context"
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

// TestClientPickerLoopEndToEnd drives the full client-picker path through
// the real attach attempt with the palette wire transport: Welcome, then a
// PickerSnapshot opens the loop and installs the pump-side hook, a Down op
// repaints locally and completes without daemon evidence, Enter queues a
// typed PickerSelection on the wire, and the daemon-side PickerClose +
// same-peer handoff resolve the session. Zero picker bytes reach the PTY:
// no MsgInput frame carries picker data.
func TestClientPickerLoopEndToEnd(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	// Daemon opens the picker: two rows, cursor on the first.
	snapshot := openPickerOnHarness(t, transport)
	attached := ports.UIStatusAttached
	pickerText := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &pickerText}})
	require.NoError(t, err)

	// Cursor op completes locally once the repaint flushes: no daemon
	// evidence needed, and the frame carries the second row.
	downDone := make(chan struct{})
	var downResult ports.UIActionResult
	var downErr error
	go func() {
		defer close(downDone)
		downResult, downErr = ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Down"}})
	}()
	<-downDone
	require.NoError(t, downErr)
	require.Equal(t, ports.UIActionProcessed, downResult.Status)
	waitForText(t, ctx, ui, downResult.ActionID, "second")

	// Commit op stays pending: the typed selection crosses the wire with
	// the action as cause, and zero picker bytes reach the PTY.
	committed := make(chan ports.UIActionResult, 1)
	commitErr := make(chan error, 1)
	go func() {
		action, err := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Enter"}})
		if err != nil {
			commitErr <- err
			return
		}
		committed <- action
	}()
	selection := awaitPickerSelection(t, transport)
	require.Equal(t, uint64(7), selection.InteractionID)
	require.Equal(t, "serving", selection.SourceID)
	require.Equal(t, uint64(1), selection.SourceRevision)
	require.Equal(t, protocol.PickerActionNavigate, selection.Action)
	require.Equal(t, "bb/second", selection.Key)
	require.NotZero(t, selection.CauseActionID)
	requireNoPickerInput(t, transport)

	// Daemon resolves: the close confirmation ends the client interaction
	// exactly like a resolved selection, then the same-peer handoff and full
	// paint commit the session.
	second := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "second"}
	transport.detached <- wire.Frame{Type: wire.MsgPickerClosedServer, Payload: wire.MarshalPickerClosed(protocol.PickerClosed{InteractionID: snapshot.InteractionID, BarrierEpoch: 2, BarrierState: 1})}
	transport.detached <- wire.Frame{Type: wire.MsgAttachTarget, Payload: wire.MarshalAttachTarget(protocol.AttachTarget{Session: second.SessionName, Intent: protocol.IntentAttach, ExactTarget: &second, SamePeer: true, CauseActionID: selection.CauseActionID})}
	identityPayload, err := wire.MarshalCommittedRouteIdentity(protocol.CommittedRouteIdentity{Target: second})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgCommittedRouteIdentity, Payload: identityPayload}
	view := protocol.ViewContext{Publication: 2, Route: protocol.CommittedRouteIdentity{Target: second}, TabID: "t_abc123", FocusedPaneID: "p_def456"}
	outputPayload, err := wire.MarshalOutput(protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view, Data: []byte("\x1b[2J\x1b[Hsecond")})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgOutput, Payload: outputPayload}
	select {
	case action := <-committed:
		require.Equal(t, ports.UIActionProcessed, action.Status)
		require.Equal(t, "second", action.Context.Route.Target.SessionName)
	case err := <-commitErr:
		t.Fatalf("commit failed after daemon resolve: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("commit never completed after daemon resolve")
	}
	requireNoPickerInput(t, transport)
}

// TestClientPickerCompositeMoveCompletesFromResult drives the ui-driver action
// through the composite move+follow sequence: the daemon closes the
// interaction, publishes the destination route and its full paint on the same
// connection, then reports the correlated PickerResult. The action must
// complete as processed without a same-peer handoff, exactly like an in-place
// mutation.
func TestClientPickerCompositeMoveCompletesFromResult(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	offer := pickerOffer()
	offer.Intent = protocol.PickerIntentMoveTab
	offer.Title = " Move "
	transport.detached <- wire.Frame{Type: wire.MsgPickerOffer, Payload: wire.MarshalPickerOffer(offer)}
	snapshot := pickerSnapshot()
	for i := range snapshot.Lines {
		snapshot.Lines[i].Actions = protocol.PickerCanMove
	}
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)}
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	committed := make(chan ports.UIActionResult, 1)
	commitErr := make(chan error, 1)
	go func() {
		action, err := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Enter"}})
		if err != nil {
			commitErr <- err
			return
		}
		committed <- action
	}()
	selection := awaitPickerSelection(t, transport)
	require.Equal(t, protocol.PickerActionMove, selection.Action)
	require.NotZero(t, selection.CauseActionID)
	requireNoPickerInput(t, transport)

	// The daemon retires the interaction, then publishes the destination route
	// and full paint without a same-peer attach offer.
	second := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "second"}
	transport.detached <- wire.Frame{Type: wire.MsgPickerClosedServer, Payload: wire.MarshalPickerClosed(protocol.PickerClosed{InteractionID: snapshot.InteractionID, BarrierEpoch: 2, BarrierState: 1})}
	identityPayload, err := wire.MarshalCommittedRouteIdentity(protocol.CommittedRouteIdentity{Target: second})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgCommittedRouteIdentity, Payload: identityPayload}
	view := protocol.ViewContext{Publication: 2, Route: protocol.CommittedRouteIdentity{Target: second}, TabID: "t_abc123", FocusedPaneID: "p_def456"}
	outputPayload, err := wire.MarshalOutput(protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view, Data: []byte("\x1b[2J\x1b[Hsecond")})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgOutput, Payload: outputPayload}
	// Completion rides the fresh effect the move postcommit admitted on the
	// published destination capability.
	resultPayload := wire.MarshalPickerResult(protocol.PickerResult{
		CauseActionID: selection.CauseActionID, InteractionID: snapshot.InteractionID,
		SourceID: "serving", Key: selection.Key, Action: protocol.PickerActionMove,
	})
	transport.detached <- wire.Frame{Type: wire.MsgPickerResult, Payload: resultPayload}

	select {
	case action := <-committed:
		require.Equal(t, ports.UIActionProcessed, action.Status)
		require.Equal(t, "second", action.Context.Route.Target.SessionName)
	case err := <-commitErr:
		t.Fatalf("composite move failed: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("move action never completed from the correlated PickerResult")
	}
	requireNoPickerInput(t, transport)
}

// TestClientPickerRequestsAndRendersTheDisplayedRowPreview drives the preview
// path end to end: the debounce settles on the displayed row, the request names
// it, the daemon's viewport is drawn in the modal, and a late answer for a row
// the user already left leaves the displayed viewport untouched.
func TestClientPickerRequestsAndRendersTheDisplayedRowPreview(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	snapshot := openPickerOnHarness(t, transport)
	attached := ports.UIStatusAttached
	first := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	harness.awaitPreviewTimer(t).fire()
	request, err := wire.UnmarshalPickerPreviewRequest(awaitWireFrame(t, transport, wire.MsgPickerPreviewRequest))
	require.NoError(t, err)
	require.Equal(t, snapshot.InteractionID, request.InteractionID)
	require.Equal(t, "serving", request.SourceID)
	require.Equal(t, "aa/first", request.Key)
	// The client asks for the preview pane it will draw into, not the whole
	// terminal: both sides resolve the same modal geometry.
	preview := picker.PreviewRect(domain.Size{Cols: 80, Rows: 24})
	require.Equal(t, uint16(preview.Width), request.Width)
	require.Equal(t, uint16(preview.Height), request.Height)

	viewport := protocol.PickerPreview{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: snapshot.InteractionID, SourceID: "serving",
		Key: "aa/first", Status: protocol.PickerPreviewOK, Width: 4, Height: 1,
		Cells: []renderer.Cell{{Rune: 'v'}, {Rune: 'i'}, {Rune: 'e'}, {Rune: 'w'}},
	}
	transport.detached <- wire.Frame{Type: wire.MsgPickerPreview, Payload: wire.MarshalPickerPreview(viewport)}
	awaitTerminalText(t, harness.terminal, "view")

	// A late viewport for a row the user left must not replace the displayed one.
	late := viewport
	late.Key = "bb/second"
	late.Cells = []renderer.Cell{{Rune: 's'}, {Rune: 't'}, {Rune: 'a'}, {Rune: 'l'}}
	transport.detached <- wire.Frame{Type: wire.MsgPickerPreview, Payload: wire.MarshalPickerPreview(late)}
	awaitTerminalText(t, harness.terminal, "view")
	require.NotContains(t, uiSnapshotText(mustCapture(t, ui)), "stal")
}

func mustCapture(t *testing.T, ui *UI) ports.UISnapshot {
	t.Helper()
	snapshot, err := ui.Capture(ui.Handle())
	require.NoError(t, err)
	return snapshot
}

// TestClientPickerCancelReleasesOnAuthoritativeFullPaint drives the cancel
// path through the same real attempt: an idle Escape retires the
// interaction locally and types the close on the wire, the picker frame
// stays and the action stays pending, and only the daemon's authoritative
// full paint releases the terminal and completes the cancel.
func TestClientPickerCancelReleasesOnAuthoritativeFullPaint(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	snapshot := openPickerOnHarness(t, transport)
	attached := ports.UIStatusAttached
	pickerText := "first"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &pickerText}})
	require.NoError(t, err)
	requireNoPickerInput(t, transport)

	cancelled := make(chan ports.UIActionResult, 1)
	cancelErr := make(chan error, 1)
	go func() {
		action, err := ui.Action(ctx, ports.UIActionRequest{Attachment: ui.Handle(), Generation: 1, Keys: []string{"Escape"}})
		if err != nil {
			cancelErr <- err
			return
		}
		cancelled <- action
	}()
	closePayload := awaitWireFrame(t, transport, wire.MsgPickerCloseClient)
	closeMessage, err := wire.UnmarshalPickerClose(closePayload)
	require.NoError(t, err)
	require.Equal(t, snapshot.InteractionID, closeMessage.InteractionID)
	requireNoPickerInput(t, transport)

	// The close alone releases nothing: the action is still pending and the
	// picker frame still owns the screen.
	select {
	case action := <-cancelled:
		t.Fatalf("cancel completed before the release paint: %+v", action)
	case <-time.After(100 * time.Millisecond):
	}
	snapshotState, err := ui.Capture(ui.Handle())
	require.NoError(t, err)
	require.Contains(t, uiSnapshotText(snapshotState), "second")

	// The daemon confirms and repaints authoritatively: the frame is
	// displayed and the pending cancel completes against that boundary.
	transport.detached <- wire.Frame{Type: wire.MsgPickerClosedServer, Payload: wire.MarshalPickerClosed(protocol.PickerClosed{InteractionID: snapshot.InteractionID, BarrierEpoch: 2, BarrierState: 1})}
	view := protocol.ViewContext{Publication: 2, Route: protocol.CommittedRouteIdentity{Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "fixture"}}, TabID: "t_abc123", FocusedPaneID: "p_def456"}
	outputPayload, err := wire.MarshalOutput(protocol.Output{Epoch: 2, New: 1, Full: true, Size: domain.Size{Cols: 80, Rows: 24}, Context: &view, Data: []byte("\x1b[2J\x1b[Hreleased")})
	require.NoError(t, err)
	transport.detached <- wire.Frame{Type: wire.MsgOutput, Payload: outputPayload}
	select {
	case action := <-cancelled:
		require.Equal(t, ports.UIActionProcessed, action.Status)
	case err := <-cancelErr:
		t.Fatalf("cancel failed after the release paint: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("cancel never completed after the release paint")
	}
	after, err := ui.Capture(ui.Handle())
	require.NoError(t, err)
	require.NotContains(t, uiSnapshotText(after), "second")
	require.Contains(t, uiSnapshotText(after), "released")
}

// TestClientPickerDrawsAFloatingModalOverTheSession pins the presentation the
// picker must keep: a centred bordered box over the session, not a full-screen
// takeover. The session text painted before the picker opened stays visible
// around the box, and only the box's own cells are written.
func TestClientPickerDrawsAFloatingModalOverTheSession(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport
	attached := ports.UIStatusAttached
	ready := "ready"
	_, err := ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &ready}})
	require.NoError(t, err, "the session owns the screen before the picker opens")

	openPickerOnHarness(t, transport)
	first := "first"
	_, err = ui.Wait(ctx, ports.UIWaitRequest{Attachment: ui.Handle(), Expect: ports.UIExpect{Status: &attached, TextContains: &first}})
	require.NoError(t, err)

	text := uiSnapshotText(mustCapture(t, ui))
	require.Contains(t, text, "first", "the picker rows must be drawn")
	require.Contains(t, text, "ready", "the session must stay visible around the floating box")
	require.Contains(t, text, "─", "the modal border must be drawn")

	// The box is the shared picker modal, not the whole terminal.
	size := domain.Size{Cols: 80, Rows: 24}
	presentation := picker.Modal.Resolve(size)
	require.Less(t, presentation.Bounds.Width, size.Cols)
	require.Less(t, presentation.Bounds.Height, size.Rows)
	require.Contains(t, text, picker.Modal.Title, "the picker box keeps its title")
}
