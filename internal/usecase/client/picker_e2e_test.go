package client

import (
	"context"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
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
	snapshot := pickerSnapshot()
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)}
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
	require.Equal(t, uint64(1), selection.Revision)
	require.Equal(t, "bb/second", selection.Key)
	require.NotZero(t, selection.CauseActionID)
	requireNoPickerInput(t, transport)

	// Daemon resolves: PickerClose ends the client interaction exactly
	// like resolvePickerClientSelection, then the same-peer handoff and
	// full paint commit the session.
	second := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "second"}
	transport.detached <- wire.Frame{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision})}
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

// TestClientPickerCancelReleasesOnAuthoritativeFullPaint drives the cancel
// path through the same real attempt: an idle Escape retires the
// interaction locally and types the close on the wire, the picker frame
// stays and the action stays pending, and only the daemon's authoritative
// full paint releases the terminal and completes the cancel.
func TestClientPickerCancelReleasesOnAuthoritativeFullPaint(t *testing.T) {
	ctx := context.Background()
	harness := startPickerE2E(t)
	ui, transport := harness.ui, harness.transport

	snapshot := pickerSnapshot()
	transport.detached <- wire.Frame{Type: wire.MsgPickerSnapshot, Payload: wire.MarshalPickerSnapshot(snapshot)}
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
	close, err := wire.UnmarshalPickerClose(closePayload)
	require.NoError(t, err)
	require.Equal(t, snapshot.InteractionID, close.InteractionID)
	require.Equal(t, snapshot.Revision, close.Revision)
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
	transport.detached <- wire.Frame{Type: wire.MsgPickerCloseServer, Payload: wire.MarshalPickerClose(protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision})}
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
