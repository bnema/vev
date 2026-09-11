package daemon

import (
	"fmt"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// pickerClientTestUnit builds a fixture attachment and one extra session so
// snapshots are non-empty. It returns an admitted effect for the guarded
// sends.
func pickerClientTestUnit(t *testing.T) (*Daemon, *session, *attachedClient, chan wire.Frame, *attachmentEffect) {
	t.Helper()
	p, releasePTY := newBlockingPTY(t)
	return pickerClientTestUnitWithPTY(t, p, releasePTY)
}

// pickerClientTestUnitWithPTY builds the same fixture over a caller-owned
// PTY, so tests can observe whether session input reached the child.
func pickerClientTestUnitWithPTY(t *testing.T, p ports.PTY, releasePTY func()) (*Daemon, *session, *attachedClient, chan wire.Frame, *attachmentEffect) {
	t.Helper()
	d, sess, ac, sends := newManualSessionWithPTYs(t, p)
	t.Cleanup(releasePTY)
	d.ptys = newFactorySeq(t, newQuietPTY())
	_, err := createSessionForTest(d, "second", false, "", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, nil)
	require.NoError(t, err)
	effect, ok := ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
	require.True(t, ok)
	t.Cleanup(effect.End)
	return d, sess, ac, sends, effect
}

func openTestPicker(t *testing.T, d *Daemon, ac *attachedClient, effect *attachmentEffect, sends chan wire.Frame, intent protocol.PickerIntent) protocol.PickerOffer {
	t.Helper()
	require.NoError(t, d.openPickerForAttachment(ac, effect, intent, moveSourceLocator{}, 0))
	return awaitPickerOffer(t, sends)
}

func awaitPickerOffer(t *testing.T, sends chan wire.Frame) protocol.PickerOffer {
	t.Helper()
	for {
		frame := awaitTestValue(t, sends, "picker offer was not published")
		if frame.Type != wire.MsgPickerOffer {
			continue
		}
		offer, err := wire.UnmarshalPickerOffer(frame.Payload)
		require.NoError(t, err)
		return offer
	}
}

func awaitPickerSnapshot(t *testing.T, sends chan wire.Frame) protocol.PickerSnapshot {
	t.Helper()
	for {
		frame := awaitTestValue(t, sends, "picker snapshot was not published")
		if frame.Type != wire.MsgPickerSnapshot {
			continue
		}
		snapshot, err := wire.UnmarshalPickerSnapshot(frame.Payload)
		require.NoError(t, err)
		return snapshot
	}
}

func awaitPickerFailure(t *testing.T, sends chan wire.Frame) protocol.PickerFailure {
	t.Helper()
	for {
		frame := awaitTestValue(t, sends, "picker failure was not published")
		if frame.Type != wire.MsgPickerFailure {
			continue
		}
		failure, err := wire.UnmarshalPickerFailure(frame.Payload)
		require.NoError(t, err)
		return failure
	}
}

func pickerClientCapability(t *testing.T, ac *attachedClient, sess *session) attachmentCapability {
	t.Helper()
	return captureAttachmentCapability(sess, ac, ac.transport())
}

// navigateSelection builds one typed commit for the displayed snapshot.
func navigateSelection(snapshot protocol.PickerSnapshot, key string) protocol.PickerSelection {
	return protocol.PickerSelection{
		InteractionID: snapshot.InteractionID, SourceID: servingPickerSourceID,
		SourceRevision: snapshot.SourceRevision, Key: key, Action: protocol.PickerActionNavigate,
	}
}

// firstSelectableKey returns the first line the source authorised for the
// requested action.
func firstSelectableKey(t *testing.T, snapshot protocol.PickerSnapshot, action protocol.PickerLineActions) string {
	t.Helper()
	for _, line := range snapshot.Lines {
		if line.Actions&action != 0 {
			return line.Key
		}
	}
	t.Fatalf("snapshot published no line admitting %d", action)
	return ""
}

func TestPickerOfferAndSnapshotPublishResolvableLines(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	offer := openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)

	require.NotZero(t, offer.InteractionID)
	require.Equal(t, protocol.PickerIntentNavigation, offer.Intent)
	require.Equal(t, offer.InteractionID, snapshot.InteractionID)
	require.Equal(t, servingPickerSourceID, snapshot.SourceID)
	require.Equal(t, uint64(1), snapshot.SourceRevision)
	require.NotEmpty(t, snapshot.Lines)

	// Every selectable line resolves through the published keys, and the keys
	// carry session names for the unchanged handoff.
	ac.overlays.pickerMu.Lock()
	keys := ac.overlays.pickerKeys
	ac.overlays.pickerMu.Unlock()
	selectable := 0
	for _, line := range snapshot.Lines {
		if line.Actions == 0 {
			continue
		}
		selectable++
		target, ok := keys[line.Key]
		require.True(t, ok, "line %q has no resolved target", line.Key)
		require.NotEmpty(t, target.Name, "resolved target carries no session name")
	}
	require.NotZero(t, selectable)
}

func TestPickerOpenInstallsNoPresentationModel(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	_ = awaitPickerSnapshot(t, sends)

	// The daemon publishes data and actions only: no model, no cursor, no
	// search state of its own.
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerOpen)
	require.NotZero(t, ac.overlays.pickerInteraction)
	require.NotEmpty(t, ac.overlays.pickerKeys)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerSelectionRejectsStaleAndUnknown(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)

	capability := pickerClientCapability(t, ac, sess)
	// Unknown key on the open interaction reports UnknownKey without
	// closing the interaction.
	require.False(t, d.handleAttachmentClientMessage(capability, navigateSelection(snapshot, "ff00/ghost")))
	failure := awaitPickerFailure(t, sends)
	require.Equal(t, protocol.PickerUnknownKey, failure.Code)
	require.Equal(t, snapshot.InteractionID, failure.InteractionID)

	// A foreign source is rejected before any key lookup.
	foreign := navigateSelection(snapshot, firstSelectableKey(t, snapshot, protocol.PickerCanNavigate))
	foreign.SourceID = "elsewhere"
	require.False(t, d.handleAttachmentClientMessage(capability, foreign))
	require.Equal(t, protocol.PickerUnknownSource, awaitPickerFailure(t, sends).Code)

	// A retired interaction reports StaleRevision.
	d.closePickerForAttachment(ac, effect, snapshot.InteractionID)
	require.False(t, d.handleAttachmentClientMessage(capability, navigateSelection(snapshot, firstSelectableKey(t, snapshot, protocol.PickerCanNavigate))))
	require.Equal(t, protocol.PickerStaleRevision, awaitPickerFailure(t, sends).Code)
}

func TestPickerMoveIntentPublishesMoveLinesOnly(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)
	source := moveSourceLocator{
		Session: moveSessionLocator{ID: sess.id, Incarnation: sess.incarnation, Name: sess.name},
		TabID:   domain.TabStableID(testAttachmentTab(sess).stableID),
	}
	ac.overlays.pickerMu.Lock()
	require.False(t, ac.overlays.pickerOpen)
	ac.overlays.pickerMu.Unlock()

	require.NoError(t, d.openPickerForAttachment(ac, effect, protocol.PickerIntentMoveTab, source, 0))
	offer := awaitPickerOffer(t, sends)
	snapshot := awaitPickerSnapshot(t, sends)
	require.Equal(t, protocol.PickerIntentMoveTab, offer.Intent)

	// The source session never appears as its own destination, and rows that
	// cannot accept a move carry no action.
	for _, line := range snapshot.Lines {
		require.NotEqual(t, sourceMoveKey(source), line.Key)
		if line.Actions != 0 {
			require.Equal(t, protocol.PickerCanMove, line.Actions)
		}
	}
}

func sourceMoveKey(source moveSourceLocator) string {
	return pickerMoveSourceKey(source)
}

func TestPickerOpenRepaintsWithoutSessionChange(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	// The open invalidates the session even when nothing else changed:
	// without this repaint the opener's fence would never retire and
	// the palette Enter would hang until its deadline.
	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	_ = awaitPickerSnapshot(t, sends)
	select {
	case frame := <-sends:
		require.Equal(t, wire.MsgOutput, frame.Type, "open must trigger an authoritative repaint")
	default:
		t.Fatal("open produced no repaint after the snapshot")
	}
}

func TestPickerSessionPickerCommandOpensInteraction(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	effect := beginRecentRoutePaletteEffect(t, d, current, ac)
	effect.uiActionID = 77

	// Full path through overlay dispatch: palette query, then Enter on
	// the same effect. The Enter routes through handlePaletteInput via
	// the overlay runtime, exactly like a real client frame.
	d.enterPalette(current, ac)
	d.handlePaletteInput(ac, []byte("sessionpicker"), effect)
	require.NotNil(t, ac.overlays.palette)
	d.handlePaletteInput(ac, []byte("\r"), effect)

	offer := awaitPickerOffer(t, sends)
	snapshot := awaitPickerSnapshot(t, sends)
	require.NotZero(t, offer.InteractionID)
	require.Equal(t, protocol.PickerIntentNavigation, offer.Intent)
	require.NotEmpty(t, snapshot.Lines)
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerOpen)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerCancelPublishesClosedAndAuthoritativeFullPaint(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)

	// The client cancels: the daemon retires the interaction, confirms the
	// close with its restore barrier, and publishes the authoritative full
	// paint the client's release path waits for.
	capability := pickerClientCapability(t, ac, sess)
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerClose{InteractionID: snapshot.InteractionID}))
	ac.overlays.pickerMu.Lock()
	require.False(t, ac.overlays.pickerOpen)
	ac.overlays.pickerMu.Unlock()

	var sawClosed bool
	for settled := false; !settled; {
		select {
		case frame := <-sends:
			if frame.Type == wire.MsgPickerClosedServer {
				closed, err := wire.UnmarshalPickerClosed(frame.Payload)
				require.NoError(t, err)
				require.Equal(t, snapshot.InteractionID, closed.InteractionID)
				sawClosed = true
				continue
			}
			if frame.Type == wire.MsgOutput {
				output, err := wire.UnmarshalOutput(frame.Payload)
				require.NoError(t, err)
				require.True(t, sawClosed, "the restore paint must follow the close")
				require.True(t, output.Full, "cancel release paint must be authoritative")
				settled = true
			}
		case <-time.After(200 * time.Millisecond):
			settled = true
		}
	}
	require.True(t, sawClosed, "cancel published no close confirmation")
}

func TestPickerPaletteFramesOpenInteraction(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, _, ac, sends := newManualSessionWithPTYs(t, p)
	capability := pickerClientCapability(t, ac, testAttachmentSession(t, ac))

	// Drive the real attached-client frame path instead of calling the
	// palette handler: Alt+Space opens the palette, the query selects the
	// session-picker command, and Enter executes it.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 1, ActionID: 4, Data: []byte("\x1b ")}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 2, ActionID: 5, Data: []byte("SSP")}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 3, ActionID: 6, Data: []byte("\r")}))

	_ = awaitPickerOffer(t, sends)
	snapshot := awaitPickerSnapshot(t, sends)
	require.NotZero(t, snapshot.InteractionID)
	require.NotEmpty(t, snapshot.Lines)
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerOpen)
	ac.overlays.pickerMu.Unlock()
}

func testAttachmentSession(t *testing.T, ac *attachedClient) *session {
	t.Helper()
	sess := ac.currentAttachmentSession()
	require.NotNil(t, sess)
	return sess
}

func TestPickerSelectionRequiresExactSourceRevision(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	first := awaitPickerSnapshot(t, sends)
	// A refresh publishes a newer revision: the client may only commit the
	// model it is displaying, so an older revision must fail even though it
	// is not newer than the daemon's current one.
	d.refreshPickerSnapshot(ac)
	second := awaitPickerSnapshot(t, sends)
	require.Greater(t, second.SourceRevision, first.SourceRevision)

	capability := pickerClientCapability(t, ac, sess)
	key := firstSelectableKey(t, second, protocol.PickerCanNavigate)
	for _, revision := range []uint64{second.SourceRevision - 1, second.SourceRevision + 1} {
		selection := navigateSelection(second, key)
		selection.SourceRevision = revision
		require.False(t, d.handleAttachmentClientMessage(capability, selection))
		failure := awaitPickerFailure(t, sends)
		require.Equal(t, protocol.PickerStaleRevision, failure.Code, "revision %d must be rejected", revision)
		ac.overlays.pickerMu.Lock()
		require.True(t, ac.overlays.pickerOpen, "a rejected revision must not close the interaction")
		ac.overlays.pickerMu.Unlock()
	}
}

func TestPickerInteractionDropsRawInput(t *testing.T) {
	writes := make(chan []byte, 8)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends, effect := pickerClientTestUnitWithPTY(t, p, releasePTY)
	capability := pickerClientCapability(t, ac, sess)

	// A normal attachment forwards keys to the session.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 1, ActionID: 1, Data: []byte("x")}))
	require.Equal(t, []byte("x"), awaitTestValue(t, writes, "session input never reached the pty"))

	// Positive control: with the pane reporting mouse mode, a mouse report
	// reaches the pane's pty. Without this control the negative assertion
	// below would pass even if mouse routing were broken outright.
	tb := testAttachmentTab(sess)
	require.NotNil(t, tb)
	pane := tb.panes["pane-1"]
	require.NotNil(t, pane)
	pane.screen.Write([]byte("\x1b[?1000h\x1b[?1006h"))
	mouseReport := []byte(fmt.Sprintf("\x1b[<0;2;%dM", clientTopBarRows+2))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 1, ActionID: 1, Data: mouseReport}))
	require.NotEmpty(t, awaitTestValue(t, writes, "positive control: mouse never reached the pane"))

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)

	// While the picker is open it owns user input: keys and mouse reports
	// must not reach the session, and the interaction stays open.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 2, ActionID: 2, Data: []byte("hidden")}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 3, ActionID: 3, Data: mouseReport}))
	select {
	case frame := <-writes:
		t.Fatalf("picker-owned input reached the pty: %q", frame)
	case <-time.After(100 * time.Millisecond):
	}

	// Closing the interaction restores normal routing, mouse included.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerClose{InteractionID: snapshot.InteractionID}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 4, ActionID: 4, Data: []byte("visible")}))
	require.Equal(t, []byte("visible"), awaitTestValue(t, writes, "session input did not resume after the close"))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 5, ActionID: 5, Data: mouseReport}))
	require.NotEmpty(t, awaitTestValue(t, writes, "mouse routing did not resume after the close"))
}

func TestPickerSelectionClosesBeforeTheDestinationPaint(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)
	capability := pickerClientCapability(t, ac, sess)

	// Selecting the current session is a same-peer handoff. The interaction
	// must be retired before that handoff: the client releases only on a
	// paint accepted after the daemon's close, so the destination paint has
	// to follow it on the wire. Drain the open's own repaint first.
	drainAllFrames(sends)
	key := firstSelectableKey(t, snapshot, protocol.PickerCanNavigate)
	require.False(t, d.handleAttachmentClientMessage(capability, navigateSelection(snapshot, key)))

	// Collect the handoff frames for a short settle window, then require the
	// close to precede any paint.
	var order []wire.MsgType
	for settled := false; !settled; {
		select {
		case frame := <-sends:
			order = append(order, frame.Type)
		case <-time.After(200 * time.Millisecond):
			settled = true
		}
	}
	require.NotEmpty(t, order, "the handoff published nothing")
	require.Equal(t, wire.MsgPickerClosedServer, order[0], "the close must precede the handoff")
	for i, frameType := range order {
		if frameType == wire.MsgOutput {
			require.NotZero(t, i, "a paint must not precede the close")
		}
	}
	ac.overlays.pickerMu.Lock()
	require.False(t, ac.overlays.pickerOpen)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerInteractionDropsClipboardImage(t *testing.T) {
	writes := make(chan []byte, 8)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends, effect := pickerClientTestUnitWithPTY(t, p, releasePTY)
	capability := pickerClientCapability(t, ac, sess)

	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	push := protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: []byte("\x89PNG\r\n\x1a\n")}

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)

	// An image push is user input too: the clipboard path must not reach the
	// session while the picker owns input.
	require.False(t, d.handleAttachmentClientMessage(capability, push))
	select {
	case frame := <-writes:
		t.Fatalf("clipboard path reached the pty during the interaction: %q", frame)
	case <-time.After(100 * time.Millisecond):
	}

	// After the close the same push is delivered again.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerClose{InteractionID: snapshot.InteractionID}))
	push.InputSeq = 2
	require.False(t, d.handleAttachmentClientMessage(capability, push))
	select {
	case frame := <-writes:
		require.Contains(t, string(frame), ".png")
	case <-time.After(2 * time.Second):
		t.Fatal("clipboard path did not resume after the close")
	}
}

func TestPickerSnapshotRefreshThroughDirectoryNotification(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	first := awaitPickerSnapshot(t, sends)

	// The row set changes for real: a new session appears in the catalogue.
	_, err := createSessionForTest(d, "third", false, "", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, nil)
	require.NoError(t, err)

	// The catalogue notification path must reach the picker even though the
	// daemon installs no overlay model: refreshRemoteDirectoryViews is the
	// entry the directory subscription calls.
	d.refreshRemoteDirectoryViews()
	second := awaitPickerSnapshot(t, sends)
	require.Equal(t, first.InteractionID, second.InteractionID)
	require.Greater(t, second.SourceRevision, first.SourceRevision)
	require.Greater(t, len(second.Lines), len(first.Lines), "the new session must appear as a line")
}

func TestPickerRefreshPublishesNewerSourceRevision(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	first := awaitPickerSnapshot(t, sends)
	require.Equal(t, uint64(1), first.SourceRevision)

	// A row-set or size change republishes a full snapshot with a newer
	// revision through the current attachment capability.
	d.refreshPickerSnapshot(ac)
	second := awaitPickerSnapshot(t, sends)
	require.Equal(t, first.InteractionID, second.InteractionID)
	require.Greater(t, second.SourceRevision, first.SourceRevision)
	require.Len(t, second.Lines, len(first.Lines))

	// A closed interaction republishes nothing: only the close confirmation
	// and the authoritative repaint may follow.
	require.True(t, d.closePickerForAttachment(ac, effect, first.InteractionID))
	for _, frame := range drainAllFrames(sends) {
		require.NotEqual(t, wire.MsgPickerSnapshot, frame.Type, "closed interaction republished a snapshot")
	}
}
