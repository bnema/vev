package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

// pickerClientTestUnit builds a fixture attachment with the client-picker
// capability set and one extra session so snapshots are non-empty. It
// returns an admitted effect for the guarded sends.
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
	ac.navigationCapabilities |= protocol.NavigationCapabilityClientPicker
	effect, ok := ac.beginAttachmentEffect(captureAttachmentCapability(sess, ac, ac.transport()))
	require.True(t, ok)
	t.Cleanup(effect.End)
	return d, sess, ac, sends, effect
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

func TestPickerClientSnapshotMirrorsOverlayModel(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	snapshot := awaitPickerSnapshot(t, sends)

	require.Equal(t, uint64(11), snapshot.InteractionID)
	require.Equal(t, uint64(1), snapshot.Revision)
	require.NotEmpty(t, snapshot.Rows)
	require.NotEmpty(t, snapshot.Title)

	// Every snapshot row resolves through the published keys, and the keys
	// carry session names for the unchanged handoff.
	ac.overlays.pickerMu.Lock()
	keys := ac.overlays.pickerClientKeys
	ac.overlays.pickerMu.Unlock()
	require.Len(t, keys, len(snapshot.Rows))
	for _, row := range snapshot.Rows {
		target, ok := keys[row.Key]
		require.True(t, ok, "snapshot row %q has no resolved target", row.Key)
		require.NotEmpty(t, target.Name, "resolved target carries no session name")
	}
}

func TestPickerClientOpenLeavesOverlayUnpublished(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	_ = awaitPickerSnapshot(t, sends)

	// rt.picker stays nil: commit flows through typed selection, never
	// handlePickerInput — yet the interaction is open.
	ac.overlays.pickerMu.Lock()
	require.Nil(t, ac.overlays.picker)
	require.True(t, ac.overlays.pickerClientOpen)
	require.Equal(t, uint64(11), ac.overlays.pickerClientInteraction)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientSelectionRejectsStaleAndUnknown(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	snapshot := awaitPickerSnapshot(t, sends)

	capability := pickerClientCapability(t, ac, sess)
	// Unknown key on the open interaction reports UnknownKey without
	// closing the interaction.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerSelection{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision, Key: "ff00/ghost"}))
	failure := awaitPickerFailure(t, sends)
	require.Equal(t, protocol.PickerUnknownKey, failure.Code)
	require.Equal(t, snapshot.InteractionID, failure.InteractionID)

	// Closed interaction reports StaleRevision.
	d.closePickerClientForAttachment(ac, effect, snapshot.InteractionID)
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerSelection{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision, Key: snapshot.Rows[0].Key}))
	closed := awaitPickerFailure(t, sends)
	require.Equal(t, protocol.PickerStaleRevision, closed.Code)
}

func TestPickerClientMoveIntentNeverSnapshots(t *testing.T) {
	_, _, ac, _, _ := pickerClientTestUnit(t)
	// Move intents always render through the overlay picker, even when the
	// capability is advertised: enterPickerForClient is only invoked for
	// navigate intent, so a fresh attachment has no client interaction.
	ac.overlays.pickerMu.Lock()
	require.False(t, ac.overlays.pickerClientOpen)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientWithoutCapabilityRendersOverlay(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	t.Cleanup(releasePTY)
	// No capability: enterPicker installs the overlay model byte-identically
	// to the pre-pilot path.
	d.enterPicker(sess, ac)
	ac.overlays.pickerMu.Lock()
	require.NotNil(t, ac.overlays.picker)
	require.False(t, ac.overlays.pickerClientOpen)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientOpenRepaintsWithoutSessionChange(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	// The open invalidates the session even when nothing else changed:
	// without this repaint the opener's fence would never retire and
	// the palette Enter would hang until its deadline.
	d.openPickerClientForAttachment(ac, effect, 11)
	_ = awaitPickerSnapshot(t, sends)
	select {
	case frame := <-sends:
		require.Equal(t, wire.MsgOutput, frame.Type, "open must trigger an authoritative repaint")
	default:
		t.Fatal("open produced no repaint after the snapshot")
	}
}

func TestPickerClientSessionPickerCommandOpensSnapshot(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, current, ac, sends := newManualSessionWithPTYs(t, p)
	ac.navigationCapabilities = protocol.NavigationCapabilityClientPicker
	effect := beginRecentRoutePaletteEffect(t, d, current, ac)
	effect.uiActionID = 77

	// Full path through overlay dispatch: palette query, then Enter on
	// the same effect. The Enter routes through handlePaletteInput via
	// the overlay runtime, exactly like a real client frame.
	d.enterPalette(current, ac)
	d.handlePaletteInput(ac, []byte("sessionpicker"), effect)
	require.NotNil(t, ac.overlays.palette)
	d.handlePaletteInput(ac, []byte("\r"), effect)

	snapshot := awaitPickerSnapshot(t, sends)
	require.NotZero(t, snapshot.InteractionID)
	require.NotZero(t, snapshot.Revision)
	require.NotEmpty(t, snapshot.Rows)
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerClientOpen)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientCancelPublishesAuthoritativeFullPaint(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	snapshot := awaitPickerSnapshot(t, sends)

	// The client cancels: the daemon retires the interaction and must send
	// the authoritative full paint the client's release path waits for.
	capability := pickerClientCapability(t, ac, sess)
	d.handleAttachmentClientMessage(capability, protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision})
	ac.overlays.pickerMu.Lock()
	require.False(t, ac.overlays.pickerClientOpen)
	ac.overlays.pickerMu.Unlock()
	var output protocol.Output
	frame := awaitTestValue(t, sends, "cancel published no authoritative paint")
	require.Equal(t, wire.MsgOutput, frame.Type)
	output, err := wire.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full, "cancel release paint must be authoritative")
}

func TestPickerClientPaletteFramesOpenInteraction(t *testing.T) {
	d, sess, ac, sends, _ := pickerClientTestUnit(t)
	capability := pickerClientCapability(t, ac, sess)

	// Drive the real attached-client frame path instead of calling the
	// palette handler: Alt+Space opens the palette, the query selects the
	// session-picker command, and Enter executes it.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 1, ActionID: 4, Data: []byte("\x1b ")}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 2, ActionID: 5, Data: []byte("SSP")}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 3, ActionID: 6, Data: []byte("\r")}))

	snapshot := awaitPickerSnapshot(t, sends)
	require.NotZero(t, snapshot.InteractionID)
	require.NotEmpty(t, snapshot.Rows)
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerClientOpen)
	require.Nil(t, ac.overlays.picker, "client mode never installs the overlay model")
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientSelectionRequiresExactRevision(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	first := awaitPickerSnapshot(t, sends)
	// A refresh publishes a newer revision: the client may only commit the
	// model it is displaying, so an older revision must fail even though it
	// is not newer than the daemon's current one.
	d.refreshPickerClientSnapshot(ac)
	second := awaitPickerSnapshot(t, sends)
	require.Greater(t, second.Revision, first.Revision)

	capability := pickerClientCapability(t, ac, sess)
	key := second.Rows[0].Key
	for _, revision := range []uint64{second.Revision - 1, second.Revision + 1} {
		require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerSelection{
			CauseActionID: 9, InteractionID: second.InteractionID, Revision: revision, Key: key,
		}))
		failure := awaitPickerFailure(t, sends)
		require.Equal(t, protocol.PickerStaleRevision, failure.Code, "revision %d must be rejected", revision)
		ac.overlays.pickerMu.Lock()
		require.True(t, ac.overlays.pickerClientOpen, "a rejected revision must not close the interaction")
		ac.overlays.pickerMu.Unlock()
	}
}

func TestPickerClientInteractionDropsRawInput(t *testing.T) {
	writes := make(chan []byte, 8)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends, effect := pickerClientTestUnitWithPTY(t, p, releasePTY)
	capability := pickerClientCapability(t, ac, sess)

	// A normal attachment forwards keys to the session.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 1, ActionID: 1, Data: []byte("x")}))
	require.Equal(t, []byte("x"), awaitTestValue(t, writes, "session input never reached the pty"))

	d.openPickerClientForAttachment(ac, effect, 11)
	snapshot := awaitPickerSnapshot(t, sends)

	// While the client picker is open it owns user input: keys and mouse
	// reports must not reach the session, and the interaction stays open.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 2, ActionID: 2, Data: []byte("hidden")}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 3, ActionID: 3, Data: []byte("\x1b[<0;5;5M")}))
	select {
	case frame := <-writes:
		t.Fatalf("picker-owned input reached the pty: %q", frame)
	case <-time.After(100 * time.Millisecond):
	}

	// Closing the interaction restores normal routing.
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision}))
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.Input{InputSeq: 4, ActionID: 4, Data: []byte("visible")}))
	require.Equal(t, []byte("visible"), awaitTestValue(t, writes, "session input did not resume after the close"))
}

func TestPickerClientSelectionClosesBeforeTheDestinationPaint(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	snapshot := awaitPickerSnapshot(t, sends)
	capability := pickerClientCapability(t, ac, sess)

	// Selecting the current session is a same-peer handoff. The interaction
	// must be retired before that handoff: the client releases only on a
	// paint accepted after the daemon's close, so the destination paint has
	// to follow it on the wire. Drain the open's own repaint first.
	drainAllFrames(sends)
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerSelection{
		CauseActionID: 9, InteractionID: snapshot.InteractionID, Revision: snapshot.Revision, Key: snapshot.Rows[0].Key,
	}))

	// Collect the handoff frames for a short settle window, then require the
	// close to precede any paint: the client only releases on a paint
	// accepted after the daemon's close, so a paint emitted during the
	// handoff must follow it on the wire.
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
	require.Equal(t, wire.MsgPickerCloseServer, order[0], "the close must precede the handoff")
	for i, frameType := range order {
		if frameType == wire.MsgOutput {
			require.NotZero(t, i, "a paint must not precede the close")
		}
	}
	ac.overlays.pickerMu.Lock()
	require.False(t, ac.overlays.pickerClientOpen)
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientInteractionDropsClipboardImage(t *testing.T) {
	writes := make(chan []byte, 8)
	p, releasePTY := newBlockingPTYWithWrites(t, writes)
	d, sess, ac, sends, effect := pickerClientTestUnitWithPTY(t, p, releasePTY)
	capability := pickerClientCapability(t, ac, sess)

	temp := t.TempDir()
	t.Setenv("TMPDIR", temp)
	push := protocol.ImagePush{InputSeq: 1, Mime: "image/png", Data: []byte("\x89PNG\r\n\x1a\n")}

	d.openPickerClientForAttachment(ac, effect, 11)
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
	require.False(t, d.handleAttachmentClientMessage(capability, protocol.PickerClose{InteractionID: snapshot.InteractionID, Revision: snapshot.Revision}))
	push.InputSeq = 2
	require.False(t, d.handleAttachmentClientMessage(capability, push))
	select {
	case frame := <-writes:
		require.Contains(t, string(frame), ".png")
	case <-time.After(2 * time.Second):
		t.Fatal("clipboard path did not resume after the close")
	}
}

func TestPickerClientSnapshotRefreshThroughDirectoryNotification(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	first := awaitPickerSnapshot(t, sends)

	// The row set changes for real: a new session appears in the catalogue.
	_, err := createSessionForTest(d, "third", false, "", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, nil)
	require.NoError(t, err)

	// The catalogue notification path must reach the client picker even
	// though it installs no overlay model: refreshRemoteDirectoryViews is
	// the entry the directory subscription calls.
	d.refreshRemoteDirectoryViews()
	second := awaitPickerSnapshot(t, sends)
	require.Equal(t, first.InteractionID, second.InteractionID)
	require.Greater(t, second.Revision, first.Revision)
	require.Greater(t, len(second.Rows), len(first.Rows), "the new session must appear as a row")
	ac.overlays.pickerMu.Lock()
	require.Nil(t, ac.overlays.picker, "a client-owned picker never installs the overlay model")
	ac.overlays.pickerMu.Unlock()
}

func TestPickerClientRefreshPublishesNewerRevision(t *testing.T) {
	d, _, ac, sends, effect := pickerClientTestUnit(t)

	d.openPickerClientForAttachment(ac, effect, 11)
	first := awaitPickerSnapshot(t, sends)
	require.Equal(t, uint64(1), first.Revision)

	// A row-set or size change republishes a full snapshot with a newer
	// revision through the current attachment capability.
	d.refreshPickerClientSnapshot(ac)
	second := awaitPickerSnapshot(t, sends)
	require.Equal(t, first.InteractionID, second.InteractionID)
	require.Greater(t, second.Revision, first.Revision)
	require.Equal(t, len(first.Rows), len(second.Rows))

	// A closed interaction republishes nothing: only the close notification
	// and the authoritative repaint may follow.
	require.True(t, d.closePickerClientForAttachment(ac, effect, first.InteractionID))
	for _, frame := range drainAllFrames(sends) {
		require.NotEqual(t, wire.MsgPickerSnapshot, frame.Type, "closed interaction republished a snapshot")
	}
}
