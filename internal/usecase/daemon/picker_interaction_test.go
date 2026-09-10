package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
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
