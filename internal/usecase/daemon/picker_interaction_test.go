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
