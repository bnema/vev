package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/picker"
)

func awaitPickerPreview(t *testing.T, sends chan wire.Frame) protocol.PickerPreview {
	t.Helper()
	for {
		frame := awaitTestValue(t, sends, "picker preview was not published")
		if frame.Type != wire.MsgPickerPreview {
			continue
		}
		preview, err := wire.UnmarshalPickerPreview(frame.Payload)
		require.NoError(t, err)
		return preview
	}
}

func pickerPreviewRequestFor(snapshot protocol.PickerSnapshot, key string, width, height uint16) protocol.PickerPreviewRequest {
	return protocol.PickerPreviewRequest{
		Version: protocol.PickerPreviewSchemaVersion, InteractionID: snapshot.InteractionID,
		SourceID: servingPickerSourceID, Key: key, Width: width, Height: height,
	}
}

func TestPickerPreviewRequestPublishesCapturedViewport(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)
	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)
	key := firstSelectableKey(t, snapshot, protocol.PickerCanNavigate)

	capability := pickerClientCapability(t, ac, sess)
	request := pickerPreviewRequestFor(snapshot, key, 20, 5)
	require.NoError(t, protocol.ValidatePickerPreviewRequest(request))
	d.handleAttachmentClientMessage(capability, request)

	preview := awaitPickerPreview(t, sends)
	require.Equal(t, snapshot.InteractionID, preview.InteractionID)
	require.Equal(t, servingPickerSourceID, preview.SourceID)
	require.Equal(t, key, preview.Key)
	require.Equal(t, protocol.PickerPreviewOK, preview.Status, "the fixture session must capture a viewport")
	require.Equal(t, uint16(20), preview.Width)
	require.LessOrEqual(t, preview.Height, uint16(5))
	require.Len(t, preview.Cells, int(preview.Width)*int(preview.Height))
	require.NotNil(t, preview.FrameRows())
}

func TestPickerPreviewUnknownKeyReportsNoSuchTarget(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)
	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)

	capability := pickerClientCapability(t, ac, sess)
	request := pickerPreviewRequestFor(snapshot, "zz/retired", 20, 5)
	d.handleAttachmentClientMessage(capability, request)

	preview := awaitPickerPreview(t, sends)
	require.Equal(t, protocol.PickerPreviewNoSuchTarget, preview.Status)
	require.Zero(t, preview.Width)
	require.Empty(t, preview.Cells)
}

func TestPickerPreviewStaleRequestPublishesNothing(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)
	offer := openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)
	key := firstSelectableKey(t, snapshot, protocol.PickerCanNavigate)
	capability := pickerClientCapability(t, ac, sess)

	for range 3 {
		request := pickerPreviewRequestFor(snapshot, key, 20, 5)
		request.InteractionID = offer.InteractionID + 1
		d.handleAttachmentClientMessage(capability, request)
	}
	for _, frame := range drainAllFrames(sends) {
		require.NotEqual(t, wire.MsgPickerPreview, frame.Type, "a request for another interaction must publish nothing")
	}

	// A superseded generation is dropped even though its request was valid.
	live := pickerPreviewRequestFor(snapshot, key, 20, 5)
	d.handleAttachmentClientMessage(capability, live)
	first := awaitPickerPreview(t, sends)
	ac.overlays.pickerMu.Lock()
	generation := ac.overlays.pickerPreviewGeneration
	target, known := ac.overlays.pickerKeys[live.Key]
	ac.overlays.pickerMu.Unlock()
	require.True(t, known)
	d.publishPickerPreviewForAttachment(ac, live, generation-1, target, protocol.PickerIntentNavigation, known)
	for _, frame := range drainAllFrames(sends) {
		require.NotEqual(t, wire.MsgPickerPreview, frame.Type, "a superseded generation must not publish")
	}
	require.Equal(t, key, first.Key)
}

func TestPickerPreviewRetiredInteractionStopsPublishing(t *testing.T) {
	d, sess, ac, sends, effect := pickerClientTestUnit(t)
	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)
	key := firstSelectableKey(t, snapshot, protocol.PickerCanNavigate)
	capability := pickerClientCapability(t, ac, sess)
	d.handleAttachmentClientMessage(capability, pickerPreviewRequestFor(snapshot, key, 20, 5))
	require.Equal(t, protocol.PickerPreviewOK, awaitPickerPreview(t, sends).Status)

	require.True(t, d.closePickerForAttachment(ac, admitPickerEffectForTest(t, sess, ac), snapshot.InteractionID))
	_ = drainAllFrames(sends)
	ac.overlays.pickerMu.Lock()
	generation := ac.overlays.pickerPreviewGeneration
	ac.overlays.pickerMu.Unlock()
	require.Zero(t, generation, "retiring the interaction must clear the preview generation")

	for _, frame := range drainAllFrames(sends) {
		require.NotEqual(t, wire.MsgPickerPreview, frame.Type)
	}
}

func TestClampPickerPreviewFitsTheRequestedBounds(t *testing.T) {
	captured := picker.Preview{
		Width: 4, Height: 2,
		Rows: [][]renderer.Cell{
			{{Rune: 'a'}, {Rune: 'b'}, {Rune: 'c'}, {Rune: 'd'}},
			{{Rune: 'e'}, {Rune: '界'}, {Continuation: true}, {Rune: 'h'}},
		},
	}
	fitted := clampPickerPreview(captured, 3, 1)
	require.Equal(t, protocol.PickerPreviewOK, fitted.Status)
	require.Equal(t, uint16(3), fitted.Width)
	require.Equal(t, uint16(1), fitted.Height)
	require.Len(t, fitted.Cells, 3)
	require.Equal(t, 'a', fitted.Cells[0].Rune)

	// A double-width cell whose continuation column is cropped out becomes
	// blank, so the run never ends on a wide cell.
	wide := clampPickerPreview(captured, 2, 2)
	require.Equal(t, protocol.PickerPreviewOK, wide.Status)
	require.Len(t, wide.Cells, 4)
	require.Equal(t, 'e', wide.Cells[2].Rune)
	require.Zero(t, wide.Cells[3].Rune)
	require.False(t, wide.Cells[3].Continuation)

	// Every row pads to the requested width, and the height stays the number of
	// captured rows (never more than the request).
	padded := clampPickerPreview(captured, 4, 3)
	require.Equal(t, protocol.PickerPreviewOK, padded.Status)
	require.Len(t, padded.Cells, 8)
	require.Equal(t, uint16(2), padded.Height)

	require.Equal(t, protocol.PickerPreviewUnavailable, clampPickerPreview(picker.Preview{}, 4, 2).Status)
	require.Equal(t, protocol.PickerPreviewUnavailable, clampPickerPreview(captured, 0, 2).Status)
}
