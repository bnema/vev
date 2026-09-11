package daemon

import (
	"errors"
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
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

// TestPickerPreviewCapturesRemoteRowsThroughThePreviewClient pins the daemon
// half of the coexistence contract: a remote picker row is still previewed
// through the daemon's own remote viewport client and its cache, independent of
// whatever the launching client resolves for itself.
func TestPickerPreviewCapturesRemoteRowsThroughThePreviewClient(t *testing.T) {
	clock := &remotePreviewTestClock{now: time.Unix(100, 0)}
	client := &remotePreviewTestClient{result: remotePreviewCacheResult(remotePreviewCacheTarget(), 1)}
	d := newTestDaemon(t, nil, clock)
	d.remotePreviewClient = client
	target := remotePreviewCacheTarget()

	viewport := d.capturePickerPreview(picker.Target{RemoteTarget: &target}, protocol.PickerIntentNavigation, 1, 1)
	require.Equal(t, protocol.PickerPreviewOK, viewport.Status)
	require.Equal(t, uint16(1), viewport.Width)
	require.Equal(t, 'x', viewport.Cells[0].Rune)
	require.Equal(t, target, client.lastTarget)
	require.Equal(t, uint16(1), client.lastWidth)

	// A failed remote fetch reports unavailable instead of a stale viewport.
	client.err = errors.New("remote host unreachable")
	client.calls = 0
	d.remotePreview.cache = nil
	failed := d.capturePickerPreview(picker.Target{RemoteTarget: &target}, protocol.PickerIntentNavigation, 1, 1)
	require.Equal(t, protocol.PickerPreviewUnavailable, failed.Status)
	require.Zero(t, failed.Width)
	require.Empty(t, failed.Cells)
}

// TestPickerOpenSurvivesOneHostRegisteredTwice pins the row-key contract: the
// same machine can be registered under more than one target (a pinned alias and
// a target learned by attach), and both endpoints legitimately expose the same
// session lifecycle and name. Row keys must therefore stay unique per endpoint,
// or the snapshot is rejected and the picker never opens at all.
func TestPickerOpenSurvivesOneHostRegisteredTwice(t *testing.T) {
	d := newRemotePickerDaemon()
	lifecycle := remoteLifecycleForTest()
	session := catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle, Name: "work", State: catalogue.RemoteCatalogSessionUp,
		Tabs: []catalogue.RemoteCatalogTab{{ID: "tab-1", Index: 0, Name: "1"}},
	}
	seedRemoteDirectory(t, d,
		reachableDirectoryHost("remote", time.Unix(10, 0), session),
		reachableDirectoryHost("demo@remote", time.Unix(10, 0), session),
	)
	sess, ac, sends := addRemoteRefreshPickerOwner(t, d, "local")
	effect := admitPickerEffectForTest(t, sess, ac)

	require.NoError(t, d.openPickerForAttachment(ac, effect, protocol.PickerIntentNavigation, moveSourceLocator{}, 0))
	offer := awaitPickerOffer(t, sends)
	require.Equal(t, protocol.PickerIntentNavigation, offer.Intent)
	snapshot := awaitPickerSnapshot(t, sends)

	seen := make(map[string]struct{}, len(snapshot.Lines))
	workRows := 0
	for _, line := range snapshot.Lines {
		if line.Key == "" {
			continue
		}
		_, duplicate := seen[line.Key]
		require.False(t, duplicate, "row key %q was published twice", line.Key)
		seen[line.Key] = struct{}{}
		if line.Kind == protocol.PickerLineSession && line.Label == "work" {
			workRows++
		}
	}
	require.NotEmpty(t, snapshot.Lines)
	require.Equal(t, 2, workRows, "both endpoints of the same host must publish their rows")
}
