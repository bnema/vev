package daemon

import (
	"sync"
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/picker"
)

func awaitPickerPreview(t *testing.T, sends chan wire.Envelope) protocol.PickerPreview {
	t.Helper()
	for {
		frame := awaitTestValue(t, sends, "picker preview was not published")
		if preview, ok := decodeServerMessage(t, frame).(protocol.PickerPreview); ok {
			return preview
		}
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
		require.NotEqual(t, "PickerPreview", envelopeMessageName(t, frame.Payload), "a request for another interaction must publish nothing")
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
		require.NotEqual(t, "PickerPreview", envelopeMessageName(t, frame.Payload), "a superseded generation must not publish")
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
		require.NotEqual(t, "PickerPreview", envelopeMessageName(t, frame.Payload))
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

// pickerPreviewKeyForSession returns the opaque row key whose target resolves
// to sess. The preview request names a key, so a cross-session test must read
// the same mapping the serving source published.
func pickerPreviewKeyForSession(t *testing.T, ac *attachedClient, sess *session) string {
	t.Helper()
	ac.overlays.pickerMu.Lock()
	defer ac.overlays.pickerMu.Unlock()
	for key, target := range ac.overlays.pickerKeys {
		if target.RemoteTarget == nil && target.Session == sess.id {
			return key
		}
	}
	return ""
}

// TestPickerPreviewCrossSessionFollowsTheTargetsRenderCoordinator pins the
// cross-session contract: the subscription that refreshes the displayed row
// lives on the target session's coordinator. A target render wakes the preview
// and keeps the headless target renderable, while a viewer-session render must
// not drive the target's preview at all.
func TestPickerPreviewCrossSessionFollowsTheTargetsRenderCoordinator(t *testing.T) {
	viewerPTY, releaseViewer := newBlockingPTY(t)
	t.Cleanup(releaseViewer)
	d, viewerSess, ac, sends := newManualSessionWithPTYs(t, viewerPTY)
	d.ptys = newFactorySeq(t, newQuietPTY())
	targetSess, err := createSessionForTest(d, "second", false, "", domain.Size{Cols: 80, Rows: 24}, terminalEnv{}, nil)
	require.NoError(t, err)

	effect, ok := ac.beginAttachmentEffect(captureAttachmentCapability(viewerSess, ac, ac.transport()))
	require.True(t, ok)
	openTestPicker(t, d, ac, effect, sends, protocol.PickerIntentNavigation)
	snapshot := awaitPickerSnapshot(t, sends)
	key := pickerPreviewKeyForSession(t, ac, targetSess)
	require.NotEmpty(t, key, "the snapshot must offer a navigable row for the target session")

	clock := newCoordinatorMockClock(t, 16)
	d.clock = clock.clock
	viewerCoordinator := attachmentRenderCoordinator(viewerSess)
	require.Nil(t, attachmentRenderCoordinator(targetSess), "an un-previewed target owns no coordinator")

	d.handleAttachmentClientMessage(pickerClientCapability(t, ac, viewerSess), pickerPreviewRequestFor(snapshot, key, 20, 5))
	first := awaitPickerPreview(t, sends)
	require.Equal(t, protocol.PickerPreviewOK, first.Status)

	targetCoordinator := attachmentRenderCoordinator(targetSess)
	require.NotNil(t, targetCoordinator, "the preview must subscribe on the target session's coordinator")
	require.True(t, targetCoordinator.hasPreviewSubscribers())
	if viewerCoordinator != nil {
		require.False(t, viewerCoordinator.hasPreviewSubscribers(), "the viewer session must not own the target's subscription")
	}
	targetTab := targetSess.tabs[0]
	require.True(t, d.paneRenderable(targetSess, targetTab, targetTab.focusedPane()),
		"a previewed headless target must stay renderable")

	// A target render wakes the target's subscription and republishes.
	d.processPTYData(targetSess, targetTab, targetTab.focusedPane(), []byte("target repaint"), false)
	fireCoordinatorTimer(t, targetCoordinator, drainCoordinatorTimers(clock), minOutputRenderDeadline)
	refreshed := awaitPickerPreview(t, sends)
	require.Equal(t, key, refreshed.Key)
	require.Equal(t, protocol.PickerPreviewOK, refreshed.Status)

	// A viewer-session render must not publish the target's preview. The direct
	// invalidateRender path is urgent, so it arms the urgent deadline.
	viewerCoordinator = d.attachCoordinator(viewerSess, nil, ac, true)
	d.invalidateRender(viewerSess, ac, true, "picker_preview_test.go:viewer")
	fireCoordinatorTimer(t, viewerCoordinator, drainCoordinatorTimers(clock), urgentRenderDeadline)
	for _, frame := range drainAllFrames(sends) {
		require.NotEqual(t, "PickerPreview", envelopeMessageName(t, frame.Payload),
			"a viewer-session render must not publish the target's preview")
	}

	// Retiring the interaction removes the subscription from the target's own
	// coordinator, so the headless target stops being renderable. A nil effect
	// retires the interaction without a close confirmation.
	require.True(t, d.closePickerForAttachment(ac, nil, snapshot.InteractionID))
	require.False(t, targetCoordinator.hasPreviewSubscribers(),
		"teardown must remove the exact target subscription")
	require.False(t, d.paneRenderable(targetSess, targetTab, targetTab.focusedPane()),
		"a retired preview must stop keeping the target renderable")
}

// manualPreviewClock is a deterministic ports.Clock for the remote preview
// refresher. Timers never fire on their own, so a test can prove the worker
// armed one and then advance it explicitly without any wall-clock sleep.
type manualPreviewClock struct {
	mu      sync.Mutex
	now     time.Time
	waiting chan time.Time
	armed   chan struct{}
}

func newManualPreviewClock(now time.Time) *manualPreviewClock {
	return &manualPreviewClock{now: now, armed: make(chan struct{}, 64)}
}

func (c *manualPreviewClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualPreviewClock) NewTimer(time.Duration) ports.Timer {
	ch := make(chan time.Time, 1)
	c.mu.Lock()
	c.waiting = ch
	c.mu.Unlock()
	select {
	case c.armed <- struct{}{}:
	default:
	}
	return manualPreviewTimer{ch: ch}
}

func (c *manualPreviewClock) awaitTimer(t *testing.T) {
	t.Helper()
	select {
	case <-c.armed:
	case <-time.After(2 * time.Second):
		t.Fatal("quiet viewer never armed a remote preview refresh")
	}
}

func (c *manualPreviewClock) fire(now time.Time) {
	c.mu.Lock()
	ch := c.waiting
	c.waiting = nil
	c.now = now
	c.mu.Unlock()
	if ch != nil {
		ch <- now
	}
}

type manualPreviewTimer struct{ ch chan time.Time }

func (t manualPreviewTimer) C() <-chan time.Time      { return t.ch }
func (t manualPreviewTimer) Reset(time.Duration) bool { return false }
func (t manualPreviewTimer) Stop() bool               { return true }

// TestRemotePickerPreviewRefreshesWithoutViewerRenderWake is the regression for
// the quiet-viewer stale delay: a selected remote row must keep refreshing on
// its own cadence even when the viewer renders nothing at all. Relying on a
// viewer render wake left a quiet viewer stale indefinitely.
