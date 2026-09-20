package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// TestMultiplexLatestValidGeometryClaimant proves three independently admitted
// attachments arbitrate shared content by claim sequence, while a transaction
// captured before a later claim cannot publish. Removing the current claimant
// would make the stale transaction win and fail this test.
func TestMultiplexLatestValidGeometryClaimant(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, first, _ := newManualSessionWithPTYs(t, pty)
	first.setGeometry(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}, PixelWidth: 800, PixelHeight: 480})
	addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}, PixelWidth: 1000, PixelHeight: 600})
	third := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}, PixelWidth: 1200, PixelHeight: 800})

	require.True(t, sess.geometry.reconcile(d, sess, nil))
	requireMultiplexGeometry(t, sess, third, domain.Size{Cols: 120, Rows: 38})

	// Capture an earlier claimant's new request, then supersede it before the
	// commit fence. A stale transaction must not overwrite the third claim.
	first.setGeometry(domain.Geometry{Size: domain.Size{Cols: 90, Rows: 28}, PixelWidth: 900, PixelHeight: 560})
	var superseded bool
	d.beforeSessionResizePublication = func() {
		if superseded {
			return
		}
		superseded = true
		third.setGeometry(domain.Geometry{Size: domain.Size{Cols: 130, Rows: 42}, PixelWidth: 1300, PixelHeight: 840})
		_, ok := sess.geometry.claimAttachment(sess, third)
		require.True(t, ok)
	}
	require.False(t, sess.geometry.reconcile(d, sess, first))
	tab := sess.tabs[0]
	tab.mu.Lock()
	require.Equal(t, domain.Size{Cols: 120, Rows: 38}, tab.size, "stale transaction committed before the current-claim fence rejected it")
	tab.mu.Unlock()
	d.beforeSessionResizePublication = nil
	require.True(t, sess.geometry.reconcile(d, sess, nil))
	requireMultiplexGeometry(t, sess, third, domain.Size{Cols: 130, Rows: 40})
}

// TestMultiplexClaimantSuspendDisconnectFallback proves removal chooses the
// newest remaining claim, not attachment insertion order or a default. Picking
// the oldest claim would produce 80x22 after third is suspended.
func TestMultiplexClaimantSuspendDisconnectFallback(t *testing.T) {
	d, sess, first, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	first.setGeometry(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	second := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}})
	third := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}})
	_, claimed := sess.geometry.claimAttachment(sess, first)
	require.True(t, claimed)
	_, claimed = sess.geometry.claimAttachment(sess, second)
	require.True(t, claimed)
	_, claimed = sess.geometry.claimAttachment(sess, third)
	require.True(t, claimed)
	require.True(t, sess.geometry.reconcile(d, sess, nil))

	rc := d.attachCoordinator(sess, nil, first, true)
	d.attachCoordinator(sess, nil, second, true)
	d.attachCoordinator(sess, nil, third, true)
	thirdToken := sess.captureAttachmentCapability(third, third.transport())
	thirdToken.lease = rc.attachmentLease(third)
	third.installTestAttachmentCapability(thirdToken)
	require.NoError(t, d.suspendAttachment(thirdToken, protocol.SuspendAttachment{RequestID: 1}))
	requireMultiplexGeometry(t, sess, second, domain.Size{Cols: 100, Rows: 28})

	d.clientGone(sess, second, second.transport(), true)
	requireMultiplexGeometry(t, sess, first, domain.Size{Cols: 80, Rows: 22})
}

// TestMultiplexClaimantRemovalDoesNotTouchUnrelatedSession proves geometry and
// attachment registries are session-owned. Accidentally using daemon-global
// claimant state would mutate the unrelated session and fail these snapshots.
func TestMultiplexClaimantRemovalDoesNotTouchUnrelatedSession(t *testing.T) {
	d, affected, first, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	first.setGeometry(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}})
	claimant := addMultiplexTestAttachment(t, affected, domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}})
	require.True(t, affected.geometry.reconcile(d, affected, nil))

	other := &session{sessionCore: sessionCore{id: "other", name: "other"}, tabs: []*tab{newTab(newQuietPTY(), domain.Size{Cols: 95, Rows: 34})}}
	otherAttachment := addMultiplexTestAttachment(t, other, domain.Geometry{Size: domain.Size{Cols: 95, Rows: 35}})
	require.True(t, other.geometry.reconcile(d, other, nil))
	otherBefore := other.geometry.appliedSnapshot()

	d.clientGone(affected, claimant, claimant.transport(), true)
	require.True(t, affected.geometry.reconcile(d, affected, first))
	requireMultiplexGeometry(t, affected, first, domain.Size{Cols: 80, Rows: 22})
	require.Equal(t, otherBefore, other.geometry.appliedSnapshot())
	attachments := other.snapshotAttachments()
	require.Len(t, attachments, 1)
	require.Same(t, otherAttachment, attachments[0])
}

// TestMultiplexAttachmentLocalStateAndFrames proves view, renderer cache,
// overlay runtime, and command-result carriage are attachment-owned. Aliasing
// any of those fields, or routing a result to the peer, breaks the snapshots.
func TestMultiplexAttachmentLocalStateAndFrames(t *testing.T) {
	d, sess, first, firstFrames := newManualSessionWithPTYs(t, newQuietPTY())
	peerTransport, peerFrames := newCapturingTransport(t)
	peer, err := d.attachClient(sess, peerTransport, domain.Size{Cols: 90, Rows: 30}, attachClientOptions{})
	require.NoError(t, err)
	require.NotSame(t, first.output, peer.output)
	require.NotSame(t, first.overlays, peer.overlays)

	peerView := peer.viewSnapshot()
	require.True(t, sess.updateAttachmentView(first, func(view *attachmentView) { view.windowTop = 7; view.liveBottom = false }))
	require.Equal(t, peerView, peer.viewSnapshot())

	first.overlays.paletteMu.Lock()
	first.overlays.paletteFeedback = "first-only"
	first.overlays.paletteMu.Unlock()
	peer.overlays.paletteMu.Lock()
	require.Empty(t, peer.overlays.paletteFeedback)
	peer.overlays.paletteMu.Unlock()

	rc := d.attachCoordinator(sess, nil, first, true)
	d.attachCoordinator(sess, nil, peer, true)
	firstToken := sess.captureAttachmentCapability(first, first.transport())
	firstToken.lease = rc.attachmentLease(first)
	first.installTestAttachmentCapability(firstToken)
	peerToken := sess.captureAttachmentCapability(peer, peer.transport())
	peerToken.lease = rc.attachmentLease(peer)
	peer.installTestAttachmentCapability(peerToken)
	require.False(t, d.handleAttachmentClientMessage(firstToken, protocol.CommandRequest{Version: protocol.Version, RequestID: 77, Attached: true, Slug: "next-tab"}))
	result, ok := decodeServerMessage(t, awaitFrame(t, firstFrames, "CommandResult")).(protocol.CommandResult)
	require.True(t, ok)
	require.Equal(t, uint64(77), result.RequestID)
	require.True(t, result.Outcome == protocol.CommandSucceeded, result.Text)
	select {
	case frame := <-peerFrames:
		if command, ok := decodeServerMessage(t, frame).(protocol.CommandResult); ok && command.RequestID == 77 {
			t.Fatal("command result crossed attachment stream")
		}
	default:
	}
}

// TestMultiplexReconnectGenerationFencesOldEffects proves an older attachment
// capability cannot act after a production suspension/activation reconnect.
// Accepting the old token would change size or execute its command.
func TestMultiplexReconnectGenerationFencesOldEffects(t *testing.T) {
	d, sess, reconnecting, oldFrames := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	peer := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}})
	rc := d.attachCoordinator(sess, nil, reconnecting, true)
	d.attachCoordinator(sess, nil, peer, true)
	old := sess.captureAttachmentCapability(reconnecting, reconnecting.transport())
	old.lease = rc.attachmentLease(reconnecting)
	reconnecting.installTestAttachmentCapability(old)

	require.NoError(t, d.suspendAttachment(old, protocol.SuspendAttachment{RequestID: 1}))
	for len(oldFrames) != 0 {
		<-oldFrames
	}
	fresh, _ := newCapturingTransport(t)
	reconnecting.replaceTransport(fresh)
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	require.NoError(t, d.activateAttachment(reconnecting, reconnecting.transportSnapshot(), protocol.ActivateAttachment{RequestID: 2, Target: target, Size: reconnecting.sizeSnapshot()}))
	current := sess.captureAttachmentCapability(reconnecting, fresh)
	require.Greater(t, current.generation, old.generation, "production reconnect did not advance the attachment generation")
	require.NotEqual(t, current.transport.incarnation, old.transport.incarnation)

	// Isolate the generation clause from transport-incarnation fencing: use the
	// current transport and lease with only the previous production generation.
	staleGeneration := current
	staleGeneration.generation = old.generation
	require.False(t, staleGeneration.current(), "previous generation remained current after reconnect")
	before := reconnecting.geometrySnapshot()
	require.False(t, d.handleAttachmentClientMessage(staleGeneration, protocol.Resize{Size: domain.Size{Cols: 140, Rows: 50}}))
	require.False(t, d.handleAttachmentClientMessage(staleGeneration, protocol.CommandRequest{Version: protocol.Version, RequestID: 9, Attached: true, Slug: "next-tab"}))
	require.Equal(t, before, reconnecting.geometrySnapshot())
	select {
	case <-oldFrames:
		t.Fatal("stale generation emitted a frame")
	default:
	}
	require.Contains(t, sess.snapshotAttachments(), peer)
}

// TestMultiplexSingleStreamErrorIsolation proves one failed attachment is
// removed without retiring its siblings or session. Detaching every attachment
// on one transport error, or deleting the session, fails the registry checks.
func TestMultiplexSingleStreamErrorIsolation(t *testing.T) {
	d, sess, failed, _ := newManualSessionWithPTYs(t, &transactionalResizePTY{})
	failedTransport := &closeTrackingTransport{}
	failed.replaceTransport(failedTransport)
	peer := addMultiplexTestAttachment(t, sess, domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}})
	d.attachCoordinator(sess, nil, failed, true)
	d.attachCoordinator(sess, nil, peer, true)
	failed.resumeCapable = false
	failedToken := sess.captureAttachmentCapability(failed, failedTransport)
	failed.installTestAttachmentCapability(failedToken)

	d.detachOnAttachmentSendError(failedToken, failedTransport)
	require.NotContains(t, sess.snapshotAttachments(), failed)
	require.Contains(t, sess.snapshotAttachments(), peer)
	require.Same(t, sess, peer.currentSession())
	d.mu.Lock()
	require.Same(t, sess, d.sessions[sess.id])
	d.mu.Unlock()
	require.True(t, failedTransport.Closed())
}

func addMultiplexTestAttachment(t *testing.T, sess *session, geometry domain.Geometry) *attachedClient {
	t.Helper()
	transport, _ := newCapturingTransport(t)
	attachment := &attachedClient{tr: transport, output: newOutputStateStream()}
	attachment.output.attachment = attachment
	attachment.initOverlays()
	attachment.setGeometry(geometry)
	attachment.setSession(sess)
	require.True(t, sess.registerAttachment(attachment))
	return attachment
}

func requireMultiplexGeometry(t *testing.T, sess *session, claimant *attachedClient, content domain.Size) {
	t.Helper()
	source, ok := sess.geometry.sourceSnapshot(sess)
	require.True(t, ok)
	require.Same(t, claimant, source.attachment)
	require.Equal(t, claimant.geometrySnapshot(), sess.geometry.appliedSnapshot())
	tab := sess.tabs[0]
	tab.mu.Lock()
	defer tab.mu.Unlock()
	require.Equal(t, content, tab.size)
	pane := tab.focusedPane()
	pane.mu.Lock()
	defer pane.mu.Unlock()
	require.Equal(t, content, domain.Size{Cols: pane.screen.Columns(), Rows: pane.screen.Rows()})
}
