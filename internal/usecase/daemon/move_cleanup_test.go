package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

func TestMovePaneReleasesResizeFencesBeforeAttachmentCleanup(t *testing.T) {
	movedPTY, releaseMoved := newBlockingPTY(t)
	remainingPTY, releaseRemaining := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	defer releaseMoved()
	defer releaseRemaining()
	defer releaseDestination()

	d, source, client, _ := newManualSessionWithPTYs(t, movedPTY, remainingPTY)
	require.NotNil(t, d.attachCoordinator(source, nil, client, true))
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	client.sendMu.Lock()
	client.captureFrames = map[*pane]capturedPaneRenderState{moved: {}}
	client.sendMu.Unlock()

	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})},
		active: 0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	moved.mu.Lock()
	owner := moved.effectLeaseLocked()
	moved.mu.Unlock()
	members := []resizeMember{{session: source, tab: sourceTab, pane: moved, owner: owner}}
	rc := source.renderCoordinator()
	require.NotNil(t, rc)
	lease := rc.attachmentLease(client)
	epoch := rc.recordResizeRequestForLease(client.size, client, lease)
	ticket, admitted := beginActiveLeaseEffect(source, client, lease)
	require.True(t, admitted)
	defer ticket.End()

	sendLocked := make(chan struct{})
	allowResizeFence := make(chan struct{})
	d.afterResizeCommitSendLocked = func() {
		close(sendLocked)
		<-allowResizeFence
	}
	defer func() { d.afterResizeCommitSendLocked = nil }()
	resizeDone := make(chan bool, 1)
	go func() {
		resizeDone <- d.publishResizeCommit(members, source, client, lease, epoch, ticket, client.size)
	}()
	awaitTestCompletion(t, sendLocked, "resize commit did not acquire sendMu")

	insideMoveCommit := make(chan struct{})
	allowMoveCommit := make(chan struct{})
	d.beforeMovePaneCommit = func() {
		close(insideMoveCommit)
		<-allowMoveCommit
	}
	defer func() { d.beforeMovePaneCommit = nil }()
	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.movePane(movePaneRequest{
			Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
			SourceTabID:      domain.TabStableID(sourceTab.stableID),
			SourcePaneID:     domain.PaneStableID(moved.stableID),
			Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			DestinationTabID: domain.TabStableID(destinationTab.stableID),
		})
	}()
	awaitTestCompletion(t, insideMoveCommit, "move did not reach its commit section")

	// Resize now owns sendMu and waits for the moved pane fence. The move may
	// finish only if it releases every resize fence before capture cleanup tries
	// to acquire sendMu.
	close(allowResizeFence)
	close(allowMoveCommit)
	require.NoError(t, awaitTestValue(t, moveDone, "move deadlocked against resize commit"))
	require.False(t, awaitTestValue(t, resizeDone, "resize commit did not drop stale owner"))
}

func TestMovePaneCleanupUsesCommitPointSourceAttachmentToken(t *testing.T) {
	movedPTY, releaseMoved := newBlockingPTY(t)
	remainingPTY, releaseRemaining := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	defer releaseMoved()
	defer releaseRemaining()
	defer releaseDestination()

	d, source, displaced, _ := newManualSessionWithPTYs(t, movedPTY, remainingPTY)
	sourceTab := source.tabs[0]
	remainingTab := source.tabs[1]
	sourceTab.stableID = "source-tab"
	remainingTab.stableID = "remaining-tab"
	moved := sourceTab.focusedPane()
	require.NotNil(t, d.attachCoordinator(source, nil, displaced, true))

	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})},
		active: 0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	// Both clients carry state tied specifically to the pane. The displaced
	// client was captured before the fence wait; the replacement is the only
	// attachment that may be cleaned after commit.
	displaced.overlays.copyMu.Lock()
	displaced.overlays.copyPane = moved
	displaced.overlays.copyMode = &scopy.Mode{}
	displaced.overlays.copyMu.Unlock()
	displaced.sendMu.Lock()
	displaced.captureFrames = map[*pane]capturedPaneRenderState{moved: {}}
	displaced.sendMu.Unlock()

	replacementTransport, _ := newCapturingTransport(t)
	replacement := &attachedClient{
		tr: replacementTransport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24},
	}
	replacement.initOverlays()
	replacement.setSession(source)
	replacement.overlays.copyMu.Lock()
	replacement.overlays.copyPane = moved
	replacement.overlays.copyMode = &scopy.Mode{}
	replacement.overlays.copyMu.Unlock()
	replacement.sendMu.Lock()
	replacement.captureFrames = map[*pane]capturedPaneRenderState{moved: {}}
	replacement.sendMu.Unlock()

	// Keep the source layout fence occupied while the move has captured its
	// pre-fence source client. Replacing the client at this barrier reproduces
	// the stale cleanup race without sleeps.
	source.layoutApplyMu.Lock()
	snapshot := make(chan struct{})
	releaseSnapshot := make(chan struct{})
	d.afterMovePaneSourceSnapshot = func() {
		close(snapshot)
		<-releaseSnapshot
	}
	defer func() { d.afterMovePaneSourceSnapshot = nil }()

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.movePane(movePaneRequest{
			Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
			SourceTabID:      domain.TabStableID(sourceTab.stableID),
			SourcePaneID:     domain.PaneStableID(moved.stableID),
			Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			DestinationTabID: domain.TabStableID(destinationTab.stableID),
		})
	}()
	awaitTestCompletion(t, snapshot, "move did not capture its pre-fence source attachment")

	source.mu.Lock()
	source.registerAttachmentLocked(replacement)
	source.mu.Unlock()
	close(releaseSnapshot)
	source.layoutApplyMu.Unlock()
	require.NoError(t, awaitTestValue(t, moveDone, "move did not finish after source fence release"))

	displaced.overlays.copyMu.Lock()
	require.NotNil(t, displaced.overlays.copyMode, "displaced source client was incorrectly cleaned")
	require.Same(t, moved, displaced.overlays.copyPane)
	displaced.overlays.copyMu.Unlock()
	displaced.sendMu.Lock()
	_, displacedCaptured := displaced.captureFrames[moved]
	displaced.sendMu.Unlock()
	require.True(t, displacedCaptured, "displaced source capture was incorrectly pruned")

	replacement.overlays.copyMu.Lock()
	require.Nil(t, replacement.overlays.copyMode, "current source client copy mode was not cleaned")
	require.Nil(t, replacement.overlays.copyPane)
	replacement.overlays.copyMu.Unlock()
	replacement.sendMu.Lock()
	_, replacementCaptured := replacement.captureFrames[moved]
	replacement.sendMu.Unlock()
	require.False(t, replacementCaptured, "current source capture was not pruned")
}
