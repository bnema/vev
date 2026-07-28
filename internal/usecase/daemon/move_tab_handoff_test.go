package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
)

func setupMoveTabFinalSourceHandoff(t *testing.T) (*Daemon, *session, *attachedClient, *session, *attachedClient, *closeTrackingTransport, *tab) {
	t.Helper()
	d, source, follower, _ := newManualSessionWithPTYs(t, newQuietPTY())
	follower.replaceTransport(&closeTrackingTransport{})
	moved := source.tabs[0]
	moved.stableID = "moved-tab"
	require.NotNil(t, d.attachCoordinator(source, nil, follower, true))

	destination := addMoveTabTestSession(d, "destination", "destination-tab")
	displacedTransport := &closeTrackingTransport{}
	displaced := &attachedClient{
		tr: displacedTransport, output: newOutputStateStream(), size: follower.size,
	}
	displaced.initOverlays()
	displaced.setSession(destination)
	destination.mu.Lock()
	destination.client = displaced
	destination.mu.Unlock()
	require.NotNil(t, d.attachCoordinator(destination, nil, displaced, true))

	return d, source, follower, destination, displaced, displacedTransport, moved
}

func moveTabFinalSource(t *testing.T, d *Daemon, source, destination *session, moved *tab) {
	t.Helper()
	require.NoError(t, d.moveTab(moveTabRequest{
		Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID: domain.TabStableID(moved.stableID),
		Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
	}))
}

func detachedWithReason(frames []ports.Frame, reason uint8) bool {
	for _, frame := range frames {
		if frame.Type != ports.MsgDetached {
			continue
		}
		detached, err := ports.UnmarshalDetached(frame.Payload)
		if err == nil && detached.Reason == reason {
			return true
		}
	}
	return false
}

func TestMoveTabFinalSourceSnatchesDestinationActiveClient(t *testing.T) {
	d, source, follower, destination, displaced, displacedTransport, moved := setupMoveTabFinalSourceHandoff(t)

	moveTabFinalSource(t, d, source, destination, moved)
	d.attachmentCleanupWg.Wait()

	destination.mu.Lock()
	require.Same(t, follower, destination.client)
	require.Contains(t, destination.snatched, displaced)
	require.Same(t, moved, destination.tabs[destination.active])
	destination.mu.Unlock()
	require.Same(t, destination, follower.currentSession())
	require.Equal(t, attachmentActive, destination.attachmentRole(follower))
	require.Equal(t, attachmentSnatched, destination.attachmentRole(displaced))

	frames := displacedTransport.Sends()
	require.Len(t, frames, 1)
	require.Equal(t, ports.MsgOutput, frames[0].Type)
	output, err := ports.UnmarshalOutput(frames[0].Payload)
	require.NoError(t, err)
	require.Contains(t, string(output.Data), "Session snatched")
	require.False(t, detachedWithReason(frames, ports.ReasonReplaced))
	require.False(t, displacedTransport.Closed())
}

func TestMoveTabFinalSourcePreservesDestinationSnatchedWaiters(t *testing.T) {
	d, source, follower, destination, displaced, _, moved := setupMoveTabFinalSourceHandoff(t)

	waitingTransport := &closeTrackingTransport{}
	waiting := &attachedClient{tr: waitingTransport, output: newOutputStateStream(), size: follower.size}
	waiting.initOverlays()
	waiting.setSession(destination)
	destination.mu.Lock()
	destination.addSnatchedLocked(waiting)
	destination.mu.Unlock()

	moveTabFinalSource(t, d, source, destination, moved)
	d.attachmentCleanupWg.Wait()

	destination.mu.Lock()
	require.Contains(t, destination.snatched, displaced)
	require.Contains(t, destination.snatched, waiting)
	require.Len(t, destination.snatched, 2)
	destination.mu.Unlock()
	requireSingleOwnerAndLease(t, destination, follower, displaced, waiting)
}

func TestMoveTabFinalSourceRetiresSourceParkedAndSnatchedClients(t *testing.T) {
	d, source, follower, destination, _, _, moved := setupMoveTabFinalSourceHandoff(t)

	parkedTransport := &closeTrackingTransport{}
	parked := &attachedClient{tr: parkedTransport, output: newOutputStateStream(), size: follower.size, resumeCapable: true}
	parked.initOverlays()
	require.True(t, d.parkAttachment(source, parked))

	sourceSnatchedTransport := &closeTrackingTransport{}
	sourceSnatched := &attachedClient{tr: sourceSnatchedTransport, output: newOutputStateStream(), size: follower.size}
	sourceSnatched.initOverlays()
	sourceSnatched.setSession(source)
	source.mu.Lock()
	source.addSnatchedLocked(sourceSnatched)
	source.mu.Unlock()

	moveTabFinalSource(t, d, source, destination, moved)
	d.attachmentCleanupWg.Wait()
	d.waitNotifies()

	d.mu.Lock()
	require.Empty(t, d.parked)
	d.mu.Unlock()
	require.True(t, parkedTransport.Closed())
	require.True(t, sourceSnatchedTransport.Closed())
	require.Nil(t, sourceSnatched.currentSession())
}

func TestMoveTabFinalSourceClearsFollowerCopyCapturePreservesDestinationWaiterOverlays(t *testing.T) {
	d, source, follower, destination, _, _, moved := setupMoveTabFinalSourceHandoff(t)
	movedPane := moved.focusedPane()

	follower.overlays.copyMu.Lock()
	follower.overlays.copyPane = movedPane
	follower.overlays.copyMode = &scopy.Mode{}
	follower.overlays.copyMu.Unlock()
	follower.sendMu.Lock()
	follower.captureFrames = map[*pane]capturedPaneRenderState{movedPane: {title: "stale-source-capture"}}
	follower.sendMu.Unlock()

	otherPane := destination.tabs[0].focusedPane()
	waiting := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: follower.size}
	waiting.initOverlays()
	waiting.setSession(destination)
	waiting.overlays.copyMu.Lock()
	waiting.overlays.copyPane = otherPane
	waiting.overlays.copyMode = &scopy.Mode{}
	waiting.overlays.copyMu.Unlock()
	waiting.sendMu.Lock()
	waiting.captureFrames = map[*pane]capturedPaneRenderState{otherPane: {}}
	waiting.sendMu.Unlock()
	destination.mu.Lock()
	destination.addSnatchedLocked(waiting)
	destination.mu.Unlock()

	moveTabFinalSource(t, d, source, destination, moved)

	follower.overlays.copyMu.Lock()
	require.Nil(t, follower.overlays.copyMode)
	require.Nil(t, follower.overlays.copyPane)
	follower.overlays.copyMu.Unlock()
	follower.sendMu.Lock()
	followerCapture, followerCaptured := follower.captureFrames[movedPane]
	follower.sendMu.Unlock()
	if followerCaptured {
		require.NotEqual(t, "stale-source-capture", followerCapture.title, "source capture survived destination first paint")
	}

	waiting.overlays.copyMu.Lock()
	require.NotNil(t, waiting.overlays.copyMode)
	require.Same(t, otherPane, waiting.overlays.copyPane)
	waiting.overlays.copyMu.Unlock()
	waiting.sendMu.Lock()
	_, waitingCaptured := waiting.captureFrames[otherPane]
	waiting.sendMu.Unlock()
	require.True(t, waitingCaptured)
}

func TestMoveTabFinalSourceFirstPaintNotBlockedByDisplacedPanelCleanup(t *testing.T) {
	d, source, follower, destination, displaced, _, moved := setupMoveTabFinalSourceHandoff(t)
	followerRebased := make(chan struct{})
	follower.renderStages.handoffRebase = func() { close(followerRebased) }

	displaced.sendMu.Lock()
	panelCleanupStarted := make(chan struct{})
	d.afterDisplacedCleanupStarted = func() { close(panelCleanupStarted) }
	defer func() { d.afterDisplacedCleanupStarted = nil }()

	moveDone := make(chan struct{})
	go func() {
		moveTabFinalSource(t, d, source, destination, moved)
		close(moveDone)
	}()

	awaitTestCompletion(t, panelCleanupStarted, "displaced panel cleanup did not start")
	awaitTestCompletion(t, followerRebased, "follower reset first paint waited for displaced panel cleanup")
	awaitTestCompletion(t, moveDone, "move-tab handoff did not finish after follower first paint")

	displaced.sendMu.Unlock()
	d.attachmentCleanupWg.Wait()
}

func TestMoveTabFinalSourceConcurrentReclaimKeepsNewestOwnerAndLease(t *testing.T) {
	d, source, follower, destination, displaced, _, moved := setupMoveTabFinalSourceHandoff(t)

	paintAdmitted := make(chan struct{})
	releasePaint := make(chan struct{})
	d.afterRoleEffectAdmitted = func(token attachmentRoleToken) {
		if token.ac == follower && token.role == attachmentActive {
			close(paintAdmitted)
			<-releasePaint
		}
	}
	defer func() { d.afterRoleEffectAdmitted = nil }()

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.moveTab(moveTabRequest{
			Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
			SourceTabID: domain.TabStableID(moved.stableID),
			Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		})
	}()
	awaitTestCompletion(t, paintAdmitted, "move-tab follower first paint was not admitted")

	displacedToken := destination.attachmentToken(displaced, displaced.transportSnapshot().transport)
	reclaimDone := make(chan struct{})
	go func() {
		d.handleSnatchedClientFrame(displacedToken, ports.Frame{
			Type:    ports.MsgInput,
			Payload: ports.MarshalInput(ports.Input{Data: []byte{'r'}}),
		})
		close(reclaimDone)
	}()

	close(releasePaint)
	require.NoError(t, <-moveDone)
	awaitTestCompletion(t, reclaimDone, "concurrent destination reclaim did not finish")
	d.attachmentCleanupWg.Wait()

	requireSingleOwnerAndLease(t, destination, follower, displaced)
	destination.mu.Lock()
	require.Same(t, displaced, destination.client)
	destination.mu.Unlock()
	require.Equal(t, attachmentSnatched, destination.attachmentRole(follower))
	require.Equal(t, attachmentActive, destination.attachmentRole(displaced))
}

func TestMoveTabFinalSourcePostCommitPaintFailureDoesNotRollback(t *testing.T) {
	d, source, follower, destination, _, _, moved := setupMoveTabFinalSourceHandoff(t)

	d.afterRoleEffectAdmitted = func(token attachmentRoleToken) {
		if token.ac == follower {
			token.ac.roleGeneration.Add(1)
		}
	}
	defer func() { d.afterRoleEffectAdmitted = nil }()

	beforeTabs := append([]*tab(nil), destination.tabs...)
	moveTabFinalSource(t, d, source, destination, moved)

	require.Nil(t, source.tabs)
	require.Same(t, destination, follower.currentSession())
	require.Same(t, follower, destination.client)
	require.Equal(t, append(beforeTabs, moved), destination.tabs)
	require.Same(t, moved, destination.tabs[destination.active])
}

func TestMoveTabFinalSourceHandoffCommitHidesPartialPublication(t *testing.T) {
	d, source, follower, destination, displaced, _, moved := setupMoveTabFinalSourceHandoff(t)

	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	d.beforeMoveTabCommit = func() {
		close(commitEntered)
		<-releaseCommit
	}
	defer func() { d.beforeMoveTabCommit = nil }()

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.moveTab(moveTabRequest{
			Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
			SourceTabID: domain.TabStableID(moved.stableID),
			Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		})
	}()
	<-commitEntered

	type observation struct {
		sourceRegistered      bool
		movedInSource         bool
		movedInDestination    bool
		followerOnDestination bool
		displacedSnatched     bool
		movedActive           bool
	}
	read := func() observation {
		d.mu.Lock()
		_, sourceLive := d.sessions[source.id]
		d.mu.Unlock()
		source.mu.Lock()
		movedInSource := indexMoveTabLocked(source, moved) >= 0
		source.mu.Unlock()
		destination.mu.Lock()
		obs := observation{
			sourceRegistered:      sourceLive,
			movedInSource:         movedInSource,
			movedInDestination:    indexMoveTabLocked(destination, moved) >= 0,
			followerOnDestination: destination.client == follower,
			displacedSnatched:     false,
			movedActive:           destination.active >= 0 && destination.tabs[destination.active] == moved,
		}
		if displaced != nil {
			_, obs.displacedSnatched = destination.snatched[displaced]
		}
		destination.mu.Unlock()
		return obs
	}

	require.False(t, d.mu.TryLock(), "daemon observers must be excluded from partial move-tab publication")
	require.False(t, source.mu.TryLock(), "source observers must be excluded from partial move-tab publication")
	require.False(t, destination.mu.TryLock(), "destination observers must be excluded from partial move-tab publication")

	close(releaseCommit)
	require.NoError(t, <-moveDone)

	final := read()
	require.False(t, final.sourceRegistered)
	require.False(t, final.movedInSource)
	require.True(t, final.movedInDestination)
	require.True(t, final.followerOnDestination)
	require.True(t, final.displacedSnatched)
	require.True(t, final.movedActive)
}
