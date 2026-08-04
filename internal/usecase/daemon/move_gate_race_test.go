package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func newMoveGateRaceFixture(t *testing.T) (*Daemon, *session, *tab, *pane, *session, *tab, func()) {
	t.Helper()
	movedPTY, releaseMoved := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	d, source, client, _ := newManualSessionWithPTYs(t, movedPTY)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	require.NotNil(t, d.attachCoordinator(source, nil, client, true))

	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()
	return d, source, sourceTab, moved, destination, destinationTab, func() {
		releaseMoved()
		releaseDestination()
	}
}

func moveGateRaceRequest(source *session, sourceTab *tab, moved *pane, destination *session, destinationTab *tab) movePaneRequest {
	return movePaneRequest{
		Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	}
}

func waitMoveRace[T any](t *testing.T, ch <-chan T, name string) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-timeAfterMoveRace():
		t.Fatalf("%s did not finish", name)
		var zero T
		return zero
	}
}

// timeAfterMoveRace is kept in one helper so these barrier tests use a
// deterministic bounded wait rather than sleeps.
func timeAfterMoveRace() <-chan time.Time {
	return time.After(testWaitTimeout)
}

func TestMovePaneGateReservationRaceKillWinsWithoutDeadlock(t *testing.T) {
	d, source, sourceTab, moved, destination, destinationTab, releasePTYs := newMoveGateRaceFixture(t)
	defer releasePTYs()

	moveReserved := make(chan struct{})
	releaseMoveAdmission := make(chan struct{})
	d.afterMoveLifecycleReserved = func() {
		close(moveReserved)
		<-releaseMoveAdmission
	}
	killFrozen := make(chan struct{})
	releaseKill := make(chan struct{})
	var killFrozenOnce sync.Once
	d.afterAttachmentEffectGateFrozen = func(action string, _ *attachedClient) {
		if action != "" {
			return
		}
		killFrozenOnce.Do(func() { close(killFrozen) })
		<-releaseKill
	}
	defer func() {
		d.afterMoveLifecycleReserved = nil
		d.afterAttachmentEffectGateFrozen = nil
	}()

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.movePane(moveGateRaceRequest(source, sourceTab, moved, destination, destinationTab))
	}()
	waitMoveRace(t, moveReserved, "move reservation")

	killDone := make(chan error, 1)
	go func() { killDone <- d.killSession(source, ports.ReasonSessionKilled, false) }()
	waitMoveRace(t, killFrozen, "kill gate freeze")
	close(releaseMoveAdmission)
	moveErr := waitMoveRace(t, moveDone, "move abort after kill gate acquisition")
	close(releaseKill)
	killErr := waitMoveRace(t, killDone, "kill teardown")

	require.ErrorIs(t, moveErr, errMovePaneInvalid)
	require.NoError(t, killErr)
	d.mu.Lock()
	_, sourceLive := d.sessions[source.id]
	_, destinationLive := d.sessions[destination.id]
	d.mu.Unlock()
	require.False(t, sourceLive, "kill winner must retire the source")
	require.True(t, destinationLive)
	require.NotSame(t, moved, destination.tabs[0].panes[moved.id], "kill winner must not publish a partial destination move")
}

func TestFinalCloseDoesNotDeadlockWithMoveDispatchAdmission(t *testing.T) {
	tests := []struct {
		name  string
		close func(*Daemon, *session, *tab, *pane) error
		move  func(*Daemon, *session, *tab, *pane, *session, *tab) error
	}{
		{name: "pane", close: func(d *Daemon, source *session, sourceTab *tab, _ *pane) error {
			return d.closeTab(source, sourceTab, true)
		}, move: func(d *Daemon, source *session, sourceTab *tab, moved *pane, destination *session, destinationTab *tab) error {
			return d.movePane(moveGateRaceRequest(source, sourceTab, moved, destination, destinationTab))
		}},
		{name: "pane-reap", close: func(d *Daemon, _ *session, _ *tab, moved *pane) error {
			d.reapPaneOwner(moved)
			return nil
		}, move: func(d *Daemon, source *session, sourceTab *tab, moved *pane, destination *session, destinationTab *tab) error {
			return d.movePane(moveGateRaceRequest(source, sourceTab, moved, destination, destinationTab))
		}},
		{name: "tab", close: func(d *Daemon, source *session, sourceTab *tab, _ *pane) error {
			return d.closeTab(source, sourceTab, true)
		}, move: func(d *Daemon, source *session, sourceTab *tab, _ *pane, destination *session, _ *tab) error {
			return d.moveTab(moveTabRequest{
				Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(sourceTab.stableID),
				Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			})
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, source, sourceTab, moved, destination, destinationTab, releasePTYs := newMoveGateRaceFixture(t)
			defer releasePTYs()

			closeReady := make(chan struct{})
			releaseClose := make(chan struct{})
			d.afterAttachmentEffectParticipantsSnapshotted = func(action string, _ []*attachedClient) {
				if action != "" {
					return
				}
				close(closeReady)
				<-releaseClose
			}
			moveDispatchReady := make(chan struct{})
			d.beforeMoveDispatch = func() { close(moveDispatchReady) }
			defer func() {
				d.afterAttachmentEffectParticipantsSnapshotted = nil
				d.beforeMoveDispatch = nil
			}()

			closeDone := make(chan error, 1)
			go func() { closeDone <- test.close(d, source, sourceTab, moved) }()
			waitMoveRace(t, closeReady, "final close participant snapshot")

			moveDone := make(chan error, 1)
			go func() { moveDone <- test.move(d, source, sourceTab, moved, destination, destinationTab) }()
			waitMoveRace(t, moveDispatchReady, "move dispatch admission")
			close(releaseClose)

			require.NoError(t, waitMoveRace(t, closeDone, "final close"))
			require.ErrorIs(t, waitMoveRace(t, moveDone, "move after final close"), errMoveStaleTarget)
			d.mu.Lock()
			_, sourceLive := d.sessions[source.id]
			_, destinationLive := d.sessions[destination.id]
			d.mu.Unlock()
			require.False(t, sourceLive, "final close must retire the source")
			require.True(t, destinationLive)
			if test.name == "pane" || test.name == "pane-reap" {
				require.NotSame(t, moved, destinationTab.panes[moved.id], "a rejected move must not publish into the destination")
			}
		})
	}
}

func TestPaletteFinalCloseEndsCurrentEffectBeforeDrainingPeers(t *testing.T) {
	d, sess, current, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.attachCoordinator(sess, nil, current, true)
	otherTransport := &closeTrackingTransport{}
	other := &attachedClient{tr: otherTransport, output: newOutputStateStream(), size: current.size}
	other.initOverlays()
	other.setSession(sess)
	sess.mu.Lock()
	require.True(t, sess.registerAttachmentLocked(other))
	sess.mu.Unlock()
	d.attachCoordinator(sess, nil, other, true)

	currentToken := sess.attachmentToken(current, current.transport())
	currentEffect, admitted := current.beginAttachmentEffect(currentToken)
	require.True(t, admitted)
	t.Cleanup(currentEffect.End)
	otherToken := sess.attachmentToken(other, otherTransport)
	otherEffect, admitted := other.beginAttachmentEffect(otherToken)
	require.True(t, admitted)
	t.Cleanup(otherEffect.End)

	currentEnded := make(chan struct{})
	currentReleasedBeforePeerFreeze := make(chan struct{})
	otherFrozen := make(chan struct{})
	d.afterActionAttachmentEffectEnded = func(action string) {
		if action == "close-tab" {
			close(currentEnded)
		}
	}
	d.afterAttachmentEffectGateFrozen = func(action string, ac *attachedClient) {
		if action == "close-tab" && ac == other {
			if currentEffect.ended.Load() {
				close(currentReleasedBeforePeerFreeze)
			}
			close(otherFrozen)
		}
	}
	defer func() {
		d.afterActionAttachmentEffectEnded = nil
		d.afterAttachmentEffectGateFrozen = nil
	}()

	closeDone := make(chan error, 1)
	go func() {
		sess.dispatchMu.Lock()
		defer sess.dispatchMu.Unlock()
		closeDone <- (paletteExec{d: d, sess: sess, ac: current, effect: currentEffect}).CloseTab()
	}()
	waitMoveRace(t, otherFrozen, "peer effect gate freeze")
	waitMoveRace(t, currentReleasedBeforePeerFreeze, "current effect release before peer drain")
	waitMoveRace(t, currentEnded, "current palette effect completion")
	select {
	case err := <-closeDone:
		t.Fatalf("final close drained its own peer before release: %v", err)
	default:
	}
	otherEffect.End()
	require.NoError(t, waitMoveRace(t, closeDone, "palette final close"))
	requireAttachmentEffectGateRetired(t, current)
	requireAttachmentEffectGateRetired(t, other)
}

func TestMovePaneGateReservationRaceMoveWinsWithoutDeadlock(t *testing.T) {
	d, source, sourceTab, moved, destination, destinationTab, releasePTYs := newMoveGateRaceFixture(t)
	defer releasePTYs()

	moveGateFrozen := make(chan struct{})
	releaseMove := make(chan struct{})
	var moveGateFrozenOnce sync.Once
	d.afterAttachmentEffectGateFrozen = func(action string, _ *attachedClient) {
		if action != "move-pane" {
			return
		}
		moveGateFrozenOnce.Do(func() { close(moveGateFrozen) })
		<-releaseMove
	}
	killSnapshotted := make(chan struct{})
	var killSnapshottedOnce sync.Once
	d.afterAttachmentEffectParticipantsSnapshotted = func(action string, _ []*attachedClient) {
		if action == "" {
			killSnapshottedOnce.Do(func() { close(killSnapshotted) })
		}
	}
	defer func() {
		d.afterAttachmentEffectGateFrozen = nil
		d.afterAttachmentEffectParticipantsSnapshotted = nil
	}()

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.movePane(moveGateRaceRequest(source, sourceTab, moved, destination, destinationTab))
	}()
	waitMoveRace(t, moveGateFrozen, "move gate freeze")

	killDone := make(chan error, 1)
	go func() { killDone <- d.killSession(source, ports.ReasonSessionKilled, false) }()
	waitMoveRace(t, killSnapshotted, "kill participant snapshot")
	close(releaseMove)

	require.NoError(t, waitMoveRace(t, moveDone, "move commit"))
	require.NoError(t, waitMoveRace(t, killDone, "kill loser"))
	d.mu.Lock()
	_, sourceLive := d.sessions[source.id]
	_, destinationLive := d.sessions[destination.id]
	d.mu.Unlock()
	require.False(t, sourceLive)
	require.True(t, destinationLive)
	require.Same(t, moved, destinationTab.panes[moved.id])
}
