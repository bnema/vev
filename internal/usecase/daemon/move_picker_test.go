package daemon

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func setupMovePickerSessions(t *testing.T, extraDestinationTabs int) (*Daemon, *session, *attachedClient, *session, *tab, []func()) {
	t.Helper()
	return setupMovePickerSessionsWithClock(t, stubClock{}, extraDestinationTabs)
}

func setupMovePickerSessionsWithClock(t *testing.T, clock ports.Clock, extraDestinationTabs int) (*Daemon, *session, *attachedClient, *session, *tab, []func()) {
	t.Helper()
	sourcePTY, releaseSource := newBlockingPTY(t)
	d, source, ac, _ := newManualSessionWithPTYsClock(t, clock, sourcePTY)
	source.id, source.name, source.incarnation = "source", "source", domain.IncarnationID{1}
	d.mu.Lock()
	delete(d.sessions, domain.SessionID("manual"))
	d.sessions[source.id] = source
	d.mu.Unlock()
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	sourcePane := sourceTab.focusedPane()
	sourcePane.stableID = "source-pane"

	releases := []func(){releaseSource}
	destPTY, releaseDest := newBlockingPTY(t)
	releases = append(releases, releaseDest)
	destinationTab := newTabWithStableID("destination-tab", "destination-pane", destPTY, domain.Size{Cols: 80, Rows: 23})
	publishTiledPaneOwners(source, sourceTab)
	destination := &session{
		id: "destination", name: "destination", incarnation: domain.IncarnationID{2}, ephemeral: true,
		ctx: source.ctx, cancel: func() {}, tabs: []*tab{destinationTab}, active: 0,
	}
	publishTiledPaneOwners(destination, destinationTab)
	for range extraDestinationTabs {
		extraPTY, releaseExtra := newBlockingPTY(t)
		releases = append(releases, releaseExtra)
		extraTab := newTabWithStableID("extra-tab", "extra-pane", extraPTY, domain.Size{Cols: 80, Rows: 23})
		destination.tabs = append(destination.tabs, extraTab)
		publishTiledPaneOwners(destination, extraTab)
	}
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()
	return d, source, ac, destination, destinationTab, releases
}

func TestPaletteMovePaneCapturesSourceAndOpensPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	d.handleInput(source, ac, []byte("\x1b "))
	require.True(t, ac.overlays.paletteActive())
	d.handleInput(source, ac, []byte("MPN\r"))
	require.False(t, ac.overlays.paletteActive())
	require.True(t, ac.overlays.pickerActive())

	ac.overlays.pickerMu.Lock()
	intent, captured := ac.overlays.pickerIntent, ac.overlays.pickerSource
	ac.overlays.pickerMu.Unlock()
	require.Equal(t, pickerMovePane, intent)
	require.Equal(t, moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name}, captured.Session)
	require.Equal(t, domain.TabStableID("source-tab"), captured.TabID)
	require.Equal(t, domain.PaneStableID("source-pane"), captured.PaneID)
	require.Same(t, ac, captured.Client)
}

func TestPaletteMoveTabCapturesActiveTabAndOpensPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "active-tab"

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MTB\r"))
	require.True(t, ac.overlays.pickerActive())

	ac.overlays.pickerMu.Lock()
	intent, captured := ac.overlays.pickerIntent, ac.overlays.pickerSource
	ac.overlays.pickerMu.Unlock()
	require.Equal(t, pickerMoveTab, intent)
	require.Equal(t, domain.TabStableID("active-tab"), captured.TabID)
	require.Equal(t, domain.PaneStableID(""), captured.PaneID)
	require.Same(t, ac, captured.Client)
}

func TestPaletteMoveWithoutDestinationShowsToastAndNoPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	d.mu.Lock()
	delete(d.sessions, domain.SessionID("destination"))
	d.mu.Unlock()

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MPN\r"))

	require.False(t, ac.overlays.pickerActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "No destination available.", history[0].Message)
}

func TestMovePickerEnterCommitsMovePane(t *testing.T) {
	d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	source.mu.Lock()
	source.client = nil
	source.mu.Unlock()
	sourceTab := source.tabs[0]
	moved := sourceTab.focusedPane()

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}))
	d.handlePickerInput(ac, []byte("\r"))

	require.False(t, ac.overlays.pickerActive())
	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Same(t, destination, moved.ownerSnapshot().session)
}

func TestMovePickerEnterCommitsMovePaneViaSharedAPI(t *testing.T) {
	d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	source.mu.Lock()
	source.client = nil
	source.mu.Unlock()
	sourceTab := source.tabs[0]
	moved := sourceTab.focusedPane()

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}))
	ac.overlays.pickerMu.Lock()
	target, ok := ac.overlays.picker.Selected()
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.NoError(t, d.commitMovePickerSelection(pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}, target))

	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Same(t, destination, moved.ownerSnapshot().session)
}

func TestMovePickerEnterCommitsMoveTab(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	source.mu.Lock()
	source.client = nil
	source.mu.Unlock()
	movedTab := source.tabs[0]
	movedTab.stableID = "moved-tab"

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMoveTab, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "moved-tab",
	}))
	d.handlePickerInput(ac, []byte("\r"))

	require.False(t, ac.overlays.pickerActive())
	require.Nil(t, source.tabs)
	require.Len(t, destination.tabs, 2)
	require.Same(t, movedTab, destination.tabs[1])
}

func TestMovePickerEscapePerformsNoMutation(t *testing.T) {
	clk := &signalClock{timers: make(chan *signalTimer, 16)}
	d, source, ac, _, _, releases := setupMovePickerSessionsWithClock(t, clk, 0)
	defer releaseAll(releases)
	before := len(source.tabs)

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}))
	for len(clk.timers) > 0 {
		<-clk.timers
	}
	d.handlePickerInput(ac, []byte("\x1b"))
	timer := <-clk.timers
	timer.ch <- time.Now()
	require.Eventually(t, func() bool { return !ac.overlays.pickerActive() }, time.Second, 5*time.Millisecond)
	require.Len(t, source.tabs, before)
}

func TestMovePickerStaleDestinationKeepsFeedback(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}))
	d.mu.Lock()
	delete(d.sessions, destination.id)
	d.mu.Unlock()

	d.handlePickerInput(ac, []byte("\r"))

	require.False(t, ac.overlays.pickerActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Destination is no longer available.", history[0].Message)
	require.Len(t, source.tabs, 1)
}

func TestMovePickerStaleSourcePaneReportsPreciseFeedback(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}))
	sourceTab.mu.Lock()
	delete(sourceTab.panes, sourceTab.tree.Focus)
	sourceTab.mu.Unlock()

	d.handlePickerInput(ac, []byte("\r"))

	require.True(t, ac.overlays.pickerActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Pane no longer exists.", history[0].Message)
}

func TestMovePickerStaleSourceTabReportsPreciseFeedback(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	require.NoError(t, d.enterPickerForIntent(source, ac, pickerMoveTab, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab",
	}))
	source.mu.Lock()
	source.tabs = nil
	source.mu.Unlock()

	d.handlePickerInput(ac, []byte("\r"))

	require.True(t, ac.overlays.pickerActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Tab no longer exists.", history[0].Message)
}

func TestMovePickerSourceClientReplacementInvalidatesAction(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	require.NoError(t, paletteExec{d: d, sess: source, ac: ac}.OpenMovePanePicker())
	replacement := &attachedClient{tr: &closeTrackingTransport{}, output: newOutputStateStream(), size: ac.size}
	replacement.initOverlays()
	replacement.setSession(source)
	source.mu.Lock()
	source.client = replacement
	source.mu.Unlock()

	d.handlePickerInput(ac, []byte("\r"))

	require.True(t, ac.overlays.pickerActive())
	require.Len(t, source.tabs, 1)
	require.Len(t, destination.tabs, 1)
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Source client is no longer active.", history[0].Message)
}

func TestMovePickerFinalSourceClosesBeforeSnatchFinalization(t *testing.T) {
	d, source, follower, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	follower.replaceTransport(&closeTrackingTransport{})
	require.NotNil(t, d.attachCoordinator(source, nil, follower, true))

	displacedTransport := &closeTrackingTransport{}
	displaced := &attachedClient{tr: displacedTransport, output: newOutputStateStream(), size: follower.size}
	displaced.initOverlays()
	displaced.setSession(destination)
	destination.mu.Lock()
	destination.client = displaced
	destination.mu.Unlock()
	require.NotNil(t, d.attachCoordinator(destination, nil, displaced, true))

	var pickerClosed atomic.Bool
	d.afterDisplacedCleanupStarted = func() {
		require.False(t, follower.overlays.pickerActive(), "picker must close before snatch finalization")
		pickerClosed.Store(true)
	}
	defer func() { d.afterDisplacedCleanupStarted = nil }()

	require.NoError(t, d.enterPickerForIntent(source, follower, pickerMovePane, moveSourceLocator{
		Session: moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name},
		TabID:   "source-tab", PaneID: "source-pane",
	}))
	d.handlePickerInput(follower, []byte("\r"))
	d.attachmentCleanupWg.Wait()

	require.True(t, pickerClosed.Load())
	require.False(t, follower.overlays.pickerActive())
	frames := displacedTransport.Sends()
	require.NotEmpty(t, frames)
	require.Equal(t, ports.MsgOutput, frames[0].Type)
}

func TestMovePickerDispatchMuNotHeldWhileOpen(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MPN\r"))
	require.True(t, ac.overlays.pickerActive())
	require.True(t, source.dispatchMu.TryLock(), "dispatchMu must not remain held while the move picker is open")
	source.dispatchMu.Unlock()
}
