package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
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
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{2}, ephemeral: true}, ctx: source.ctx, cancel: func() {}, tabs: []*tab{destinationTab}}
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

// moveSourceForSession names the exact source a move command captures.
func moveSourceForSession(sess *session, tabID domain.TabStableID, paneID domain.PaneStableID) moveSourceLocator {
	return moveSourceLocator{
		Session: moveSessionLocator{ID: sess.id, Incarnation: sess.incarnation, Name: sess.name},
		TabID:   tabID,
		PaneID:  paneID,
	}
}

// openMovePickerForTest opens one move interaction on a freshly admitted
// effect, exactly like the palette command does.
func openMovePickerForTest(t *testing.T, d *Daemon, ac *attachedClient, sess *session, intent protocol.PickerIntent, source moveSourceLocator) *attachmentEffect {
	t.Helper()
	current := ac.transportSnapshot()
	_, effect, admitted := ac.beginCurrentAttachmentEffect(sess, current.transport)
	require.True(t, admitted)
	t.Cleanup(effect.End)
	require.NoError(t, d.openPickerForAttachment(ac, effect, intent, source, 0))
	return effect
}

// moveSelectionForDestination builds the typed move the client would send for
// the destination session's first selectable line.
func moveSelectionForDestination(t *testing.T, ac *attachedClient, effect *attachmentEffect, destination domain.SessionID) protocol.PickerSelection {
	t.Helper()
	ac.overlays.pickerMu.Lock()
	interaction := ac.overlays.pickerInteraction
	revision := ac.overlays.pickerRevisions[servingPickerSourceID]
	key := ""
	for candidate, target := range ac.overlays.pickerKeys {
		if target.Session == destination {
			key = candidate
			break
		}
	}
	ac.overlays.pickerMu.Unlock()
	require.NotEmpty(t, key, "no move destination resolves to session %s", destination)
	return protocol.PickerSelection{
		InteractionID: interaction, SourceID: servingPickerSourceID,
		SourceRevision: revision, Key: key, Action: protocol.PickerActionMove,
	}
}

func TestPaletteMovePaneCapturesSourceAndOpensPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	d.handleInput(source, ac, []byte("\x1b "))
	require.True(t, ac.overlays.paletteActive())
	d.handleInput(source, ac, []byte("MFP\r"))
	require.False(t, ac.overlays.paletteActive())

	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerOpen
	intent, captured := ac.overlays.pickerIntent, ac.overlays.pickerMoveSource
	ac.overlays.pickerMu.Unlock()
	require.True(t, open)
	require.Equal(t, protocol.PickerIntentMovePane, intent)
	require.Equal(t, moveSessionLocator{ID: source.id, Incarnation: source.incarnation, Name: source.name}, captured.Session)
	require.Equal(t, domain.TabStableID("source-tab"), captured.TabID)
	require.Equal(t, domain.PaneStableID("source-pane"), captured.PaneID)
	require.Same(t, ac, captured.Attachment)
}

func TestPaletteMoveTabCapturesActiveTabAndOpensPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "active-tab"

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MAT\r"))

	ac.overlays.pickerMu.Lock()
	open := ac.overlays.pickerOpen
	intent, captured := ac.overlays.pickerIntent, ac.overlays.pickerMoveSource
	ac.overlays.pickerMu.Unlock()
	require.True(t, open)
	require.Equal(t, protocol.PickerIntentMoveTab, intent)
	require.Equal(t, domain.TabStableID("active-tab"), captured.TabID)
	require.Equal(t, domain.PaneStableID(""), captured.PaneID)
	require.Same(t, ac, captured.Attachment)
}

func TestPaletteMoveWithoutDestinationShowsToastAndNoPicker(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	d.mu.Lock()
	delete(d.sessions, domain.SessionID("destination"))
	d.mu.Unlock()

	d.handleInput(source, ac, []byte("\x1b "))
	d.handleInput(source, ac, []byte("MFP\r"))

	require.False(t, ac.overlays.pickerClientActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "No destination available.", history[0].Message)
}

func TestMovePickerMoveCommitsPane(t *testing.T) {
	d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	source.mu.Lock()
	clearAttachmentsForTestLocked(source)
	source.mu.Unlock()
	sourceTab := source.tabs[0]
	moved := sourceTab.focusedPane()

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, "source-tab", "source-pane"))
	selection := moveSelectionForDestination(t, ac, effect, destination.id)
	d.resolvePickerSelection(effect, selection)

	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Same(t, destination, moved.ownerSnapshot().session)
}

func TestMovePickerCommitMovePaneViaSharedAPI(t *testing.T) {
	d, source, ac, destination, destinationTab, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	source.mu.Lock()
	clearAttachmentsForTestLocked(source)
	source.mu.Unlock()
	sourceTab := source.tabs[0]
	moved := sourceTab.focusedPane()

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, "source-tab", "source-pane"))
	selection := moveSelectionForDestination(t, ac, effect, destination.id)
	ac.overlays.pickerMu.Lock()
	target, ok := ac.overlays.pickerKeys[selection.Key]
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.NoError(t, d.commitMovePickerSelection(protocol.PickerIntentMovePane, moveSourceForSession(source, "source-tab", "source-pane"), target))

	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Same(t, destination, moved.ownerSnapshot().session)
}

func TestMovePickerMoveCommitsTab(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	source.mu.Lock()
	clearAttachmentsForTestLocked(source)
	source.mu.Unlock()
	movedTab := source.tabs[0]
	movedTab.stableID = "moved-tab"

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, moveSourceForSession(source, "moved-tab", ""))
	selection := moveSelectionForDestination(t, ac, effect, destination.id)
	d.resolvePickerSelection(effect, selection)

	require.Nil(t, source.tabs)
	require.Len(t, destination.tabs, 2)
	require.Same(t, movedTab, destination.tabs[1])
}

func TestMovePickerCancelPerformsNoMutation(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	before := len(source.tabs)

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, "source-tab", "source-pane"))
	ac.overlays.pickerMu.Lock()
	interaction := ac.overlays.pickerInteraction
	ac.overlays.pickerMu.Unlock()
	require.True(t, d.closePickerForAttachment(ac, effect, interaction))

	require.False(t, ac.overlays.pickerClientActive())
	require.Len(t, source.tabs, before)
}

func TestMovePickerStaleDestinationReportsNotice(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, "source-tab", "source-pane"))
	selection := moveSelectionForDestination(t, ac, effect, destination.id)
	d.mu.Lock()
	delete(d.sessions, destination.id)
	d.mu.Unlock()

	d.resolvePickerSelection(effect, selection)

	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Destination is no longer available.", history[0].Message)
	require.Len(t, source.tabs, 1)
}

func TestMovePickerStaleSourcePaneReportsPreciseFeedback(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceTab := source.tabs[0]

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, moveSourceForSession(source, "source-tab", "source-pane"))
	selection := moveSelectionForDestination(t, ac, effect, destination.id)
	sourceTab.mu.Lock()
	delete(sourceTab.panes, sourceTab.tree.Focus)
	sourceTab.mu.Unlock()

	d.resolvePickerSelection(effect, selection)

	require.True(t, ac.overlays.pickerClientActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Pane no longer exists.", history[0].Message)
}

func TestMovePickerStaleSourceTabReportsPreciseFeedback(t *testing.T) {
	d, source, ac, destination, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)

	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMoveTab, moveSourceForSession(source, "source-tab", ""))
	selection := moveSelectionForDestination(t, ac, effect, destination.id)
	source.mu.Lock()
	source.tabs = nil
	source.mu.Unlock()

	d.resolvePickerSelection(effect, selection)

	require.True(t, ac.overlays.pickerClientActive())
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, "Tab no longer exists.", history[0].Message)
}

// TestMovePickerSupersedingOpenRetiresTheEarlierNamespace pins that a second
// open moves the interaction on, so rows published for the retired namespace
// can never be committed again.
func TestMovePickerSupersedingOpenRetiresTheEarlierNamespace(t *testing.T) {
	d, source, ac, _, _, releases := setupMovePickerSessions(t, 0)
	defer releaseAll(releases)
	sourceSnapshot := moveSourceForSession(source, "source-tab", "source-pane")
	effect := openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, sourceSnapshot)
	ac.overlays.pickerMu.Lock()
	firstInteraction := ac.overlays.pickerInteraction
	firstRevision := ac.overlays.pickerRevisions[servingPickerSourceID]
	firstKey := ""
	for candidate := range ac.overlays.pickerKeys {
		firstKey = candidate
		break
	}
	ac.overlays.pickerMu.Unlock()
	require.NotEmpty(t, firstKey)

	openMovePickerForTest(t, d, ac, source, protocol.PickerIntentMovePane, sourceSnapshot)
	ac.overlays.pickerMu.Lock()
	secondInteraction := ac.overlays.pickerInteraction
	ac.overlays.pickerMu.Unlock()
	require.Greater(t, secondInteraction, firstInteraction)

	// A commit from the retired namespace is rejected and leaves the open
	// interaction untouched.
	d.resolvePickerSelection(effect, protocol.PickerSelection{
		InteractionID: firstInteraction, SourceID: servingPickerSourceID,
		SourceRevision: firstRevision, Key: firstKey, Action: protocol.PickerActionMove,
	})
	ac.overlays.pickerMu.Lock()
	require.True(t, ac.overlays.pickerOpen)
	require.Equal(t, secondInteraction, ac.overlays.pickerInteraction)
	ac.overlays.pickerMu.Unlock()
}
