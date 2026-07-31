package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

type moveCommitObservation struct {
	sourceRegistered      bool
	sourceTabMember       bool
	sourcePaneMember      bool
	destinationTabMember  bool
	destinationPaneMember bool
	sourceActive          *tab
	destinationActive     *tab
	sourceFocus           layout.PaneID
	destinationFocus      layout.PaneID
	ownerSession          *session
	ownerTab              *tab
	sourceRole            attachmentRole
	destinationRole       attachmentRole
	sourceClientSession   *session
}

// readMoveCommitObservation follows the same architecture lock order as the
// move commit. Every field is therefore one supported reader's coherent view,
// rather than a collection of lock-free observations that could straddle a
// publication boundary.
func readMoveCommitObservation(d *Daemon, source, destination *session, sourceTab, destinationTab *tab, movedPane *pane, sourceClient *attachedClient) moveCommitObservation {
	d.mu.Lock()
	d.notices.routingMu.Lock()
	unlockSessions := lockAttachmentSessions(source, destination)
	unlockTabs := lockMoveTabs(sourceTab, destinationTab)
	movedPane.mu.Lock()

	owner := movedPane.ownerSnapshot()
	observation := moveCommitObservation{
		sourceRegistered:      d.sessions[source.id] == source,
		sourceTabMember:       moveTabMemberLocked(source, sourceTab),
		sourcePaneMember:      sourceTab.panes[movedPane.id] == movedPane,
		destinationTabMember:  moveTabMemberLocked(destination, destinationTab),
		destinationPaneMember: destinationTab.panes[movedPane.id] == movedPane,
		sourceActive:          activeMoveTabLocked(source),
		destinationActive:     activeMoveTabLocked(destination),
		sourceFocus:           sourceTab.tree.Focus,
		destinationFocus:      destinationTab.tree.Focus,
		ownerSession:          nil,
		ownerTab:              nil,
		sourceRole:            source.attachmentRoleLocked(sourceClient),
		destinationRole:       destination.attachmentRoleLocked(sourceClient),
		sourceClientSession:   sourceClient.currentSession(),
	}
	if owner != nil {
		observation.ownerSession = owner.session
		observation.ownerTab = owner.tab
	}
	movedPane.mu.Unlock()
	unlockTabs()
	unlockSessions()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()
	return observation
}

func activeMoveTabLocked(sess *session) *tab {
	if sess == nil || sess.active < 0 || sess.active >= len(sess.tabs) {
		return nil
	}
	return sess.tabs[sess.active]
}

func TestMovePaneCommitPointHidesPartialPublication(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()

	d, source, sourceClient, _ := newManualSessionWithPTYs(t, p1)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	movedPane := sourceTab.focusedPane()

	destination := &session{sessionCore: sessionCore{id: "destination",
		name:      "destination",
		ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", p2, domain.Size{Cols: 80, Rows: 23})},
		active: 0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()
	require.NotNil(t, d.attachCoordinator(source, nil, sourceClient, true))

	commitEntered := make(chan struct{})
	releaseCommit := make(chan struct{})
	d.beforeMovePaneCommit = func() {
		close(commitEntered)
		<-releaseCommit
	}

	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.movePane(movePaneRequest{
			Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
			SourceTabID:      domain.TabStableID(sourceTab.stableID),
			SourcePaneID:     domain.PaneStableID(movedPane.stableID),
			Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			DestinationTabID: domain.TabStableID(destinationTab.stableID),
		})
	}()
	<-commitEntered

	type reader struct {
		name string
		read func(moveCommitObservation) bool
	}
	readers := []reader{
		{name: "source membership", read: func(o moveCommitObservation) bool {
			return o.sourceRegistered || o.sourceTabMember || o.sourcePaneMember
		}},
		{name: "destination membership", read: func(o moveCommitObservation) bool { return o.destinationTabMember && !o.destinationPaneMember }},
		{name: "owner", read: func(o moveCommitObservation) bool { return o.ownerSession == source && o.ownerTab == sourceTab }},
		{name: "active tab and focus", read: func(o moveCommitObservation) bool {
			return o.sourceActive == sourceTab || o.sourceFocus != layout.PaneID("pane-1") || o.destinationActive != destinationTab || o.destinationFocus != layout.PaneID("pane-2")
		}},
		{name: "attachment roles", read: func(o moveCommitObservation) bool {
			return o.sourceRole == attachmentActive && o.destinationRole == attachmentDetached && o.sourceClientSession == source
		}},
	}
	started := make(chan string, len(readers))
	done := make(chan struct {
		name        string
		observation moveCommitObservation
	}, len(readers))
	for _, current := range readers {
		go func(current reader) {
			started <- current.name
			observation := readMoveCommitObservation(d, source, destination, sourceTab, destinationTab, movedPane, sourceClient)
			done <- struct {
				name        string
				observation moveCommitObservation
			}{name: current.name, observation: observation}
		}(current)
	}
	for range readers {
		<-started
	}
	for range readers {
		select {
		case result := <-done:
			t.Fatalf("%s reader completed while the move commit was paused: %#v", result.name, result.observation)
		default:
		}
	}

	close(releaseCommit)
	require.NoError(t, <-moveDone)
	for _, current := range readers {
		result := <-done
		require.False(t, current.read(result.observation), "%s observed partial move state: %#v", current.name, result.observation)
	}

	observation := readMoveCommitObservation(d, source, destination, sourceTab, destinationTab, movedPane, sourceClient)
	require.False(t, observation.sourceRegistered)
	require.False(t, observation.sourceTabMember)
	require.False(t, observation.sourcePaneMember)
	require.True(t, observation.destinationTabMember)
	require.True(t, observation.destinationPaneMember)
	require.Nil(t, observation.sourceActive)
	require.Equal(t, layout.PaneID("pane-1"), observation.sourceFocus)
	require.Same(t, destinationTab, observation.destinationActive)
	require.Equal(t, layout.PaneID("pane-2"), observation.destinationFocus)
	require.Same(t, destination, observation.ownerSession)
	require.Same(t, destinationTab, observation.ownerTab)
	require.Equal(t, attachmentDetached, observation.sourceRole)
	require.Equal(t, attachmentActive, observation.destinationRole)
	require.Same(t, destination, observation.sourceClientSession)
}
