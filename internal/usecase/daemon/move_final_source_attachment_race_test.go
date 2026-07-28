package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

type finalMoveAttachmentMutation int

const (
	installActiveAfterFinalMoveAdmission finalMoveAttachmentMutation = iota
	installSnatchedAfterFinalMoveAdmission
	replaceSnatchedAfterFinalMoveAdmission
)

func newFinalMoveRaceClient(sess *session) *attachedClient {
	ac := &attachedClient{
		tr:     &closeTrackingTransport{},
		output: newOutputStateStream(),
		size:   domain.Size{Cols: 80, Rows: 24},
	}
	ac.initOverlays()
	ac.setSession(sess)
	return ac
}

func makeFinalMoveSourceHeadless(source *session, original *attachedClient) {
	source.mu.Lock()
	source.client = nil
	source.mu.Unlock()
	original.setSession(nil)
}

func mutateFinalMoveAttachments(source *session, mutation finalMoveAttachmentMutation) (current, replaced *attachedClient) {
	current = newFinalMoveRaceClient(source)
	source.mu.Lock()
	switch mutation {
	case installActiveAfterFinalMoveAdmission:
		source.client = current
	case installSnatchedAfterFinalMoveAdmission:
		source.addSnatchedLocked(current)
	case replaceSnatchedAfterFinalMoveAdmission:
		for ac := range source.snatched {
			replaced = ac
			delete(source.snatched, ac)
			ac.setSession(nil)
			break
		}
		source.addSnatchedLocked(current)
	}
	source.mu.Unlock()
	return current, replaced
}

func assertRejectedFinalMoveAttachmentPreserved(t *testing.T, source *session, current, replaced *attachedClient, mutation finalMoveAttachmentMutation) {
	t.Helper()
	require.Same(t, source, current.currentSession(), "the attachment admitted during the race must not be orphaned")
	switch mutation {
	case installActiveAfterFinalMoveAdmission:
		require.Equal(t, attachmentActive, source.attachmentRole(current))
	case installSnatchedAfterFinalMoveAdmission, replaceSnatchedAfterFinalMoveAdmission:
		require.Equal(t, attachmentSnatched, source.attachmentRole(current))
	}
	if replaced != nil {
		require.Nil(t, replaced.currentSession())
		require.Equal(t, attachmentDetached, source.attachmentRole(replaced))
	}
}

func TestMovePaneFinalHeadlessRejectsAttachmentChangesAfterAdmissionAtomically(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutation finalMoveAttachmentMutation
		existing bool
	}{
		{name: "new active", mutation: installActiveAfterFinalMoveAdmission},
		{name: "new snatched", mutation: installSnatchedAfterFinalMoveAdmission},
		{name: "count preserving snatched replacement", mutation: replaceSnatchedAfterFinalMoveAdmission, existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, source, sourceTab, moved, destination, destinationTab, release := newMoveGateRaceFixture(t)
			defer release()
			original := source.client
			makeFinalMoveSourceHeadless(source, original)
			if tc.existing {
				existing := newFinalMoveRaceClient(source)
				source.mu.Lock()
				source.addSnatchedLocked(existing)
				source.mu.Unlock()
			}

			sourceGeneration := sourceTab.layoutGeneration
			destinationGeneration := destinationTab.layoutGeneration
			destinationPaneAtMovedID := destinationTab.panes[moved.id]
			owner := moved.ownerSnapshot()
			barrier := make(chan struct{})
			releaseMove := make(chan struct{})
			d.afterMovePaneSourceSnapshot = func() {
				close(barrier)
				<-releaseMove
			}
			defer func() { d.afterMovePaneSourceSnapshot = nil }()

			moveDone := make(chan error, 1)
			go func() {
				moveDone <- d.movePane(moveGateRaceRequest(source, sourceTab, moved, destination, destinationTab))
			}()
			waitMoveRace(t, barrier, "move pane admission")
			current, replaced := mutateFinalMoveAttachments(source, tc.mutation)
			currentGeneration := current.roleGeneration.Load()
			close(releaseMove)
			err := waitMoveRace(t, moveDone, "move pane rejection")

			require.ErrorIs(t, err, errMoveStaleTarget)
			d.mu.Lock()
			require.Same(t, source, d.sessions[source.id])
			require.Same(t, destination, d.sessions[destination.id])
			d.mu.Unlock()
			source.mu.Lock()
			require.Equal(t, []*tab{sourceTab}, source.tabs)
			source.mu.Unlock()
			sourceTab.mu.Lock()
			require.Same(t, moved, sourceTab.panes[moved.id])
			require.Equal(t, sourceGeneration, sourceTab.layoutGeneration)
			sourceTab.mu.Unlock()
			destinationTab.mu.Lock()
			require.Same(t, destinationPaneAtMovedID, destinationTab.panes[moved.id])
			require.Equal(t, destinationGeneration, destinationTab.layoutGeneration)
			destinationTab.mu.Unlock()
			require.Equal(t, owner, moved.ownerSnapshot())
			require.Equal(t, currentGeneration, current.roleGeneration.Load())
			assertRejectedFinalMoveAttachmentPreserved(t, source, current, replaced, tc.mutation)
		})
	}
}

func TestMoveTabFinalHeadlessRejectsAttachmentChangesAfterAdmissionAtomically(t *testing.T) {
	for _, tc := range []struct {
		name     string
		mutation finalMoveAttachmentMutation
		existing bool
	}{
		{name: "new active", mutation: installActiveAfterFinalMoveAdmission},
		{name: "new snatched", mutation: installSnatchedAfterFinalMoveAdmission},
		{name: "count preserving snatched replacement", mutation: replaceSnatchedAfterFinalMoveAdmission, existing: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, source, original, _ := newManualSessionWithPTYs(t, newQuietPTY())
			moved := source.tabs[0]
			moved.stableID = "moved-tab"
			makeFinalMoveSourceHeadless(source, original)
			if tc.existing {
				existing := newFinalMoveRaceClient(source)
				source.mu.Lock()
				source.addSnatchedLocked(existing)
				source.mu.Unlock()
			}
			destination := addMoveTabTestSession(d, "destination", "destination-tab")
			destinationTabs := append([]*tab(nil), destination.tabs...)
			sourceGeneration := moved.layoutGeneration
			owners := make(map[*pane]*paneOwner, len(moved.panes))
			for _, p := range moved.panes {
				owners[p] = p.ownerSnapshot()
			}

			barrier := make(chan struct{})
			releaseMove := make(chan struct{})
			d.afterMoveTabSourceSnapshot = func() {
				close(barrier)
				<-releaseMove
			}
			defer func() { d.afterMoveTabSourceSnapshot = nil }()
			moveDone := make(chan error, 1)
			go func() {
				moveDone <- d.moveTab(moveTabRequest{
					Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
					SourceTabID: domain.TabStableID(moved.stableID),
					Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
				})
			}()
			waitMoveRace(t, barrier, "move tab admission")
			current, replaced := mutateFinalMoveAttachments(source, tc.mutation)
			currentGeneration := current.roleGeneration.Load()
			close(releaseMove)
			err := waitMoveRace(t, moveDone, "move tab rejection")

			require.ErrorIs(t, err, errMoveStaleTarget)
			d.mu.Lock()
			require.Same(t, source, d.sessions[source.id])
			require.Same(t, destination, d.sessions[destination.id])
			d.mu.Unlock()
			source.mu.Lock()
			require.Equal(t, []*tab{moved}, source.tabs)
			source.mu.Unlock()
			destination.mu.Lock()
			require.Equal(t, destinationTabs, destination.tabs)
			destination.mu.Unlock()
			moved.mu.Lock()
			require.Equal(t, sourceGeneration, moved.layoutGeneration)
			moved.mu.Unlock()
			for p, owner := range owners {
				require.Equal(t, owner, p.ownerSnapshot())
			}
			require.Equal(t, currentGeneration, current.roleGeneration.Load())
			assertRejectedFinalMoveAttachmentPreserved(t, source, current, replaced, tc.mutation)
		})
	}
}
