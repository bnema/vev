package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestMovePaneRejectsStaleDestinationIncarnationWithoutMutation(t *testing.T) {
	movedPTY, releaseMoved := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	defer releaseMoved()
	defer releaseDestination()

	d, source, _, _ := newManualSessionWithPTYs(t, movedPTY)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	destination := &session{
		id: "destination", name: "destination", ephemeral: true,
		tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}, active: 0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	beforeSource := sourceTab.tree.Clone()
	beforeDestination := destinationTab.tree.Clone()
	beforeOwner := moved.ownerSnapshot()
	staleIncarnation := destination.incarnation
	staleIncarnation[0]++
	err := d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id, Incarnation: staleIncarnation},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	})
	require.ErrorIs(t, err, errMovePaneInvalid)
	require.Equal(t, beforeSource, sourceTab.tree)
	require.Equal(t, beforeDestination, destinationTab.tree)
	require.Same(t, beforeOwner, moved.ownerSnapshot())
	require.Same(t, moved, sourceTab.panes[moved.id])
}
