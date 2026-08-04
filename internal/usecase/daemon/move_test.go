package daemon

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestMovePaneMutationSameSession(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	d, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	sourceTab, destinationTab := sess.tabs[0], sess.tabs[1]
	sourceTab.stableID = "source-tab"
	destinationTab.stableID = "destination-tab"
	moved := sourceTab.focusedPane()
	movedCtx, movedCancel := context.WithCancel(d.paneProcessCtx)
	moved.ctx, moved.cancel = movedCtx, movedCancel

	err := d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: sess.id},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: sess.id},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	})
	require.NoError(t, err)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Equal(t, layout.PaneID("pane-2"), moved.id)
	require.Equal(t, layout.PaneID("pane-2"), destinationTab.tree.Focus)
	require.NotContains(t, sourceTab.panes, layout.PaneID("pane-1"))
	require.Len(t, sess.tabs, 1)
	require.Same(t, destinationTab, sess.tabs[0])
	require.NoError(t, moved.ctx.Err(), "a same-session move must not retire the moved pane process")
}

func TestMovePaneMutationCrossSession(t *testing.T) {
	p1, release1 := newBlockingPTY(t)
	p2, release2 := newBlockingPTY(t)
	defer release1()
	defer release2()
	d, source, _, _ := newManualSessionWithPTYs(t, p1)
	source.mu.Lock()
	clearAttachmentsForTestLocked(source)
	source.mu.Unlock()
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	movedCtx, movedCancel := context.WithCancel(d.paneProcessCtx)
	moved.ctx, moved.cancel = movedCtx, movedCancel

	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTab(p2, domain.Size{Cols: 80, Rows: 23})}}
	destinationTab := destination.tabs[0]
	destinationTab.stableID = "destination-tab"
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	err := d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	})
	require.NoError(t, err)
	require.Nil(t, source.tabs)
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Equal(t, destinationTab, destination.tabs[0])
	require.Same(t, destination, moved.ownerSnapshot().session)
	require.Same(t, destinationTab, moved.ownerSnapshot().tab)
	require.NoError(t, moved.ctx.Err(), "retiring the source must not cancel the transferred pane process")
}

func TestMovePaneRetiresEmptySourceWithoutClosingTransferredResources(t *testing.T) {
	movedPTY := newQuietPTY()
	d, source, active, _ := newManualSessionWithPTYs(t, movedPTY)
	clip, err := os.CreateTemp(t.TempDir(), "move-clip-")
	require.NoError(t, err)
	require.NoError(t, clip.Close())
	source.mu.Lock()
	clearAttachmentsForTestLocked(source)
	source.clipFiles = []string{clip.Name()}
	source.mu.Unlock()
	active.setSession(nil)
	moved := source.tabs[0].focusedPane()
	movedCtx, movedCancel := context.WithCancel(d.paneProcessCtx)
	moved.ctx, moved.cancel = movedCtx, movedCancel
	movedTab := source.tabs[0]
	movedTab.stableID = "source-tab"

	destinationPTY := newQuietPTY()
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}}
	publishTiledPaneOwners(destination, destination.tabs[0])
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	err = d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id},
		SourceTabID:      domain.TabStableID(movedTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id},
		DestinationTabID: domain.TabStableID(destination.tabs[0].stableID),
	})
	require.NoError(t, err)
	select {
	case <-movedPTY.done:
		t.Fatal("retiring the source closed the transferred PTY")
	default:
	}
	select {
	case <-moved.ctx.Done():
		t.Fatal("retiring the source cancelled the transferred pane process")
	default:
	}
	select {
	case <-source.ctx.Done():
	default:
		t.Fatal("empty source lifecycle was not cancelled")
	}
	_, err = os.Stat(clip.Name())
	require.ErrorIs(t, err, os.ErrNotExist, "retired source clipboard resources must be removed")
}

func TestMovePaneRetiresSourceParkedClients(t *testing.T) {
	movedPTY := newQuietPTY()
	d, source, _, _ := newManualSessionWithPTYs(t, movedPTY)
	source.mu.Lock()
	clearAttachmentsForTestLocked(source)
	source.mu.Unlock()
	moved := source.tabs[0].focusedPane()
	movedTab := source.tabs[0]
	movedTab.stableID = "source-tab"

	transport := &closeTrackingTransport{}
	parked := &attachedClient{tr: transport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}, resumeCapable: true}
	parked.initOverlays()
	require.True(t, d.parkAttachment(source, parked))
	destinationPTY := newQuietPTY()
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}}
	publishTiledPaneOwners(destination, destination.tabs[0])
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	require.NoError(t, d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id},
		SourceTabID:      domain.TabStableID(movedTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id},
		DestinationTabID: domain.TabStableID(destination.tabs[0].stableID),
	}))
	d.mu.Lock()
	require.Empty(t, d.parked, "retired source parked clients must leave the daemon registry")
	d.mu.Unlock()
	require.True(t, transport.Closed(), "retired source parked transport was not closed")
}

func TestPrepareMovePaneCandidateIsPure(t *testing.T) {
	t.Parallel()

	newPaneRef := func(id layout.PaneID, stableID string) *pane {
		return &pane{id: id, stableID: stableID}
	}
	horizontal := func(ids ...layout.PaneID) *layout.Node {
		children := make([]*layout.Node, 0, len(ids))
		for _, id := range ids {
			children = append(children, layout.NewLeaf(id))
		}
		return &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: children}
	}
	stack := func(ids ...layout.PaneID) *layout.Node {
		children := make([]*layout.Node, 0, len(ids))
		for _, id := range ids {
			children = append(children, layout.NewLeaf(id))
		}
		return &layout.Node{Kind: layout.Stack, Children: children, Expanded: ids[0]}
	}

	tests := []struct {
		name                string
		sourceTree          *layout.Tree
		sourceFloating      floatingState
		destinationTree     *layout.Tree
		destinationSize     domain.Size
		destinationNext     int
		wantErr             error
		wantRemoveTab       bool
		wantSourceFocus     layout.PaneID
		wantDestination     layout.PaneID
		wantDestinationNext int
		wantDestinationIDs  []layout.PaneID
	}{
		{
			name:                "split source closes on clone and destination inserts right of focus",
			sourceTree:          &layout.Tree{Root: horizontal("pane-1", "pane-2"), Focus: "pane-2"},
			destinationTree:     &layout.Tree{Root: horizontal("pane-1", "pane-2"), Focus: "pane-1"},
			destinationSize:     domain.Size{Cols: 100, Rows: 4},
			destinationNext:     2,
			wantSourceFocus:     "pane-1",
			wantDestination:     "pane-3",
			wantDestinationNext: 4,
			wantDestinationIDs:  []layout.PaneID{"pane-1", "pane-3", "pane-2"},
		},
		{
			name:                "stack source remains valid after close",
			sourceTree:          &layout.Tree{Root: stack("pane-2", "pane-1"), Focus: "pane-2"},
			destinationTree:     layout.NewTree("pane-1"),
			destinationSize:     domain.Size{Cols: 41, Rows: 4},
			destinationNext:     2,
			wantSourceFocus:     "pane-1",
			wantDestination:     "pane-3",
			wantDestinationNext: 4,
			wantDestinationIDs:  []layout.PaneID{"pane-1", "pane-3"},
		},
		{
			name:                "final tiled pane requests source tab removal",
			sourceTree:          layout.NewTree("pane-2"),
			destinationTree:     layout.NewTree("pane-1"),
			destinationSize:     domain.Size{Cols: 41, Rows: 2},
			destinationNext:     2,
			wantRemoveTab:       true,
			wantDestination:     "pane-3",
			wantDestinationNext: 4,
			wantDestinationIDs:  []layout.PaneID{"pane-1", "pane-3"},
		},
		{
			name:            "final tiled pane rejects warming floating sibling",
			sourceTree:      layout.NewTree("pane-2"),
			sourceFloating:  floatingWarming,
			destinationTree: layout.NewTree("pane-1"),
			destinationSize: domain.Size{Cols: 41, Rows: 2},
			destinationNext: 2,
			wantErr:         errMoveFloatingWarming,
		},
		{
			name:            "final tiled pane rejects hidden floating sibling",
			sourceTree:      layout.NewTree("pane-2"),
			sourceFloating:  floatingHidden,
			destinationTree: layout.NewTree("pane-1"),
			destinationSize: domain.Size{Cols: 41, Rows: 2},
			destinationNext: 2,
			wantErr:         errMovePaneFloatingSibling,
		},
		{
			name:            "final tiled pane rejects visible floating sibling",
			sourceTree:      layout.NewTree("pane-2"),
			sourceFloating:  floatingVisible,
			destinationTree: layout.NewTree("pane-1"),
			destinationSize: domain.Size{Cols: 41, Rows: 2},
			destinationNext: 2,
			wantErr:         errMovePaneFloatingSibling,
		},
		{
			name:            "destination too small rejects candidate",
			sourceTree:      &layout.Tree{Root: horizontal("pane-1", "pane-2"), Focus: "pane-2"},
			destinationTree: layout.NewTree("pane-1"),
			destinationSize: domain.Size{Cols: 40, Rows: 2},
			destinationNext: 2,
			wantErr:         layout.ErrTooSmall,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			moved := newPaneRef("pane-2", "stable-moved")
			sourcePanes := map[layout.PaneID]*pane{"pane-2": moved}
			for _, id := range layout.LeafIDs(tt.sourceTree.Root) {
				if id != moved.id {
					sourcePanes[id] = newPaneRef(id, "source-"+string(id))
				}
			}
			destinationPanes := make(map[layout.PaneID]*pane)
			for _, id := range layout.LeafIDs(tt.destinationTree.Root) {
				destinationPanes[id] = newPaneRef(id, "destination-"+string(id))
			}
			source := &tab{
				tree:       tt.sourceTree,
				panes:      sourcePanes,
				nextPaneID: 17,
				size:       domain.Size{Cols: 80, Rows: 4},
				floating:   floatingSlot{state: tt.sourceFloating, generation: 9},
			}
			destination := &tab{
				tree:       tt.destinationTree,
				panes:      destinationPanes,
				nextPaneID: tt.destinationNext,
				size:       tt.destinationSize,
			}
			sourceBefore := source.tree.Clone()
			destinationBefore := destination.tree.Clone()
			sourceNextBefore, destinationNextBefore := source.nextPaneID, destination.nextPaneID
			movedIDBefore, movedStableIDBefore := moved.id, moved.stableID

			candidate, err := prepareMovePaneCandidate(source, destination, moved)

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, sourceBefore, source.tree, "preparation must not mutate the source tree")
			require.Equal(t, destinationBefore, destination.tree, "preparation must not mutate the destination tree")
			require.Equal(t, sourceNextBefore, source.nextPaneID)
			require.Equal(t, destinationNextBefore, destination.nextPaneID, "preparation must not advance the destination counter")
			require.Equal(t, movedIDBefore, moved.id, "preparation must not publish the new local ID")
			require.Equal(t, movedStableIDBefore, moved.stableID)
			require.Same(t, moved, source.panes[movedIDBefore])
			for id, p := range destinationPanes {
				require.Same(t, p, destination.panes[id])
			}
			if tt.wantErr != nil {
				require.Nil(t, candidate)
				return
			}

			require.Same(t, moved, candidate.pane)
			require.Equal(t, movedIDBefore, candidate.sourceID)
			require.Equal(t, tt.wantDestination, candidate.destinationID)
			require.NotEqual(t, candidate.sourceID, candidate.destinationID)
			require.Equal(t, tt.wantRemoveTab, candidate.removeSourceTab)
			require.Equal(t, tt.wantDestinationNext, candidate.destinationNextPaneID, "candidate counter should follow the allocated ID")
			require.Equal(t, tt.wantDestinationIDs, layout.LeafIDs(candidate.destinationTree.Root))
			require.Equal(t, tt.wantDestination, candidate.destinationTree.Focus)
			require.NotEmpty(t, candidate.destinationPlacements)
			if tt.wantRemoveTab {
				require.Nil(t, candidate.sourceTree)
				require.Nil(t, candidate.sourcePlacements)
			} else {
				require.Equal(t, tt.wantSourceFocus, candidate.sourceTree.Focus)
				require.True(t, layout.ContainsLeaf(candidate.sourceTree.Root, candidate.sourceTree.Focus))
				require.NotEmpty(t, candidate.sourcePlacements)
			}
			require.Equal(t, "stable-moved", candidate.pane.stableID)
		})
	}
}
