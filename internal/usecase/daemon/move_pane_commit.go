package daemon

import "github.com/bnema/vev/internal/usecase/layout"

// movePaneCommit owns pane-private validation and publication. The shared
// moveTransaction already holds daemon, routing, ordered Session, and resize
// fences. Every error below is returned before the first topology write.
type movePaneCommit struct {
	req                       movePaneRequest
	source, destination       *session
	sourceTab, destinationTab *tab
	movedPane                 *pane
	sourceGeneration          uint64
	destinationGeneration     uint64
}

func (c *movePaneCommit) publishTopologyLocked(d *Daemon) (moveTopologyPublication, error) {
	unlockTabs := lockMoveTabs(c.sourceTab, c.destinationTab)
	c.movedPane.mu.Lock()
	defer c.movedPane.mu.Unlock()
	defer unlockTabs()

	if !moveTabMemberLocked(c.source, c.sourceTab) || !moveTabMemberLocked(c.destination, c.destinationTab) || c.sourceTab == c.destinationTab ||
		c.sourceTab.layoutGeneration != c.sourceGeneration || c.destinationTab.layoutGeneration != c.destinationGeneration ||
		c.movedPane.stableID != string(c.req.SourcePaneID) || c.sourceTab.panes[c.movedPane.id] != c.movedPane ||
		c.sourceTab.tree == nil || c.sourceTab.tree.Root == nil || !layout.ContainsLeaf(c.sourceTab.tree.Root, c.movedPane.id) ||
		c.destinationTab.tree == nil || c.destinationTab.tree.Root == nil {
		return moveTopologyPublication{}, errMoveStaleTarget
	}
	owner := c.movedPane.ownerSnapshot()
	if owner != nil && (owner.session != c.source || owner.tab != c.sourceTab) {
		return moveTopologyPublication{}, errMoveStaleTarget
	}
	candidate, err := prepareMovePaneCandidate(c.sourceTab, c.destinationTab, c.movedPane)
	if err != nil {
		return moveTopologyPublication{}, err
	}
	if candidate.sourceGeneration != c.sourceGeneration || candidate.destinationGeneration != c.destinationGeneration {
		return moveTopologyPublication{}, errMoveStaleTarget
	}
	idx := -1
	if candidate.removeSourceTab {
		idx = indexMoveTabLocked(c.source, c.sourceTab)
		if idx < 0 {
			return moveTopologyPublication{}, errMoveStaleTarget
		}
	}

	// Everything above is fallible. The remaining topology and owner writes form
	// one in-memory publication and cannot turn into a Move rejection.
	if candidate.removeSourceTab {
		if len(c.source.tabs) > 1 {
			c.source.prepareAttachmentViewsForRemovedTabLocked(c.sourceTab, idx)
		}
		c.source.tabs = append(c.source.tabs[:idx], c.source.tabs[idx+1:]...)
	} else {
		c.sourceTab.tree = candidate.sourceTree
		c.sourceTab.bumpLayoutGenerationLocked()
	}
	delete(c.sourceTab.panes, candidate.sourceID)
	if d.beforeMovePaneCommit != nil {
		d.beforeMovePaneCommit()
	}
	c.destinationTab.tree = candidate.destinationTree
	c.destinationTab.nextPaneID = candidate.destinationNextPaneID
	c.destinationTab.panes[candidate.destinationID] = c.movedPane
	c.destinationTab.bumpLayoutGenerationLocked()
	c.movedPane.id = candidate.destinationID
	oldOwner, newOwner := c.movedPane.publishOwnerLocked(c.destination, c.destinationTab, 0)
	var syncCleanup syncTimerCleanup
	syncCleanup.append(d.migratePaneSyncOwnerLocked(c.movedPane, oldOwner, newOwner))

	return moveTopologyPublication{
		sourceTab:        c.sourceTab,
		destinationTab:   c.destinationTab,
		movedPane:        c.movedPane,
		syncCleanup:      syncCleanup,
		sourceTabRemoved: candidate.removeSourceTab,
		sourceEmpty:      len(c.source.tabs) == 0,
	}, nil
}
