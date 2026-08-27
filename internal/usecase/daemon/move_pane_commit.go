package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

// movePaneCommit owns the single architecture-locked publication section. Its
// inputs are immutable admission/fence snapshots; outputs form the postcommit
// plan. publishLocked performs all fallible checks before its first write.
type movePaneCommit struct {
	req                       movePaneRequest
	source, destination       *session
	sourceTab, destinationTab *tab
	movedPane                 *pane
	sourceAttachments         []*attachedClient
	sourceTransports          map[*attachedClient]transportSnapshot
	sourceGeneration          uint64
	destinationGeneration     uint64
	frozenEffects             attachmentTransitionGuard
	err                       error

	syncCleanup              syncTimerCleanup
	sourceEmpty              bool
	sourceTabRemoved         bool
	retiredParked            []parkedAttachmentRetirement
	retiredAttachments       []detachedAttachmentSnapshot
	sourceMetadata           domain.CatalogueMetadataUpdate
	sourceMetadataValid      bool
	destinationMetadata      domain.CatalogueMetadataUpdate
	destinationMetadataValid bool
}

func (c *movePaneCommit) publishLocked(d *Daemon) bool {
	var candidate *movePaneCandidate
	var err error
	d.mu.Lock()
	if d.closing || d.sessions[c.source.id] != c.source || d.sessions[c.destination.id] != c.destination {
		d.mu.Unlock()
		return false
	}
	d.notices.routingMu.Lock()
	unlockSessions := lockAttachmentSessions(c.source, c.destination)
	defer unlockSessions()
	defer d.notices.routingMu.Unlock()
	defer d.mu.Unlock()

	if !moveSessionLocatorCurrentLocked(c.source, c.req.Source) || !moveSessionLocatorCurrentLocked(c.destination, c.req.Destination) {
		return false
	}
	if c.req.Attachment != nil && !attachmentRegisteredLocked(c.source, c.req.Attachment) {
		return false
	}
	if c.req.AttachmentCapability.ac != nil && (c.req.AttachmentCapability.ac != c.req.Attachment || !c.req.AttachmentCapability.currentInSessionLocked(c.source)) {
		return false
	}

	unlockTabs := lockMoveTabs(c.sourceTab, c.destinationTab)
	c.movedPane.mu.Lock()
	defer c.movedPane.mu.Unlock()
	defer unlockTabs()

	if !moveTabMemberLocked(c.source, c.sourceTab) || !moveTabMemberLocked(c.destination, c.destinationTab) || c.sourceTab == c.destinationTab ||
		c.sourceTab.layoutGeneration != c.sourceGeneration || c.destinationTab.layoutGeneration != c.destinationGeneration ||
		c.movedPane.stableID != string(c.req.SourcePaneID) || c.sourceTab.panes[c.movedPane.id] != c.movedPane ||
		c.sourceTab.tree == nil || c.sourceTab.tree.Root == nil || !layout.ContainsLeaf(c.sourceTab.tree.Root, c.movedPane.id) ||
		c.destinationTab.tree == nil || c.destinationTab.tree.Root == nil {
		return false
	}
	owner := c.movedPane.ownerSnapshot()
	if owner != nil && (owner.session != c.source || owner.tab != c.sourceTab) {
		return false
	}
	candidate, err = prepareMovePaneCandidate(c.sourceTab, c.destinationTab, c.movedPane)
	if err != nil {
		c.err = err
		return false
	}
	if candidate.sourceGeneration != c.sourceGeneration || candidate.destinationGeneration != c.destinationGeneration {
		return false
	}

	sourceWillEmpty := candidate.removeSourceTab && len(c.source.tabs) == 1
	if sourceWillEmpty && c.source != c.destination && !sameMoveAttachmentsLocked(c.source, c.sourceAttachments) {
		return false
	}

	// Everything above is fallible. Shared topology and owner publication are
	// committed once; attachment views are repaired after these locks release.
	if candidate.removeSourceTab {
		idx := indexMoveTabLocked(c.source, c.sourceTab)
		if idx < 0 {
			return false
		}
		if len(c.source.tabs) > 1 {
			c.source.prepareAttachmentViewsForRemovedTabLocked(c.sourceTab, idx)
		}
		c.source.tabs = append(c.source.tabs[:idx], c.source.tabs[idx+1:]...)
		c.sourceTabRemoved = true
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
	c.syncCleanup.append(d.migratePaneSyncOwnerLocked(c.movedPane, oldOwner, newOwner))

	if sourceWillEmpty {
		d.unregisterSessionLocked(c.source)
		c.retiredAttachments = detachMoveAttachmentsLocked(c.source, c.sourceTransports)
		c.source.tabs = nil
		d.purgeParkingForSessionLocked(c.source)
		c.retiredParked = d.purgeParkedForSessionLocked(c.source)
	}
	c.sourceEmpty = len(c.source.tabs) == 0
	if !c.source.ephemeral {
		c.sourceMetadata = c.source.persistRecordLocked(max(d.nowUnixNano(), c.source.createdAt, int64(1))).MetadataUpdate()
		c.sourceMetadataValid = true
	}
	if !c.destination.ephemeral && c.destination != c.source {
		c.destinationMetadata = c.destination.persistRecordLocked(max(d.nowUnixNano(), c.destination.createdAt, int64(1))).MetadataUpdate()
		c.destinationMetadataValid = true
	}
	return true
}
