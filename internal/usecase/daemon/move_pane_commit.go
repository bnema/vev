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
	sourceClient              *attachedClient
	sourceSnatchedCount       int
	sourceGeneration          uint64
	destinationGeneration     uint64
	handoffFrozen             bool
	sourceRolesFrozen         bool
	handoffReq                attachmentTransitionRequest
	handoffPublication        *attachmentPublication
	err                       error

	handoffResult            attachmentTransitionResult
	syncCleanup              syncTimerCleanup
	sourceCleanupToken       attachmentRoleToken
	sourceEmpty              bool
	sourceTabRemoved         bool
	retiredParked            []parkedAttachmentRetirement
	retiredAttachments       []detachedAttachmentSnapshot
	sourceMetadata           domain.CatalogueMetadataUpdate
	sourceMetadataValid      bool
	destinationMetadata      domain.CatalogueMetadataUpdate
	destinationMetadataValid bool
}

func (c *movePaneCommit) releasePublication() {
	if c != nil && c.handoffPublication != nil {
		c.handoffPublication.unlockCoordinators()
		c.handoffPublication = nil
	}
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
	if c.handoffFrozen {
		if c.source.client != c.sourceClient || len(c.source.snatched) != c.sourceSnatchedCount ||
			c.handoffReq.targetTabIndex < 0 || c.handoffReq.targetTabIndex >= len(c.destination.tabs) ||
			c.destination.tabs[c.handoffReq.targetTabIndex] != c.destinationTab {
			return false
		}
		c.handoffPublication, err = d.validateAttachmentTransitionPrelocked(c.handoffReq)
		if err != nil {
			c.err = err
			return false
		}
		defer c.releasePublication()
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
	needsHandoff := sourceWillEmpty && c.source != c.destination && c.sourceClient != nil
	if needsHandoff != c.handoffFrozen || sourceWillEmpty && c.source != c.destination && c.source.client != nil && !needsHandoff {
		return false
	}
	if c.sourceRolesFrozen && len(c.source.snatched) != c.sourceSnatchedCount {
		return false
	}

	// Everything above is fallible. Topology, owner, lifecycle registry, and
	// optional attachment roles now become visible under one lock section.
	if candidate.removeSourceTab {
		idx := indexMoveTabLocked(c.source, c.sourceTab)
		if idx < 0 {
			return false
		}
		c.source.tabs = append(c.source.tabs[:idx], c.source.tabs[idx+1:]...)
		c.sourceTabRemoved = true
		if len(c.source.tabs) == 0 {
			c.source.active = -1
		} else if c.source.active > idx {
			c.source.active--
		} else if c.source.active >= len(c.source.tabs) {
			c.source.active = len(c.source.tabs) - 1
		}
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
	if c.handoffPublication != nil {
		c.handoffResult = d.publishAttachmentTransitionPrelocked(c.handoffPublication)
		c.handoffPublication.unlockCoordinators()
		c.handoffPublication = nil
	}
	c.syncCleanup.append(d.migratePaneSyncOwnerLocked(c.movedPane, oldOwner, newOwner))
	if c.handoffResult.published.ac != nil {
		c.sourceCleanupToken = c.handoffResult.published
	} else if c.source.client != nil {
		c.sourceCleanupToken = c.source.attachmentTokenLocked(c.source.client)
	}

	if candidate.removeSourceTab && len(c.source.tabs) == 0 {
		delete(d.sessions, c.source.id)
		c.retiredAttachments = retireEmptyMoveSessionLocked(c.source)
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
