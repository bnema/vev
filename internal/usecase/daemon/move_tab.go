package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
)

type moveTabAdmission struct {
	tab                *tab
	sourceIndex        int
	sourceClient       *attachedClient
	sourceSnatched     int
	layoutGeneration   uint64
	floatingState      floatingState
	floatingPane       *pane
	floatingGeneration uint64
	panes              []*pane
	destinationActive  *tab
	destinationSize    domain.Size
	finalSource        bool
}

type moveTabCommit struct {
	req                 moveTabRequest
	source              *session
	destination         *session
	admission           moveTabAdmission
	handoffFrozen       bool
	sourceRolesFrozen   bool
	handoffReq          attachmentTransitionRequest
	handoffPublication  *attachmentPublication
	handoffResult       attachmentTransitionResult
	syncCleanup         syncTimerCleanup
	sourceCleanupToken  attachmentRoleToken
	oldTabCancel        context.CancelFunc
	sourceEmpty         bool
	retiredParked       []parkedAttachmentRetirement
	retiredAttachments  []detachedAttachmentSnapshot
	sourceMetadata      domain.CatalogueMetadataUpdate
	sourceMetadataValid bool
	destMetadata        domain.CatalogueMetadataUpdate
	destMetadataValid   bool
	err                 error
}

func (d *Daemon) moveTab(req moveTabRequest) error {
	if d == nil || req.Source.ID == "" || req.Destination.ID == "" || req.SourceTabID == "" || req.Source.ID == req.Destination.ID {
		return errMovePaneInvalid
	}
	d.mu.Lock()
	source := moveSessionForLocatorLocked(d, req.Source)
	destination := moveSessionForLocatorLocked(d, req.Destination)
	d.mu.Unlock()
	if source == nil || destination == nil || source == destination {
		return errMovePaneInvalid
	}
	reservation, err := d.reserveMoveLifecycles(source, destination)
	if err != nil {
		return err
	}
	reservationHeld := true
	defer func() {
		if reservationHeld {
			reservation.Release()
		}
	}()
	if d.afterMoveLifecycleReserved != nil {
		d.afterMoveLifecycleReserved()
	}
	unlockDispatch := lockMoveDispatch(source, destination)
	dispatchHeld := true
	defer func() {
		if dispatchHeld {
			unlockDispatch()
		}
	}()

	admission, err := d.snapshotMoveTabAdmission(req, source, destination)
	if err != nil {
		return err
	}
	commit := moveTabCommit{req: req, source: source, destination: destination, admission: *admission}
	var frozen frozenRoleEffectGates
	if admission.finalSource && admission.sourceClient != nil {
		transport := admission.sourceClient.transportSnapshot()
		token := source.attachmentToken(admission.sourceClient, transport.transport)
		if token.role != attachmentActive || token.transport.transport == nil {
			return errMovePaneInvalid
		}
		commit.handoffReq = attachmentTransitionRequest{
			source: source, target: destination, next: admission.sourceClient,
			expectedRole: attachmentActive, targetRole: attachmentActive,
			expectedTransport: token.transport, sourceToken: &token,
			expectedSourceTab: admission.tab, transferExpectedSourceTab: true, ready: true, action: "move-tab",
		}
		var participants attachmentTransitionParticipants
		commit.handoffReq, participants, err = d.snapshotAttachmentTransition(commit.handoffReq)
		if err != nil {
			return err
		}
		d.mu.Lock()
		source.mu.Lock()
		for ac := range source.snatched {
			participants.clients = append(participants.clients, ac)
			if tr := ac.transportSnapshot(); tr.transport != nil {
				participants.interrupts = append(participants.interrupts, roleTransportInterrupt{ac: ac, transport: tr})
			}
		}
		source.mu.Unlock()
		d.mu.Unlock()
		frozen = tryFreezeRoleEffectGatesInterruptingObserved(participants.interrupts, func(ac *attachedClient) {
			if d.afterRoleEffectGateFrozen != nil {
				d.afterRoleEffectGateFrozen("move-tab", ac)
			}
		}, participants.clients...)
		if !frozen.acquired || !frozen.drained {
			frozen.unfreeze()
			return errMovePaneInvalid
		}
		commit.handoffFrozen = true
		commit.sourceRolesFrozen = true
		commit.handoffReq.roleEffectsFrozen = true
	} else if admission.finalSource && admission.sourceSnatched > 0 {
		d.mu.Lock()
		source.mu.Lock()
		clients := make([]*attachedClient, 0, len(source.snatched))
		interrupts := make([]roleTransportInterrupt, 0, len(source.snatched))
		for ac := range source.snatched {
			clients = append(clients, ac)
			if tr := ac.transportSnapshot(); tr.transport != nil {
				interrupts = append(interrupts, roleTransportInterrupt{ac: ac, transport: tr})
			}
		}
		source.mu.Unlock()
		d.mu.Unlock()
		frozen = tryFreezeRoleEffectGatesInterruptingObserved(interrupts, nil, clients...)
		if !frozen.acquired || !frozen.drained {
			frozen.unfreeze()
			return errMovePaneInvalid
		}
		commit.sourceRolesFrozen = true
	}
	rolesFrozen := commit.sourceRolesFrozen
	defer func() {
		if rolesFrozen {
			frozen.unfreeze()
		}
	}()

	fences := newMoveTabResizeFences(source, destination, admission.tab)
	if !fences.acquire(func() bool { return commit.publishLocked(d, fences.panes) }) {
		commit.releasePublication()
		if commit.err != nil {
			return commit.err
		}
		return errMovePaneInvalid
	}
	fences.Release()
	if commit.oldTabCancel != nil {
		commit.oldTabCancel()
	}
	for _, p := range admission.panes {
		if commit.sourceCleanupToken.current() {
			commit.sourceCleanupToken.ac.overlays.clearCopyModeForPane(p)
			commit.sourceCleanupToken.ac.pruneCaptureFrames(p)
		}
	}
	markSnapshotDirty(destination)
	if !commit.sourceEmpty {
		markSnapshotDirty(source)
	}
	commit.syncCleanup.finish()
	if rolesFrozen {
		frozen.unfreeze()
		rolesFrozen = false
	}
	unlockDispatch()
	dispatchHeld = false
	reservation.Release()
	reservationHeld = false

	if !commit.sourceEmpty {
		if sourceTab := source.activeTab(); sourceTab != nil {
			d.activateTab(source, sourceTab)
			d.applyTabLayout(source, sourceTab)
		}
	}
	d.applyTabLayout(destination, admission.tab)
	if commit.sourceMetadataValid && !commit.sourceEmpty {
		d.markCatalogueDirty(commit.sourceMetadata)
	}
	if commit.destMetadataValid {
		d.markCatalogueDirty(commit.destMetadata)
	}
	if commit.handoffResult.published.ac != nil {
		follower := commit.handoffResult.published.ac
		d.applyHostTheme(destination, follower, follower.getClientTheme(), false)
		follower.recordPreviousSession(source)
		d.deferAttachmentTransitionCleanups(commit.handoffResult)
		d.firstPaintForTransition(commit.handoffResult.published)
	}
	if commit.sourceEmpty {
		retireEmptySessionAfterMove(d, source)
	}
	for _, attachment := range commit.retiredAttachments {
		d.unregisterPreview(attachment.ac)
		attachment.ac.clearPreviousSession()
		attachment.ac.clearCaptureFrames()
		d.notifyDetachedSnapshotAsync(attachment, ports.ReasonSessionKilled)
	}
	for _, retirement := range commit.retiredParked {
		if retirement.parked != nil && retirement.parked.ac != nil {
			d.unregisterPreview(retirement.parked.ac)
			retirement.parked.ac.clearCaptureFrames()
		}
	}
	finishParkedAttachmentRetirements(commit.retiredParked)
	if commit.sourceEmpty && commit.sourceMetadataValid && d.persistEnabled {
		if err := d.beginSnapshotPurge(source.name, source.incarnation); err == nil {
			if err := d.finishSnapshotPurge(d.serveCtx, source.name, source.incarnation); err != nil {
				d.log.Warn("moving final tab source purge failed", "err", err, "session", source.name)
			}
		}
	}
	return nil
}

func (d *Daemon) snapshotMoveTabAdmission(req moveTabRequest, source, destination *session) (*moveTabAdmission, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.sessions[source.id] != source || d.sessions[destination.id] != destination {
		return nil, errMovePaneInvalid
	}
	unlockSessions := lockAttachmentSessions(source, destination)
	defer unlockSessions()
	if !moveSessionLocatorCurrentLocked(source, req.Source) || !moveSessionLocatorCurrentLocked(destination, req.Destination) {
		return nil, errMovePaneInvalid
	}
	moved := findMoveTabLocked(source, req.SourceTabID)
	if moved == nil || destination.active < 0 || destination.active >= len(destination.tabs) {
		return nil, errMovePaneInvalid
	}
	destinationActive := destination.tabs[destination.active]
	destinationActive.mu.Lock()
	destinationSize := destinationActive.size
	destinationActive.mu.Unlock()
	moved.mu.Lock()
	defer moved.mu.Unlock()
	if !moved.floatingTransferableLocked() {
		return nil, errMovePaneInvalid
	}
	if _, ok := layout.Solve(moved.tree.Root, domain.Rect{Width: destinationSize.Cols, Height: destinationSize.Rows}); !ok {
		return nil, errMovePaneInvalid
	}
	panes := make([]*pane, 0, len(moved.panes)+1)
	for _, p := range moved.panes {
		panes = append(panes, p)
	}
	if moved.floating.pane != nil && (moved.floating.state == floatingHidden || moved.floating.state == floatingVisible) {
		panes = append(panes, moved.floating.pane)
	}
	idx := indexMoveTabLocked(source, moved)
	return &moveTabAdmission{
		tab: moved, sourceIndex: idx,
		sourceClient: source.client, sourceSnatched: len(source.snatched),
		layoutGeneration: moved.layoutGeneration, floatingState: moved.floating.state,
		floatingPane: moved.floating.pane, floatingGeneration: moved.floating.generation,
		panes: panes, destinationActive: destinationActive, destinationSize: destinationSize,
		finalSource: len(source.tabs) == 1,
	}, nil
}

func (c *moveTabCommit) releasePublication() {
	if c.handoffPublication != nil {
		c.handoffPublication.unlockCoordinators()
		c.handoffPublication = nil
	}
}

func (c *moveTabCommit) publishLocked(d *Daemon, fencedPanes []*pane) bool {
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
	moved := c.admission.tab
	if indexMoveTabLocked(c.source, moved) != c.admission.sourceIndex || findMoveTabLocked(c.destination, c.req.SourceTabID) != nil ||
		len(c.source.tabs) == 1 != c.admission.finalSource {
		return false
	}
	// Attachment validation inspects the expected source tab and therefore must
	// precede moved.mu. The resize fences and ordered session locks keep topology
	// stable until the tab/pane checks below complete.
	if c.handoffFrozen {
		if c.source.client != c.admission.sourceClient || len(c.source.snatched) != c.admission.sourceSnatched {
			return false
		}
		publication, err := d.validateAttachmentTransitionPrelocked(c.handoffReq)
		if err != nil {
			c.err = err
			return false
		}
		c.handoffPublication = publication
		defer c.releasePublication()
	}
	if c.sourceRolesFrozen && len(c.source.snatched) != c.admission.sourceSnatched {
		return false
	}
	if c.destination.active < 0 || c.destination.active >= len(c.destination.tabs) || c.destination.tabs[c.destination.active] != c.admission.destinationActive {
		return false
	}
	c.admission.destinationActive.mu.Lock()
	destinationSizeCurrent := c.admission.destinationActive.size == c.admission.destinationSize
	c.admission.destinationActive.mu.Unlock()
	if !destinationSizeCurrent {
		return false
	}
	moved.mu.Lock()
	defer moved.mu.Unlock()
	if moved.layoutGeneration != c.admission.layoutGeneration || !moved.floatingTransferableLocked() ||
		moved.floating.state != c.admission.floatingState || moved.floating.pane != c.admission.floatingPane ||
		moved.floating.generation != c.admission.floatingGeneration || !sameMoveTabPanesLocked(moved, c.admission.panes, fencedPanes) {
		return false
	}
	for _, p := range fencedPanes {
		p.mu.Lock()
		defer p.mu.Unlock()
		owner := p.ownerSnapshot()
		if owner == nil || owner.session != c.source || owner.tab != moved {
			return false
		}
	}

	idx := c.admission.sourceIndex
	c.source.tabs = append(c.source.tabs[:idx], c.source.tabs[idx+1:]...)
	if len(c.source.tabs) == 0 {
		c.source.active = -1
	} else if c.source.active > idx {
		c.source.active--
	} else if c.source.active == idx {
		if idx >= len(c.source.tabs) {
			idx = len(c.source.tabs) - 1
		}
		c.source.active = idx
	}
	c.destination.tabs = append(c.destination.tabs, moved)
	moved.size = c.admission.destinationSize
	moved.bumpLayoutGenerationLocked()
	c.oldTabCancel = moved.cancel
	parent := c.destination.ctx
	if parent == nil {
		parent = d.serveCtx
	}
	moved.ctx, moved.cancel = context.WithCancel(parent)
	type ownerChange struct {
		pane     *pane
		oldOwner paneEffectLease
		newOwner paneEffectLease
	}
	ownerChanges := make([]ownerChange, 0, len(fencedPanes))
	for _, p := range fencedPanes {
		floatingGeneration := uint64(0)
		if p == moved.floating.pane {
			floatingGeneration = moved.floating.generation
		}
		oldOwner, newOwner := p.publishOwnerLocked(c.destination, moved, floatingGeneration)
		ownerChanges = append(ownerChanges, ownerChange{pane: p, oldOwner: oldOwner, newOwner: newOwner})
	}
	if d.beforeMoveTabCommit != nil {
		d.beforeMoveTabCommit()
	}
	if c.handoffPublication != nil {
		c.destination.active = len(c.destination.tabs) - 1
		c.handoffResult = d.publishAttachmentTransitionPrelocked(c.handoffPublication)
		c.handoffPublication.unlockCoordinators()
		c.handoffPublication = nil
	}
	// Attachment publication retains source/destination coordinator locks.
	// Release those first, then migrate synchronized-output batches exactly as
	// Move pane does, while pane parsing remains fenced.
	for _, change := range ownerChanges {
		c.syncCleanup.append(d.migratePaneSyncOwnerLocked(change.pane, change.oldOwner, change.newOwner))
	}
	if c.handoffResult.published.ac != nil {
		c.sourceCleanupToken = c.handoffResult.published
	} else if c.source.client != nil {
		c.sourceCleanupToken = c.source.attachmentTokenLocked(c.source.client)
	}
	if len(c.source.tabs) == 0 {
		delete(d.sessions, c.source.id)
		c.retiredAttachments = retireEmptyMoveSessionLocked(c.source)
		c.retiredParked = d.purgeParkedForSessionLocked(c.source)
	}
	c.sourceEmpty = len(c.source.tabs) == 0
	if !c.source.ephemeral {
		c.sourceMetadata = c.source.persistRecordLocked(max(d.nowUnixNano(), c.source.createdAt, int64(1))).MetadataUpdate()
		c.sourceMetadataValid = true
	}
	if !c.destination.ephemeral {
		c.destMetadata = c.destination.persistRecordLocked(max(d.nowUnixNano(), c.destination.createdAt, int64(1))).MetadataUpdate()
		c.destMetadataValid = true
	}
	return true
}

func sameMoveTabPanesLocked(tb *tab, admitted, fenced []*pane) bool {
	current := make(map[*pane]struct{}, len(tb.panes)+1)
	for _, p := range tb.panes {
		current[p] = struct{}{}
	}
	if tb.floating.pane != nil && (tb.floating.state == floatingHidden || tb.floating.state == floatingVisible) {
		current[tb.floating.pane] = struct{}{}
	}
	if len(current) != len(admitted) || len(current) != len(fenced) {
		return false
	}
	for _, p := range admitted {
		if _, ok := current[p]; !ok {
			return false
		}
	}
	for _, p := range fenced {
		if _, ok := current[p]; !ok {
			return false
		}
	}
	return true
}
