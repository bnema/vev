package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

type moveTabAdmission struct {
	tab                *tab
	sourceIndex        int
	sourceAttachments  []*attachedClient
	sourceTransports   map[*attachedClient]transportSnapshot
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
	sourceEffectsFrozen bool
	frozenEffects       frozenAttachmentEffectGates
	syncCleanup         syncTimerCleanup
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

func (d *Daemon) moveTab(req moveTabRequest) (result error) {
	if d == nil {
		return errMovePaneInvalid
	}
	d.log.Info("tab move requested",
		"source_session_id", req.Source.ID,
		"source_tab_id", req.SourceTabID,
		"destination_session_id", req.Destination.ID,
	)
	defer func() {
		result = normalizeMoveRejection(result)
		if result != nil {
			d.log.Warn("tab move rejected",
				"err", result,
				"source_session_id", req.Source.ID,
				"source_tab_id", req.SourceTabID,
				"destination_session_id", req.Destination.ID,
			)
		}
	}()
	if req.Source.ID == "" || req.Destination.ID == "" || req.SourceTabID == "" || req.Source.ID == req.Destination.ID {
		return errMovePaneInvalid
	}
	d.mu.Lock()
	source := moveSessionForLocatorLocked(d, req.Source)
	destination := moveSessionForLocatorLocked(d, req.Destination)
	d.mu.Unlock()
	if source == nil || destination == nil {
		return errMoveStaleTarget
	}
	if source == destination {
		return errMovePaneInvalid
	}
	// Dispatch admission must precede lifecycle reservation. A final close holds
	// dispatchMu while it waits for teardown ownership; reserving first would let
	// that close wait on a move which is itself blocked on dispatchMu.
	if d.beforeMoveDispatch != nil {
		d.beforeMoveDispatch()
	}
	unlockDispatch := lockMoveDispatch(source, destination)
	dispatchHeld := true
	defer func() {
		if dispatchHeld {
			unlockDispatch()
		}
	}()

	reservation, err := d.reserveMoveLifecycles(source, destination)
	if err != nil {
		return errMoveStaleTarget
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

	admission, err := d.snapshotMoveTabAdmission(req, source, destination)
	if err != nil {
		return err
	}
	if d.afterMoveTabSourceSnapshot != nil {
		d.afterMoveTabSourceSnapshot()
	}
	commit := moveTabCommit{req: req, source: source, destination: destination, admission: *admission}
	var frozen frozenAttachmentEffectGates
	if admission.finalSource && len(admission.sourceAttachments) != 0 {
		interrupts := make([]attachmentTransportInterrupt, 0, len(admission.sourceAttachments))
		for _, ac := range admission.sourceAttachments {
			if transport := admission.sourceTransports[ac]; transport.transport != nil {
				interrupts = append(interrupts, attachmentTransportInterrupt{ac: ac, transport: transport})
			}
		}
		frozen = freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{interrupts: interrupts, nonblocking: true, afterFrozen: func(ac *attachedClient) {
			if d.afterAttachmentEffectGateFrozen != nil {
				d.afterAttachmentEffectGateFrozen("move-tab", ac)
			}
		}}, admission.sourceAttachments...)
		if !frozen.acquired || !frozen.drained {
			frozen.unfreeze()
			return errMovePaneInvalid
		}
		commit.sourceEffectsFrozen = true
	}
	commit.frozenEffects = frozen
	effectsFrozen := commit.sourceEffectsFrozen
	defer func() {
		if effectsFrozen {
			frozen.unfreeze()
		}
	}()

	fences := newMoveTabResizeFences(source, destination, admission.tab)
	if !fences.acquire(func() bool { return commit.publishLocked(d, fences.panes) }) {
		if commit.err != nil {
			return commit.err
		}
		return errMoveStaleTarget
	}
	fences.Release()
	postcommit := movePanePostcommitPlan{
		source:                   source,
		destination:              destination,
		sourceTab:                admission.tab,
		destinationTab:           admission.tab,
		movedPane:                firstMovePane(admission.panes),
		movedPanes:               admission.panes,
		operation:                "tab",
		sourceAttachments:        admission.sourceAttachments,
		syncCleanup:              commit.syncCleanup,
		frozenEffects:            frozen,
		effectsFrozen:            effectsFrozen,
		unlockDispatch:           unlockDispatch,
		reservation:              reservation,
		oldTabCancel:             commit.oldTabCancel,
		sourceTabRemoved:         true,
		sourceEmpty:              commit.sourceEmpty,
		retiredParked:            commit.retiredParked,
		retiredAttachments:       commit.retiredAttachments,
		sourceMetadata:           commit.sourceMetadata,
		sourceMetadataValid:      commit.sourceMetadataValid,
		destinationMetadata:      commit.destMetadata,
		destinationMetadataValid: commit.destMetadataValid,
	}
	effectsFrozen = false
	dispatchHeld = false
	reservationHeld = false
	postcommit.execute(d)
	return nil
}

func (d *Daemon) snapshotMoveTabAdmission(req moveTabRequest, source, destination *session) (*moveTabAdmission, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.sessions[source.id] != source || d.sessions[destination.id] != destination {
		return nil, errMoveStaleTarget
	}
	unlockSessions := lockAttachmentSessions(source, destination)
	defer unlockSessions()
	if !moveSessionLocatorCurrentLocked(source, req.Source) || !moveSessionLocatorCurrentLocked(destination, req.Destination) {
		return nil, errMoveStaleTarget
	}
	if req.Attachment != nil && !attachmentRegisteredLocked(source, req.Attachment) {
		return nil, errMoveStaleTarget
	}
	if req.AttachmentToken.ac != nil && (req.AttachmentToken.ac != req.Attachment || !moveAttachmentTokenCurrentLocked(req.AttachmentToken, source)) {
		return nil, errMoveStaleTarget
	}
	moved := findMoveTabLocked(source, req.SourceTabID)
	if moved == nil || len(destination.tabs) == 0 {
		return nil, errMoveStaleTarget
	}
	destinationActive := destination.tabs[0]
	destinationActive.mu.Lock()
	destinationSize := destinationActive.size
	destinationActive.mu.Unlock()
	moved.mu.Lock()
	defer moved.mu.Unlock()
	if !moved.floatingTransferableLocked() {
		return nil, errMoveFloatingWarming
	}
	if moved.tree == nil || moved.tree.Root == nil {
		return nil, errMovePaneInvalid
	}
	if _, ok := layout.Solve(moved.tree.Root, domain.Rect{Width: destinationSize.Cols, Height: destinationSize.Rows}); !ok {
		return nil, errMoveTooSmall
	}
	panes := make([]*pane, 0, len(moved.panes)+1)
	for _, p := range moved.panes {
		panes = append(panes, p)
	}
	if moved.floating.pane != nil && (moved.floating.state == floatingHidden || moved.floating.state == floatingVisible) {
		panes = append(panes, moved.floating.pane)
	}
	idx := indexMoveTabLocked(source, moved)
	attachments := source.snapshotAttachmentsLocked()
	transports := make(map[*attachedClient]transportSnapshot, len(attachments))
	for _, ac := range attachments {
		transports[ac] = ac.transportSnapshot()
	}
	return &moveTabAdmission{
		tab: moved, sourceIndex: idx, sourceAttachments: attachments, sourceTransports: transports,
		layoutGeneration: moved.layoutGeneration, floatingState: moved.floating.state,
		floatingPane: moved.floating.pane, floatingGeneration: moved.floating.generation,
		panes: panes, destinationActive: destinationActive, destinationSize: destinationSize,
		finalSource: len(source.tabs) == 1,
	}, nil
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
	if c.req.Attachment != nil && !attachmentRegisteredLocked(c.source, c.req.Attachment) {
		return false
	}
	if c.req.AttachmentToken.ac != nil && (c.req.AttachmentToken.ac != c.req.Attachment || !moveAttachmentTokenCurrentLocked(c.req.AttachmentToken, c.source)) {
		return false
	}
	moved := c.admission.tab
	if indexMoveTabLocked(c.source, moved) != c.admission.sourceIndex || findMoveTabLocked(c.destination, c.req.SourceTabID) != nil ||
		len(c.source.tabs) == 1 != c.admission.finalSource {
		return false
	}
	if c.admission.finalSource && !sameMoveAttachmentsLocked(c.source, c.admission.sourceAttachments) {
		return false
	}
	if len(c.destination.tabs) == 0 || c.destination.tabs[0] != c.admission.destinationActive {
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
	if len(c.source.tabs) > 1 {
		c.source.prepareAttachmentViewsForRemovedTabLocked(moved, idx)
	}
	c.source.tabs = append(c.source.tabs[:idx], c.source.tabs[idx+1:]...)
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
	for _, change := range ownerChanges {
		c.syncCleanup.append(d.migratePaneSyncOwnerLocked(change.pane, change.oldOwner, change.newOwner))
	}
	if len(c.source.tabs) == 0 {
		d.unregisterSessionLocked(c.source)
		c.retiredAttachments = detachMoveAttachmentsLocked(c.source, c.admission.sourceTransports)
		c.source.tabs = nil
		d.purgeParkingForSessionLocked(c.source)
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
