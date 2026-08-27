package daemon

import (
	"context"

	"github.com/bnema/vev/internal/domain"
)

// moveTopology is the private policy Interface consumed by moveTransaction.
// Admission runs under daemon and ordered Session locks and must be read-only.
// Publication runs under daemon, routing, ordered Session, and resize fences;
// it must finish every fallible check before its first write.
type moveTopology interface {
	transactionRequest() moveTransactionRequest
	validRequest() bool
	admitLocked(*Daemon, *session, *session) error
	afterAdmission(*Daemon)
	willRetireSource() bool
	resizeFences(*session, *session) *moveResizeFences
	publishLocked(*Daemon, *session, *session) (moveTopologyPublication, error)
}

type moveTransactionRequest struct {
	operation            string
	attachment           *attachedClient
	attachmentCapability attachmentCapability
	source               moveSessionLocator
	destination          moveSessionLocator
	logAttrs             []any
}

// moveTopologyPublication contains immutable topology outputs needed after the
// in-memory publication point. No field grants authority to perform additional
// topology writes.
type moveTopologyPublication struct {
	sourceTab, destinationTab *tab
	movedPane                 *pane
	movedPanes                []*pane
	syncCleanup               syncTimerCleanup
	oldTabCancel              context.CancelFunc
	sourceTabRemoved          bool
	sourceEmpty               bool
}

// moveTransaction owns the shared pane/tab Move phase protocol. Topology and
// owner policy remain private Implementations behind moveTopology.
type moveTransaction struct {
	daemon   *Daemon
	topology moveTopology
	request  moveTransactionRequest

	source      *session
	destination *session

	sourceAttachments []*attachedClient
	sourceTransports  map[*attachedClient]transportSnapshot
	frozenEffects     attachmentTransitionGuard
	effectsFrozen     bool

	publication moveTopologyPublication
	publishErr  error

	retiredParked      []parkedAttachmentRetirement
	retiredAttachments []detachedAttachmentSnapshot
	sourceMetadata     domain.CatalogueMetadataUpdate
	sourceMetadataOK   bool
	destMetadata       domain.CatalogueMetadataUpdate
	destMetadataOK     bool
	sourceName         string
	destinationName    string
}

func (d *Daemon) executeMove(topology moveTopology) (result error) {
	if d == nil || topology == nil {
		return errMovePaneInvalid
	}
	t := moveTransaction{daemon: d, topology: topology, request: topology.transactionRequest()}
	d.log.Info(t.request.operation+" move requested", t.request.logAttrs...)
	defer func() {
		result = normalizeMoveRejection(result)
		if result != nil {
			attrs := make([]any, 0, len(t.request.logAttrs)+2)
			attrs = append(attrs, "err", result)
			attrs = append(attrs, t.request.logAttrs...)
			d.log.Warn(t.request.operation+" move rejected", attrs...)
		}
	}()
	if !t.validRequest() {
		return errMovePaneInvalid
	}
	if err := t.resolveSessions(); err != nil {
		return err
	}

	if d.beforeMoveDispatch != nil {
		d.beforeMoveDispatch()
	}
	unlockDispatch := lockMoveDispatch(t.source, t.destination)
	dispatchHeld := true
	defer func() {
		if dispatchHeld {
			unlockDispatch()
		}
	}()

	reservation, err := d.reserveMoveLifecycles(t.source, t.destination)
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

	if err := t.admit(); err != nil {
		return err
	}
	t.topology.afterAdmission(d)
	if err := t.freezeSourceAttachments(); err != nil {
		return err
	}
	defer func() {
		if t.effectsFrozen {
			t.frozenEffects.unfreeze()
		}
	}()

	fences := t.topology.resizeFences(t.source, t.destination)
	if fences == nil || !fences.acquire(func() bool {
		t.publishErr = t.publishLocked()
		return t.publishErr == nil
	}) {
		if t.publishErr != nil {
			return t.publishErr
		}
		return errMoveStaleTarget
	}
	fences.Release()

	postcommit := t.postcommitPlan(unlockDispatch, reservation)
	t.effectsFrozen = false
	dispatchHeld = false
	reservationHeld = false
	postcommit.execute(d)
	return nil
}

func (t *moveTransaction) validRequest() bool {
	return t != nil && t.topology != nil && t.request.operation != "" &&
		t.request.source.ID != "" && t.request.destination.ID != "" && t.topology.validRequest()
}

func (t *moveTransaction) resolveSessions() error {
	d := t.daemon
	d.mu.Lock()
	t.source = moveSessionForLocatorLocked(d, t.request.source)
	t.destination = moveSessionForLocatorLocked(d, t.request.destination)
	d.mu.Unlock()
	if t.source == nil || t.destination == nil {
		return errMoveStaleTarget
	}
	return nil
}

func (t *moveTransaction) attachmentAuthorityCurrentLocked() bool {
	req := t.request
	if req.attachment != nil && !attachmentRegisteredLocked(t.source, req.attachment) {
		return false
	}
	if req.attachmentCapability.ac != nil &&
		(req.attachmentCapability.ac != req.attachment || !req.attachmentCapability.currentInSessionLocked(t.source)) {
		return false
	}
	return true
}

// admit snapshots exact common Attachment participants while the topology
// Implementation snapshots only its private identity and layout state.
func (t *moveTransaction) admit() error {
	d := t.daemon
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.sessions[t.source.id] != t.source || d.sessions[t.destination.id] != t.destination {
		return errMoveStaleTarget
	}
	unlockSessions := lockAttachmentSessions(t.source, t.destination)
	defer unlockSessions()
	if !moveSessionLocatorCurrentLocked(t.source, t.request.source) ||
		!moveSessionLocatorCurrentLocked(t.destination, t.request.destination) ||
		!t.attachmentAuthorityCurrentLocked() {
		return errMoveStaleTarget
	}
	if err := t.topology.admitLocked(d, t.source, t.destination); err != nil {
		return err
	}
	t.sourceAttachments = t.source.snapshotAttachmentsLocked()
	t.sourceTransports = make(map[*attachedClient]transportSnapshot, len(t.sourceAttachments))
	for _, attachment := range t.sourceAttachments {
		t.sourceTransports[attachment] = attachment.transportSnapshot()
	}
	return nil
}

// freezeSourceAttachments delegates effect admission and drain authority to the
// attachment lifecycle Module. Move uses nonblocking acquisition to preserve
// its ordering with final teardown.
func (t *moveTransaction) freezeSourceAttachments() error {
	if !t.topology.willRetireSource() || len(t.sourceAttachments) == 0 {
		return nil
	}
	interrupts := make([]attachmentTransportInterrupt, 0, len(t.sourceAttachments))
	for _, attachment := range t.sourceAttachments {
		if transport := t.sourceTransports[attachment]; transport.transport != nil {
			interrupts = append(interrupts, attachmentTransportInterrupt{ac: attachment, transport: transport})
		}
	}
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{
		interrupts:  interrupts,
		nonblocking: true,
		afterFrozen: func(attachment *attachedClient) {
			if t.daemon.afterAttachmentEffectGateFrozen != nil {
				t.daemon.afterAttachmentEffectGateFrozen("move-"+t.request.operation, attachment)
			}
		},
	}, t.sourceAttachments...)
	if !frozen.acquired || !frozen.drained {
		frozen.unfreeze()
		return errMovePaneInvalid
	}
	t.frozenEffects = frozen
	t.effectsFrozen = true
	return nil
}

// publishLocked is the only shared architecture-lock window. Topology
// Implementations perform all fallible private validation before their first
// write. Common retirement and metadata capture are non-failing in-memory work.
func (t *moveTransaction) publishLocked() error {
	d := t.daemon
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || d.sessions[t.source.id] != t.source || d.sessions[t.destination.id] != t.destination {
		return errMoveStaleTarget
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	unlockSessions := lockAttachmentSessions(t.source, t.destination)
	defer unlockSessions()
	if !moveSessionLocatorCurrentLocked(t.source, t.request.source) ||
		!moveSessionLocatorCurrentLocked(t.destination, t.request.destination) ||
		!t.attachmentAuthorityCurrentLocked() {
		return errMoveStaleTarget
	}
	if t.topology.willRetireSource() && !sameMoveAttachmentsLocked(t.source, t.sourceAttachments) {
		return errMoveStaleTarget
	}

	publication, err := t.topology.publishLocked(d, t.source, t.destination)
	if err != nil {
		return err
	}
	t.publication = publication
	if publication.sourceEmpty {
		d.unregisterSessionLocked(t.source)
		t.retiredAttachments = detachMoveAttachmentsLocked(t.source, t.sourceTransports)
		t.source.tabs = nil
		d.purgeParkingForSessionLocked(t.source)
		t.retiredParked = d.purgeParkedForSessionLocked(t.source)
	}
	t.sourceName = t.source.name
	t.destinationName = t.destination.name
	if !t.source.ephemeral {
		t.sourceMetadata = t.source.persistRecordLocked(max(d.nowUnixNano(), t.source.createdAt, int64(1))).MetadataUpdate()
		t.sourceMetadataOK = true
	}
	if !t.destination.ephemeral && t.destination != t.source {
		t.destMetadata = t.destination.persistRecordLocked(max(d.nowUnixNano(), t.destination.createdAt, int64(1))).MetadataUpdate()
		t.destMetadataOK = true
	}
	return nil
}

func (t *moveTransaction) postcommitPlan(unlockDispatch func(), reservation *moveLifecycleReservation) movePostcommitPlan {
	publication := t.publication
	return movePostcommitPlan{
		source:                   t.source,
		destination:              t.destination,
		sourceName:               t.sourceName,
		destinationName:          t.destinationName,
		sourceTab:                publication.sourceTab,
		destinationTab:           publication.destinationTab,
		movedPane:                publication.movedPane,
		movedPanes:               publication.movedPanes,
		operation:                t.request.operation,
		sourceAttachments:        t.sourceAttachments,
		syncCleanup:              publication.syncCleanup,
		frozenEffects:            t.frozenEffects,
		effectsFrozen:            t.effectsFrozen,
		unlockDispatch:           unlockDispatch,
		reservation:              reservation,
		oldTabCancel:             publication.oldTabCancel,
		sourceTabRemoved:         publication.sourceTabRemoved,
		sourceEmpty:              publication.sourceEmpty,
		retiredParked:            t.retiredParked,
		retiredAttachments:       t.retiredAttachments,
		sourceMetadata:           t.sourceMetadata,
		sourceMetadataValid:      t.sourceMetadataOK,
		destinationMetadata:      t.destMetadata,
		destinationMetadataValid: t.destMetadataOK,
	}
}
