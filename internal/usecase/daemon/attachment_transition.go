package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
)

var errAttachmentTransition = errors.New("attachment transition is no longer valid")

type attachmentLifecycleFence struct {
	name             string
	createdAt        int64
	checkCreatedAt   bool
	incarnation      domain.IncarnationID
	checkIncarnation bool
	tabID            domain.TabStableID
	tabIndex         int
	checkTab         bool
}

type attachmentTransitionRequest struct {
	source                *session
	target                *session
	next                  *attachedClient
	expectedTransport     transportSnapshot
	sourceToken           *attachmentConnectionToken
	action                string
	activateTargetTab     bool
	targetTabIndex        int
	copySourceEnvironment bool
	// preserveAttachment commits attachment-local navigation state without
	// changing session membership. The initiating connection token remains the
	// exact authority for this mutation.
	preserveAttachment bool
	expectedSourceTab  *tab
	// transferExpectedSourceTab permits an installed visible floating pane only
	// when the caller atomically transfers this exact tab under its fences.
	transferExpectedSourceTab bool
	createTargetLocked        func() (*session, error)
	ready                     bool
	expectedTargetLifecycle   *attachmentLifecycleFence
	preflighted               bool
	attachmentEffectsFrozen   bool
}

func transitionSourceTokenMatchesRequest(token attachmentConnectionToken, source *session, req attachmentTransitionRequest) bool {
	return source != nil && token.sess != nil && token.sess == source && token.ac == req.next &&
		token.generation == req.next.connectionGeneration.Load() &&
		token.transport.transport == req.expectedTransport.transport &&
		token.transport.incarnation == req.expectedTransport.incarnation
}

// transitionSourceTokenCurrentLocked requires source.core().mu and, when a
// lease exists, sourceCoordinator.mu. It is the canonical exact-connection
// handoff check for client-originated navigation and lifecycle mutations.
func transitionSourceTokenCurrentLocked(token attachmentConnectionToken, source *session, sourceCoordinator *renderCoordinator, req attachmentTransitionRequest) bool {
	if token.sess != source || token.ac != req.next ||
		token.generation != req.next.connectionGeneration.Load() ||
		token.sess == nil || token.ac.currentAttachmentSession() != source ||
		!attachmentRegisteredLocked(source, req.next) ||
		!req.next.transportSnapshotCurrent(token.transport) {
		return false
	}
	if token.lease == nil {
		return true
	}
	return sourceCoordinator != nil && token.lease.attachment == req.next &&
		sourceCoordinator.leaseCurrentLocked(token.lease, true)
}

type attachmentTransitionResult struct {
	published             attachmentConnectionToken
	cleanups              []renderLifecycleCleanup
	sourceGeometrySession *session
}

type attachmentTransitionParticipants struct {
	clients    []*attachedClient
	interrupts []attachmentTransportInterrupt
}

// snapshotAttachmentTransition discovers the exact connection participant
// while d.mu is held, then releases architecture locks before effect gates are
// frozen and drained. Existing target attachments are never displaced.
func (d *Daemon) snapshotAttachmentTransition(req attachmentTransitionRequest) (attachmentTransitionRequest, attachmentTransitionParticipants, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	source := req.source
	if source == nil {
		source = req.target
	}
	if source == nil || (req.target == nil && req.createTargetLocked == nil) {
		return attachmentTransitionRequest{}, attachmentTransitionParticipants{}, errAttachmentTransition
	}
	sourceCore := source.core()
	var targetCore *sessionCore
	if req.target != nil {
		targetCore = req.target.core()
	}
	if sourceCore == nil || req.target != nil && targetCore == nil || req.next == nil || d.closing ||
		d.sessions[sourceCore.id] != source || req.target != nil && d.sessions[targetCore.id] != req.target {
		return attachmentTransitionRequest{}, attachmentTransitionParticipants{}, errAttachmentTransition
	}
	if req.sourceToken != nil && !transitionSourceTokenMatchesRequest(*req.sourceToken, source, req) {
		return attachmentTransitionRequest{}, attachmentTransitionParticipants{}, errAttachmentTransition
	}
	req.preflighted = true
	return req, attachmentTransitionParticipants{clients: []*attachedClient{req.next}}, nil
}

// freezeAttachmentTransition drains the snapshotted connection without any
// architecture lock held. The returned guard remains frozen through publication.
func (d *Daemon) freezeAttachmentTransition(req attachmentTransitionRequest, participants attachmentTransitionParticipants) (frozenAttachmentEffectGates, error) {
	if d.afterAttachmentEffectParticipantsSnapshotted != nil {
		d.afterAttachmentEffectParticipantsSnapshotted(req.action, participants.clients)
	}
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{interrupts: participants.interrupts, afterFrozen: func(ac *attachedClient) {
		if d.afterAttachmentEffectGateFrozen != nil {
			d.afterAttachmentEffectGateFrozen(req.action, ac)
		}
	}}, participants.clients...)
	if !frozen.acquired || !frozen.drained {
		return frozen, errAttachmentTransition
	}
	if d.afterAttachmentEffectsFrozen != nil {
		d.afterAttachmentEffectsFrozen()
	}
	return frozen, nil
}

// publishAttachmentTransition revalidates the frozen source, creates an
// optional target, and performs exact membership publication under d.mu.
func (d *Daemon) publishAttachmentTransition(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	req.attachmentEffectsFrozen = true
	if req.target == nil {
		if !d.transitionSourcePreflightLocked(req) {
			return attachmentTransitionResult{}, errAttachmentTransition
		}
		var err error
		req.target, err = req.createTargetLocked()
		if err != nil {
			return attachmentTransitionResult{}, err
		}
	}
	result, err := d.transitionAttachmentLocked(req)
	if err != nil {
		return result, err
	}
	if req.source != nil && req.target != nil && req.source != req.target {
		result.sourceGeometrySession = req.source
	}
	return result, nil
}

func (d *Daemon) transitionAttachment(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	req, participants, err := d.snapshotAttachmentTransition(req)
	if err != nil {
		return attachmentTransitionResult{}, err
	}
	if req.sourceToken != nil {
		d.endActionAttachmentEffect(req.sourceToken.effect, req.action)
	}

	frozen, err := d.freezeAttachmentTransition(req, participants)
	defer frozen.unfreeze()
	if err != nil {
		return attachmentTransitionResult{}, err
	}

	result, err := d.publishAttachmentTransition(req)
	if err != nil {
		return result, err
	}
	if result.sourceGeometrySession != nil {
		d.recalculateSessionGeometryAndInvalidateAsync(result.sourceGeometrySession, "attachment_transition.go")
	}
	return result, nil
}

func (d *Daemon) deferAttachmentTransitionCleanups(result attachmentTransitionResult) {
	for _, cleanup := range result.cleanups {
		d.attachmentCleanupWg.Go(cleanup.finish)
	}
}
