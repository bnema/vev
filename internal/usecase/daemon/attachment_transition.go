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
	sourceCapability      *attachmentCapability
	sourceEffect          *attachmentEffect
	action                string
	activateTargetTab     bool
	targetTabIndex        int
	copySourceEnvironment bool
	// preserveAttachment commits Attachment-local navigation state without
	// changing Session membership. The initiating capability remains the exact
	// authority for this mutation.
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

type attachmentTransitionResult struct {
	published             attachmentCapability
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
	if req.sourceCapability != nil && !req.sourceCapability.matchesConnectionSnapshot(source, req.next, req.expectedTransport) {
		return attachmentTransitionRequest{}, attachmentTransitionParticipants{}, errAttachmentTransition
	}
	req.preflighted = true
	return req, attachmentTransitionParticipants{clients: []*attachedClient{req.next}}, nil
}

// freezeAttachmentTransition drains the snapshotted connection without any
// architecture lock held. The returned guard remains frozen through publication.
func (d *Daemon) freezeAttachmentTransition(req attachmentTransitionRequest, participants attachmentTransitionParticipants) (attachmentTransitionGuard, error) {
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
// optional target, and performs exact capability publication under d.mu.
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
	if req.sourceEffect != nil {
		d.endActionAttachmentEffect(req.sourceEffect, req.action)
	}

	frozen, err := d.freezeAttachmentTransition(req, participants)
	if err != nil {
		frozen.unfreeze()
		return attachmentTransitionResult{}, err
	}

	result, err := d.publishAttachmentTransition(req)
	frozen.unfreeze()
	if err != nil {
		return result, err
	}
	// A not-ready publication is still inside the Hello handshake. Welcome
	// carries the committed identity for those attachments and must remain the
	// first server frame. Ready transitions notify an already-running client.
	if req.ready && result.published.ac != nil && result.published.ac.routeSnapshotCopy().Generation != 0 {
		identityErr := errAttachmentTransition
		if effect, admitted := result.published.ac.beginAttachmentEffect(result.published); admitted {
			identityErr = d.sendCommittedRouteIdentityForAttachment(effect)
			effect.End()
		}
		if identityErr != nil {
			d.abortPublishedAttachmentTransition(result)
			return attachmentTransitionResult{}, identityErr
		}
	}
	if result.sourceGeometrySession != nil {
		result.sourceGeometrySession.geometry.reconcileAndInvalidateAsync(d, result.sourceGeometrySession, "attachment_transition.go")
	}
	return result, nil
}

// abortPublishedAttachmentTransition tears down the newly published link before
// a post-publication control failure escapes to callers. The transition result is
// deliberately not handed back because its target membership is no longer valid.
func (d *Daemon) abortPublishedAttachmentTransition(result attachmentTransitionResult) {
	if result.published.ac != nil && result.published.transport.transport != nil {
		d.clientGoneWithoutNotice(result.published.sess, result.published.ac, result.published.transport.transport, true)
	}
	d.deferAttachmentTransitionCleanups(result)
}

func (d *Daemon) deferAttachmentTransitionCleanups(result attachmentTransitionResult) {
	for _, cleanup := range result.cleanups {
		d.attachmentCleanupWg.Go(cleanup.finish)
	}
}
