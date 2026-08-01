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
	source                attachmentSession
	target                attachmentSession
	next                  *attachedClient
	expectedRole          attachmentRole
	targetRole            attachmentRole
	expectedTransport     transportSnapshot
	sourceToken           *attachmentRoleToken
	action                string
	activateTargetTab     bool
	targetTabIndex        int
	copySourceEnvironment bool
	// preserveRole commits navigation state without replacing attachment
	// ownership. It is used for same-session picker selections after the exact
	// initiating capability has been frozen and revalidated.
	preserveRole      bool
	expectedSourceTab *tab
	// transferExpectedSourceTab permits an installed visible floating pane only
	// when the caller atomically transfers this exact tab under its own tab and
	// floating-generation fences. Ordinary navigation still rejects visibility.
	transferExpectedSourceTab          bool
	createTargetLocked                 func() (*session, error)
	ready                              bool
	expectedTargetCurrent              *attachedClient
	expectedTargetTransport            transportSnapshot
	expectedTargetLifecycle            *attachmentLifecycleFence
	preflighted                        bool
	roleEffectsFrozen                  bool
	expectedTargetTransportInterrupted bool
	// activationBarrier is used when promoting an already connected snatched
	// attachment. Its role gate is deadline-drained before sendMu is acquired,
	// preserving the gate -> sendMu -> daemon/routing/session publication order.
	activationBarrier bool
}

func transitionSourceTokenMatchesRequest(token attachmentRoleToken, source attachmentSession, req attachmentTransitionRequest) bool {
	return token.sess == source && token.ac == req.next && token.role == req.expectedRole &&
		token.transport.transport == req.expectedTransport.transport &&
		token.transport.incarnation == req.expectedTransport.incarnation
}

// transitionSourceTokenCurrentLocked requires source.mu and, for an active
// source, sourceCoordinator.mu. This is the canonical handoff preflight: every
// client-originated navigation, delete, or create intent is bound to one exact
// role generation, transport incarnation, and coordinator lease.
func transitionSourceTokenCurrentLocked(token attachmentRoleToken, source attachmentSession, sourceCoordinator *renderCoordinator, req attachmentTransitionRequest) bool {
	if token.sess != source || token.ac != req.next || token.role != req.expectedRole ||
		token.generation != req.next.roleGeneration.Load() || attachmentSessionRoleLocked(source, req.next) != token.role ||
		req.next.currentAttachmentSession() != source || !req.next.transportSnapshotCurrent(token.transport) {
		return false
	}
	if token.role != attachmentActive {
		return true
	}
	return sourceCoordinator != nil && token.lease != nil && token.lease.attachment == req.next &&
		sourceCoordinator.leaseCurrentLocked(token.lease, true)
}

type attachmentTransitionResult struct {
	published            attachmentRoleToken
	displaced            attachmentRoleToken
	terminalDisplaced    detachedAttachmentSnapshot
	displacedInterrupted bool
	cleanups             []renderLifecycleCleanup
}

type attachmentTransitionParticipants struct {
	clients    []*attachedClient
	interrupts []roleTransportInterrupt
}

// snapshotAttachmentTransition discovers the exact participants while d.mu is
// held. It releases every architecture lock before any role effect is ended or
// any gate is frozen and drained.
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
		d.sessions[sourceCore.id] != source || req.target != nil && d.sessions[targetCore.id] != req.target ||
		req.activationBarrier && (req.sourceToken == nil || source != req.target ||
			req.expectedRole != attachmentSnatched || req.targetRole != attachmentActive) {
		return attachmentTransitionRequest{}, attachmentTransitionParticipants{}, errAttachmentTransition
	}
	if req.target != nil {
		targetCore.mu.Lock()
		req.expectedTargetCurrent = targetCore.client
		if req.expectedTargetCurrent != nil && req.expectedTargetCurrent != req.next && req.targetRole == attachmentActive {
			req.expectedTargetTransport = req.expectedTargetCurrent.transportSnapshot()
		}
		targetCore.mu.Unlock()
	}
	if req.sourceToken != nil && !transitionSourceTokenMatchesRequest(*req.sourceToken, source, req) {
		return attachmentTransitionRequest{}, attachmentTransitionParticipants{}, errAttachmentTransition
	}
	req.preflighted = true
	return req, attachmentTransitionParticipants{
		clients: []*attachedClient{req.next, req.expectedTargetCurrent},
		interrupts: []roleTransportInterrupt{{
			ac: req.expectedTargetCurrent, transport: req.expectedTargetTransport,
		}},
	}, nil
}

// freezeAttachmentTransition drains the snapshotted participants without any
// architecture lock held. The returned guard remains frozen through atomic
// publication and must be released by the caller.
func (d *Daemon) freezeAttachmentTransition(req attachmentTransitionRequest, participants attachmentTransitionParticipants) (frozenRoleEffectGates, bool, error) {
	if d.afterRoleEffectParticipantsSnapshotted != nil {
		d.afterRoleEffectParticipantsSnapshotted(req.action, participants.clients)
	}
	var drainDeadline *roleEffectDrainDeadline
	var drainDone func() <-chan struct{}
	if req.activationBarrier {
		drainDeadline = newRoleEffectDrainDeadline(d.clock)
		drainDone = drainDeadline.Done
	}
	frozen := freezeRoleEffectGatesWith(roleEffectFreezeOptions{interrupts: participants.interrupts, done: drainDone, afterFrozen: func(ac *attachedClient) {
		if d.afterRoleEffectGateFrozen != nil {
			d.afterRoleEffectGateFrozen(req.action, ac)
		}
	}}, participants.clients...)
	if drainDeadline != nil {
		drainDeadline.stop()
	}
	if !frozen.acquired || !frozen.drained {
		if req.activationBarrier {
			_ = req.next.closeCapturedTransport(req.expectedTransport.transport)
			return frozen, false, errSendTimedOut
		}
		return frozen, false, errAttachmentTransition
	}
	interrupted := frozen.interrupted(req.expectedTargetCurrent, req.expectedTargetTransport)
	if d.afterRoleEffectsFrozen != nil {
		d.afterRoleEffectsFrozen()
	}
	return frozen, interrupted, nil
}

// publishAttachmentTransition revalidates the frozen source, creates an
// optional target, and performs publication while d.mu remains held.
func (d *Daemon) publishAttachmentTransition(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	req.roleEffectsFrozen = true
	if req.target == nil {
		if !d.transitionSourcePreflightLocked(req) {
			return attachmentTransitionResult{}, errAttachmentTransition
		}
		var err error
		req.target, err = req.createTargetLocked()
		if err != nil {
			return attachmentTransitionResult{}, err
		}
		req.expectedTargetCurrent = nil
	}
	return d.transitionAttachmentLocked(req)
}

// prepareActivatedAttachment rebases the first active paint while the caller
// still owns the activation barrier (and therefore sendMu).
func (d *Daemon) prepareActivatedAttachment(result *attachmentTransitionResult) {
	ac := result.published.ac
	if ac.output != nil {
		ac.output.rebase()
	}
	ac.captureFrames = nil
	if sess, ok := localSession(result.published.sess); ok {
		d.applyHostThemeSendLocked(sess, ac, ac.getClientTheme(), false)
	}
	// The first paint still requests a reset, but the old panel dependency chain
	// has already been rebased atomically under the activation barrier.
	result.published.rebase = false
}

func (d *Daemon) transitionAttachment(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	req, participants, err := d.snapshotAttachmentTransition(req)
	if err != nil {
		return attachmentTransitionResult{}, err
	}
	if req.sourceToken != nil {
		d.endActionRoleEffect(req.sourceToken.effect, req.action)
	}

	frozen, interrupted, err := d.freezeAttachmentTransition(req, participants)
	defer frozen.unfreeze()
	if err != nil {
		return attachmentTransitionResult{}, err
	}
	req.expectedTargetTransportInterrupted = interrupted

	var releaseActivation func()
	if req.activationBarrier {
		releaseActivation, err = d.acquireActivationBarrier(*req.sourceToken)
		if err != nil {
			return attachmentTransitionResult{}, err
		}
		defer func() {
			if releaseActivation != nil {
				releaseActivation()
			}
		}()
	}

	result, err := d.publishAttachmentTransition(req)
	if err != nil {
		return result, err
	}
	if req.activationBarrier && result.published.role == attachmentActive {
		d.prepareActivatedAttachment(&result)
	}
	if releaseActivation != nil {
		releaseActivation()
		releaseActivation = nil
	}
	if result.published.role == attachmentActive {
		result.published.ac.clearSnatchedInput()
	}
	// Warm-proxy timers are external clock operations. Apply them only after
	// frozen ownership publication and every architecture lock have completed;
	// the lifecycle helper revalidates exact registry pointers and clients.
	d.proxyAttachmentTransitionCommitted(req.source, req.target, req.next, req.preserveRole)
	return result, nil
}
