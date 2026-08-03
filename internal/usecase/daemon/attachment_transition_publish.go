package daemon

import (
	"slices"

	"github.com/bnema/vev/internal/domain"
)

// transitionSourcePreflightLocked validates an initiating role while its gate
// is frozen and d.mu is held. No caller may create or delete a target before
// this succeeds.
func (d *Daemon) transitionSourcePreflightLocked(req attachmentTransitionRequest) bool {
	if req.sourceToken == nil || req.source == nil || req.source.core() == nil || d.sessions[req.source.core().id] != req.source {
		return false
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	req.source.core().mu.Lock()
	defer req.source.core().mu.Unlock()
	coordinator := req.source.core().coordinator.Load()
	if coordinator == nil {
		return false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if !transitionSourceTokenCurrentLocked(*req.sourceToken, req.source, coordinator, req) {
		return false
	}
	return transitionSourceTabCurrentLocked(req.source, req.expectedSourceTab)
}

func (d *Daemon) sourceRoleTokenCurrentFrozen(token attachmentRoleToken) bool {
	if token.sess == nil || token.ac == nil || token.role != attachmentActive {
		return false
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closing || token.sess.core() == nil || d.sessions[token.sess.core().id] != token.sess {
		return false
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	token.sess.core().mu.Lock()
	defer token.sess.core().mu.Unlock()
	coordinator := token.sess.core().coordinator.Load()
	if coordinator == nil {
		return false
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	req := attachmentTransitionRequest{
		source: token.sess, next: token.ac, expectedRole: token.role,
		expectedTransport: token.transport,
	}
	return transitionSourceTokenCurrentLocked(token, token.sess, coordinator, req)
}

func transitionSourceTabCurrentLocked(source attachmentSession, expected *tab) bool {
	return transitionSourceTabCurrentForRequestLocked(source, expected, false)
}

func transitionSourceTabCurrentForRequestLocked(source attachmentSession, expected *tab, transferExpected bool) bool {
	if expected == nil {
		return true
	}
	local, ok := localSession(source)
	if !ok || !slices.Contains(local.tabs, expected) {
		return false
	}
	expected.mu.Lock()
	defer expected.mu.Unlock()
	return transferExpected || expected.floating.state != floatingVisible
}

// transitionAttachmentLocked is the publication half of transitionAttachment.
// Caller holds d.mu after the lock-free gate freeze/drain has completed.
func (d *Daemon) transitionAttachmentLocked(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	return d.transitionAttachmentRoutedLocked(req)
}

// attachmentPublication is the validated, coordinator-locked input to the
// non-failing ownership and lease publication step. Its caller continues to
// own d.mu, notices.routingMu, and the ordered source/target session locks.
type attachmentPublication struct {
	req                 attachmentTransitionRequest
	source              attachmentSession
	old                 *attachedClient
	sourceCoordinator   *renderCoordinator
	targetCoordinator   *renderCoordinator
	releaseCoordinators func()
	nextGeneration      uint64
}

func (p *attachmentPublication) unlockCoordinators() {
	p.releaseCoordinators()
}

func attachmentLifecycleCurrentLocked(entry attachmentSession, fence *attachmentLifecycleFence) bool {
	if fence == nil {
		return true
	}
	target, ok := localSession(entry)
	if !ok {
		return false
	}
	if fence.checkCreatedAt && (target.name != fence.name || target.createdAt != fence.createdAt) {
		return false
	}
	if fence.checkIncarnation && target.incarnation != fence.incarnation {
		return false
	}
	return !fence.checkTab || fence.tabIndex >= 0 && fence.tabIndex < len(target.tabs) &&
		domain.TabStableID(target.tabs[fence.tabIndex].stableID) == fence.tabID
}

// validateAttachmentTransitionPrelocked performs every fallible transition
// check without acquiring d.mu, notices.routingMu, or either session lock.
// The caller holds those locks in that order, with session locks ordered by ID.
// On success the returned validation retains the ordered coordinator locks
// through publishAttachmentTransitionPrelocked.
func (d *Daemon) validateAttachmentTransitionPrelocked(req attachmentTransitionRequest) (*attachmentPublication, error) {
	source := req.source
	if source == nil {
		source = req.target
	}
	if source == nil || req.target == nil {
		return nil, errAttachmentTransition
	}
	sourceCore := source.core()
	targetCore := req.target.core()
	if sourceCore == nil || targetCore == nil ||
		!req.preflighted || !req.roleEffectsFrozen ||
		!validAttachmentTransitionRole(req.expectedRole, true) || !validAttachmentTransitionRole(req.targetRole, false) ||
		req.next == nil || d.closing ||
		d.sessions[sourceCore.id] != source || d.sessions[targetCore.id] != req.target ||
		req.expectedTransport.transport == nil || !req.next.transportSnapshotCurrent(req.expectedTransport) ||
		req.preserveRole && req.sourceToken == nil {
		return nil, errAttachmentTransition
	}

	var targetCoordinator *renderCoordinator
	if req.targetRole == attachmentActive {
		targetCoordinator = d.ensureAttachmentRenderCoordinatorPrelocked(req.target)
	}
	sourceCoordinator := sourceCore.coordinator.Load()
	if attachmentSessionRoleLocked(source, req.next) != req.expectedRole ||
		!req.next.transportSnapshotCurrent(req.expectedTransport) {
		return nil, errAttachmentTransition
	}

	old := (*attachedClient)(nil)
	if req.expectedTargetCurrent != nil {
		return nil, errAttachmentTransition
	}
	publication := &attachmentPublication{
		req: req, source: source, old: old,
		sourceCoordinator: sourceCoordinator, targetCoordinator: targetCoordinator,
		releaseCoordinators: lockAttachmentCoordinators(source, sourceCoordinator, req.target, targetCoordinator),
	}
	invalid := req.sourceToken != nil && (!transitionSourceTokenCurrentLocked(*req.sourceToken, source, sourceCoordinator, req) ||
		!transitionSourceTabCurrentForRequestLocked(source, req.expectedSourceTab, req.transferExpectedSourceTab))
	invalid = invalid || targetCoordinator != nil && targetCoordinator.lease == nil && !req.preserveRole && !targetCoordinator.canReplaceLocked(old, req.next)
	invalid = invalid || req.sourceToken == nil && source != req.target && req.expectedRole == attachmentActive && sourceCoordinator != nil &&
		(sourceCoordinator.lease == nil || !sourceCoordinator.lease.active || sourceCoordinator.lease.attachment != req.next)
	invalid = invalid || !attachmentLifecycleCurrentLocked(req.target, req.expectedTargetLifecycle)
	invalid = invalid || req.activateTargetTab && !req.target.activateTargetLocked(req.targetTabIndex)
	if invalid {
		publication.unlockCoordinators()
		return nil, errAttachmentTransition
	}
	return publication, nil
}

// applyTargetStateLocked commits navigation metadata before role ownership, as
// part of the same locked publication. It reports role-preserving transitions.
func (d *Daemon) applyTargetStateLocked(publication *attachmentPublication) bool {
	req := publication.req
	if req.activateTargetTab {
		if target, ok := localSession(req.target); ok {
			target.activateAttachmentViewLocked(req.next, req.targetTabIndex)
		}
	}
	if req.copySourceEnvironment {
		source, sourceOK := localSession(publication.source)
		target, targetOK := localSession(req.target)
		if sourceOK && targetOK {
			target.terminal = source.terminal
			target.env = copyEnvironment(source.env)
		}
	}
	return req.preserveRole
}

// publishAttachmentOwnershipLocked atomically changes session membership,
// generations, and currentSession while all role gates remain frozen.
func publishAttachmentOwnershipLocked(publication *attachmentPublication) {
	req := publication.req
	// Session membership is the sole routing publication. Existing attachments
	// remain registered and retain their independent views and transports.
	registerAttachmentSessionLocked(req.target, req.next)
	if publication.source != req.target {
		unregisterAttachmentSessionLocked(publication.source, req.next)
	}
	publication.nextGeneration = req.next.roleGeneration.Add(1)
	req.next.setSession(req.target)
}

// buildAttachmentPostcommitPlanLocked binds coordinator lifecycle cleanup and
// constructs the exact published tokens after ownership has committed.
func buildAttachmentPostcommitPlanLocked(publication *attachmentPublication) attachmentTransitionResult {
	req := publication.req
	result := attachmentTransitionResult{}
	if publication.source != req.target && req.expectedRole == attachmentActive && publication.sourceCoordinator != nil {
		result.cleanups = append(result.cleanups, publication.sourceCoordinator.beginDetachLocked(req.next))
	}
	var lease *attachmentLease
	if publication.targetCoordinator != nil {
		var cleanup renderLifecycleCleanup
		if publication.targetCoordinator.lease == nil {
			cleanup, lease = publication.targetCoordinator.beginReplaceLocked(nil, req.next, req.ready)
			result.cleanups = append(result.cleanups, cleanup)
		} else {
			// Secondary attachments use membership admission; the coordinator's
			// optional primary lease is not reused by another attachment.
			lease = nil
		}
	}
	result.published = attachmentRoleToken{
		sess:       req.target,
		ac:         req.next,
		role:       req.targetRole,
		generation: publication.nextGeneration,
		transport:  req.expectedTransport,
		lease:      lease,
		rebase:     publication.source != req.target && req.expectedRole == attachmentActive,
	}

	// Membership, currentSession, lease, generation, and both exact admission
	// capabilities become visible as one frozen publication. The gates remain
	// frozen until every architecture lock has been released.
	if req.roleEffectsFrozen {
		req.next.publishFrozenRoleCapability(result.published)
		if result.displaced.ac != nil {
			result.displaced.ac.publishFrozenRoleCapability(result.displaced)
		}
	}
	return result
}

// publishAttachmentTransitionPrelocked performs only non-failing metadata,
// ownership, generation, and coordinator-lease publication. The caller retains
// every lock required by validateAttachmentTransitionPrelocked.
func (d *Daemon) publishAttachmentTransitionPrelocked(publication *attachmentPublication) attachmentTransitionResult {
	if publication.req.preserveRole {
		d.applyTargetStateLocked(publication)
		return attachmentTransitionResult{published: *publication.req.sourceToken}
	}
	publishAttachmentOwnershipLocked(publication)
	d.applyTargetStateLocked(publication)
	return buildAttachmentPostcommitPlanLocked(publication)
}

// transitionAttachmentRoutedLocked is the ordinary lock-acquiring wrapper for
// the composable prelocked validation/publication seam. Caller holds d.mu and
// notices.routingMu; this function acquires ordered source/target session locks.
func (d *Daemon) transitionAttachmentRoutedLocked(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	source := req.source
	if source == nil {
		source = req.target
	}
	if source == nil || req.target == nil {
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	unlockSessions := lockAttachmentSessions(source, req.target)
	defer unlockSessions()

	publication, err := d.validateAttachmentTransitionPrelocked(req)
	if err != nil {
		return attachmentTransitionResult{}, err
	}
	defer publication.unlockCoordinators()
	return d.publishAttachmentTransitionPrelocked(publication), nil
}

func validAttachmentTransitionRole(role attachmentRole, expected bool) bool {
	if role == attachmentActive || role == attachmentSnatched {
		return true
	}
	return expected && role == attachmentDetached
}

// lockAttachmentSessions gives every two-session transition one stable order.
// Session IDs are immutable and unique while their lifecycles are registered.
func lockAttachmentSessions(a, b attachmentSession) func() {
	aCore := attachmentSessionCore(a)
	bCore := attachmentSessionCore(b)
	if aCore == nil || bCore == nil {
		return func() {}
	}
	if aCore == bCore {
		aCore.mu.Lock()
		return aCore.mu.Unlock
	}
	first, second := aCore, bCore
	if first.id > second.id {
		first, second = second, first
	}
	first.mu.Lock()
	second.mu.Lock()
	return func() {
		second.mu.Unlock()
		first.mu.Unlock()
	}
}

// lockAttachmentCoordinators follows the same immutable session-ID order as
// lockAttachmentSessions. Callers already hold the ordered session locks.
func lockAttachmentCoordinators(a attachmentSession, aCoordinator *renderCoordinator, b attachmentSession, bCoordinator *renderCoordinator) func() {
	if aCoordinator == nil && bCoordinator == nil {
		return func() {}
	}
	if a == b || aCoordinator == bCoordinator {
		coordinator := aCoordinator
		if coordinator == nil {
			coordinator = bCoordinator
		}
		coordinator.mu.Lock()
		return coordinator.mu.Unlock
	}
	first, second := aCoordinator, bCoordinator
	if a.core().id > b.core().id {
		first, second = second, first
	}
	if first != nil {
		first.mu.Lock()
	}
	if second != nil {
		second.mu.Lock()
	}
	return func() {
		if second != nil {
			second.mu.Unlock()
		}
		if first != nil {
			first.mu.Unlock()
		}
	}
}
