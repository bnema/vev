package daemon

import "github.com/bnema/vev/internal/domain"

// transitionSourcePreflightLocked validates an initiating connection while its
// effect gate is frozen and d.mu is held. No target is created before this
// succeeds.
func (d *Daemon) transitionSourcePreflightLocked(req attachmentTransitionRequest) bool {
	if req.sourceToken == nil || req.source == nil || req.source.core() == nil || d.sessions[req.source.core().id] != req.source {
		return false
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	req.source.core().mu.Lock()
	defer req.source.core().mu.Unlock()
	coordinator := req.source.core().coordinator.Load()
	if coordinator != nil {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
	}
	if !transitionSourceTokenCurrentLocked(*req.sourceToken, req.source, coordinator, req) {
		return false
	}
	return transitionSourceTabCurrentLocked(req.source, req.expectedSourceTab)
}

func (d *Daemon) sourceAttachmentTokenCurrentFrozen(token attachmentConnectionToken) bool {
	if token.sess == nil || token.ac == nil {
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
	if coordinator != nil {
		coordinator.mu.Lock()
		defer coordinator.mu.Unlock()
	}
	req := attachmentTransitionRequest{source: token.sess, next: token.ac, expectedTransport: token.transport}
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
	if !ok || !containsTab(local.tabs, expected) {
		return false
	}
	expected.mu.Lock()
	defer expected.mu.Unlock()
	return transferExpected || expected.floating.state != floatingVisible
}

func containsTab(tabs []*tab, want *tab) bool {
	for _, tb := range tabs {
		if tb == want {
			return true
		}
	}
	return false
}

// transitionAttachmentLocked is the publication half of transitionAttachment.
// Caller holds d.mu after the lock-free effect freeze/drain.
func (d *Daemon) transitionAttachmentLocked(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	return d.transitionAttachmentRoutedLocked(req)
}

type attachmentPublication struct {
	req                 attachmentTransitionRequest
	source              attachmentSession
	targetCoordinator   *renderCoordinator
	sourceCoordinator   *renderCoordinator
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

// validateAttachmentTransitionPrelocked performs every fallible check before
// the non-failing membership and lease publication. Caller holds d.mu,
// notices.routingMu, and ordered source/target session locks.
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
	if sourceCore == nil || targetCore == nil || !req.preflighted || !req.attachmentEffectsFrozen ||
		req.next == nil || d.closing || d.sessions[sourceCore.id] != source || d.sessions[targetCore.id] != req.target ||
		req.expectedTransport.transport == nil || !req.next.transportSnapshotCurrent(req.expectedTransport) ||
		req.preserveAttachment && req.sourceToken == nil {
		return nil, errAttachmentTransition
	}

	sourceCoordinator := sourceCore.coordinator.Load()
	targetCoordinator := d.ensureAttachmentRenderCoordinatorPrelocked(req.target)
	if req.sourceToken != nil && !transitionSourceTokenCurrentLocked(*req.sourceToken, source, sourceCoordinator, req) {
		return nil, errAttachmentTransition
	}
	if source != req.target && !attachmentRegisteredLocked(source, req.next) {
		return nil, errAttachmentTransition
	}
	if !req.preserveAttachment && source != req.target && attachmentRegisteredLocked(req.target, req.next) {
		return nil, errAttachmentTransition
	}
	if req.preserveAttachment && !attachmentRegisteredLocked(req.target, req.next) {
		return nil, errAttachmentTransition
	}
	if !attachmentLifecycleCurrentLocked(req.target, req.expectedTargetLifecycle) ||
		req.activateTargetTab && !req.target.validTargetTabLocked(req.targetTabIndex) ||
		req.sourceToken != nil && !transitionSourceTabCurrentForRequestLocked(source, req.expectedSourceTab, req.transferExpectedSourceTab) {
		return nil, errAttachmentTransition
	}

	publication := &attachmentPublication{
		req: req, source: source,
		sourceCoordinator: sourceCoordinator, targetCoordinator: targetCoordinator,
		releaseCoordinators: lockAttachmentCoordinators(source, sourceCoordinator, req.target, targetCoordinator),
	}
	return publication, nil
}

// applyTargetStateLocked commits attachment-local navigation metadata and
// shared environment state as part of the same publication.
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
	return req.preserveAttachment
}

// publishAttachmentOwnershipLocked adds/removes only the exact attachment.
// Existing target attachments and their independent views/transports remain
// untouched.
func publishAttachmentOwnershipLocked(publication *attachmentPublication) {
	req := publication.req
	if publication.source != req.target {
		unregisterAttachmentSessionLocked(publication.source, req.next)
	}
	registerAttachmentSessionLocked(req.target, req.next)
	publication.nextGeneration = req.next.connectionGeneration.Add(1)
	req.next.setSession(req.target)
}

func buildAttachmentPostcommitPlanLocked(publication *attachmentPublication) attachmentTransitionResult {
	req := publication.req
	result := attachmentTransitionResult{}
	if publication.source != req.target && publication.sourceCoordinator != nil {
		result.cleanups = append(result.cleanups, publication.sourceCoordinator.beginDetachLocked(req.next))
	}
	var lease *attachmentLease
	if publication.targetCoordinator != nil && !req.preserveAttachment {
		if publication.source == req.target {
			lease = publication.targetCoordinator.rebindAttachmentWithReadinessLocked(req.next, req.ready)
		} else {
			lease = publication.targetCoordinator.attachWithReadinessLocked(req.next, req.ready)
		}
	}
	result.published = attachmentConnectionToken{
		sess: req.target, ac: req.next,
		generation: publication.nextGeneration,
		transport:  req.expectedTransport, lease: lease,
		rebase: publication.source != req.target,
	}
	if req.attachmentEffectsFrozen {
		req.next.publishFrozenAttachmentCapability(result.published)
	}
	return result
}

func (d *Daemon) publishAttachmentTransitionPrelocked(publication *attachmentPublication) attachmentTransitionResult {
	if publication.req.preserveAttachment {
		d.applyTargetStateLocked(publication)
		return attachmentTransitionResult{published: *publication.req.sourceToken}
	}
	publishAttachmentOwnershipLocked(publication)
	d.applyTargetStateLocked(publication)
	return buildAttachmentPostcommitPlanLocked(publication)
}

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

// lockAttachmentSessions gives every two-session transition one stable order.
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
// lockAttachmentSessions. Callers already hold ordered session locks.
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
