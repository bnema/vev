package daemon

// transitionSourcePreflightLocked validates an initiating role while its gate
// is frozen and d.mu is held. No caller may create or delete a target before
// this succeeds.
func (d *Daemon) transitionSourcePreflightLocked(req attachmentTransitionRequest) bool {
	if req.sourceToken == nil || req.source == nil || d.sessions[req.source.id] != req.source {
		return false
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	req.source.mu.Lock()
	defer req.source.mu.Unlock()
	coordinator := req.source.renderCoordinator()
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
	if d.closing || d.sessions[token.sess.id] != token.sess {
		return false
	}
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	token.sess.mu.Lock()
	defer token.sess.mu.Unlock()
	coordinator := token.sess.renderCoordinator()
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

func transitionSourceTabCurrentLocked(source *session, expected *tab) bool {
	if expected == nil {
		return true
	}
	if source.active < 0 || source.active >= len(source.tabs) || source.tabs[source.active] != expected {
		return false
	}
	expected.mu.Lock()
	defer expected.mu.Unlock()
	return expected.floating.state != floatingVisible
}

// transitionAttachmentLocked is the publication half of transitionAttachment.
// Caller holds d.mu after the lock-free gate freeze/drain has completed.
func (d *Daemon) transitionAttachmentLocked(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	d.notices.routingMu.Lock()
	defer d.notices.routingMu.Unlock()
	return d.transitionAttachmentRoutedLocked(req)
}

// attachmentPublication owns the ordered session and coordinator locks and
// accumulates the values needed to construct postcommit capabilities.
type attachmentPublication struct {
	req                 attachmentTransitionRequest
	source              *session
	old                 *attachedClient
	sourceCoordinator   *renderCoordinator
	targetCoordinator   *renderCoordinator
	unlockSessions      func()
	unlockCoordinators  func()
	displacedTransport  transportSnapshot
	nextGeneration      uint64
	displacedGeneration uint64
}

// unlock releases coordinator locks before the ordered session locks. The
// caller still owns d.mu and notices.routingMu; no external I/O is permitted.
func (p *attachmentPublication) unlock() {
	p.unlockCoordinators()
	p.unlockSessions()
}

// preflightAttachmentPublicationLocked revalidates source and target authority
// and acquires every lock needed by publication. Caller holds d.mu and
// notices.routingMu. On success, the returned guard owns ordered session and
// coordinator locks until unlock is called.
func (d *Daemon) preflightAttachmentPublicationLocked(req attachmentTransitionRequest) (*attachmentPublication, error) {
	source := req.source
	if source == nil {
		source = req.target
	}
	if !req.preflighted || !req.roleEffectsFrozen ||
		!validAttachmentTransitionRole(req.expectedRole, true) || !validAttachmentTransitionRole(req.targetRole, false) ||
		source == nil || req.target == nil || req.next == nil || d.closing ||
		d.sessions[source.id] != source || d.sessions[req.target.id] != req.target ||
		req.expectedTransport.transport == nil || !req.next.transportSnapshotCurrent(req.expectedTransport) {
		return nil, errAttachmentTransition
	}

	// Install the target coordinator before taking both session locks. Lease
	// binding remains part of the atomic publication below.
	var targetCoordinator *renderCoordinator
	if req.targetRole == attachmentActive {
		targetCoordinator = d.ensureRenderCoordinator(req.target)
	}
	sourceCoordinator := source.renderCoordinator()
	unlockSessions := lockAttachmentSessions(source, req.target)
	if source.attachmentRoleLocked(req.next) != req.expectedRole ||
		!req.next.transportSnapshotCurrent(req.expectedTransport) {
		unlockSessions()
		return nil, errAttachmentTransition
	}

	old := req.target.client
	if old != req.expectedTargetCurrent || source != req.target && old == req.next {
		unlockSessions()
		return nil, errAttachmentTransition
	}
	publication := &attachmentPublication{
		req: req, source: source, old: old,
		sourceCoordinator: sourceCoordinator, targetCoordinator: targetCoordinator,
		unlockSessions:     unlockSessions,
		unlockCoordinators: lockAttachmentCoordinators(source, sourceCoordinator, req.target, targetCoordinator),
	}
	if req.sourceToken != nil && (!transitionSourceTokenCurrentLocked(*req.sourceToken, source, sourceCoordinator, req) ||
		!transitionSourceTabCurrentLocked(source, req.expectedSourceTab)) {
		publication.unlock()
		return nil, errAttachmentTransition
	}
	if targetCoordinator != nil && !req.preserveRole && !targetCoordinator.canReplaceLocked(old, req.next) {
		publication.unlock()
		return nil, errAttachmentTransition
	}
	if req.sourceToken == nil && source != req.target && req.expectedRole == attachmentActive && sourceCoordinator != nil &&
		(sourceCoordinator.lease == nil || !sourceCoordinator.lease.active || sourceCoordinator.lease.attachment != req.next) {
		publication.unlock()
		return nil, errAttachmentTransition
	}
	if req.activateTargetTab && (req.targetTabIndex < 0 || req.targetTabIndex >= len(req.target.tabs)) {
		publication.unlock()
		return nil, errAttachmentTransition
	}
	return publication, nil
}

// applyTargetStateLocked commits navigation metadata before role ownership, as
// part of the same locked publication. It reports role-preserving transitions.
func (d *Daemon) applyTargetStateLocked(publication *attachmentPublication) bool {
	req := publication.req
	if req.targetRole == attachmentActive {
		d.demoteParkedActiveForSessionLocked(req.target)
	}
	if req.copySourceEnvironment {
		req.target.terminal = publication.source.terminal
		req.target.env = copyEnvironment(publication.source.env)
	}
	if req.activateTargetTab {
		req.target.active = req.targetTabIndex
	}
	return req.preserveRole
}

// publishAttachmentOwnershipLocked atomically changes session membership,
// generations, and currentSession while all role gates remain frozen.
func publishAttachmentOwnershipLocked(publication *attachmentPublication) {
	req := publication.req
	if req.targetRole == attachmentActive && publication.old != nil && publication.old != req.next {
		publication.displacedTransport = publication.old.transportSnapshot()
	}
	if publication.source != req.target {
		if publication.source.client == req.next {
			publication.source.client = nil
		}
		delete(publication.source.snatched, req.next)
	}
	if req.targetRole == attachmentActive {
		delete(req.target.snatched, req.next)
		if publication.old != nil && publication.old != req.next {
			req.target.addSnatchedLocked(publication.old)
		}
		req.target.client = req.next
	} else {
		req.target.addSnatchedLocked(req.next)
	}
	publication.nextGeneration = req.next.roleGeneration.Add(1)
	if publication.old != nil && publication.old != req.next && req.targetRole == attachmentActive {
		publication.displacedGeneration = publication.old.roleGeneration.Add(1)
	}
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
		cleanup, lease = publication.targetCoordinator.beginReplaceLocked(publication.old, req.next, req.ready)
		result.cleanups = append(result.cleanups, cleanup)
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
	if publication.displacedGeneration != 0 {
		result.displaced = attachmentRoleToken{
			sess:       req.target,
			ac:         publication.old,
			role:       attachmentSnatched,
			generation: publication.displacedGeneration,
			transport:  publication.displacedTransport,
		}
		result.displacedInterrupted = req.expectedTargetTransportInterrupted
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

// transitionAttachmentRoutedLocked performs the centralized role publication.
// Caller holds d.mu and notices.routingMu. Every fallible lifecycle, role, and
// transport check occurs before session.client is changed. Coordinator
// finalization after that commit point cannot roll ownership back.
func (d *Daemon) transitionAttachmentRoutedLocked(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	publication, err := d.preflightAttachmentPublicationLocked(req)
	if err != nil {
		return attachmentTransitionResult{}, err
	}
	defer publication.unlock()

	if d.applyTargetStateLocked(publication) {
		return attachmentTransitionResult{published: *req.sourceToken}, nil
	}
	publishAttachmentOwnershipLocked(publication)
	return buildAttachmentPostcommitPlanLocked(publication), nil
}

func validAttachmentTransitionRole(role attachmentRole, expected bool) bool {
	if role == attachmentActive || role == attachmentSnatched {
		return true
	}
	return expected && role == attachmentDetached
}

// lockAttachmentSessions gives every two-session transition one stable order.
// Session IDs are immutable and unique while their lifecycles are registered.
func lockAttachmentSessions(a, b *session) func() {
	if a == b {
		a.mu.Lock()
		return a.mu.Unlock
	}
	first, second := a, b
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
func lockAttachmentCoordinators(a *session, aCoordinator *renderCoordinator, b *session, bCoordinator *renderCoordinator) func() {
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
	if a.id > b.id {
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
