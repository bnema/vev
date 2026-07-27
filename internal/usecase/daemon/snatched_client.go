package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/ports"
)

type attachmentRole uint8

const (
	attachmentDetached attachmentRole = iota
	attachmentActive
	attachmentSnatched
)

// attachmentRole derives the attachment's role from the session-owned
// registries. There is deliberately no second role field that could drift.
func (s *session) attachmentRole(ac *attachedClient) attachmentRole {
	if s == nil || ac == nil {
		return attachmentDetached
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.attachmentRoleLocked(ac)
}

// attachmentRoleLocked requires s.mu. Active membership wins if an invalid
// intermediate state contains the same attachment in both registries.
func (s *session) attachmentRoleLocked(ac *attachedClient) attachmentRole {
	if ac == nil {
		return attachmentDetached
	}
	if s.client == ac {
		return attachmentActive
	}
	if _, ok := s.snatched[ac]; ok {
		return attachmentSnatched
	}
	return attachmentDetached
}

func (s *session) addSnatchedLocked(ac *attachedClient) {
	if ac == nil {
		return
	}
	if s.snatched == nil {
		s.snatched = make(map[*attachedClient]struct{})
	}
	s.snatched[ac] = struct{}{}
}

var errAttachmentTransition = errors.New("attachment transition is no longer valid")

type attachmentTransitionRequest struct {
	source                *session
	target                *session
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
	preserveRole                       bool
	expectedSourceTab                  *tab
	createTargetLocked                 func() (*session, error)
	ready                              bool
	expectedTargetCurrent              *attachedClient
	expectedTargetTransport            transportSnapshot
	preflighted                        bool
	roleEffectsFrozen                  bool
	expectedTargetTransportInterrupted bool
}

func transitionSourceTokenMatchesRequest(token attachmentRoleToken, source *session, req attachmentTransitionRequest) bool {
	return token.sess == source && token.ac == req.next && token.role == req.expectedRole &&
		token.transport.transport == req.expectedTransport.transport &&
		token.transport.incarnation == req.expectedTransport.incarnation
}

// transitionSourceTokenCurrentLocked requires source.mu and, for an active
// source, sourceCoordinator.mu. This is the canonical handoff preflight: every
// client-originated navigation, delete, or create intent is bound to one exact
// role generation, transport incarnation, and coordinator lease.
func transitionSourceTokenCurrentLocked(token attachmentRoleToken, source *session, sourceCoordinator *renderCoordinator, req attachmentTransitionRequest) bool {
	if token.sess != source || token.ac != req.next || token.role != req.expectedRole ||
		token.generation != req.next.roleGeneration.Load() || source.attachmentRoleLocked(req.next) != token.role ||
		req.next.currentSession() != source || !req.next.transportSnapshotCurrent(token.transport) {
		return false
	}
	if token.role != attachmentActive {
		return true
	}
	return sourceCoordinator != nil && token.lease != nil && token.lease.attachment == req.next &&
		sourceCoordinator.leaseCurrentLocked(token.lease, true)
}

type attachmentRoleToken struct {
	sess       *session
	ac         *attachedClient
	role       attachmentRole
	generation uint64
	transport  transportSnapshot
	lease      *attachmentLease
	effect     *roleEffectTicket
	// rebase marks a cross-session ownership move whose first paint must start a
	// dependency-free output chain. The rebase is deliberately deferred until
	// after transition publication releases all architecture locks.
	rebase bool
}

func (s *session) attachmentToken(ac *attachedClient, tr ports.Transport) attachmentRoleToken {
	if s == nil || ac == nil || tr == nil {
		return attachmentRoleToken{}
	}
	s.mu.Lock()
	transport := ac.transportSnapshot()
	if transport.transport != tr {
		s.mu.Unlock()
		return attachmentRoleToken{}
	}
	token := attachmentRoleToken{
		sess:       s,
		ac:         ac,
		role:       s.attachmentRoleLocked(ac),
		generation: ac.roleGeneration.Load(),
		transport:  transport,
	}
	s.mu.Unlock()
	if token.role == attachmentActive {
		if rc := s.renderCoordinator(); rc != nil {
			token.lease = rc.attachmentLease(ac)
		}
	}
	ac.bootstrapRoleCapability(token)
	return token
}

func (t attachmentRoleToken) current() bool {
	return t.sess != nil && t.ac != nil &&
		t.ac.roleGeneration.Load() == t.generation &&
		t.ac.transportSnapshotCurrent(t.transport) &&
		t.sess.attachmentRole(t.ac) == t.role
}

// activeCurrent admits post-transition effects only for the exact published
// role, transport incarnation, current-session link, and coordinator lease.
func (t attachmentRoleToken) activeCurrent() bool {
	if t.role != attachmentActive || t.lease == nil || !t.current() || t.ac.currentSession() != t.sess {
		return false
	}
	rc := t.sess.renderCoordinator()
	return rc != nil && t.lease.attachment == t.ac && rc.leaseCurrent(t.lease, true)
}

// activeEffect is the canonical admission check for a client-frame mutation.
// Tokens bind role generation, transport incarnation, session ownership, and
// coordinator lease, so handlers never rediscover authority from mutable state.
func (t attachmentRoleToken) activeEffect() bool {
	if t.effect != nil && !t.effect.ended.Load() {
		return true
	}
	return t.activeCurrent()
}

func beginActiveLeaseEffect(sess *session, ac *attachedClient, lease *attachmentLease) (*roleEffectTicket, bool) {
	if sess == nil || ac == nil || lease == nil {
		return nil, false
	}
	token := sess.attachmentToken(ac, ac.transport())
	token.lease = lease
	return ac.beginRoleEffect(token)
}

func (t *attachmentRoleToken) endRoleEffect() {
	if t == nil || t.effect == nil {
		return
	}
	t.effect.End()
	t.effect = nil
}

// activeEffectSessionLocked is the same admission check while t.sess.mu is
// already held at a session-owned mutation boundary.
func (t attachmentRoleToken) activeEffectSessionLocked() bool {
	if t.effect != nil && !t.effect.ended.Load() {
		return true
	}
	if t.role != attachmentActive || t.lease == nil || t.sess == nil || t.ac == nil ||
		t.ac.roleGeneration.Load() != t.generation || !t.ac.transportSnapshotCurrent(t.transport) ||
		t.ac.currentSession() != t.sess || t.sess.attachmentRoleLocked(t.ac) != attachmentActive {
		return false
	}
	rc := t.sess.renderCoordinator()
	return rc != nil && t.lease.attachment == t.ac && rc.leaseCurrent(t.lease, true)
}

func (t attachmentRoleToken) sendActiveControl(frame ports.Frame) error {
	if t.ac == nil {
		return errAttachmentTransition
	}
	t.ac.sendMu.Lock()
	defer t.ac.sendMu.Unlock()
	if t.effect == nil || t.effect.ended.Load() || !t.ac.transportSnapshotCurrent(t.transport) ||
		!t.effect.beginTransportSend(t.transport) {
		return errAttachmentTransition
	}
	err := t.transport.transport.Send(frame)
	if err != nil {
		t.effect.reportTransportFailure(t.transport)
	}
	t.effect.endTransportSend()
	return err
}

type attachmentTransitionResult struct {
	published            attachmentRoleToken
	displaced            attachmentRoleToken
	displacedInterrupted bool
	cleanups             []renderLifecycleCleanup
}

func (d *Daemon) transitionAttachment(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	// Preflight identifies every gate the publication can affect. No gate is
	// frozen while an architecture lock is held.
	d.mu.Lock()
	source := req.source
	if source == nil {
		source = req.target
	}
	if source == nil || (req.target == nil && req.createTargetLocked == nil) || req.next == nil || d.closing ||
		d.sessions[source.id] != source || req.target != nil && d.sessions[req.target.id] != req.target {
		d.mu.Unlock()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	if req.target != nil {
		req.target.mu.Lock()
		req.expectedTargetCurrent = req.target.client
		if req.expectedTargetCurrent != nil && req.expectedTargetCurrent != req.next && req.targetRole == attachmentActive {
			req.expectedTargetTransport = req.expectedTargetCurrent.transportSnapshot()
		}
		req.target.mu.Unlock()
	}
	if req.sourceToken != nil && !transitionSourceTokenMatchesRequest(*req.sourceToken, source, req) {
		d.mu.Unlock()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	req.preflighted = true
	d.mu.Unlock()

	if req.sourceToken != nil {
		d.endActionRoleEffect(req.sourceToken.effect, req.action)
	}

	participants := []*attachedClient{req.next, req.expectedTargetCurrent}
	if d.afterRoleEffectParticipantsSnapshotted != nil {
		d.afterRoleEffectParticipantsSnapshotted(req.action, participants)
	}
	interrupts := []roleTransportInterrupt{{
		ac: req.expectedTargetCurrent, transport: req.expectedTargetTransport,
	}}
	frozen := freezeRoleEffectGatesInterruptingObserved(interrupts, func(ac *attachedClient) {
		if d.afterRoleEffectGateFrozen != nil {
			d.afterRoleEffectGateFrozen(req.action, ac)
		}
	}, participants...)
	defer frozen.unfreeze()
	req.expectedTargetTransportInterrupted = frozen.interrupted(req.expectedTargetCurrent, req.expectedTargetTransport)
	if d.afterRoleEffectsFrozen != nil {
		d.afterRoleEffectsFrozen()
	}

	d.mu.Lock()
	req.roleEffectsFrozen = true
	if req.target == nil {
		if !d.transitionSourcePreflightLocked(req) {
			d.mu.Unlock()
			return attachmentTransitionResult{}, errAttachmentTransition
		}
		var err error
		req.target, err = req.createTargetLocked()
		if err != nil {
			d.mu.Unlock()
			return attachmentTransitionResult{}, err
		}
		req.expectedTargetCurrent = nil
	}
	result, err := d.transitionAttachmentLocked(req)
	d.mu.Unlock()
	return result, err
}

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

// transitionAttachmentRoutedLocked performs the centralized role publication.
// Caller holds d.mu and notices.routingMu. Every fallible lifecycle, role, and
// transport check occurs before session.client is changed. Coordinator
// finalization after that commit point cannot roll ownership back.
func (d *Daemon) transitionAttachmentRoutedLocked(req attachmentTransitionRequest) (attachmentTransitionResult, error) {
	source := req.source
	if source == nil {
		source = req.target
	}
	if !req.preflighted || !req.roleEffectsFrozen ||
		!validAttachmentTransitionRole(req.expectedRole, true) || !validAttachmentTransitionRole(req.targetRole, false) ||
		source == nil || req.target == nil || req.next == nil || d.closing ||
		d.sessions[source.id] != source || d.sessions[req.target.id] != req.target ||
		req.expectedTransport.transport == nil || !req.next.transportSnapshotCurrent(req.expectedTransport) {
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	// Install the target coordinator object before taking both session locks;
	// lease binding itself remains part of the atomic publication below.
	var targetCoordinator *renderCoordinator
	if req.targetRole == attachmentActive {
		targetCoordinator = d.ensureRenderCoordinator(req.target)
	}
	sourceCoordinator := source.renderCoordinator()

	unlockSessions := lockAttachmentSessions(source, req.target)
	if source.attachmentRoleLocked(req.next) != req.expectedRole ||
		!req.next.transportSnapshotCurrent(req.expectedTransport) {
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}

	old := req.target.client
	if req.preflighted && old != req.expectedTargetCurrent {
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	if source != req.target && old == req.next {
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	unlockCoordinators := lockAttachmentCoordinators(source, sourceCoordinator, req.target, targetCoordinator)
	if req.sourceToken != nil && (!transitionSourceTokenCurrentLocked(*req.sourceToken, source, sourceCoordinator, req) ||
		!transitionSourceTabCurrentLocked(source, req.expectedSourceTab)) {
		unlockCoordinators()
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	if targetCoordinator != nil && !req.preserveRole && !targetCoordinator.canReplaceLocked(old, req.next) {
		unlockCoordinators()
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	if req.sourceToken == nil && source != req.target && req.expectedRole == attachmentActive && sourceCoordinator != nil &&
		(sourceCoordinator.lease == nil || !sourceCoordinator.lease.active || sourceCoordinator.lease.attachment != req.next) {
		unlockCoordinators()
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}
	if req.activateTargetTab && (req.targetTabIndex < 0 || req.targetTabIndex >= len(req.target.tabs)) {
		unlockCoordinators()
		unlockSessions()
		return attachmentTransitionResult{}, errAttachmentTransition
	}

	if req.copySourceEnvironment {
		req.target.terminal = source.terminal
		req.target.env = copyEnvironment(source.env)
	}
	if req.activateTargetTab {
		req.target.active = req.targetTabIndex
	}
	if req.preserveRole {
		unlockCoordinators()
		unlockSessions()
		return attachmentTransitionResult{published: *req.sourceToken}, nil
	}

	var displacedTransport transportSnapshot
	if req.targetRole == attachmentActive && old != nil && old != req.next {
		displacedTransport = old.transportSnapshot()
	}
	if source != req.target {
		if source.client == req.next {
			source.client = nil
		}
		delete(source.snatched, req.next)
	}
	if req.targetRole == attachmentActive {
		delete(req.target.snatched, req.next)
		if old != nil && old != req.next {
			req.target.addSnatchedLocked(old)
		}
		req.target.client = req.next
	} else {
		req.target.addSnatchedLocked(req.next)
	}
	nextGeneration := req.next.roleGeneration.Add(1)
	var displacedGeneration uint64
	if old != nil && old != req.next && req.targetRole == attachmentActive {
		displacedGeneration = old.roleGeneration.Add(1)
	}
	req.next.setSession(req.target)

	result := attachmentTransitionResult{}
	if source != req.target && req.expectedRole == attachmentActive && sourceCoordinator != nil {
		result.cleanups = append(result.cleanups, sourceCoordinator.beginDetachLocked(req.next))
	}
	var lease *attachmentLease
	if targetCoordinator != nil {
		var cleanup renderLifecycleCleanup
		cleanup, lease = targetCoordinator.beginReplaceLocked(old, req.next, req.ready)
		result.cleanups = append(result.cleanups, cleanup)
	}
	result.published = attachmentRoleToken{
		sess:       req.target,
		ac:         req.next,
		role:       req.targetRole,
		generation: nextGeneration,
		transport:  req.expectedTransport,
		lease:      lease,
		rebase:     source != req.target && req.expectedRole == attachmentActive,
	}
	if displacedGeneration != 0 {
		result.displaced = attachmentRoleToken{
			sess:       req.target,
			ac:         old,
			role:       attachmentSnatched,
			generation: displacedGeneration,
			transport:  displacedTransport,
		}
		result.displacedInterrupted = req.expectedTargetTransportInterrupted
	}
	// Membership, currentSession, lease, generation, and both exact admission
	// capabilities become visible as one frozen publication. The gates remain
	// frozen until every architecture lock below has been released.
	if req.roleEffectsFrozen {
		req.next.publishFrozenRoleCapability(result.published)
		if result.displaced.ac != nil {
			result.displaced.ac.publishFrozenRoleCapability(result.displaced)
		}
	}
	unlockCoordinators()
	unlockSessions()
	return result, nil
}

func (d *Daemon) clearCaptureFramesForSnatch(token attachmentRoleToken) bool {
	ac := token.ac
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	if ac.roleGeneration.Load() != token.generation || !ac.transportSnapshotCurrent(token.transport) {
		return false
	}
	ac.captureFrames = nil
	return true
}

func (d *Daemon) sendSnatchedControl(token attachmentRoleToken, frame ports.Frame) error {
	ac := token.ac
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	if ac.roleGeneration.Load() != token.generation {
		return errSnatchedOutputStale
	}
	if !ac.transportSnapshotCurrent(token.transport) {
		return errTransportReplaced
	}
	if token.effect == nil || !token.effect.beginTransportSend(token.transport) {
		return errAttachmentTransition
	}
	err := token.transport.transport.Send(frame)
	if err != nil {
		token.effect.reportTransportFailure(token.transport)
	}
	token.effect.endTransportSend()
	return err
}

// removeSnatchedAttachment removes exactly one role generation and transport
// from session routing. It never touches the active attachment or closes a
// transport, allowing callers that already interrupted a blocked send to avoid
// closing the same link twice.
func (d *Daemon) removeSnatchedAttachment(token attachmentRoleToken) bool {
	if token.sess == nil || token.ac == nil || token.transport.transport == nil {
		return false
	}
	frozen := freezeRoleEffectGates(token.ac)
	defer frozen.unfreeze()
	d.notices.routingMu.Lock()
	token.sess.mu.Lock()
	current := token.sess.attachmentRoleLocked(token.ac) == attachmentSnatched &&
		token.ac.roleGeneration.Load() == token.generation &&
		token.ac.transportSnapshotCurrent(token.transport)
	if current {
		delete(token.sess.snatched, token.ac)
		token.ac.roleGeneration.Add(1)
		token.ac.invalidateFrozenRoleCapability()
	}
	token.sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	if !current {
		return false
	}

	d.unregisterPreview(token.ac)
	token.ac.clearPreviousSession()
	token.ac.setSession(nil)
	token.ac.clearCaptureFrames()
	return true
}

// cleanupInterruptedSnatchedAttachment owns terminal cleanup for a displaced
// transport that was closed to drain an admitted send. It waits on the gate's
// condition rather than ordinary effect admission, then removes only the exact
// published snatched incarnation. Attachment-local cleanup runs without routing
// or session locks while the terminal capability remains frozen.
func (d *Daemon) cleanupInterruptedSnatchedAttachment(token attachmentRoleToken) bool {
	if token.sess == nil || token.ac == nil || token.transport.transport == nil {
		return false
	}
	frozen := freezeRoleEffectGates(token.ac)
	defer frozen.unfreeze()

	d.notices.routingMu.Lock()
	token.sess.mu.Lock()
	current := token.sess.attachmentRoleLocked(token.ac) == attachmentSnatched &&
		token.ac.currentSession() == token.sess && token.ac.roleGeneration.Load() == token.generation &&
		token.ac.transportSnapshotCurrent(token.transport)
	token.sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	if !current {
		return false
	}

	// The frozen gate prevents a promotion from publishing between overlay
	// cleanup and terminal registry publication.
	d.clearForSnatch(token)

	d.notices.routingMu.Lock()
	token.sess.mu.Lock()
	current = token.sess.attachmentRoleLocked(token.ac) == attachmentSnatched &&
		token.ac.currentSession() == token.sess && token.ac.roleGeneration.Load() == token.generation &&
		token.ac.transportSnapshotCurrent(token.transport)
	if current {
		delete(token.sess.snatched, token.ac)
		token.ac.roleGeneration.Add(1)
		token.ac.invalidateFrozenRoleCapability()
	}
	token.sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	if !current {
		return false
	}

	_ = token.ac.closeCapturedTransport(token.ac.revokeTransport(token.transport.transport))
	d.unregisterPreview(token.ac)
	token.ac.clearPreviousSession()
	token.ac.setSession(nil)
	token.ac.clearCaptureFrames()
	return true
}

// dropSnatchedAttachment removes and closes only the captured snatched link.
func (d *Daemon) dropSnatchedAttachment(token attachmentRoleToken) bool {
	if !d.removeSnatchedAttachment(token) {
		return false
	}
	_ = token.ac.closeCapturedTransport(token.ac.revokeTransport(token.transport.transport))
	return true
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

// deferAttachmentTransitionCleanups retires coordinator timers and accounting
// after ownership publication. Cleanup never gates the attaching handshake. A
// displaced render that already owns sendMu is interrupted by closing only its
// captured transport; an idle healthy snatched transport remains connected and
// receives a dependency-free reset panel.
func (d *Daemon) deferAttachmentTransitionCleanups(result attachmentTransitionResult) {
	if token := result.displaced; token.ac != nil && token.transport.transport != nil {
		blockedRender := result.displacedInterrupted
		if !blockedRender {
			blockedRender = !token.ac.sendMu.TryLock()
			if !blockedRender {
				token.ac.sendMu.Unlock()
			}
		}
		d.attachmentCleanupWg.Go(func() {
			if d.afterDisplacedCleanupStarted != nil {
				d.afterDisplacedCleanupStarted()
			}
			if blockedRender {
				// This path owns terminal cleanup independently of ordinary role
				// admission: a stale send-error detach may transiently freeze the gate.
				_ = token.ac.closeCapturedTransport(token.transport.transport)
				d.cleanupInterruptedSnatchedAttachment(token)
				return
			}
			ticket, admitted := token.ac.beginRoleEffect(token)
			if !admitted {
				return
			}
			defer ticket.End()
			if !token.current() || !d.clearForSnatch(token) || !d.clearCaptureFramesForSnatch(token) {
				return
			}
			if err := d.sendSnatchedPanel(token.ac, token.transport, token.generation, "", ticket); err != nil {
				ticket.End()
				d.dropSnatchedAttachment(token)
			}
		})
	}
	for _, cleanup := range result.cleanups {
		d.attachmentCleanupWg.Go(cleanup.finish)
	}
}
