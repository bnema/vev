package daemon

import "github.com/bnema/vev/internal/ports"

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

// attachmentTokenLocked captures the exact role, transport incarnation, and
// active coordinator lease at an architecture commit point. The caller must
// hold s.mu; coordinator acquisition follows the canonical session ->
// coordinator order.
func (s *session) attachmentTokenLocked(ac *attachedClient) attachmentRoleToken {
	if s == nil || ac == nil {
		return attachmentRoleToken{}
	}
	transport := ac.transportSnapshot()
	if transport.transport == nil {
		return attachmentRoleToken{}
	}
	token := attachmentRoleToken{
		sess:       s,
		ac:         ac,
		role:       s.attachmentRoleLocked(ac),
		generation: ac.roleGeneration.Load(),
		transport:  transport,
	}
	if token.role == attachmentActive {
		if rc := s.renderCoordinator(); rc != nil {
			token.lease = rc.attachmentLease(ac)
		}
	}
	return token
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
