package daemon

import "github.com/bnema/vev/internal/ports"

type attachmentRole uint8

// attachmentSessionCore normalizes nil interfaces, typed-nil implementations,
// and implementations that deliberately expose no attachment core.
func attachmentSessionCore(entry attachmentSession) *sessionCore {
	if entry == nil {
		return nil
	}
	return entry.core()
}

const (
	attachmentDetached attachmentRole = iota
	attachmentActive
	attachmentSnatched
)

// attachmentRole derives the attachment's role from the session-owned
// registries. There is deliberately no second role field that could drift.
func attachmentSessionRole(entry attachmentSession, ac *attachedClient) attachmentRole {
	if entry == nil || ac == nil || entry.core() == nil {
		return attachmentDetached
	}
	core := entry.core()
	core.mu.Lock()
	defer core.mu.Unlock()
	return attachmentSessionRoleLocked(entry, ac)
}

// attachmentSessionRoleLocked requires entry.core().mu. Active membership wins
// if an invalid intermediate state contains the same attachment in both
// registries.
func attachmentSessionRoleLocked(entry attachmentSession, ac *attachedClient) attachmentRole {
	if entry == nil || ac == nil || entry.core() == nil {
		return attachmentDetached
	}
	core := entry.core()
	// Membership is the lifecycle authority. Keep the primary pointer as a
	// narrow construction-time fallback for older headless fixtures; attached
	// routes always publish into the collection.
	if _, ok := core.attachments[ac]; ok || core.client == ac {
		return attachmentActive
	}
	if _, ok := core.snatched[ac]; ok {
		return attachmentSnatched
	}
	return attachmentDetached
}

func addSnatchedLocked(entry attachmentSession, ac *attachedClient) {
	if entry == nil || ac == nil || entry.core() == nil {
		return
	}
	core := entry.core()
	if core.snatched == nil {
		core.snatched = make(map[*attachedClient]struct{})
	}
	core.snatched[ac] = struct{}{}
}

func (s *session) attachmentRole(ac *attachedClient) attachmentRole {
	return attachmentSessionRole(s, ac)
}

func (s *session) attachmentRoleLocked(ac *attachedClient) attachmentRole {
	return attachmentSessionRoleLocked(s, ac)
}

func (s *session) addSnatchedLocked(ac *attachedClient) { addSnatchedLocked(s, ac) }

type attachmentRoleToken struct {
	sess       attachmentSession
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
func attachmentTokenLocked(entry attachmentSession, ac *attachedClient) attachmentRoleToken {
	if entry == nil || entry.core() == nil || ac == nil {
		return attachmentRoleToken{}
	}
	transport := ac.transportSnapshot()
	if transport.transport == nil {
		return attachmentRoleToken{}
	}
	token := attachmentRoleToken{
		sess:       entry,
		ac:         ac,
		role:       attachmentSessionRoleLocked(entry, ac),
		generation: ac.roleGeneration.Load(),
		transport:  transport,
	}
	if token.role == attachmentActive {
		if rc := entry.core().coordinator.Load(); rc != nil {
			token.lease = rc.attachmentLease(ac)
		}
	}
	return token
}

func (s *session) attachmentTokenLocked(ac *attachedClient) attachmentRoleToken {
	return attachmentTokenLocked(s, ac)
}

func attachmentToken(entry attachmentSession, ac *attachedClient, tr ports.Transport) attachmentRoleToken {
	if entry == nil || entry.core() == nil || ac == nil || tr == nil {
		return attachmentRoleToken{}
	}
	core := entry.core()
	core.mu.Lock()
	transport := ac.transportSnapshot()
	if transport.transport != tr {
		core.mu.Unlock()
		return attachmentRoleToken{}
	}
	token := attachmentRoleToken{
		sess:       entry,
		ac:         ac,
		role:       attachmentSessionRoleLocked(entry, ac),
		generation: ac.roleGeneration.Load(),
		transport:  transport,
	}
	core.mu.Unlock()
	if token.role == attachmentActive {
		if rc := core.coordinator.Load(); rc != nil {
			token.lease = rc.attachmentLease(ac)
		}
	}
	ac.bootstrapRoleCapability(token)
	return token
}

func (s *session) attachmentToken(ac *attachedClient, tr ports.Transport) attachmentRoleToken {
	return attachmentToken(s, ac, tr)
}

func (t attachmentRoleToken) current() bool {
	return t.sess != nil && t.ac != nil &&
		t.ac.roleGeneration.Load() == t.generation &&
		t.ac.transportSnapshotCurrent(t.transport) &&
		attachmentSessionRole(t.sess, t.ac) == t.role
}

// activeCurrent admits post-transition effects only for the exact published
// role, transport incarnation, current-session link, and coordinator lease.
func (t attachmentRoleToken) activeCurrent() bool {
	if t.role != attachmentActive || !t.current() || t.ac.currentAttachmentSession() != t.sess {
		return false
	}
	// Membership grants each attachment its own protocol capability. The
	// session coordinator may still have a primary render lease while a second
	// attachment is joining; that attachment must not be treated as displaced.
	if t.lease == nil {
		return true
	}
	rc := t.sess.core().coordinator.Load()
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

func beginActiveLeaseEffect(sess attachmentSession, ac *attachedClient, lease *attachmentLease) (*roleEffectTicket, bool) {
	if sess == nil || ac == nil || lease == nil {
		return nil, false
	}
	token := attachmentToken(sess, ac, ac.transport())
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
	if t.role != attachmentActive || t.sess == nil || t.ac == nil ||
		t.ac.roleGeneration.Load() != t.generation || !t.ac.transportSnapshotCurrent(t.transport) ||
		t.ac.currentAttachmentSession() != t.sess || attachmentSessionRoleLocked(t.sess, t.ac) != attachmentActive {
		return false
	}
	if t.lease == nil {
		return true
	}
	rc := t.sess.core().coordinator.Load()
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
