package daemon

import "github.com/bnema/vev/internal/ports"

// attachmentSessionCore normalizes nil interfaces and implementations that
// deliberately expose no attachment core.
func attachmentSessionCore(entry attachmentSession) *sessionCore {
	if entry == nil {
		return nil
	}
	return entry.core()
}

// attachmentConnectionToken is the exact capability for one registered
// attachment connection incarnation. It is invalid as soon as any of the
// session, attachment membership, connection generation, transport link, or
// coordinator lease changes.
type attachmentConnectionToken struct {
	sess       attachmentSession
	ac         *attachedClient
	generation uint64
	transport  transportSnapshot
	lease      *attachmentLease
	effect     *attachmentEffectTicket
	// rebase marks a cross-session move whose first paint starts a dependency-
	// free output chain after publication releases architecture locks.
	rebase bool
}

// attachmentTokenLocked captures the exact registered attachment, transport
// incarnation, generation, and coordinator lease. Caller holds entry.core().mu.
func attachmentTokenLocked(entry attachmentSession, ac *attachedClient) attachmentConnectionToken {
	if entry == nil || entry.core() == nil || ac == nil || !attachmentRegisteredLocked(entry, ac) {
		return attachmentConnectionToken{}
	}
	transport := ac.transportSnapshot()
	if transport.transport == nil {
		return attachmentConnectionToken{}
	}
	token := attachmentConnectionToken{
		sess:       entry,
		ac:         ac,
		generation: ac.connectionGeneration.Load(),
		transport:  transport,
	}
	if rc := entry.core().coordinator.Load(); rc != nil {
		token.lease = rc.attachmentLease(ac)
	}
	return token
}

func (s *session) attachmentTokenLocked(ac *attachedClient) attachmentConnectionToken {
	return attachmentTokenLocked(s, ac)
}

func attachmentToken(entry attachmentSession, ac *attachedClient, tr ports.Transport) attachmentConnectionToken {
	if entry == nil || entry.core() == nil || ac == nil || tr == nil {
		return attachmentConnectionToken{}
	}
	core := entry.core()
	core.mu.Lock()
	transport := ac.transportSnapshot()
	if transport.transport != tr {
		core.mu.Unlock()
		return attachmentConnectionToken{}
	}
	token := attachmentTokenLocked(entry, ac)
	core.mu.Unlock()
	if token.ac == nil {
		return attachmentConnectionToken{}
	}
	ac.bootstrapAttachmentCapability(token)
	return token
}

func (s *session) attachmentToken(ac *attachedClient, tr ports.Transport) attachmentConnectionToken {
	return attachmentToken(s, ac, tr)
}

// current validates every immutable identity captured by the token. Membership
// is the sole authority; there is no owner, replacement, or compatibility role.
func (t attachmentConnectionToken) current() bool {
	return t.sess != nil && t.ac != nil &&
		t.ac.connectionGeneration.Load() == t.generation &&
		t.ac.currentAttachmentSession() == t.sess &&
		t.ac.transportSnapshotCurrent(t.transport) &&
		attachmentRegistered(t.sess, t.ac)
}

// attachmentCurrent additionally validates the optional coordinator lease used
// by render and resize effects. Attachments without that optional lease still
// own their independent connection lifecycle.
func (t attachmentConnectionToken) attachmentCurrent() bool {
	if !t.current() || t.ac.currentAttachmentSession() != t.sess {
		return false
	}
	if t.lease == nil {
		return true
	}
	rc := t.sess.core().coordinator.Load()
	return rc != nil && t.lease.attachment == t.ac && rc.leaseCurrent(t.lease, true)
}

func (t attachmentConnectionToken) attachmentEffectCurrent() bool {
	if t.effect != nil && !t.effect.ended.Load() {
		return true
	}
	return t.attachmentCurrent()
}

func beginAttachmentLeaseEffect(sess attachmentSession, ac *attachedClient, lease *attachmentLease) (*attachmentEffectTicket, bool) {
	if sess == nil || ac == nil || lease == nil {
		return nil, false
	}
	token := attachmentToken(sess, ac, ac.transport())
	token.lease = lease
	return ac.beginAttachmentEffect(token)
}

func (t *attachmentConnectionToken) endAttachmentEffect() {
	if t == nil || t.effect == nil {
		return
	}
	t.effect.End()
	t.effect = nil
}

// attachmentEffectCurrentSessionLocked is the same check at a session-owned
// mutation boundary where sess.core().mu is already held.
func (t attachmentConnectionToken) attachmentEffectCurrentSessionLocked() bool {
	if t.effect != nil && !t.effect.ended.Load() {
		return true
	}
	if t.sess == nil || t.ac == nil ||
		t.ac.connectionGeneration.Load() != t.generation ||
		!t.ac.transportSnapshotCurrent(t.transport) ||
		t.ac.currentAttachmentSession() != t.sess ||
		!attachmentRegisteredLocked(t.sess, t.ac) {
		return false
	}
	if t.lease == nil {
		return true
	}
	rc := t.sess.core().coordinator.Load()
	return rc != nil && t.lease.attachment == t.ac && rc.leaseCurrent(t.lease, true)
}

func (t attachmentConnectionToken) sendControl(frame ports.Frame) error {
	if t.ac == nil || t.transport.transport == nil {
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
