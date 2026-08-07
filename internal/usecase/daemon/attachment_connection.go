package daemon

import "github.com/bnema/vev/internal/ports"

// attachmentConnectionToken is the exact capability for one registered
// attachment connection incarnation. It is invalid as soon as the owner,
// attachment membership, connection generation, transport link, or
// coordinator lease changes.
type attachmentConnectionToken struct {
	owner      attachmentOwner
	ac         *attachedClient
	generation uint64
	transport  transportSnapshot
	lease      *attachmentLease
	effect     *attachmentEffectTicket
	// rebase marks a cross-session move whose first paint starts a dependency-
	// free output chain after publication releases architecture locks.
	rebase bool
}

// attachmentToken retains the local-session constructor for the existing
// local route. Remote-view token construction is added with remote attachment
// membership in Phase 4; it must not manufacture a local-session capability.
func attachmentToken(entry *session, ac *attachedClient, tr ports.Transport) attachmentConnectionToken {
	return attachmentOwnerToken(entry, ac, tr)
}

func attachmentOwnerToken(owner attachmentOwner, ac *attachedClient, tr ports.Transport) attachmentConnectionToken {
	owner = normalizeAttachmentOwner(owner)
	if owner == nil || ac == nil || tr == nil {
		return attachmentConnectionToken{}
	}
	// Capture one concrete link incarnation before taking the owner lock. The
	// same snapshot is revalidated below; do not take a second snapshot and
	// accidentally bind a replacement link.
	transport := ac.transportSnapshot()
	if transport.transport != tr {
		return attachmentConnectionToken{}
	}
	if ac.beforeAttachmentTokenValidation != nil {
		ac.beforeAttachmentTokenValidation()
	}

	token := attachmentConnectionToken{owner: owner, ac: ac, transport: transport}
	switch entry := owner.(type) {
	case *session:
		if entry.core() == nil {
			return attachmentConnectionToken{}
		}
		core := entry.core()
		core.mu.Lock()
		if !attachmentRegisteredLocked(entry, ac) ||
			!sameAttachmentOwner(ac.currentAttachmentOwner(), owner) ||
			!ac.transportSnapshotCurrent(transport) {
			core.mu.Unlock()
			return attachmentConnectionToken{}
		}
		token.generation = ac.connectionGeneration.Load()
		if rc := core.coordinator.Load(); rc != nil {
			token.lease = rc.attachmentLease(ac)
		}
		core.mu.Unlock()
	case *remoteView:
		entry.mu.Lock()
		_, registered := entry.attachments[ac]
		if !registered || !sameAttachmentOwner(ac.currentAttachmentOwner(), owner) || !ac.transportSnapshotCurrent(transport) {
			entry.mu.Unlock()
			return attachmentConnectionToken{}
		}
		token.generation = ac.connectionGeneration.Load()
		entry.mu.Unlock()
	default:
		return attachmentConnectionToken{}
	}
	if !token.current() {
		return attachmentConnectionToken{}
	}
	ac.bootstrapAttachmentCapability(token)
	return token
}

func (s *session) attachmentToken(ac *attachedClient, tr ports.Transport) attachmentConnectionToken {
	return attachmentToken(s, ac, tr)
}

// localSession narrows a token only for explicitly local operations. It is
// intentionally nil for a remote owner, so local PTY/persistence paths fail
// closed rather than treating remote metadata as a local session.
func (t attachmentConnectionToken) localSession() *session {
	return localSession(t.owner)
}

// current validates every immutable identity captured by the token. The owner
// binding is the sole mutable authority; the local session is only a derived
// capability for local attachment membership and coordinator validation.
func (t attachmentConnectionToken) current() bool {
	return t.owner != nil && t.ac != nil &&
		t.ac.connectionGeneration.Load() == t.generation &&
		sameAttachmentOwner(t.ac.currentAttachmentOwner(), t.owner) &&
		t.ac.transportSnapshotCurrent(t.transport) &&
		attachmentOwnerRegistered(t.owner, t.ac)
}

// attachmentCurrent additionally validates the optional coordinator lease used
// by render and resize effects. Attachments without that optional lease still
// own their independent connection lifecycle.
func (t attachmentConnectionToken) attachmentCurrent() bool {
	if !t.current() {
		return false
	}
	if t.lease == nil {
		return true
	}
	entry := t.localSession()
	if entry == nil {
		return false
	}
	rc := entry.core().coordinator.Load()
	return rc != nil && t.lease.attachment == t.ac && rc.leaseCurrent(t.lease, true)
}

func (t attachmentConnectionToken) attachmentEffectCurrent() bool {
	if t.effect != nil && !t.effect.ended.Load() {
		return true
	}
	return t.attachmentCurrent()
}

func beginAttachmentLeaseEffect(sess *session, ac *attachedClient, lease *attachmentLease) (*attachmentEffectTicket, bool) {
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

// attachmentEffectCurrentSessionLocked is the same check at a local
// session-owned mutation boundary where sess.core().mu is already held.
func (t attachmentConnectionToken) attachmentEffectCurrentSessionLocked() bool {
	entry := t.localSession()
	if entry == nil {
		return false
	}
	if t.effect != nil && !t.effect.ended.Load() {
		return true
	}
	if t.ac == nil ||
		t.ac.connectionGeneration.Load() != t.generation ||
		!t.ac.transportSnapshotCurrent(t.transport) ||
		!sameAttachmentOwner(t.ac.currentAttachmentOwner(), t.owner) ||
		!attachmentRegisteredLocked(entry, t.ac) {
		return false
	}
	if t.lease == nil {
		return true
	}
	rc := entry.core().coordinator.Load()
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
