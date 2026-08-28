package daemon

import "github.com/bnema/vev/internal/ports"

// attachmentCapability is the exact capability for one registered
// Attachment Connection incarnation. It is invalid as soon as its Session
// membership, generation, Transport incarnation, or render lease changes.
type attachmentCapability struct {
	sess       *session
	ac         *attachedClient
	generation uint64
	transport  transportSnapshot
	lease      *attachmentLease
	// rebase marks a cross-session move whose first paint starts a dependency-
	// free output chain after publication releases architecture locks.
	rebase bool
}

func captureAttachmentCapability(entry *session, ac *attachedClient, tr ports.ServerConnection) attachmentCapability {
	if entry == nil || entry.core() == nil || ac == nil || tr == nil {
		return attachmentCapability{}
	}
	// Capture one concrete link incarnation before taking the session lock. The
	// same snapshot is revalidated below; do not take a second snapshot and
	// accidentally bind a replacement link.
	transport := ac.transportSnapshot()
	if transport.transport != tr {
		return attachmentCapability{}
	}
	if ac.beforeAttachmentCapabilityValidation != nil {
		ac.beforeAttachmentCapabilityValidation()
	}
	core := entry.core()
	core.mu.Lock()
	if !attachmentRegisteredLocked(entry, ac) || !ac.transportSnapshotCurrent(transport) {
		core.mu.Unlock()
		return attachmentCapability{}
	}
	generation := ac.lifecycle.generation.Load()
	token := attachmentCapability{
		sess:       entry,
		ac:         ac,
		generation: generation,
		transport:  transport,
	}
	if rc := core.coordinator.Load(); rc != nil {
		token.lease = rc.attachmentLease(ac)
	}
	core.mu.Unlock()
	if !token.current() {
		return attachmentCapability{}
	}
	ac.lifecycle.installInitialCapability(token)
	return token
}

func (s *session) captureAttachmentCapability(ac *attachedClient, tr ports.ServerConnection) attachmentCapability {
	return captureAttachmentCapability(s, ac, tr)
}

func (t attachmentCapability) sameIdentity(other attachmentCapability) bool {
	return t.sess != nil && t.ac != nil && t.sess == other.sess && t.ac == other.ac &&
		t.generation == other.generation && t.transport.transport == other.transport.transport &&
		t.transport.incarnation == other.transport.incarnation && t.lease == other.lease
}

func (t attachmentCapability) matchesConnectionSnapshot(sess *session, ac *attachedClient, transport transportSnapshot) bool {
	return t.sess == sess && t.ac == ac && ac != nil && t.transport == transport &&
		ac.lifecycle.identityCurrent(t)
}

func (l *attachmentLifecycle) identityCurrent(capability attachmentCapability) bool {
	return l != nil && capability.sess != nil && capability.ac != nil &&
		capability.ac.lifecycle.generation.Load() == capability.generation &&
		capability.ac.currentAttachmentSession() == capability.sess &&
		capability.ac.transportSnapshotCurrent(capability.transport)
}

// current validates every immutable identity captured by the capability.
// Membership is the sole authority; there is no owner, replacement, or
// compatibility role.
func (l *attachmentLifecycle) current(capability attachmentCapability) bool {
	if !l.identityCurrent(capability) || !attachmentRegistered(capability.sess, capability.ac) {
		return false
	}
	if capability.lease == nil {
		return true
	}
	rc := capability.sess.core().coordinator.Load()
	return rc != nil && capability.lease.attachment == capability.ac && rc.leaseCurrent(capability.lease, false)
}

func (t attachmentCapability) current() bool {
	return t.ac != nil && t.ac.lifecycle.current(t)
}

func beginAttachmentLeaseEffect(sess *session, ac *attachedClient, lease *attachmentLease) (*attachmentEffect, bool) {
	if sess == nil || ac == nil || lease == nil {
		return nil, false
	}
	token := captureAttachmentCapability(sess, ac, ac.transport())
	token.lease = lease
	return ac.beginAttachmentEffect(token)
}

func (t attachmentCapability) currentInSessionLocked(sess *session) bool {
	return t.sess == sess && t.ac != nil && t.ac.lifecycle.currentSessionLocked(t)
}

func (t attachmentCapability) currentInSessionAndLeaseLocked(sess *session, ac *attachedClient, coordinator *renderCoordinator) bool {
	return t.sess == sess && t.ac == ac && ac != nil &&
		ac.lifecycle.currentSessionAndLeaseLocked(t, coordinator)
}

// currentSessionLocked validates capability authority where sess.core().mu is
// already held. An admitted effect does not use this comparison: a transition
// cannot change identity until that effect ends.
func (l *attachmentLifecycle) currentSessionLocked(capability attachmentCapability) bool {
	if !l.identityCurrent(capability) || !attachmentRegisteredLocked(capability.sess, capability.ac) {
		return false
	}
	if capability.lease == nil {
		return true
	}
	rc := capability.sess.core().coordinator.Load()
	return rc != nil && capability.lease.attachment == capability.ac && rc.leaseCurrent(capability.lease, false)
}

// currentSessionAndLeaseLocked validates the same capability tuple when the
// caller already holds both the Session and coordinator locks.
func (l *attachmentLifecycle) currentSessionAndLeaseLocked(capability attachmentCapability, coordinator *renderCoordinator) bool {
	if !l.identityCurrent(capability) || !attachmentRegisteredLocked(capability.sess, capability.ac) {
		return false
	}
	if capability.lease == nil {
		return true
	}
	return coordinator != nil && capability.lease.attachment == capability.ac &&
		coordinator.leaseCurrentLocked(capability.lease, false)
}
