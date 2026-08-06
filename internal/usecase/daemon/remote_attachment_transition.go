package daemon

// transitionToRemoteView publishes an attachment-owned remote surface without
// changing the local session registry. Candidate construction and remote-link
// I/O deliberately happen before this function; this function only performs
// the frozen, exact local ownership hand-off.
func (d *Daemon) transitionToRemoteView(token attachmentConnectionToken, target *remoteView) (attachmentConnectionToken, error) {
	source := token.localSession()
	if d == nil || source == nil || token.ac == nil || target == nil {
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	if token.effect != nil {
		d.endActionAttachmentEffect(token.effect, "remote-view-transition")
	}
	frozen := freezeAttachmentEffectGates(token.ac)
	defer frozen.unfreeze()
	if !frozen.acquired || !frozen.drained {
		return attachmentConnectionToken{}, errAttachmentTransition
	}

	d.mu.Lock()
	if d.closing || d.sessions[source.id] != source || !d.attachmentOwnerRegisteredByDaemonLocked(target) {
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	d.notices.routingMu.Lock()
	source.mu.Lock()
	target.mu.Lock()
	current := sameAttachmentOwner(token.owner, source) &&
		token.generation == token.ac.connectionGeneration.Load() &&
		sameAttachmentOwner(token.ac.currentAttachmentOwner(), source) &&
		token.ac.transportSnapshotCurrent(token.transport) &&
		attachmentRegisteredLocked(source, token.ac) &&
		!target.closed
	if !current {
		target.mu.Unlock()
		source.mu.Unlock()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	if !target.registerAttachmentLocked(token.ac) {
		target.mu.Unlock()
		source.mu.Unlock()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	unregisterAttachmentSessionLocked(source, token.ac)
	generation := token.ac.connectionGeneration.Add(1)
	token.ac.setAttachmentOwner(target)
	published := attachmentConnectionToken{
		owner:      target,
		ac:         token.ac,
		generation: generation,
		transport:  token.transport,
		rebase:     true,
	}
	token.ac.publishFrozenAttachmentCapability(published)
	target.mu.Unlock()
	source.mu.Unlock()
	d.notices.routingMu.Unlock()
	d.mu.Unlock()

	// The old coordinator is no longer a lease owner for this attachment. Its
	// cleanup runs after publication and outside architecture locks.
	if coordinator := source.renderCoordinator(); coordinator != nil {
		coordinator.noteDetach(token.ac)
	}
	d.recalculateSessionGeometryAndInvalidateAsync(source, "remote_attachment_transition.go")
	token.ac.recordPreviousOwner(source)
	return published, nil
}
