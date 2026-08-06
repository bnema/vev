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
	d.activateRemoteView(target)

	// The old coordinator is no longer a lease owner for this attachment. Its
	// cleanup runs after publication and outside architecture locks.
	if coordinator := source.renderCoordinator(); coordinator != nil {
		coordinator.noteDetach(token.ac)
	}
	d.recalculateSessionGeometryAndInvalidateAsync(source, "remote_attachment_transition.go")
	token.ac.recordPreviousOwner(source)
	return published, nil
}

// transitionFromRemoteView moves an attachment from one exact daemon-owned
// remote view back to its previous local session. The remote view has no
// coordinator lease, so only the target session coordinator is attached during
// this frozen publication.
func (d *Daemon) transitionFromRemoteView(token attachmentConnectionToken, source *remoteView, target *session) (attachmentConnectionToken, error) {
	if d == nil || source == nil || target == nil || token.ac == nil || token.transport.transport == nil {
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	if token.effect != nil {
		d.endActionAttachmentEffect(token.effect, "remote-view-back")
	}
	frozen := freezeAttachmentEffectGates(token.ac)
	defer frozen.unfreeze()
	if !frozen.acquired || !frozen.drained {
		return attachmentConnectionToken{}, errAttachmentTransition
	}

	targetCore := target.core()
	if targetCore == nil {
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	d.mu.Lock()
	if d.closing || !d.attachmentOwnerRegisteredByDaemonLocked(source) || d.sessions[targetCore.id] != target {
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	d.notices.routingMu.Lock()
	target.mu.Lock()
	targetCoordinator := d.ensureAttachmentRenderCoordinatorPrelocked(target)
	if targetCoordinator == nil {
		target.mu.Unlock()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	targetCoordinator.mu.Lock()
	// remoteView.mu is last: remote owners must never acquire daemon, session,
	// or coordinator locks while they are held.
	source.mu.Lock()
	unlock := func() {
		source.mu.Unlock()
		targetCoordinator.mu.Unlock()
		target.mu.Unlock()
		d.notices.routingMu.Unlock()
		d.mu.Unlock()
	}
	isCurrent := func() bool {
		_, sourceRegistered := source.attachments[token.ac]
		return sameAttachmentOwner(token.owner, source) &&
			token.generation == token.ac.connectionGeneration.Load() &&
			sameAttachmentOwner(token.ac.currentAttachmentOwner(), source) &&
			token.ac.transportSnapshotCurrent(token.transport) &&
			sourceRegistered && !source.closed &&
			sameAttachmentOwner(token.ac.previousOwner.Get(), target) &&
			!attachmentRegisteredLocked(target, token.ac)
	}
	if !isCurrent() || targetCoordinator.torndown {
		unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	// Attach session membership before the coordinator lease. The attachment
	// gate is frozen and the local owner remains remote until the final owner
	// publication, so this temporary overlap is unobservable to effects.
	if !registerAttachmentSessionLocked(target, token.ac) {
		unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	lease := targetCoordinator.attachWithReadinessLocked(token.ac, true)
	if lease == nil {
		unregisterAttachmentSessionLocked(target, token.ac)
		unlock()
		return attachmentConnectionToken{}, errAttachmentTransition
	}
	source.unregisterAttachmentLocked(token.ac)
	generation := token.ac.connectionGeneration.Add(1)
	token.ac.setAttachmentOwner(target)
	published := attachmentConnectionToken{
		owner:      target,
		ac:         token.ac,
		generation: generation,
		transport:  token.transport,
		lease:      lease,
		rebase:     true,
	}
	token.ac.publishFrozenAttachmentCapability(published)
	unlock()

	// Record the reverse toggle while the attachment gate is still frozen, so a
	// concurrent handoff cannot observe the new local owner without its history.
	token.ac.recordPreviousOwner(source)
	d.parkRemoteViewWarm(source)
	return published, nil
}
