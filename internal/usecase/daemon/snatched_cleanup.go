package daemon

// removeSnatchedAttachment removes exactly one role generation and transport
// from session routing. It never touches the active attachment or closes a
// transport, allowing callers that already interrupted a blocked send to avoid
// closing the same link twice.
func (d *Daemon) removeSnatchedAttachment(token attachmentRoleToken) bool {
	return d.unrouteSnatchedAttachment(token, true)
}

func (d *Daemon) unrouteSnatchedAttachment(token attachmentRoleToken, terminal bool) bool {
	if token.sess == nil || token.ac == nil || token.transport.transport == nil {
		return false
	}
	frozen := freezeRoleEffectGates(token.ac)
	defer frozen.unfreeze()
	d.notices.routingMu.Lock()
	core := token.sess.core()
	core.mu.Lock()
	current := attachmentSessionRoleLocked(token.sess, token.ac) == attachmentSnatched &&
		token.ac.roleGeneration.Load() == token.generation &&
		token.ac.transportSnapshotCurrent(token.transport)
	if current {
		delete(core.snatched, token.ac)
		token.ac.roleGeneration.Add(1)
		token.ac.invalidateFrozenRoleCapability()
	}
	core.mu.Unlock()
	d.notices.routingMu.Unlock()
	if !current {
		return false
	}

	token.ac.clearSnatchedInput()
	d.unregisterPreview(token.ac)
	if terminal {
		token.ac.clearPreviousSession()
	}
	token.ac.setSession(nil)
	token.ac.clearCaptureFrames()
	if terminal {
		// A losing park path may have published a same-attachment marker before
		// this terminal unroute won. Retire only that attachment's marker so
		// waiters fail closed instead of hanging; park paths (terminal=false)
		// keep the marker until parkAttachmentAs or abandonment cleanup.
		d.clearParkingInFlight(token.ac.resumeToken, token.ac)
	}
	return true
}

// parkSnatchedAttachment removes only the exact lost snatched incarnation and
// retains its resume identity for a role-preserving reconnect.
func (d *Daemon) parkSnatchedAttachment(token attachmentRoleToken) bool {
	sess, ok := localSession(token.sess)
	if !ok || token.ac == nil || !token.ac.resumeCapable {
		return false
	}
	// Advertise parking before unroute so IntentResume never observes both the
	// snatched seat and parking/parked registries empty for a still-valid token.
	parkingToken := d.markParkingInFlight(sess, token.ac)
	if !d.unrouteSnatchedAttachment(token, false) {
		d.clearParkingInFlightIfAbandoned(sess, token.ac, parkingToken)
		return false
	}
	if d.afterSnatchedUnrouteBeforePark != nil {
		d.afterSnatchedUnrouteBeforePark()
	}
	if d.parkAttachmentAs(sess, token.ac, attachmentSnatched) {
		captured := token.transport.transport
		_ = token.ac.revokeTransport(captured)
		_ = token.ac.closeCapturedTransport(captured)
		return true
	}
	d.clearParkingInFlight(parkingToken, token.ac)
	token.ac.clearPreviousSession()
	captured := token.transport.transport
	_ = token.ac.revokeTransport(captured)
	_ = token.ac.closeCapturedTransport(captured)
	return false
}

func (d *Daemon) parkOrDropSnatchedAttachment(token attachmentRoleToken) bool {
	if d.parkSnatchedAttachment(token) {
		return true
	}
	return d.dropSnatchedAttachment(token)
}

// cleanupInterruptedSnatchedAttachment owns terminal cleanup for a displaced
// transport that was closed to drain an admitted send. It waits on the gate's
// condition rather than ordinary effect admission, then removes only the exact
// published snatched incarnation. Attachment-local cleanup runs without routing
// or session locks while the terminal capability remains frozen.
func (d *Daemon) cleanupInterruptedSnatchedAttachment(token attachmentRoleToken) bool {
	sess, ok := localSession(token.sess)
	if !ok || token.ac == nil || token.transport.transport == nil {
		return false
	}
	frozen := freezeRoleEffectGates(token.ac)
	defer frozen.unfreeze()

	d.notices.routingMu.Lock()
	sess.mu.Lock()
	current := attachmentSessionRoleLocked(token.sess, token.ac) == attachmentSnatched &&
		token.ac.currentAttachmentSession() == token.sess && token.ac.roleGeneration.Load() == token.generation &&
		token.ac.transportSnapshotCurrent(token.transport)
	sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	if !current {
		return false
	}

	// Advertise parking before ownership removal so IntentResume can wait out
	// the unroute→park gap for a still-valid snatched credential.
	var parkingToken uint64
	if token.ac.resumeCapable {
		parkingToken = d.markParkingInFlight(sess, token.ac)
	}

	// The frozen gate prevents a promotion from publishing between overlay
	// cleanup and terminal registry publication.
	d.clearForSnatch(token)

	d.notices.routingMu.Lock()
	sess.mu.Lock()
	current = attachmentSessionRoleLocked(token.sess, token.ac) == attachmentSnatched &&
		token.ac.currentAttachmentSession() == token.sess && token.ac.roleGeneration.Load() == token.generation &&
		token.ac.transportSnapshotCurrent(token.transport)
	if current {
		delete(sess.snatched, token.ac)
		token.ac.roleGeneration.Add(1)
		token.ac.invalidateFrozenRoleCapability()
	}
	sess.mu.Unlock()
	d.notices.routingMu.Unlock()
	if !current {
		d.clearParkingInFlightIfAbandoned(sess, token.ac, parkingToken)
		return false
	}

	token.ac.setSession(nil)
	if d.afterSnatchedUnrouteBeforePark != nil {
		d.afterSnatchedUnrouteBeforePark()
	}
	if token.ac.resumeCapable && d.parkAttachmentAs(sess, token.ac, attachmentSnatched) {
		captured := token.transport.transport
		_ = token.ac.revokeTransport(captured)
		_ = token.ac.closeCapturedTransport(captured)
		token.ac.clearSnatchedInput()
		d.unregisterPreview(token.ac)
		return true
	}
	d.clearParkingInFlight(parkingToken, token.ac)
	captured := token.transport.transport
	_ = token.ac.revokeTransport(captured)
	_ = token.ac.closeCapturedTransport(captured)
	token.ac.clearSnatchedInput()
	d.unregisterPreview(token.ac)
	token.ac.clearPreviousSession()
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

// deferAttachmentTransitionCleanups retires coordinator timers and accounting
// after ownership publication. Cleanup never gates the attaching handshake. A
// displaced render that already owns sendMu is interrupted by closing only its
// captured transport; an idle healthy snatched transport remains connected and
// receives a dependency-free reset panel.
func (d *Daemon) deferAttachmentTransitionCleanups(result attachmentTransitionResult) {
	if token := result.displaced; token.ac != nil && token.transport.transport != nil {
		blockedRender := result.displacedInterrupted
		if !blockedRender && !token.ac.initialSnatchedPanelClaimed(token.generation) {
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
			if err := d.sendInitialSnatchedPanel(token, ticket); err != nil {
				ticket.End()
				d.parkOrDropSnatchedAttachment(token)
			}
		})
	}
	for _, cleanup := range result.cleanups {
		d.attachmentCleanupWg.Go(cleanup.finish)
	}
}
