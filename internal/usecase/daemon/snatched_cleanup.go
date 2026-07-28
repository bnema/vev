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

	token.ac.clearSnatchedInput()
	d.unregisterPreview(token.ac)
	if terminal {
		token.ac.clearPreviousSession()
	}
	token.ac.setSession(nil)
	token.ac.clearCaptureFrames()
	return true
}

// parkSnatchedAttachment removes only the exact lost snatched incarnation and
// retains its resume identity for a role-preserving reconnect.
func (d *Daemon) parkSnatchedAttachment(token attachmentRoleToken) bool {
	if token.ac == nil || !token.ac.resumeCapable || !d.unrouteSnatchedAttachment(token, false) {
		return false
	}
	if d.parkAttachmentAs(token.sess, token.ac, attachmentSnatched) {
		_ = token.ac.closeCapturedTransport(token.ac.revokeTransport(token.transport.transport))
		return true
	}
	token.ac.clearPreviousSession()
	_ = token.ac.closeCapturedTransport(token.ac.revokeTransport(token.transport.transport))
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

	token.ac.setSession(nil)
	if token.ac.resumeCapable && d.parkAttachmentAs(token.sess, token.ac, attachmentSnatched) {
		_ = token.ac.closeCapturedTransport(token.ac.revokeTransport(token.transport.transport))
		token.ac.clearSnatchedInput()
		d.unregisterPreview(token.ac)
		return true
	}
	_ = token.ac.closeCapturedTransport(token.ac.revokeTransport(token.transport.transport))
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
