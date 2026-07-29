package daemon

import "github.com/bnema/vev/internal/ports"

// clientGone detaches ac if it is still the session's current client. The
// session remains registered and headless after the client is gone.
func (d *Daemon) clientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	expected := transportSnapshot{}
	if failed != nil {
		expected = ac.transportSnapshot()
		if expected.transport != failed {
			return // stale connection loop; a newer transport owns this client
		}
	}
	if d.beforeClientGoneDetach != nil {
		d.beforeClientGoneDetach()
	}
	if !d.detachIfCurrentTransport(sess, ac, expected) {
		return // displaced, or the link was rebound after the precheck
	}
	d.finishClientGone(sess, ac, failed, explicit)
}

func (d *Daemon) clientGoneForRole(token attachmentRoleToken, explicit bool) bool {
	if token.effect == nil {
		return false
	}
	token.effect.bindActionEnd(d, "detach")
	token.effect.End()
	if !d.detachIfRoleCurrent(token) {
		return false
	}
	d.finishClientGone(token.sess, token.ac, token.transport.transport, explicit)
	return true
}

func (d *Daemon) finishClientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	d.unregisterPreview(ac)
	sess.mu.Lock()
	ephemeral := sess.ephemeral
	sess.mu.Unlock()
	if !ephemeral {
		d.refreshSessionCwd(sess)
	}
	d.log.Info("client detach begin", "session", sess.name, "explicit", explicit, "ephemeral", ephemeral)
	oldTr := failed
	if oldTr == nil {
		oldTr = ac.transport()
	}
	if !explicit && d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(oldTr)
		d.log.Info("client parked", "session", sess.name)
		return
	}

	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	if explicit {
		// Synchronous so the ack is delivered before the transport closes
		// (the client is actively awaiting it), but deadline-bounded so a
		// wedged client cannot pin this conn handler and hang Serve's
		// connWg.Wait.
		d.boundedSend(ac, frameDetached(ports.ReasonDetach))
	}
	_ = ac.closeCapturedTransport(oldTr)
	d.log.Info("client detached", "session", sess.name, "explicit", explicit)
}

// detachOnSendError drops a client whose transport failed, leaving the session
// registered and headless.
func (d *Daemon) detachOnSendError(sess *session, ac *attachedClient, failed ports.Transport) {
	expected := transportSnapshot{}
	if failed != nil {
		expected = ac.transportSnapshot()
		if expected.transport != failed {
			return
		}
	}
	if d.detachIfCurrentTransport(sess, ac, expected) {
		d.finishSendErrorDetach(sess, ac, failed)
	}
}

func (d *Daemon) detachOnRoleSendError(token attachmentRoleToken, failed ports.Transport) {
	d.detachOnRoleSendErrorUntil(token, failed, nil)
}

func (d *Daemon) detachOnRoleSendErrorUntil(token attachmentRoleToken, failed ports.Transport, done func() <-chan struct{}) {
	if failed != token.transport.transport {
		return
	}
	if d.detachIfRoleCurrentUntil(token, done) {
		d.finishSendErrorDetach(token.sess, token.ac, failed)
	}
}

// reserveRoleSendErrorCleanup accounts for cleanup before End releases the
// role gate. This closes the WaitGroup Add/Wait race with terminal teardown;
// the returned launch function must be invoked immediately after ticket End.
func (d *Daemon) reserveRoleSendErrorCleanup(token attachmentRoleToken, failed ports.Transport) func() {
	d.attachmentCleanupWg.Add(1)
	return func() {
		go func() {
			defer d.attachmentCleanupWg.Done()
			if d.afterRoleSendErrorCleanup != nil {
				defer d.afterRoleSendErrorCleanup()
			}
			if d.beforeRoleSendErrorCleanup != nil {
				d.beforeRoleSendErrorCleanup(token)
			}
			deadline := newRoleEffectDrainDeadline(d.clock)
			defer deadline.stop()
			d.detachOnRoleSendErrorUntil(token, failed, deadline.Done)
		}()
	}
}

func (d *Daemon) finishSendErrorDetach(sess *session, ac *attachedClient, failed ports.Transport) {
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	d.unregisterPreview(ac)
	if d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(failed)
		d.log.Warn("parked client after send error", "session", sess.name)
		return
	}
	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	_ = ac.closeCapturedTransport(failed)
	d.log.Warn("detached client after send error", "session", sess.name)
}
