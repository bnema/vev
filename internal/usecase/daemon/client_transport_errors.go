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
	// Advertise parking before detach so IntentResume never observes both the
	// live seat and parking/parked registries empty for a still-valid token.
	var parkingToken uint64
	if !explicit {
		parkingToken = d.markParkingInFlight(sess, ac)
	}
	if d.beforeClientGoneDetach != nil {
		d.beforeClientGoneDetach()
	}
	if !d.detachIfCurrentTransport(sess, ac, expected) {
		d.clearParkingInFlightIfAbandoned(sess, ac, parkingToken)
		return // displaced, or the link was rebound after the precheck
	}
	d.finishClientGone(sess, ac, failed, explicit)
}

func (d *Daemon) clientGoneForAttachment(token attachmentConnectionToken, explicit bool) bool {
	if token.effect == nil {
		return false
	}
	token.effect.bindActionEnd(d, "detach")
	token.effect.End()
	if proxy, ok := token.sess.(*proxySession); ok {
		if !d.detachIfAttachmentCurrent(token) {
			return false
		}
		d.finishProxyClientGone(proxy, token.ac, token.transport.transport, explicit)
		d.armProxyWarm(proxy)
		return true
	}
	sess, ok := localSession(token.sess)
	if !ok {
		return false
	}
	var parkingToken uint64
	if !explicit {
		parkingToken = d.markParkingInFlight(sess, token.ac)
	}
	if !d.detachIfAttachmentCurrent(token) {
		d.clearParkingInFlightIfAbandoned(sess, token.ac, parkingToken)
		return false
	}
	d.finishClientGone(sess, token.ac, token.transport.transport, explicit)
	return true
}

func (d *Daemon) finishClientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	if d.afterClientGoneDetach != nil {
		d.afterClientGoneDetach()
	}
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
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
	// Explicit winners (and non-explicit park failures) must drop any same-
	// attachment parking marker left by a raced non-explicit teardown so
	// IntentResume waiters are not stranded on a never-published park.
	d.clearParkingInFlight(ac.resumeToken, ac)
	d.closePicker(ac)

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
	parkingToken := d.markParkingInFlight(sess, ac)
	if d.detachIfCurrentTransport(sess, ac, expected) {
		d.finishSendErrorDetach(sess, ac, failed)
		return
	}
	d.clearParkingInFlightIfAbandoned(sess, ac, parkingToken)
}

// detachProxyOnSendError is detachOnSendError for an attachment that has no
// local session behind it. Proxy attachments own no resumable parking
// lifecycle, so the remote link is retained as a warm headless session instead
// of being parked for the same client to resume.
func (d *Daemon) detachProxyOnSendError(p *proxySession, ac *attachedClient, failed ports.Transport) {
	if d == nil || p == nil || ac == nil {
		return
	}
	expected := transportSnapshot{}
	oldTr := failed
	if failed != nil {
		expected = ac.transportSnapshot()
		if expected.transport != failed {
			return
		}
	} else {
		oldTr = ac.transport()
	}
	if !d.detachProxyIfCurrentTransport(p, ac, expected) {
		return
	}
	d.finishProxyClientGone(p, ac, oldTr, false)
}

// finishProxyClientGone retires external client ownership after the exact proxy
// attachment has been unpublished. Warm-timer publication remains with the caller so
// each detach path can arm once, after membership is empty.
func (d *Daemon) finishProxyClientGone(p *proxySession, ac *attachedClient, failed ports.Transport, explicit bool) {
	if rc := p.coordinator.Load(); rc != nil {
		rc.noteDetach(ac)
	}
	if ac.overlays != nil {
		d.closePicker(ac)
	}
	ac.clearPreviousSession()
	if explicit {
		d.boundedSend(ac, frameDetached(ports.ReasonDetach))
	}
	_ = ac.closeCapturedTransport(failed)
	d.log.Warn("detached proxy client", "host", p.key.Host, "session", p.key.Name, "explicit", explicit)
}

func (d *Daemon) detachOnAttachmentSendError(token attachmentConnectionToken, failed ports.Transport) {
	d.detachOnAttachmentSendErrorUntil(token, failed, nil)
}

func (d *Daemon) detachOnAttachmentSendErrorUntil(token attachmentConnectionToken, failed ports.Transport, done func() <-chan struct{}) {
	// A delayed sender may report after the client has rebound. Only the exact
	// transport captured by this attachment is allowed to detach either lifecycle.
	if failed == nil || failed != token.transport.transport {
		return
	}
	if proxy, ok := token.sess.(*proxySession); ok {
		if d.detachIfAttachmentCurrentUntil(token, done) {
			d.finishProxyClientGone(proxy, token.ac, failed, false)
			d.armProxyWarm(proxy)
		}
		return
	}
	sess, ok := localSession(token.sess)
	if !ok {
		return
	}
	parkingToken := d.markParkingInFlight(sess, token.ac)
	if d.detachIfAttachmentCurrentUntil(token, done) {
		d.finishSendErrorDetach(sess, token.ac, failed)
		return
	}
	d.clearParkingInFlightIfAbandoned(sess, token.ac, parkingToken)
}

// reserveAttachmentSendErrorCleanup accounts for cleanup before End releases the
// attachment gate. This closes the WaitGroup Add/Wait race with terminal teardown;
// the returned launch function must be invoked immediately after ticket End.
func (d *Daemon) reserveAttachmentSendErrorCleanup(token attachmentConnectionToken, failed ports.Transport) func() {
	d.attachmentCleanupWg.Add(1)
	return func() {
		go func() {
			defer d.attachmentCleanupWg.Done()
			if d.afterAttachmentSendErrorCleanup != nil {
				defer d.afterAttachmentSendErrorCleanup()
			}
			if d.beforeAttachmentSendErrorCleanup != nil {
				d.beforeAttachmentSendErrorCleanup(token)
			}
			deadline := newAttachmentEffectDrainDeadline(d.clock)
			defer deadline.stop()
			d.detachOnAttachmentSendErrorUntil(token, failed, deadline.Done)
		}()
	}
}

func (d *Daemon) finishSendErrorDetach(sess *session, ac *attachedClient, failed ports.Transport) {
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	if d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(failed)
		d.log.Warn("parked client after send error", "session", sess.name)
		return
	}
	d.clearParkingInFlight(ac.resumeToken, ac)
	d.closePicker(ac)
	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	_ = ac.closeCapturedTransport(failed)
	d.log.Warn("detached client after send error", "session", sess.name)
}
