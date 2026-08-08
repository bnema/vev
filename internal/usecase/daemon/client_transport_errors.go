package daemon

import "github.com/bnema/vev/internal/ports"

// clientGone detaches ac if it is still the session's current client. The
// session remains registered and headless after the client is gone.
func (d *Daemon) clientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	d.clientGoneWithNotice(sess, ac, failed, explicit, true)
}

func (d *Daemon) clientGoneWithoutNotice(sess *session, ac *attachedClient, failed ports.Transport, explicit bool) {
	d.clientGoneWithNotice(sess, ac, failed, explicit, false)
}

func (d *Daemon) clientGoneWithNotice(sess *session, ac *attachedClient, failed ports.Transport, explicit, notice bool) {
	if sess == nil || ac == nil {
		return
	}
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
	d.finishClientGone(sess, ac, failed, explicit, notice)
}

func (d *Daemon) clientGoneForAttachment(token attachmentConnectionToken, explicit bool) bool {
	if token.effect == nil {
		return false
	}
	token.effect.bindActionEnd(d, "detach")
	token.effect.End()
	sess := token.sess
	if sess == nil {
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
	d.finishClientGone(sess, token.ac, token.transport.transport, explicit, true)
	return true
}

func (d *Daemon) finishClientGone(sess *session, ac *attachedClient, failed ports.Transport, explicit, notice bool) {
	if sess == nil || ac == nil {
		return
	}
	if d.afterClientGoneDetach != nil {
		d.afterClientGoneDetach()
	}
	if rc := sess.renderCoordinator(); rc != nil {
		rc.noteDetach(ac)
	}
	d.recalculateSessionGeometryAndInvalidate(sess, nil, "client_transport_errors.go")
	sess.mu.Lock()
	ephemeral := sess.ephemeral
	name := sess.name
	sess.mu.Unlock()
	if !ephemeral {
		d.refreshSessionCwd(sess)
	}
	d.log.Info("client detach begin", "session", name, "explicit", explicit, "ephemeral", ephemeral)
	oldTr := failed
	if oldTr == nil {
		oldTr = ac.transport()
	}
	if !explicit && d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(oldTr)
		d.log.Info("client parked", "session", sess.nameSnapshot())
		return
	}
	// Explicit winners (and non-explicit park failures) must drop any same-
	// attachment parking marker left by a raced non-explicit teardown so
	// IntentResume waiters are not stranded on a never-published park.
	d.clearParkingInFlight(d.resumeTokenSnapshot(ac), ac)
	d.closePicker(ac)

	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	if explicit && notice {
		// Synchronous so the ack is delivered before the transport closes
		// (the client is actively awaiting it), but deadline-bounded so a
		// wedged client cannot pin this conn handler and hang Serve's
		// connWg.Wait.
		d.boundedSend(ac, frameDetached(ports.ReasonDetach))
	}
	_ = ac.closeCapturedTransport(oldTr)
	d.log.Info("client detached", "session", sess.nameSnapshot(), "explicit", explicit)
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

func (d *Daemon) detachOnAttachmentSendError(token attachmentConnectionToken, failed ports.Transport) {
	d.detachOnAttachmentSendErrorUntil(token, failed, nil)
}

func (d *Daemon) detachOnAttachmentSendErrorUntil(token attachmentConnectionToken, failed ports.Transport, done func() <-chan struct{}) {
	// A delayed sender may report after the client has rebound. Only the exact
	// transport captured by this attachment is allowed to detach either lifecycle.
	if failed == nil || failed != token.transport.transport {
		return
	}
	sess := token.sess
	if sess == nil {
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
	d.recalculateSessionGeometryAndInvalidate(sess, nil, "client_transport_errors.go")
	if d.parkAttachment(sess, ac) {
		_ = ac.closeCapturedTransport(failed)
		d.log.Warn("parked client after send error", "session", sess.nameSnapshot())
		return
	}
	d.clearParkingInFlight(d.resumeTokenSnapshot(ac), ac)
	d.closePicker(ac)
	d.resetScreenDefaultColors(sess)
	ac.clearPreviousSession()
	_ = ac.closeCapturedTransport(failed)
	d.log.Warn("detached client after send error", "session", sess.nameSnapshot())
}
