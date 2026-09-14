package daemon

import "github.com/bnema/vev/internal/protocol"

func (ac *attachedClient) attachmentActivity() attachmentActivity {
	ac.lifecycle.mu.Lock()
	defer ac.lifecycle.mu.Unlock()
	return ac.lifecycle.activity
}

// Suspension retains membership for transport ownership and session teardown,
// but removes the render lease and geometry claim. The lifecycle gate, rather
// than membership alone, admits all ordinary attachment effects.
func (d *Daemon) suspendAttachment(token attachmentCapability, request protocol.SuspendAttachment) error {
	if request.Validate() != nil || token.ac == nil {
		return errAttachmentTransition
	}
	ac, sess := token.ac, token.sess
	deadline := newAttachmentEffectDrainDeadline(d.clock)
	defer deadline.stop()
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{done: deadline.Done, afterFrozen: func(ac *attachedClient) {
		if d.afterAttachmentEffectGateFrozen != nil {
			d.afterAttachmentEffectGateFrozen("suspend", ac)
		}
	}}, ac)
	defer frozen.unfreeze()
	if !frozen.acquired || !frozen.drained || !d.sourceAttachmentCapabilityCurrentFrozen(token) || ac.attachmentActivity() != attachmentActive {
		return errAttachmentTransition
	}
	d.mu.Lock()
	sess.mu.Lock()
	if d.closing || d.sessions[sess.id] != sess || !token.currentInSessionLocked(sess) {
		sess.mu.Unlock()
		d.mu.Unlock()
		return errAttachmentTransition
	}
	target := protocol.ExactSessionTarget{LifecycleID: sess.incarnation, SessionName: sess.name}
	var cleanup renderLifecycleCleanup
	if rc := sess.coordinator.Load(); rc != nil {
		cleanup = rc.beginDetach(ac)
	}
	sess.geometry.removeAttachmentLocked(sess.core(), ac)
	token.lease = nil
	ac.publishFrozenAttachmentCapability(token)
	ac.lifecycle.mu.Lock()
	ac.lifecycle.activity = attachmentSuspended
	ac.lifecycle.retained = target
	ac.lifecycle.mu.Unlock()
	sess.mu.Unlock()
	d.mu.Unlock()
	cleanup.finish()
	ac.clearSamePeerOffer()
	d.retirePicker(ac)
	d.closePalette(ac)
	sess.geometry.reconcileAndInvalidate(d, sess, nil, "attachment_suspension.go")
	// Ordered SendServer is a transport publication barrier after drained renders.
	return d.boundedSendErr(ac, protocol.AttachmentSuspended{RequestID: request.RequestID, Target: target})
}

func (d *Daemon) activateAttachment(ac *attachedClient, expected transportSnapshot, request protocol.ActivateAttachment) error {
	if ac == nil || request.Validate() != nil {
		return errAttachmentTransition
	}
	deadline := newAttachmentEffectDrainDeadline(d.clock)
	defer deadline.stop()
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{done: deadline.Done}, ac)
	defer frozen.unfreeze()
	if !frozen.acquired || !frozen.drained {
		return errAttachmentTransition
	}
	sess := ac.currentAttachmentSession()
	if sess == nil {
		return errAttachmentTransition
	}
	d.mu.Lock()
	sess.mu.Lock()
	ac.lifecycle.mu.Lock()
	valid := ac.lifecycle.activity == attachmentSuspended && ac.lifecycle.retained == request.Target
	ac.lifecycle.mu.Unlock()
	if !valid || d.closing || d.sessions[sess.id] != sess || sess.incarnation != request.Target.LifecycleID || sess.name != request.Target.SessionName || !attachmentRegisteredLocked(sess, ac) || !ac.transportSnapshotCurrent(expected) {
		sess.mu.Unlock()
		d.mu.Unlock()
		return errAttachmentTransition
	}
	ac.setGeometry(request.Geometry())
	rc := d.ensureRenderCoordinatorPrelocked(sess)
	rc.mu.Lock()
	lease := rc.attachWithReadinessLocked(ac, true)
	rc.mu.Unlock()
	sess.geometry.claimLocked(sess.core(), ac, request.Geometry())
	token := ac.publishFrozenAttachmentCapability(attachmentCapability{sess: sess, ac: ac, transport: expected, lease: lease})
	// Reserve the sole activation effect under the still-frozen gate. No ordinary
	// effect can enter until the full output and correlated success both commit.
	ac.lifecycle.mu.Lock()
	ac.lifecycle.activity = attachmentActivating
	ac.lifecycle.inFlight++
	ac.lifecycle.mu.Unlock()
	effect := &attachmentEffect{attachmentCapability: token, lifecycle: &ac.lifecycle}
	defer effect.End()
	sess.mu.Unlock()
	d.mu.Unlock()
	sess.geometry.reconcileAndInvalidate(d, sess, nil, "attachment_suspension.go")
	ac.sendMu.Lock()
	view := ac.viewSnapshot()
	view.windowRows = request.Size.Rows
	view.windowSet = true
	view.windowTop = 0
	view.revision++
	ac.publishView(view)
	ac.rebaseOutput()
	ac.pipelineCache = composeCacheInput{}
	ac.pipelineScratch = composeCacheInput{}
	ac.captureFrames = nil
	ac.sendMu.Unlock()
	if d.paintWithActivationEffect(sess, ac, true, lease, effect) != paintEmitted || !effect.current() {
		return errAttachmentTransition
	}
	ac.sendMu.Lock()
	ac.output.lockView()
	output := ac.output
	ack := protocol.AttachmentActivated{RequestID: request.RequestID, Identity: output.lastViewContext.Route, Epoch: output.epoch, State: output.next, ViewPublication: output.lastViewContext.Publication}
	ac.output.unlockView()
	ac.sendMu.Unlock()
	if ack.Validate() != nil {
		return errAttachmentTransition
	}
	if err := d.boundedSendErr(ac, ack); err != nil {
		return err
	}
	ac.lifecycle.mu.Lock()
	ac.lifecycle.activity = attachmentActive
	ac.lifecycle.retained = protocol.ExactSessionTarget{}
	ac.lifecycle.mu.Unlock()
	return nil
}
