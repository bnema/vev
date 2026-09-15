package daemon

import (
	"sync"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

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
	refreshObservations := false
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{done: deadline.Done, afterFrozen: func(ac *attachedClient) {
		if d.afterAttachmentEffectGateFrozen != nil {
			d.afterAttachmentEffectGateFrozen("suspend", ac)
		}
	}}, ac)
	defer func() {
		frozen.unfreeze()
		if refreshObservations {
			d.reconcileAllRouteHistories()
		}
	}()
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
	if err := d.boundedSendErr(ac, protocol.AttachmentSuspended{RequestID: request.RequestID, Target: target}); err != nil {
		return err
	}
	// Arm the daemon-owned safety expiry only after the ACK is on the wire and
	// while the transition is still frozen, so a racing activation must either
	// observe the record or already own this gate.
	d.armSuspendedExpiry(ac, sess)
	refreshObservations = true
	return nil
}

func (d *Daemon) activateAttachment(ac *attachedClient, expected transportSnapshot, request protocol.ActivateAttachment) error {
	if ac == nil || request.Validate() != nil {
		return errAttachmentTransition
	}
	deadline := newAttachmentEffectDrainDeadline(d.clock)
	defer deadline.stop()
	refreshObservations := false
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{done: deadline.Done}, ac)
	defer func() {
		frozen.unfreeze()
		if refreshObservations {
			d.reconcileAllRouteHistories()
		}
	}()
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
	geometry := request.Geometry()
	ac.setGeometry(geometry)
	rc := d.ensureRenderCoordinatorPrelocked(sess)
	rc.mu.Lock()
	lease := rc.attachWithReadinessLocked(ac, true)
	rc.mu.Unlock()
	sess.geometry.claimLocked(sess.core(), ac, geometry)
	// A committed activation ends the suspended lifetime: cancel the daemon-owned
	// safety expiry before the new capability generation is published.
	d.clearSuspendedExpiryLocked(ac)
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
	paintDone := make(chan struct{})
	paintTimer := d.clock.NewTimer(detachNotifyTimeout)
	go func() {
		select {
		case <-paintTimer.C():
			select {
			case <-paintDone:
				return
			default:
			}
			d.log.Warn("activation publication timed out; force closing client transport")
			_ = ac.closeCapturedTransport(expected.transport)
		case <-paintDone:
			paintTimer.Stop()
		}
	}()
	paintResult := d.paintWithActivationEffect(sess, ac, true, lease, effect)
	close(paintDone)
	if paintResult != paintEmitted || !effect.current() {
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
	refreshObservations = true
	return nil
}

// suspendedAttachmentRetention is the daemon-owned safety expiry for exactly one
// suspended attachment incarnation. generation and transport pin the suspension
// that armed it, so a stale record can never retire a later activation, a
// rebound transport, or a different session lifetime.
type suspendedAttachmentRetention struct {
	ac         *attachedClient
	sess       *session
	generation uint64
	transport  transportSnapshot
	timer      ports.Timer
	done       chan struct{}
	doneOnce   sync.Once
}

// stop ends the record exactly once: the timer is cancelled and any watcher is
// released. It is safe to call from the watcher itself.
func (r *suspendedAttachmentRetention) stop() {
	if r == nil {
		return
	}
	if r.timer != nil {
		r.timer.Stop()
	}
	r.closeDone()
}

func (r *suspendedAttachmentRetention) closeDone() {
	if r == nil || r.done == nil {
		return
	}
	r.doneOnce.Do(func() { close(r.done) })
}

// armSuspendedExpiry publishes the daemon-owned safety expiry for a suspension
// that has already committed and acknowledged. The caller must still hold ac's
// effect-gate freeze, so a racing activation is serialized behind the same gate
// and cannot miss the record. The watcher is admitted from the connection
// handler, so daemon shutdown joins it only after connWg drains: no Add can race
// the terminal Wait.
func (d *Daemon) armSuspendedExpiry(ac *attachedClient, sess *session) {
	if d == nil || ac == nil || sess == nil || d.suspendedSafetyExpiry <= 0 {
		return
	}
	transport := ac.transportSnapshot()
	if transport.transport == nil {
		return
	}
	record := &suspendedAttachmentRetention{
		ac:         ac,
		sess:       sess,
		generation: ac.lifecycle.generationValue(),
		transport:  transport,
		timer:      d.clock.NewTimer(d.suspendedSafetyExpiry),
		done:       make(chan struct{}),
	}
	d.mu.Lock()
	if d.closing {
		d.mu.Unlock()
		record.stop()
		return
	}
	// Re-suspension replaces any prior record for this attachment; the stale
	// watcher must not outlive the suspension that armed it.
	if previous := d.suspended[ac]; previous != nil {
		previous.stop()
	}
	d.suspended[ac] = record
	d.attachmentCleanupWg.Add(1)
	d.mu.Unlock()
	go func() {
		defer d.attachmentCleanupWg.Done()
		select {
		case <-record.timer.C():
			d.expireSuspendedAttachment(record)
		case <-record.done:
		}
	}()
}

// clearSuspendedExpiryLocked cancels ac's safety expiry. Caller holds d.mu.
func (d *Daemon) clearSuspendedExpiryLocked(ac *attachedClient) {
	if d == nil || ac == nil {
		return
	}
	record := d.suspended[ac]
	if record == nil {
		return
	}
	delete(d.suspended, ac)
	record.stop()
}

// clearSuspendedExpiry cancels ac's safety expiry. Callers must not hold daemon,
// session, or routing locks.
func (d *Daemon) clearSuspendedExpiry(ac *attachedClient) {
	if d == nil || ac == nil {
		return
	}
	d.mu.Lock()
	d.clearSuspendedExpiryLocked(ac)
	d.mu.Unlock()
}

// purgeSuspendedForSessionLocked cancels every safety expiry owned by sess.
// Caller holds d.mu.
func (d *Daemon) purgeSuspendedForSessionLocked(sess *session) {
	if d == nil || sess == nil {
		return
	}
	for ac, record := range d.suspended {
		if record.sess != sess {
			continue
		}
		delete(d.suspended, ac)
		record.stop()
	}
}

// purgeAllSuspendedLocked cancels every safety expiry. Caller holds d.mu.
func (d *Daemon) purgeAllSuspendedLocked() {
	if d == nil {
		return
	}
	for ac, record := range d.suspended {
		delete(d.suspended, ac)
		record.stop()
	}
}

// expireSuspendedAttachment force-retires one suspended attachment whose
// daemon-owned safety timer fired. It freezes and drains the exact gate before
// taking any architecture lock, then revalidates the record, lifecycle
// generation, transport incarnation, suspended activity, and session
// registration under that same freeze. The retained session name is
// deliberately not compared: a rename keeps the incarnation and must not
// postpone terminal eviction. Expiry is a no-notice terminal eviction with no
// resume credential, and it leaves the session to existing headless lifetime
// policy.
func (d *Daemon) expireSuspendedAttachment(record *suspendedAttachmentRetention) {
	if d == nil || record == nil || record.ac == nil {
		return
	}
	ac := record.ac
	if d.beforeSuspendedExpiryFreeze != nil {
		d.beforeSuspendedExpiryFreeze(ac)
	}
	// Freeze and drain without a bound: terminal eviction acts on a suspended
	// attachment whose gate has no admitted effects, and a gate owner always
	// releases, so waiting here can never strand the expiry the way an expired
	// acquisition budget would.
	frozen := freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{afterFrozen: func(ac *attachedClient) {
		if d.afterAttachmentEffectGateFrozen != nil {
			d.afterAttachmentEffectGateFrozen("expire", ac)
		}
	}}, ac)
	if !frozen.acquired || !frozen.drained {
		frozen.unfreeze()
		return
	}
	d.mu.Lock()
	sess := record.sess
	detached := false
	if d.suspended[ac] == record && !d.closing && sess != nil && d.sessions[sess.id] == sess && ac.currentAttachmentSession() == sess {
		sess.mu.Lock()
		ac.lifecycle.mu.Lock()
		current := ac.lifecycle.activity == attachmentSuspended &&
			ac.lifecycle.generationValue() == record.generation &&
			ac.transportSnapshotCurrent(record.transport) &&
			attachmentRegisteredLocked(sess, ac)
		ac.lifecycle.mu.Unlock()
		if current {
			detached = detachFrozenAttachmentLocked(sess, ac)
		}
		sess.mu.Unlock()
		if detached {
			delete(d.suspended, ac)
		}
	}
	d.mu.Unlock()
	frozen.unfreeze()
	if !detached {
		return
	}
	// The expiry owns the record now; release a watcher that may still be
	// selecting on the fired timer channel.
	record.stop()
	d.log.Warn("suspended attachment expired", "session", sess.nameSnapshot(), "expiry", d.suspendedSafetyExpiry)
	// Shared terminal eviction: geometry reconciliation, coordinator detach,
	// picker/palette/output cleanup, and the physical transport close all run
	// outside every architecture lock. Expiry never parks a resume credential.
	d.finishClientGone(sess, ac, record.transport.transport, true, false)
}
