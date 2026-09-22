package client

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// Picker overlay over a live attachment (ADR 001 Amendment 1, Plan 003 A1).
//
// The daemon's session-picker command no longer ends the attachment: it sends
// a navigation PickerOffer and the attached worker asks the supervisor for the
// client picker. The supervisor then composes the same client-owned picker it
// presents with no attachment over the live one:
//
//   - The foreground's overlay slot suppresses attachment output (frames are
//     still applied and acknowledged) and diverts every authorized input
//     delivery to the picker, so no picker byte reaches the session and the
//     single terminal reader is never duplicated.
//   - Broker publications, previews, and resizes repaint the picker while the
//     overlay is open, exactly as in the plain picker.
//   - Cancel (Escape, q, Ctrl+C) and a commit to the attached session release
//     the slot; the worker then asks the daemon for an authoritative repaint.
//     The attachment is never reconnected.
//   - A commit to another target detaches the live attachment cleanly and
//     swaps to the committed request through Connecting.
//
// Every method runs on the supervisor's run goroutine.
type attachmentPickerOverlay struct {
	sup     *Supervisor
	run     *attachmentRun
	service ports.BrokerService
	request ports.BrokerOpenStreamRequest
	active  bool
	// swapping is set once a commit elsewhere asked the live attachment to
	// detach; the overlay keeps the terminal until the attachment settles.
	swapping bool
}

func (o *attachmentPickerOverlay) picker() pickerHost { return o.sup.cfg.Picker }

// changed is the broker publication wake while the overlay is presented.
func (o *attachmentPickerOverlay) changed() <-chan struct{} {
	if !o.active || supervisorNil(o.sup.readySub) {
		return nil
	}
	return o.sup.readySub.Changed()
}

func (o *attachmentPickerOverlay) previewChanged() <-chan struct{} {
	if !o.active {
		return nil
	}
	return o.sup.preview.changed()
}

func (o *attachmentPickerOverlay) ops() <-chan struct{} {
	if !o.active || o.swapping {
		return nil
	}
	return o.picker().OpsReady()
}

// invalidation is consumed only while the overlay owns the terminal; otherwise
// the attachment foreground does and the signal stays buffered.
func (o *attachmentPickerOverlay) invalidation() <-chan struct{} {
	if !o.active {
		return nil
	}
	return o.sup.presentationInvalidation()
}

// enter composes the picker over the attachment. It is refused unless the
// attachment is committed and presented, and while a swap is in progress.
func (o *attachmentPickerOverlay) enter() {
	s := o.sup
	picker := o.picker()
	if o.active || o.swapping || picker == nil || s.State().Presentation != PresentAttached {
		return
	}
	picker.ApplySnapshot(o.service.Snapshot())
	// A fresh overlay starts on the attached session's tab.
	s.setPickerCurrent(o.current())
	// A decision recorded before the overlay existed never applies to it.
	picker.TakeOp()
	picker.SetOwnsInput(true)
	if !s.attachments.beginNavigationOverlay(picker) {
		picker.SetOwnsInput(false)
		return
	}
	o.active = true
	s.invalidatePickerPresentation()
	s.transition(supervisorEvent{kind: supervisorOverlayOpened})
	s.refreshPreview(o.service)
	s.renderCurrent()
}

// exit returns the terminal to the same live attachment.
func (o *attachmentPickerOverlay) exit() {
	if !o.active {
		return
	}
	// Release the slot before the picker stops owning input, so a delivery
	// racing the cancel reaches the session instead of a picker that no
	// longer consumes it.
	o.sup.attachments.endNavigationOverlay()
	o.drop()
	o.sup.transition(supervisorEvent{kind: supervisorOverlayClosed})
}

// drop forgets the overlay without touching the foreground: the attachment
// settled, so its grant (and overlay slot) is already gone.
func (o *attachmentPickerOverlay) drop() {
	if !o.active {
		return
	}
	o.active = false
	o.sup.preview.close(o.picker())
	o.picker().SetOwnsInput(false)
}

func (o *attachmentPickerOverlay) applyPublication() {
	o.picker().ApplySnapshot(o.service.Snapshot())
	o.sup.refreshPreview(o.service)
	o.sup.renderCurrent()
}

func (o *attachmentPickerOverlay) publishPreview() {
	if o.sup.preview.publish(o.picker()) {
		o.sup.renderCurrent()
	}
}

func (o *attachmentPickerOverlay) resize() {
	o.sup.refreshPreview(o.service)
	o.sup.renderResizeInvalidation()
}

// takeOp carries out one picker decision over the live attachment.
func (o *attachmentPickerOverlay) takeOp() {
	s := o.sup
	op, key := o.picker().TakeOp()
	switch {
	case op.close:
		// Escape, q, and Ctrl+C all cancel back to the attachment: there is an
		// attachment to return to, so nothing here ends the process.
		o.exit()
	case op.commit && key != "":
		request, tab, err := s.resolveCommittedStreamRequest(o.service, key)
		if err != nil {
			// The picker already shows the bounded refusal; the attachment
			// stays live under the overlay.
			s.renderCurrent()
			return
		}
		if o.sameTarget(request) && tab.stopped == nil {
			// Another tab of the attached session switches in place; the
			// attachment is never reconnected for it.
			_, current, _ := s.attachments.committedView()
			if tab.preferred != "" && tab.preferred != current {
				s.attachments.requestTabSelection(o.run.token, tab.preferred)
			}
			o.exit()
			return
		}
		s.pendingSwap = &pickerAttachmentTarget{request: request, tab: tab}
		o.swapping = true
		s.preview.close(o.picker())
		s.attachments.requestDetach(o.run.token)
	case op.kill && key != "":
		s.startPickerKill(o.service, key)
		s.renderCurrent()
	default:
		s.refreshPreview(o.service)
		s.renderCurrent()
	}
}

// current names the attachment this overlay is presented over, so the picker
// opens on its session and tab.
func (o *attachmentPickerOverlay) current() pickerCurrent {
	target, tab, known := o.sup.attachments.committedView()
	if !known {
		if o.request.Admission != ports.BrokerAdmissionExact {
			return pickerCurrent{}
		}
		target = o.request.Target
	}
	if target.LifecycleID == (domain.SessionLifecycleID{}) {
		return pickerCurrent{}
	}
	return pickerCurrent{known: true, local: o.request.Local, endpoint: o.request.Endpoint, lifecycle: target.LifecycleID, tab: tab}
}

// sameTarget reports whether request names the session this attachment is
// already showing, in which case committing it is a close.
func (o *attachmentPickerOverlay) sameTarget(request ports.BrokerOpenStreamRequest) bool {
	return sameAttachmentTarget(o.request, o.sup.attachments.committedTargetOrZero(), request)
}

// sameAttachmentTarget compares one committed selection with the live
// attachment: same daemon authority and same session lifecycle.
func sameAttachmentTarget(current ports.BrokerOpenStreamRequest, committed protocol.ExactSessionTarget, next ports.BrokerOpenStreamRequest) bool {
	if next.Admission != ports.BrokerAdmissionExact {
		return false
	}
	if current.Local != next.Local || current.Endpoint != next.Endpoint {
		return false
	}
	if committed != (protocol.ExactSessionTarget{}) {
		return committed.LifecycleID == next.Target.LifecycleID
	}
	return current.Admission == ports.BrokerAdmissionExact && current.Target.LifecycleID == next.Target.LifecycleID
}
