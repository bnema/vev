package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func (ac *attachedClient) offerSamePeerTarget(target protocol.ExactSessionTarget) {
	ac.samePeerOfferMu.Lock()
	ac.samePeerOffer = &target
	ac.samePeerOfferMu.Unlock()
}

func (ac *attachedClient) clearSamePeerOffer() {
	ac.samePeerOfferMu.Lock()
	ac.samePeerOffer = nil
	ac.samePeerOfferMu.Unlock()
}

// consumeSamePeerOffer linearizes the client confirmation with the daemon's
// endpoint-empty offer. A client cannot select an arbitrary local session by
// manufacturing this frame.
func (ac *attachedClient) consumeSamePeerOffer(target protocol.ExactSessionTarget) bool {
	ac.samePeerOfferMu.Lock()
	defer ac.samePeerOfferMu.Unlock()
	if ac.samePeerOffer == nil || *ac.samePeerOffer != target {
		return false
	}
	ac.samePeerOffer = nil
	return true
}

// switchSamePeerForAttachment commits one client-confirmed endpoint-empty
// target. The request never names an endpoint: its authority is the current
// authenticated attachment plus the target's exact lifecycle identity.
func (d *Daemon) switchSamePeerForAttachment(effect *attachmentEffect, request protocol.SamePeerSwitchRequest) {
	if err := request.Validate(); err != nil || !effect.current() || effect.sess == nil || effect.ac == nil {
		return
	}
	if !effect.ac.consumeSamePeerOffer(request.Target) {
		d.sendSamePeerSwitchFailure(effect, request.RequestID, protocol.SamePeerSwitchStaleTarget)
		return
	}

	target, targetTabIndex, ok := d.samePeerTarget(request)
	if !ok || target == effect.sess {
		d.sendSamePeerSwitchFailure(effect, request.RequestID, protocol.SamePeerSwitchStaleTarget)
		return
	}

	capability := effect.capability()
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            effect.sess,
		target:            target,
		next:              effect.ac,
		expectedTransport: effect.transport,
		sourceCapability:  &capability,
		sourceEffect:      effect,
		action:            "same-peer-switch",
		expectedTargetLifecycle: &attachmentLifecycleFence{
			name:             request.Target.SessionName,
			incarnation:      request.Target.LifecycleID,
			checkIncarnation: true,
		},
		activateTargetTab: true,
		targetTabIndex:    targetTabIndex,
		ready:             true,
	})
	if err != nil {
		d.sendSamePeerSwitchFailure(effect, request.RequestID, protocol.SamePeerSwitchStaleTarget)
		return
	}

	if fresh, admitted := effect.ac.beginAttachmentEffect(transition.published); admitted {
		d.closePicker(effect.ac)
		fresh.End()
	}
	d.touchMRU(target)
	d.deferAttachmentTransitionCleanups(transition)
	d.firstPaintForTransition(transition.published)
}

// samePeerTarget resolves the exact target and the client-owned tab cursor
// under the daemon/session locks. A missing cursor deliberately falls back to
// the target session's current default rather than leaking the source view.
func (d *Daemon) samePeerTarget(request protocol.SamePeerSwitchRequest) (*session, int, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	target := d.findByNameLocked(request.Target.SessionName)
	if target == nil {
		return nil, 0, false
	}
	target.mu.Lock()
	defer target.mu.Unlock()
	if target.incarnation != request.Target.LifecycleID || len(target.tabs) == 0 {
		return nil, 0, false
	}
	tabIndex := 0
	if request.PreferredTabID == "" {
		return target, tabIndex, true
	}
	for index, tab := range target.tabs {
		if tab != nil && domain.TabStableID(tab.stableID) == request.PreferredTabID {
			return target, index, true
		}
	}
	return target, tabIndex, true
}

func (d *Daemon) sendSamePeerSwitchFailure(effect *attachmentEffect, requestID uint64, code protocol.SamePeerSwitchFailureCode) {
	if effect == nil || effect.ac == nil {
		return
	}
	sender := effect
	if !sender.current() {
		var admitted bool
		sender, admitted = effect.ac.beginAttachmentEffect(effect.capability())
		if !admitted {
			return
		}
		defer sender.End()
	}
	failure := protocol.SamePeerSwitchFailure{RequestID: requestID, Code: code}
	if failure.Validate() != nil {
		return
	}
	if err := sender.sendControl(failure); err != nil {
		d.detachOnAttachmentSendError(sender.capability(), sender.transport.transport)
	}
}
