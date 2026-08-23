package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func (ac *attachedClient) offerSamePeerTarget(target ports.ExactSessionTarget) {
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
func (ac *attachedClient) consumeSamePeerOffer(target ports.ExactSessionTarget) bool {
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
func (d *Daemon) switchSamePeerForAttachment(token attachmentConnectionToken, request ports.SamePeerSwitchRequest) {
	if err := request.Validate(); err != nil || token.sess == nil || token.ac == nil {
		return
	}
	if !token.ac.consumeSamePeerOffer(request.Target) {
		d.sendSamePeerSwitchFailure(token, request.RequestID, ports.SamePeerSwitchStaleTarget)
		return
	}

	target, targetTabIndex, ok := d.samePeerTarget(request)
	if !ok || target == token.sess {
		d.sendSamePeerSwitchFailure(token, request.RequestID, ports.SamePeerSwitchStaleTarget)
		return
	}

	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source:            token.sess,
		target:            target,
		next:              token.ac,
		expectedTransport: token.transport,
		sourceToken:       &token,
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
		d.sendSamePeerSwitchFailure(token, request.RequestID, ports.SamePeerSwitchStaleTarget)
		return
	}

	if fresh, admitted := token.ac.beginAttachmentEffect(transition.published); admitted {
		d.closePicker(token.ac)
		fresh.End()
	}
	d.touchMRU(target)
	d.deferAttachmentTransitionCleanups(transition)
	d.firstPaintForTransition(transition.published)
}

// samePeerTarget resolves the exact target and the client-owned tab cursor
// under the daemon/session locks. A missing cursor deliberately falls back to
// the target session's current default rather than leaking the source view.
func (d *Daemon) samePeerTarget(request ports.SamePeerSwitchRequest) (*session, int, bool) {
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

func (d *Daemon) sendSamePeerSwitchFailure(token attachmentConnectionToken, requestID uint64, code ports.SamePeerSwitchFailureCode) {
	if !token.attachmentCurrent() {
		return
	}
	payload, err := ports.MarshalSamePeerSwitchFailure(ports.SamePeerSwitchFailure{RequestID: requestID, Code: code})
	if err != nil {
		return
	}
	if err := token.sendControl(ports.Frame{Type: ports.MsgSamePeerSwitchFailure, Payload: payload}); err != nil {
		d.detachOnAttachmentSendError(token, token.transport.transport)
	}
}
