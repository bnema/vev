package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/protocol"
)

func (d *Daemon) registerAttachmentUIFence(effect *attachmentEffect, actionID uint64) bool {
	if !effect.current() {
		return false
	}
	capability := effect.capability()
	rc := attachmentRenderCoordinator(capability.sess)
	return rc != nil && rc.registerUIFence(capability.lease, actionID, func(id uint64) {
		d.expireAttachmentUIFence(capability, id)
	})
}

func (d *Daemon) expireAttachmentUIFence(capability attachmentCapability, actionID uint64) {
	effect, ok := capability.ac.beginAttachmentEffect(capability)
	if !ok {
		return
	}
	defer effect.End()
	// Unavailability confirms no publication. In particular, do not synthesize
	// a successful boundary from mutable output state when rendering timed out.
	if err := effect.sendControl(protocol.UIReceipt{ActionID: actionID, Outcome: protocol.UIReceiptUnavailable}); err != nil {
		launchCleanup := d.reserveAttachmentSendErrorCleanup(capability, capability.transport.transport)
		effect.End()
		launchCleanup()
	}
}

// sendUIReceiptLocked shares the attachment's Output/UIViewUpdate send ordering.
// Callers hold sendMu, never coordinator.mu. A nil effect is the existing direct
// render fixture path, not a separate production transport owner.
func sendUIReceiptLocked(effect *attachmentEffect, transport transportSnapshot, receipt protocol.UIReceipt) error {
	if transport.transport == nil {
		return errors.New("client transport is nil")
	}
	if effect != nil {
		if !effect.current() || !effect.beginTransportSend(transport) {
			return errAttachmentTransition
		}
		defer effect.endTransportSend()
	}
	var err error
	if transport.transport.Capabilities().AsyncSend {
		err = transport.transport.SendServerAsync(receipt)
	} else {
		err = transport.transport.SendServer(receipt)
	}
	if err != nil && effect != nil {
		effect.reportTransportFailure(transport)
	}
	return err
}
