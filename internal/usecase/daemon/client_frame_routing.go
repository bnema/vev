package daemon

import "github.com/bnema/vev/internal/ports"

// runConnLoop is the per-connection input router: it pumps client messages
// until detach, EOF, or a transport error. Role is resolved after every Recv so
// a transport displaced while blocked in Recv immediately adopts its restricted
// snatched routing rather than executing one stale active frame.
func (d *Daemon) runConnLoop(ac *attachedClient) {
	tr := ac.transport()
	if tr == nil {
		return
	}
	for {
		if !ac.currentTransportIs(tr) {
			return
		}
		sess := ac.currentSession()
		if sess == nil {
			return
		}
		f, err := tr.Recv()
		if err != nil {
			token := sess.attachmentToken(ac, tr)
			if token.role == attachmentSnatched {
				d.parkOrDropSnatchedAttachment(token)
			} else {
				d.clientGone(sess, ac, tr, false)
			}
			return
		}
		if !ac.currentTransportIs(tr) {
			return
		}
		sess = ac.currentSession()
		if sess == nil {
			return
		}
		token := sess.attachmentToken(ac, tr)
		switch token.role {
		case attachmentActive:
			if d.handleActiveClientFrame(token, f) {
				return
			}
		case attachmentSnatched:
			if d.handleSnatchedClientFrame(token, f) {
				return
			}
		default:
			return
		}
	}
}

// handleActiveClientFrame owns the ordinary attached-client protocol. Keeping
// it separate makes the restricted snatched protocol an explicit allow-list.
func (d *Daemon) handleActiveClientFrame(token attachmentRoleToken, f ports.Frame) bool {
	if !token.activeEffect() {
		return false
	}
	if d.afterActiveFrameDispatch != nil {
		d.afterActiveFrameDispatch(token)
	}
	ticket, admitted := token.ac.beginRoleEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endRoleEffect()
	if d.afterRoleEffectAdmitted != nil {
		d.afterRoleEffectAdmitted(token)
	}
	switch f.Type {
	case ports.MsgInput:
		if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
			d.handleSequencedInputForRole(token, in.InputSeq, in.Data)
		}
	case ports.MsgResize:
		if rz, derr := ports.UnmarshalResize(f.Payload); derr == nil && token.activeEffect() {
			if sess, ok := localSession(token.sess); ok {
				d.requestTransactionalResizeForLease(sess, token.ac, token.lease, rz.Size, false)
			}
		}
	case ports.MsgTheme:
		if th, derr := ports.UnmarshalTheme(f.Payload); derr == nil {
			d.applyThemeForRole(token, th)
		}
	case ports.MsgImagePush:
		if ip, derr := ports.UnmarshalImagePush(f.Payload); derr == nil {
			d.handleSequencedImagePushForRole(token, ip.InputSeq, ip)
		}
	case ports.MsgClientNotice:
		if notice, derr := ports.UnmarshalClientNotice(f.Payload); derr == nil {
			d.handleClientNoticeForRole(token, notice)
		} else {
			d.log.Warn("malformed client notice", "err", derr)
		}
	case ports.MsgDetach:
		if token.activeEffect() {
			d.clientGoneForRole(token, true)
			return true
		}
	case ports.MsgAck:
		if ack, derr := ports.UnmarshalAck(f.Payload); derr == nil {
			d.ackActiveOutput(token, ack.AckedStateNum)
		}
	case ports.MsgOutputResetRequest:
		if _, derr := ports.UnmarshalOutputResetRequest(f.Payload); derr == nil {
			d.resetActiveOutput(token)
		}
	case ports.MsgCommand:
		if token.ac.proxied {
			request, derr := ports.UnmarshalCommandRequest(f.Payload)
			if derr != nil {
				return false
			}
			// Attached commands are intentionally admitted only on the proxied
			// attachment path. Phase 6 supplies execution; until then every
			// decoded request is rejected with its original correlation ID.
			valid := request.Version == ports.ProtocolVersion && request.Attached && request.RequestID != 0 &&
				request.TargetSession == "" && request.TargetTab == "" && request.TargetPane == ""
			if !valid {
				d.log.Warn("invalid attached command", "request_id", request.RequestID)
			}
			result := ports.CommandResult{
				RequestID: request.RequestID,
				Code:      ports.ErrNotScriptable,
				Text:      "attached command relay is not enabled",
			}
			if err := token.sendActiveControl(frameCommandResult(result)); err != nil {
				token.endRoleEffect()
				d.detachOnRoleSendError(token, token.transport.transport)
				return true
			}
		}
	case ports.MsgPing:
		if err := token.sendActiveControl(framePong()); err != nil {
			token.endRoleEffect()
			d.detachOnRoleSendError(token, token.transport.transport)
			return true
		}
	default:
		// Unknown/out-of-band client messages are ignored so a newer
		// client can add message types without breaking an older daemon.
	}
	return false
}

// resetActiveOutput rebases only the exact proxied role and transport admitted
// by the frame router. sendMu serializes the rebase with every output send;
// transport revalidation under that lock rejects a link replaced while waiting.
func (d *Daemon) resetActiveOutput(token attachmentRoleToken) bool {
	ac := token.ac
	if ac == nil || !ac.proxied || token.effect == nil || token.effect.ended.Load() {
		return false
	}
	ac.sendMu.Lock()
	if token.effect.ended.Load() || !ac.proxied || !token.activeCurrent() || ac.output == nil {
		ac.sendMu.Unlock()
		return false
	}
	ac.output.rebase()
	ac.sendMu.Unlock()

	rc := token.sess.core().coordinator.Load()
	return rc != nil && rc.invalidateForLease(ac, token.lease, renderInvalidation{
		class: invalidateUrgent, reset: true, producer: "output-reset-request",
	})
}

func (d *Daemon) ackActiveOutput(token attachmentRoleToken, state uint64) bool {
	ac := token.ac
	if token.effect == nil || token.effect.ended.Load() {
		return false
	}
	ac.sendMu.Lock()
	ac.output.ack(state)
	ac.sendMu.Unlock()
	if rc := token.sess.core().coordinator.Load(); rc != nil {
		rc.notifyAckForLease(token.lease)
	}
	return true
}

// handleSnatchedClientFrame is intentionally an allow-list. Strict resume and
// quit actions are introduced separately; until then ordinary input is ignored.
func (d *Daemon) handleSnatchedClientFrame(token attachmentRoleToken, f ports.Frame) bool {
	if !token.current() {
		return false
	}
	ticket, admitted := token.ac.beginRoleEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endRoleEffect()
	if d.afterRoleEffectAdmitted != nil {
		d.afterRoleEffectAdmitted(token)
	}
	switch f.Type {
	case ports.MsgInput:
		if in, err := ports.UnmarshalInput(f.Payload); err == nil && d.handleSnatchedInput(token, in) {
			return true
		}
	case ports.MsgResize:
		if resize, err := ports.UnmarshalResize(f.Payload); err == nil && resize.Size.Valid() {
			ac := token.ac
			ac.sendMu.Lock()
			if ac.roleGeneration.Load() != token.generation || !ac.transportSnapshotCurrent(token.transport) {
				ac.sendMu.Unlock()
				return false
			}
			ac.size = resize.Size
			ac.sendMu.Unlock()
			if err := d.sendSnatchedPanel(ac, token.transport, token.generation, "", token.effect); err != nil {
				token.endRoleEffect()
				d.parkOrDropSnatchedAttachment(token)
				return true
			}
		}
	case ports.MsgTheme:
		if theme, err := ports.UnmarshalTheme(f.Payload); err == nil {
			ac := token.ac
			ac.sendMu.Lock()
			if ac.roleGeneration.Load() != token.generation || !ac.transportSnapshotCurrent(token.transport) {
				ac.sendMu.Unlock()
				return false
			}
			clientTheme := themeFromMessage(theme)
			ac.setClientTheme(clientTheme)
			ac.setAppliedTheme(d.resolveAppliedTheme(clientTheme))
			ac.sendMu.Unlock()
			if err := d.sendSnatchedPanel(ac, token.transport, token.generation, "", token.effect); err != nil {
				token.endRoleEffect()
				d.parkOrDropSnatchedAttachment(token)
				return true
			}
		}
	case ports.MsgAck:
		if ack, err := ports.UnmarshalAck(f.Payload); err == nil {
			token.ac.ackOutputState(ack.AckedStateNum)
		}
	case ports.MsgPing:
		if err := d.sendSnatchedControl(token, framePong()); err != nil {
			token.endRoleEffect()
			d.parkOrDropSnatchedAttachment(token)
			return true
		}
	case ports.MsgDetach:
		_ = d.sendSnatchedControl(token, frameDetached(ports.ReasonDetach))
		token.endRoleEffect()
		d.dropSnatchedAttachment(token)
		return true
	}
	return false
}
