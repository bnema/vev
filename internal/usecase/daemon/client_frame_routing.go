package daemon

import (
	"errors"
	"runtime"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
)

const connectionSnapshotAttempts = 8

// currentAttachmentConnection retries the lock-free session-to-role snapshot when a
// handoff lands between those reads. The attachment gate still performs the final
// mutation admission after this function returns.
func (d *Daemon) currentAttachmentConnection(ac *attachedClient, tr ports.Transport) (*session, attachmentCapability, bool) {
	for range connectionSnapshotAttempts {
		if !ac.currentTransportIs(tr) {
			break
		}
		sess := ac.currentAttachmentSession()
		if sess == nil {
			return nil, attachmentCapability{}, false
		}
		if d.afterConnectionSessionSnapshot != nil {
			d.afterConnectionSessionSnapshot(sess)
		}
		token := captureAttachmentCapability(sess, ac, tr)
		if token.ac == nil || ac.currentAttachmentSession() != sess {
			runtime.Gosched()
			continue
		}
		return sess, token, true
	}
	return nil, attachmentCapability{}, false
}

// runConnLoop is the per-connection input router: it pumps client messages
// until detach, EOF, or a transport error. The attachment generation is
// resolved after every Recv so a stale transport cannot execute a new frame.
func (d *Daemon) runConnLoop(ac *attachedClient) {
	tr := ac.transport()
	if tr == nil {
		return
	}
	for {
		if !ac.currentTransportIs(tr) || ac.currentAttachmentSession() == nil {
			return
		}
		f, err := tr.Recv()
		if err != nil {
			for range connectionSnapshotAttempts {
				sess, _, ok := d.currentAttachmentConnection(ac, tr)
				if !ok {
					return
				}
				d.clientGone(sess, ac, tr, false)
				current := ac.currentAttachmentSession()
				if current == sess || !ac.currentTransportIs(tr) || current == nil {
					return
				}
			}
			return
		}
		_, token, ok := d.currentAttachmentConnection(ac, tr)
		if !ok {
			return
		}
		if d.handleAttachmentClientFrame(token, f) {
			return
		}
	}
}

// handleAttachmentClientFrame owns the attached-client protocol.
func (d *Daemon) handleAttachmentClientFrame(capability attachmentCapability, f ports.Frame) bool {
	if !capability.current() {
		return false
	}
	if d.afterAttachmentFrameDispatch != nil {
		d.afterAttachmentFrameDispatch(capability)
	}
	effect, admitted := capability.ac.beginAttachmentEffect(capability)
	if !admitted {
		return false
	}
	defer effect.End()
	if d.afterAttachmentEffectAdmitted != nil {
		d.afterAttachmentEffectAdmitted(effect.capability())
	}
	switch f.Type {
	case ports.MsgInput:
		if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
			d.handleSequencedInputForAttachment(effect, in.InputSeq, in.Data)
		}
	case ports.MsgResize:
		if rz, derr := ports.UnmarshalResize(f.Payload); derr == nil && effect.current() {
			d.resizeAttachmentGeometryForLease(effect, rz.Geometry())
		}
	case ports.MsgTheme:
		if th, derr := ports.UnmarshalTheme(f.Payload); derr == nil {
			d.applyThemeForAttachment(effect, th)
		}
	case ports.MsgImagePush:
		if ip, derr := ports.UnmarshalImagePush(f.Payload); derr == nil {
			d.handleSequencedImagePushForAttachment(effect, ip.InputSeq, ip)
		}
	case ports.MsgClientNotice:
		if notice, derr := ports.UnmarshalClientNotice(f.Payload); derr == nil {
			d.handleClientNoticeForAttachment(effect, notice)
		} else {
			d.log.Warn("malformed client notice", "err", derr)
		}
	case ports.MsgDetach:
		if effect.current() {
			d.clientGoneForAttachment(effect, true)
			return true
		}
	case ports.MsgAck:
		if ack, derr := ports.UnmarshalAck(f.Payload); derr == nil {
			d.ackOutput(effect, ack.Epoch, ack.State)
		}
	case ports.MsgOutputResetRequest:
		if _, derr := ports.UnmarshalOutputResetRequest(f.Payload); derr == nil {
			d.resetOutput(effect)
		}
	case ports.MsgSamePeerSwitchRequest:
		request, derr := ports.UnmarshalSamePeerSwitchRequest(f.Payload)
		if derr != nil {
			d.log.Warn("malformed same-peer switch request", "err", derr)
			break
		}
		d.switchSamePeerForAttachment(effect, request)
	case ports.MsgParkedRouteRequest:
		request, derr := ports.UnmarshalParkedRouteRequest(f.Payload)
		if derr != nil {
			d.log.Warn("malformed parked-route request", "err", derr)
			break
		}
		d.handleParkedRouteRequest(effect, request)
	case ports.MsgRecentRouteSnapshot:
		snapshot, derr := ports.UnmarshalRecentRouteSnapshot(f.Payload)
		if derr != nil {
			d.log.Warn("malformed recent route snapshot", "err", derr)
			break
		}
		replayIdentity := effect.ac.setRouteSnapshot(snapshot)
		d.invalidateRender(effect.sess, effect.ac, false, "client_frame_routing.go:route-snapshot")
		if !replayIdentity {
			break
		}
		if err := d.sendCommittedRouteIdentityForAttachment(effect); err != nil {
			d.detachOnAttachmentSendError(effect.capability(), effect.transport.transport)
		}
	case ports.MsgRouteAttentionSubscription:
		subscription, derr := ports.UnmarshalRouteAttentionSubscription(f.Payload)
		if derr != nil {
			d.log.Warn("malformed route attention subscription", "err", derr)
			break
		}
		effect.ac.setRouteAttentionSubscription(subscription)
		d.invalidateRender(effect.sess, effect.ac, false, "client_frame_routing.go:route-attention")
	case ports.MsgRouteNavigationFailure:
		failure, derr := ports.UnmarshalRouteNavigationFailure(f.Payload)
		if derr != nil {
			d.log.Warn("malformed route navigation failure", "err", derr)
			break
		}
		message := "route navigation failed"
		switch failure.Code {
		case ports.RouteFailureStaleSelection:
			message = "that recent route is no longer available"
		case ports.RouteFailureTargetChanged:
			message = "that recent route changed before attach"
		case ports.RouteFailureOriginUnavailable:
			message = "the original route is no longer available"
		case ports.RouteFailureUnavailable:
			message = "the selected route is unavailable"
		case ports.RouteFailureNoSuchRoute:
			message = "that recent route no longer exists"
		}
		d.notify(effect.sess, domain.NoticeWarn, domain.NoticeSessionUnavailable, message, nil)
	case ports.MsgCommand:
		request, derr := ports.UnmarshalCommandRequest(f.Payload)
		if derr != nil {
			return false
		}
		result := d.executeAttachedCommand(effect, request)
		resultEffect, ok := d.commandResultEffect(effect)
		if !ok {
			// A remote handoff or explicit detach deliberately retired the old
			// attachment; it must not be treated as a stale-result send error.
			return true
		}
		if err := resultEffect.sendControl(frameCommandResult(result)); err != nil {
			resultEffect.End()
			d.detachOnAttachmentSendError(resultEffect.capability(), resultEffect.transport.transport)
			return true
		}
		resultEffect.End()
	case ports.MsgPing:
		if err := effect.sendControl(framePong()); err != nil {
			effect.End()
			d.detachOnAttachmentSendError(effect.capability(), effect.transport.transport)
			return true
		}
	default:
		// Unknown/out-of-band client messages are ignored so a newer
		// client can add message types without breaking an older daemon.
	}
	return false
}

// commandResultEffect renews admission after a local navigation transition
// ends the command's initiating effect. A retired Transport or Session means
// the command deliberately handed off or detached and has no result carriage.
func (d *Daemon) commandResultEffect(effect *attachmentEffect) (*attachmentEffect, bool) {
	if effect == nil || effect.ac == nil || !effect.ac.currentTransportIs(effect.transport.transport) {
		return nil, false
	}
	if effect.current() && effect.attachmentCapability.current() {
		return effect, true
	}
	sess := effect.ac.currentAttachmentSession()
	if sess == nil {
		return nil, false
	}
	capability := captureAttachmentCapability(sess, effect.ac, effect.transport.transport)
	if capability.ac == nil {
		return nil, false
	}
	return effect.ac.beginAttachmentEffect(capability)
}

func (d *Daemon) executeAttachedCommand(effect *attachmentEffect, request ports.CommandRequest) ports.CommandResult {
	result := ports.CommandResult{RequestID: request.RequestID}
	if request.Version != ports.ProtocolVersion {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "unsupported command protocol version"
		return result
	}
	if !request.Attached {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "attached command flag is required"
		return result
	}
	if request.RequestID == 0 {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "command request id is required"
		return result
	}
	if request.Self {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "attached commands cannot target themselves"
		return result
	}
	if request.TargetSession != "" || request.TargetTab != "" || request.TargetPane != "" {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "attached commands cannot override their active session target"
		return result
	}
	sess := effect.sess
	if sess == nil || !effect.current() {
		result.Code = ports.ErrNoSuchTarget
		result.Text = "attached session is no longer active"
		return result
	}
	cmd, ok := command.BySlug(request.Slug)
	if !ok {
		result.Code = ports.ErrUnknownCommand
		result.Text = "unknown command: " + request.Slug
		return result
	}
	if !cmd.PaletteVisible {
		result.Code = ports.ErrNotScriptable
		result.Text = request.Slug + " is not available in the palette"
		return result
	}
	if cmd.Run == nil {
		result.Code = ports.ErrNotScriptable
		result.Text = request.Slug + " is not scriptable"
		return result
	}
	// The frame's attachment token is the target capability. No registry/name lookup is
	// performed, so the command cannot escape to another remote session.
	err := sess.runMutation(func() error {
		return cmd.Run(paletteExec{d: d, sess: sess, ac: effect.ac, effect: effect}, request.Args)
	})
	if err == nil {
		result.OK = true
		return result
	}
	if errors.Is(err, command.ErrInvalidArguments) {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "usage: " + cmd.Usage
		return result
	}
	result.Code = ports.ErrInternal
	result.Text = err.Error()
	return result
}

// resetOutput rebases only the exact attachment and transport admitted
// by the frame router. sendMu serializes the rebase with every output send;
// transport revalidation under that lock rejects a link replaced while waiting.
func (d *Daemon) resetOutput(effect *attachmentEffect) bool {
	if !effect.current() || effect.ac == nil {
		return false
	}
	ac := effect.ac
	ac.sendMu.Lock()
	if !effect.current() || ac.output == nil {
		ac.sendMu.Unlock()
		return false
	}
	ac.rebaseOutput()
	ac.pipelineCache = composeCacheInput{}
	ac.pipelineScratch = composeCacheInput{}
	ac.sendMu.Unlock()
	go d.paint(effect.sess, ac, true, effect.lease)
	return true
}

func (d *Daemon) ackOutput(effect *attachmentEffect, epoch, state uint64) bool {
	if !effect.current() || effect.ac == nil {
		return false
	}
	ac := effect.ac
	ac.sendMu.Lock()
	if ac.output == nil {
		ac.sendMu.Unlock()
		return false
	}
	acknowledged := ac.output.ack(epoch, state)
	ac.sendMu.Unlock()
	if acknowledged {
		if rc := effect.sess.renderCoordinator(); rc != nil {
			rc.notifyAckForLease(effect.lease)
		}
	}
	return acknowledged
}
