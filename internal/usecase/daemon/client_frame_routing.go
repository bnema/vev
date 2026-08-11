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
func (d *Daemon) currentAttachmentConnection(ac *attachedClient, tr ports.Transport) (*session, attachmentConnectionToken, bool) {
	for range connectionSnapshotAttempts {
		if !ac.currentTransportIs(tr) {
			break
		}
		sess := ac.currentAttachmentSession()
		if sess == nil {
			return nil, attachmentConnectionToken{}, false
		}
		if d.afterConnectionSessionSnapshot != nil {
			d.afterConnectionSessionSnapshot(sess)
		}
		token := attachmentToken(sess, ac, tr)
		if token.ac == nil || ac.currentAttachmentSession() != sess {
			runtime.Gosched()
			continue
		}
		return sess, token, true
	}
	return nil, attachmentConnectionToken{}, false
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
func (d *Daemon) handleAttachmentClientFrame(token attachmentConnectionToken, f ports.Frame) bool {
	if !token.attachmentEffectCurrent() {
		return false
	}
	if d.afterAttachmentFrameDispatch != nil {
		d.afterAttachmentFrameDispatch(token)
	}
	ticket, admitted := token.ac.beginAttachmentEffect(token)
	if !admitted {
		return false
	}
	token.effect = ticket
	defer token.endAttachmentEffect()
	if d.afterAttachmentEffectAdmitted != nil {
		d.afterAttachmentEffectAdmitted(token)
	}
	switch f.Type {
	case ports.MsgInput:
		if in, derr := ports.UnmarshalInput(f.Payload); derr == nil {
			d.handleSequencedInputForAttachment(token, in.InputSeq, in.Data)
		}
	case ports.MsgResize:
		if rz, derr := ports.UnmarshalResize(f.Payload); derr == nil && token.attachmentEffectCurrent() {
			d.resizeAttachmentForLease(token, rz.Size)
		}
	case ports.MsgTheme:
		if th, derr := ports.UnmarshalTheme(f.Payload); derr == nil {
			d.applyThemeForAttachment(token, th)
		}
	case ports.MsgImagePush:
		if ip, derr := ports.UnmarshalImagePush(f.Payload); derr == nil {
			d.handleSequencedImagePushForAttachment(token, ip.InputSeq, ip)
		}
	case ports.MsgClientNotice:
		if notice, derr := ports.UnmarshalClientNotice(f.Payload); derr == nil {
			d.handleClientNoticeForAttachment(token, notice)
		} else {
			d.log.Warn("malformed client notice", "err", derr)
		}
	case ports.MsgDetach:
		if token.attachmentEffectCurrent() {
			d.clientGoneForAttachment(token, true)
			return true
		}
	case ports.MsgAck:
		if ack, derr := ports.UnmarshalAck(f.Payload); derr == nil {
			d.ackOutput(token, ack.Epoch, ack.State)
		}
	case ports.MsgOutputResetRequest:
		if _, derr := ports.UnmarshalOutputResetRequest(f.Payload); derr == nil {
			d.resetOutput(token)
		}
	case ports.MsgRecentRouteSnapshot:
		snapshot, derr := ports.UnmarshalRecentRouteSnapshot(f.Payload)
		if derr == nil {
			token.ac.setRouteSnapshot(snapshot)
		} else {
			d.log.Warn("malformed recent route snapshot", "err", derr)
		}
	case ports.MsgRouteAttentionSubscription:
		subscription, derr := ports.UnmarshalRouteAttentionSubscription(f.Payload)
		if derr != nil {
			d.log.Warn("malformed route attention subscription", "err", derr)
			break
		}
		token.ac.setRouteAttentionSubscription(subscription)
		d.invalidateRender(token.sess, token.ac, false, "client_frame_routing.go:route-attention")
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
		d.notify(token.sess, domain.NoticeWarn, domain.NoticeSessionUnavailable, message, nil)
	case ports.MsgCommand:
		request, derr := ports.UnmarshalCommandRequest(f.Payload)
		if derr != nil {
			return false
		}
		result := d.executeAttachedCommand(token, request)
		resultToken, ok := d.commandResultToken(token)
		if !ok {
			// A remote handoff or explicit detach deliberately retired the old
			// attachment; it must not be treated as a stale-result send error.
			return true
		}
		if err := resultToken.sendControl(frameCommandResult(result)); err != nil {
			resultToken.endAttachmentEffect()
			d.detachOnAttachmentSendError(resultToken, resultToken.transport.transport)
			return true
		}
		resultToken.endAttachmentEffect()
	case ports.MsgPing:
		if err := token.sendControl(framePong()); err != nil {
			token.endAttachmentEffect()
			d.detachOnAttachmentSendError(token, token.transport.transport)
			return true
		}
	default:
		// Unknown/out-of-band client messages are ignored so a newer
		// client can add message types without breaking an older daemon.
	}
	return false
}

// commandResultToken renews the attachment effect after a local navigation
// transition ends the command's initiating effect. A retired transport/session
// means the command deliberately handed off or detached and has no result to
// send on the old carriage.
func (d *Daemon) commandResultToken(token attachmentConnectionToken) (attachmentConnectionToken, bool) {
	if token.ac == nil || !token.ac.currentTransportIs(token.transport.transport) {
		return attachmentConnectionToken{}, false
	}
	sess := token.ac.currentAttachmentSession()
	if sess == nil {
		return attachmentConnectionToken{}, false
	}
	resultToken := attachmentToken(sess, token.ac, token.transport.transport)
	if resultToken.ac == nil {
		return attachmentConnectionToken{}, false
	}
	if token.effect != nil && !token.effect.ended.Load() && token.current() {
		return token, true
	}
	ticket, admitted := token.ac.beginAttachmentEffect(resultToken)
	if !admitted {
		return attachmentConnectionToken{}, false
	}
	resultToken.effect = ticket
	return resultToken, true
}

func (d *Daemon) executeAttachedCommand(token attachmentConnectionToken, request ports.CommandRequest) ports.CommandResult {
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
	sess := token.sess
	if sess == nil || !token.attachmentEffectCurrent() {
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
		return cmd.Run(paletteExec{d: d, sess: sess, ac: token.ac, effect: token.effect}, request.Args)
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
func (d *Daemon) resetOutput(token attachmentConnectionToken) bool {
	ac := token.ac
	if ac == nil || token.effect == nil || token.effect.ended.Load() {
		return false
	}
	ac.sendMu.Lock()
	if token.effect.ended.Load() || !token.attachmentCurrent() || ac.output == nil {
		ac.sendMu.Unlock()
		return false
	}
	ac.rebaseOutput()
	ac.pipelineCache = composeCacheInput{}
	ac.pipelineScratch = composeCacheInput{}
	ac.sendMu.Unlock()
	go d.paint(token.sess, ac, true, token.lease)
	return true
}

func (d *Daemon) ackOutput(token attachmentConnectionToken, epoch, state uint64) bool {
	ac := token.ac
	if token.effect == nil || token.effect.ended.Load() {
		return false
	}
	ac.sendMu.Lock()
	if ac.output == nil {
		ac.sendMu.Unlock()
		return false
	}
	acknowledged := ac.output.ack(epoch, state)
	ac.sendMu.Unlock()
	if acknowledged {
		if rc := token.sess.renderCoordinator(); rc != nil {
			rc.notifyAckForLease(token.lease)
		}
	}
	return acknowledged
}
