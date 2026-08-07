package daemon

import (
	"errors"
	"runtime"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/command"
)

const connectionSnapshotAttempts = 8

// currentAttachmentConnection retries the lock-free session-to-role snapshot when a
// handoff lands between those reads. The attachment gate still performs the final
// mutation admission after this function returns.
func (d *Daemon) currentAttachmentConnection(ac *attachedClient, tr ports.Transport) (attachmentOwner, attachmentConnectionToken, bool) {
	for range connectionSnapshotAttempts {
		if !ac.currentTransportIs(tr) {
			break
		}
		owner := ac.currentAttachmentOwner()
		if owner == nil {
			return nil, attachmentConnectionToken{}, false
		}
		if sess := localSession(owner); sess != nil && d.afterConnectionSessionSnapshot != nil {
			d.afterConnectionSessionSnapshot(sess)
		}
		token := attachmentOwnerToken(owner, ac, tr)
		if token.ac == nil || !sameAttachmentOwner(ac.currentAttachmentOwner(), owner) {
			runtime.Gosched()
			continue
		}
		return owner, token, true
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
		if !ac.currentTransportIs(tr) || ac.currentAttachmentOwner() == nil {
			return
		}
		f, err := tr.Recv()
		if err != nil {
			for range connectionSnapshotAttempts {
				owner, token, ok := d.currentAttachmentConnection(ac, tr)
				if !ok {
					return
				}
				if sess := localSession(owner); sess != nil {
					d.clientGone(sess, ac, tr, false)
				} else if view, remote := owner.(*remoteView); remote {
					d.clientGoneRemote(view, token, false)
				}
				current := ac.currentAttachmentOwner()
				if sameAttachmentOwner(current, owner) || !ac.currentTransportIs(tr) || current == nil {
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
	case ports.MsgCommand:
		request, derr := ports.UnmarshalCommandRequest(f.Payload)
		if derr != nil {
			return false
		}
		result := d.executeAttachedCommand(token, request)
		if err := token.sendControl(frameCommandResult(result)); err != nil {
			token.endAttachmentEffect()
			d.detachOnAttachmentSendError(token, token.transport.transport)
			return true
		}
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
	if _, remote := token.owner.(*remoteView); remote {
		result.Code = ports.ErrNoSuchTarget
		result.Text = "attached session is no longer active"
		return result
	}
	if request.TargetSession != "" || request.TargetTab != "" || request.TargetPane != "" {
		result.Code = ports.ErrInvalidCommandArgs
		result.Text = "attached commands cannot override their active session target"
		return result
	}
	sess := token.localSession()
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
	if view, remote := token.owner.(*remoteView); remote {
		d.scheduleRemoteViewPaint(view, ac, true)
	} else if sess := token.localSession(); sess != nil {
		go d.paint(sess, ac, true, token.lease)
	}
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
	advanced := ac.output.ack(epoch, state)
	ac.sendMu.Unlock()
	if !advanced {
		return true
	}
	if sess := token.localSession(); sess != nil {
		if rc := sess.renderCoordinator(); rc != nil {
			rc.notifyAckForLease(token.lease)
		}
	} else if view, remote := token.owner.(*remoteView); remote {
		// Remote views do not have a session render coordinator to consume the
		// newly available output-window capacity. Recompose from the retained
		// private VT so an update previously blocked by the local client window
		// is not stranded until another remote frame arrives.
		d.scheduleRemoteViewPaint(view, ac, false)
	}
	return true
}
