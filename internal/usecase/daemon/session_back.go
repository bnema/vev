package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

// backSession toggles this attachment between its current session and the
// immediately preceding successfully activated session.
func (d *Daemon) backSessionForAttachment(token attachmentConnectionToken) error {
	if d == nil || token.sess == nil || token.ac == nil {
		return nil
	}
	targetID := token.ac.previousSession.Get()
	target := d.sessionByID(targetID)
	if target == nil || target == token.sess {
		token.ac.clearPreviousSessionIf(targetID)
		return nil
	}
	pickerTarget := picker.Target{Session: targetID, TabIndex: -1}
	return d.switchToTargetForAttachment(token, pickerTarget, sessionHandoffGuard{}, "back-session")
}

func (d *Daemon) backSession(current *session, ac *attachedClient) {
	if d == nil || current == nil || ac == nil {
		return
	}
	targetID := ac.previousSession.Get()
	target := d.sessionByID(targetID)
	if target == nil || target == current {
		ac.clearPreviousSessionIf(targetID)
		d.invalidateRender(current, ac, true, "session_back.go")
		return
	}
	token := attachmentToken(current, ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	if !admitted {
		d.reportError(current, domain.UserErr(domain.NoticeSessionUnavailable, "couldn't switch to that session", errAttachmentTransition))
		return
	}
	token.effect = effect
	defer effect.End()
	if err := d.backSessionForAttachment(token); err != nil {
		d.reportError(current, err)
	}
}

// clearPreviousSessionIf clears target only if it has not been replaced since
// the caller observed it. This preserves a concurrent successful hand-off.
func (ac *attachedClient) clearPreviousSessionIf(targetID domain.SessionID) {
	if ac == nil {
		return
	}
	ac.previousSession.With(func(previous *domain.SessionID) {
		if *previous == targetID {
			*previous = ""
		}
	})
}

// recordPreviousSession is called only after a cross-session hand-off has
// committed. Keeping this after the commit makes failed/displaced switches
// leave the toggle pair intact.
func (ac *attachedClient) recordPreviousSession(origin *session) {
	if ac != nil && origin != nil {
		ac.previousSession.Set(origin.id)
	}
}
