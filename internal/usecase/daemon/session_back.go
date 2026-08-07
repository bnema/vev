package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

// backSession toggles this attachment between its current local session and
// the immediately preceding successfully activated local session. A remote
// owner is retained in the owner history but is not interpreted as a local
// session; remote back navigation is published with remote transitions.
func (d *Daemon) backSessionForAttachment(token attachmentConnectionToken, afterRemoteTransition ...func()) error {
	if d == nil || token.ac == nil {
		return nil
	}
	if view, remote := normalizeAttachmentOwner(token.owner).(*remoteView); remote {
		previous := token.ac.previousOwner.Get()
		target := localSession(previous)
		if target == nil {
			// A non-local predecessor is not a valid reverse target. Keep it as
			// stable history for the owner-specific transition that can interpret it.
			return nil
		}
		if target.core() == nil || d.sessionByID(target.core().id) != target {
			token.ac.clearPreviousOwnerIf(previous)
			return nil
		}
		published, err := d.transitionFromRemoteView(token, view, target)
		if err != nil {
			if !token.current() {
				return nil
			}
			return err
		}
		if len(afterRemoteTransition) != 0 && afterRemoteTransition[0] != nil {
			afterRemoteTransition[0]()
		}
		d.touchMRU(target)
		d.firstPaintForTransition(published)
		return nil
	}

	current := token.localSession()
	if current == nil {
		return nil
	}
	previous := token.ac.previousOwner.Get()
	target := localSession(previous)
	if target == nil {
		// A remote predecessor remains stable history for the remote transition
		// path; the local-only back path must not discard it.
		return nil
	}
	if target == current || target.core() == nil || d.sessionByID(target.core().id) != target {
		token.ac.clearPreviousOwnerIf(previous)
		return nil
	}
	pickerTarget := picker.Target{Session: target.core().id, TabIndex: -1}
	return d.switchToTargetForAttachment(token, pickerTarget, sessionHandoffGuard{}, "back-session")
}

func (d *Daemon) backSession(current *session, ac *attachedClient) {
	if d == nil || current == nil || ac == nil {
		return
	}
	previous := ac.previousOwner.Get()
	target := localSession(previous)
	if target == nil {
		if previous == nil {
			d.invalidateRender(current, ac, true, "session_back.go")
		}
		return
	}
	if target == current || target.core() == nil || d.sessionByID(target.core().id) != target {
		ac.clearPreviousOwnerIf(previous)
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

// clearPreviousOwnerIf clears target only if it has not been replaced since
// the caller observed it. This preserves a concurrent successful hand-off.
func (ac *attachedClient) clearPreviousOwnerIf(target attachmentOwner) {
	if ac == nil {
		return
	}
	ac.previousOwner.With(func(previous *attachmentOwner) {
		if sameAttachmentOwner(*previous, target) {
			*previous = nil
		}
	})
}

// recordPreviousOwner is called only after a cross-owner hand-off has
// committed. Keeping this after the commit makes failed/displaced switches
// leave the toggle pair intact.
func (ac *attachedClient) recordPreviousOwner(origin attachmentOwner) {
	origin = normalizeAttachmentOwner(origin)
	if ac != nil && origin != nil {
		ac.previousOwner.Set(origin)
	}
}

func (ac *attachedClient) recordPreviousSession(origin *session) {
	ac.recordPreviousOwner(origin)
}
