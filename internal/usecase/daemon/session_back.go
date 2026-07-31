package daemon

import "github.com/bnema/vev/internal/usecase/picker"

// backSession toggles this attachment between its current session and the
// immediately preceding successfully activated session.
func (d *Daemon) backSessionForRole(token attachmentRoleToken) error {
	if d == nil || token.sess == nil || token.ac == nil {
		return nil
	}
	target := token.ac.previousSession.Get()
	if target == nil || target == token.sess || target.core() == nil || d.sessionByID(target.core().id) != target {
		token.ac.clearPreviousSessionIf(target)
		return nil
	}
	return d.switchToTargetForRole(token, picker.Target{Session: target.core().id, TabIndex: -1}, sessionHandoffGuard{}, "back-session")
}

func (d *Daemon) backSession(current *session, ac *attachedClient) {
	if d == nil || current == nil || ac == nil {
		return
	}
	target := ac.previousSession.Get()
	if target == nil || target == current || target.core() == nil || d.sessionByID(target.core().id) != target {
		ac.clearPreviousSessionIf(target)
		d.invalidateRender(current, ac, true, "session_back.go")
		return
	}
	if err := d.switchToTarget(current, ac, picker.Target{Session: target.core().id, TabIndex: -1}); err != nil {
		d.reportError(current, err)
	}
}

// clearPreviousSessionIf clears target only if it has not been replaced since
// the caller observed it. This preserves a concurrent successful hand-off.
func (ac *attachedClient) clearPreviousSessionIf(target attachmentSession) {
	if ac == nil {
		return
	}
	ac.previousSession.With(func(previous *attachmentSession) {
		if *previous == target {
			*previous = nil
		}
	})
}

// recordPreviousSession is called only after a cross-session hand-off has
// committed. Keeping this after the commit makes failed/displaced switches
// leave the toggle pair intact.
func (ac *attachedClient) recordPreviousSession(origin attachmentSession) {
	if ac != nil && origin != nil {
		ac.previousSession.Set(origin)
	}
}
