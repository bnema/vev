package daemon

import "github.com/bnema/vev/internal/usecase/picker"

// backSession toggles this attachment between its current session and the
// immediately preceding successfully activated session.
func (d *Daemon) backSession(current *session, ac *attachedClient) {
	if d == nil || current == nil || ac == nil {
		return
	}
	target := ac.previousSession.Get()
	if target == nil || target == current || d.sessionByID(target.id) != target {
		d.paint(current, ac, true)
		return
	}
	d.switchToTarget(current, ac, picker.Target{Session: target.id, TabIndex: -1})
}

// recordPreviousSession is called only after a cross-session hand-off has
// committed. Keeping this after the commit makes failed/displaced switches
// leave the toggle pair intact.
func (ac *attachedClient) recordPreviousSession(origin *session) {
	if ac != nil && origin != nil {
		ac.previousSession.Set(origin)
	}
}
