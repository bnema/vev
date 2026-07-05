package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/picker"
)

type recentSessionNavigator struct {
	ids   []domain.SessionID
	index int
}

func recentSessionTrail(cur *session, state barState) []domain.SessionID {
	if cur == nil {
		return nil
	}
	ids := make([]domain.SessionID, 0, 1+len(state.mru))
	ids = append(ids, cur.id)
	for _, entry := range state.mru {
		if entry.id != "" {
			ids = append(ids, entry.id)
		}
	}
	return ids
}

func (n *recentSessionNavigator) reset(ids []domain.SessionID) {
	n.ids = append(n.ids[:0], ids...)
	n.index = 0
}

func (d *Daemon) navigateRecentSession(sess *session, ac *attachedClient, delta int) {
	if d == nil || sess == nil || ac == nil || delta == 0 {
		return
	}
	trail := recentSessionTrail(sess, d.barStateFor(sess, ""))
	if len(trail) == 0 {
		d.paint(sess, ac, true)
		return
	}
	if !d.recentNavValid(&ac.recentNav) || ac.recentNav.index < 0 || ac.recentNav.index >= len(ac.recentNav.ids) || ac.recentNav.ids[ac.recentNav.index] != sess.id {
		ac.recentNav.reset(trail)
	}

	next := ac.recentNav.index + delta
	if next < 0 || next >= len(ac.recentNav.ids) {
		d.paint(sess, ac, true)
		return
	}
	targetID := ac.recentNav.ids[next]
	if d.sessionByID(targetID) == nil {
		ac.recentNav.reset(trail)
		next = ac.recentNav.index + delta
		if next < 0 || next >= len(ac.recentNav.ids) {
			d.paint(sess, ac, true)
			return
		}
		targetID = ac.recentNav.ids[next]
		if d.sessionByID(targetID) == nil {
			d.paint(sess, ac, true)
			return
		}
	}
	ac.recentNav.index = next
	d.switchToTarget(sess, ac, picker.Target{Session: targetID, TabIndex: -1})
}

func (d *Daemon) recentNavValid(n *recentSessionNavigator) bool {
	if n == nil || len(n.ids) == 0 {
		return false
	}
	for _, id := range n.ids {
		if d.sessionByID(id) == nil {
			return false
		}
	}
	return true
}
