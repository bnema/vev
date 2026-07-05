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
	if ac.recentNav.index == 0 && !sameSessionTrail(ac.recentNav.ids, trail) {
		ac.recentNav.reset(trail)
	}
	if !d.pruneRecentNav(&ac.recentNav) || ac.recentNav.ids[ac.recentNav.index] != sess.id {
		ac.recentNav.reset(trail)
	}

	next := ac.recentNav.index + delta
	if next < 0 || next >= len(ac.recentNav.ids) {
		d.paint(sess, ac, true)
		return
	}
	targetID := ac.recentNav.ids[next]
	targetSess := d.sessionByID(targetID)
	if targetSess == nil {
		d.paint(sess, ac, true)
		return
	}
	d.switchToTarget(sess, ac, picker.Target{Session: targetID, TabIndex: -1})
	if ac.currentSession() == targetSess {
		ac.recentNav.index = next
	}
}

func (d *Daemon) pruneRecentNav(n *recentSessionNavigator) bool {
	if n == nil || len(n.ids) == 0 || n.index < 0 || n.index >= len(n.ids) {
		return false
	}
	current := n.ids[n.index]
	kept := n.ids[:0]
	nextIndex := -1
	for _, id := range n.ids {
		if d.sessionByID(id) == nil {
			continue
		}
		if id == current {
			nextIndex = len(kept)
		}
		kept = append(kept, id)
	}
	n.ids = kept
	if nextIndex == -1 {
		return false
	}
	n.index = nextIndex
	return true
}

func sameSessionTrail(a, b []domain.SessionID) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
