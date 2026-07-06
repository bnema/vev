package daemon

import (
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
)

const historyNavDisplayTimeout = 1500 * time.Millisecond

type recentSessionNavigator struct {
	ids   []domain.SessionID
	index int
}

type historyNavDisplay struct {
	ids   []domain.SessionID
	index int
	gen   uint64
}

func (h *historyNavDisplay) set(ids []domain.SessionID, index int, gen uint64) {
	h.ids = append(h.ids[:0], ids...)
	h.index = index
	h.gen = gen
}

func (h *historyNavDisplay) clear() {
	h.ids = h.ids[:0]
	h.index = 0
}

func (h historyNavDisplay) active() bool {
	return len(h.ids) > 0 && h.index >= 0 && h.index < len(h.ids)
}

func (n recentSessionNavigator) activeForDisplay() bool {
	return len(n.ids) > 1 && n.index >= 0 && n.index < len(n.ids)
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
		d.clearHistoryNav(ac)
		d.paint(sess, ac, true)
		return
	}
	d.switchToTarget(sess, ac, picker.Target{Session: targetID, TabIndex: -1})
	if ac.currentSession() == targetSess {
		ac.recentNav.index = next
		d.showHistoryNav(ac)
		d.paint(targetSess, ac, true)
	}
}

func (d *Daemon) showHistoryNav(ac *attachedClient) {
	if d == nil || ac == nil || !ac.recentNav.activeForDisplay() {
		return
	}
	ac.historyNavMu.Lock()
	ac.historyNav.set(ac.recentNav.ids, ac.recentNav.index, ac.historyNav.gen+1)
	gen := ac.historyNav.gen
	ac.historyNavTimer.stop()
	ac.historyNavTimer.retain(d.clock, historyNavDisplayTimeout, func(ports.Timer) {
		ac.historyNavMu.Lock()
		if ac.historyNav.gen != gen {
			ac.historyNavMu.Unlock()
			return
		}
		ac.historyNav.clear()
		ac.historyNavMu.Unlock()
		if sess := ac.currentSession(); sess != nil {
			sess.mu.Lock()
			stillCurrent := sess.client == ac
			sess.mu.Unlock()
			if stillCurrent {
				d.paint(sess, ac, true)
			}
		}
	})
	ac.historyNavMu.Unlock()
}

func (d *Daemon) clearHistoryNav(ac *attachedClient) {
	ac.clearHistoryNav()
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
