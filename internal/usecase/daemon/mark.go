package daemon

import (
	"errors"
	"slices"
	"time"

	"bnema.dev/vev/internal/domain"
)

func (d *Daemon) toggleMark(sess *session, tb *tab) error {
	if tb == nil {
		return errors.New("tab required")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.sessions[sess.id] != sess {
		return errors.New("daemon: session closed")
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if !slices.Contains(sess.tabs, tb) {
		return errors.New("tab not found")
	}
	tb.marked = !tb.marked
	if tb.marked {
		tb.markedAt = d.daemonNow()
	} else {
		tb.markNote = ""
		tb.markedAt = time.Time{}
	}
	d.publishSessionMarkLocked(sess)
	sess.invalidateViewsLocked()
	return nil
}

func (e paletteExec) ToggleMark() error {
	return e.runAction(daemonActionRequest{kind: daemonActionToggleMark})
}

func (e paletteExec) JumpMark() error { return nil }

func (e paletteExec) EditMarkNote() error { return nil }

// resolveSessionMarks projects only live local state; route snapshots remain unchanged.
func (d *Daemon) resolveSessionMarks(entries []recentRouteDisplay) {
	if d == nil {
		return
	}
	for i := range entries {
		marked, _ := d.sessionMarks.Load(entries[i].lifecycle)
		entries[i].marked = entries[i].kind == recentRouteLocal && marked == true
	}
}

// publishSessionMarkLocked publishes the local aggregate. Caller holds sess.mu.
func (d *Daemon) publishSessionMarkLocked(sess *session) {
	for _, tb := range sess.tabs {
		if tb.marked {
			d.sessionMarks.Store(sess.incarnation, true)
			return
		}
	}
	d.sessionMarks.Delete(sess.incarnation)
}

// repaintSessionMarkViewers runs after releasing architecture locks.
func (d *Daemon) repaintSessionMarkViewers(sess *session) {
	sess.mu.Lock()
	lifecycle := sess.incarnation
	sess.mu.Unlock()
	d.requestRouteViewerRepaint(map[domain.SessionLifecycleID]struct{}{lifecycle: {}}, sess)
}
