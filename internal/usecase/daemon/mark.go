package daemon

import (
	"errors"
	"slices"
	"time"
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
	return nil
}

func (e paletteExec) ToggleMark() error {
	return e.runAction(daemonActionRequest{kind: daemonActionToggleMark})
}

func (e paletteExec) JumpMark() error { return nil }

func (e paletteExec) EditMarkNote() error { return nil }
