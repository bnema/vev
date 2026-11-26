package daemon

import (
	"errors"
	"slices"
	"strings"
	"time"

	"bnema.dev/vev/internal/domain"
)

func (d *Daemon) toggleMarkWithNote(sess *session, tb *tab, note string) error {
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
		tb.markNote = sanitizeMarkNote(note)
	} else {
		tb.markNote = ""
		tb.markedAt = time.Time{}
	}
	d.publishSessionMarkLocked(sess)
	sess.invalidateViewsLocked()
	return nil
}

func sanitizeMarkNote(note string) string {
	note = strings.TrimSpace(stripANSIAndControls(strings.ToValidUTF8(note, "")))
	runes := []rune(note)
	if len(runes) > 120 {
		note = string(runes[:120])
	}
	return note
}

func (e paletteExec) markTab() *tab {
	if e.ac == nil {
		return nil
	}
	return e.sess.tabForAttachment(e.ac)
}

func (e paletteExec) ToggleMark() error {
	tb := e.markTab()
	if tb == nil {
		return nil
	}
	e.sess.mu.Lock()
	if !slices.Contains(e.sess.tabs, tb) {
		e.sess.mu.Unlock()
		return errMoveStaleTarget
	}
	marked := tb.marked
	e.sess.mu.Unlock()
	target := daemonActionTarget{session: e.sess, tab: tb, attachment: e.ac}
	if marked {
		return e.runAction(daemonActionRequest{kind: daemonActionToggleMark, target: target})
	}
	e.d.enterPrompt(e.sess, e.ac, " Mark note (optional) ", "", func(note string) error {
		return e.sess.runMutation(func() error {
			e.sess.mu.Lock()
			if !slices.Contains(e.sess.tabs, tb) {
				e.sess.mu.Unlock()
				return errMoveStaleTarget
			}
			marked := tb.marked
			e.sess.mu.Unlock()
			if marked {
				return nil
			}
			return e.runAction(daemonActionRequest{kind: daemonActionToggleMark, target: target, name: note})
		})
	})
	return nil
}

func (e paletteExec) JumpMark() error {
	if e.effect != nil {
		e.effect.bindActionEnd(e.d, "jump-mark")
	}
	return e.d.jumpMark(e.sess, e.ac, e.effect)
}

func (e paletteExec) EditMarkNote() error {
	tb := e.markTab()
	if tb == nil {
		return nil
	}
	e.sess.mu.Lock()
	if !slices.Contains(e.sess.tabs, tb) {
		e.sess.mu.Unlock()
		return errMoveStaleTarget
	}
	marked, note := tb.marked, tb.markNote
	e.sess.mu.Unlock()
	if !marked {
		e.d.publishAttachmentInfo(e.sess, e.ac, "Tab is not marked")
		e.d.repaintForNotice(e.ac)
		return nil
	}
	e.d.enterPrompt(e.sess, e.ac, " Edit mark note ", note, func(note string) error {
		return e.sess.runMutation(func() error {
			return e.runAction(daemonActionRequest{kind: daemonActionSaveMarkNote, target: daemonActionTarget{session: e.sess, tab: tb, attachment: e.ac}, name: note})
		})
	})
	return nil
}

func (d *Daemon) saveMarkNote(sess *session, tb *tab, note string) error {
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
	if tb.marked {
		tb.markNote = sanitizeMarkNote(note)
		sess.invalidateViewsLocked()
	}
	return nil
}

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
func (d *Daemon) repaintSessionMarkViewers(sessions ...*session) {
	d.refreshMarkProjections()
	d.repaintSessionMarkBars(sessions...)
}

func (d *Daemon) repaintSessionMarkBars(sessions ...*session) {
	lifecycles := make(map[domain.SessionLifecycleID]struct{}, len(sessions))
	for _, sess := range sessions {
		sess.mu.Lock()
		lifecycles[sess.incarnation] = struct{}{}
		sess.mu.Unlock()
	}
	d.requestRouteViewerRepaint(lifecycles, nil)
}

func (d *Daemon) refreshMarkProjections() {
	d.refreshClientPickers()
	for _, current := range d.sessionsSnapshot() {
		for _, ac := range current.snapshotAttachments() {
			d.refreshPalette(ac)
		}
	}
}

// publishAttachmentInfo delivers an ephemeral attachment-only reminder, without history.
// Call without architecture locks; rendering is left to the caller.
func (d *Daemon) publishAttachmentInfo(sess *session, ac *attachedClient, message string) {
	d.publishToast(ac, domain.Notification{Code: domain.NoticeUser, Severity: domain.NoticeInfo, Message: message, Time: d.daemonNow(), Count: 1, SessionID: sess.id})
}

// publishMarkArrival consumes the identity transition recorded by view publication.
// Session -> viewMu matches navigation's lock order; toast publication runs unlocked.
func (d *Daemon) publishMarkArrival(sess *session, ac *attachedClient, tb *tab) bool {
	if sess == nil || ac == nil || tb == nil {
		return false
	}
	ac.viewMu.Lock()
	pending := ac.markArrivalPending
	ac.viewMu.Unlock()
	if !pending {
		return false
	}
	sess.mu.Lock()
	if !slices.Contains(sess.tabs, tb) {
		sess.mu.Unlock()
		return false
	}
	ac.viewMu.Lock()
	arrived := ac.markArrivalPending && ac.view.tabID == domain.TabStableID(tb.stableID) && ac.currentAttachmentSession() == sess
	if arrived {
		ac.markArrivalPending = false
	}
	note := ""
	if arrived && tb.marked {
		note = tb.markNote
	}
	ac.viewMu.Unlock()
	sess.mu.Unlock()
	if note != "" {
		d.publishAttachmentInfo(sess, ac, string(d.glyphsFor(ac).Mark)+" "+note)
		return true
	}
	return false
}
