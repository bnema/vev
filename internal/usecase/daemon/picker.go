// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (sessions own one or more PTY-backed tabs):
//
//   - Serve runs the accept loop. Each accepted connection is handled by its
//     own goroutine (handleConn): it reads the first frame and routes it to a
//     session create/attach, a list, or a kill.
//   - Per session there are exactly two long-lived goroutines: the PTY reader
//     (drains child output into the VT screen and pokes a cap-1 dirty channel)
//     and the render scheduler (debounces dirties and paints the attached
//     client). Both are tied to the session context and unwind when the
//     session is killed (pty.Close unblocks the reader; ctx cancel stops the
//     scheduler).
//   - The daemon exits (Serve returns) when the last session is removed, or
//     when the parent context is cancelled (graceful shutdown notifies any
//     attached clients with ReasonServerShutdown).
//
// Locking: a session's screen and per-client renderer shadow are both guarded
// by tab.mu; the attached-client pointer by session.mu; the registry by
// Daemon.mu. When more than one is held the order is always
// Daemon.mu > session.mu, and (for the transport) attachedClient.sendMu >
// tab.mu — the PTY reader only ever takes tab.mu, so it never blocks on
// a slow client.
package daemon

import (
	"sort"
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var pickerModal = ui.Modal{WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8, Title: " Sessions "}

func (d *Daemon) enterPicker(sess *session, ac *attachedClient) {
	views, curTab := d.pickerViews(sess)
	ac.pickerMu.Lock()
	ac.picker = picker.New(views, sess.id, curTab)
	ac.pickerPending = nil
	ac.pickerPreview = nil
	ac.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
	d.paint(sess, ac, true)
}

func (d *Daemon) pickerViews(cur *session) ([]picker.SessionView, int) {
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, s := range d.sessions {
		sessions = append(sessions, s)
	}
	d.mu.Unlock()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].name < sessions[j].name })

	views := make([]picker.SessionView, 0, len(sessions))
	curTab := 0
	for _, s := range sessions {
		s.mu.Lock()
		view := picker.SessionView{ID: s.id, Name: s.name, Active: s.active, Tabs: make([]string, len(s.tabs))}
		for i := range s.tabs {
			view.Tabs[i] = strconv.Itoa(i + 1)
		}
		if s == cur {
			curTab = s.active
		}
		s.mu.Unlock()
		views = append(views, view)
	}
	return views, curTab
}

func (d *Daemon) handlePickerInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	ac.pickerMu.Lock()
	if ac.picker == nil {
		ac.pickerPending = nil
		d.stopPickerPendingTimerLocked(ac)
		ac.pickerMu.Unlock()
		return
	}
	if len(ac.pickerPending) > 0 {
		d.stopPickerPendingTimerLocked(ac)
		combined := make([]byte, 0, len(ac.pickerPending)+len(data))
		combined = append(combined, ac.pickerPending...)
		combined = append(combined, data...)
		data = combined
		ac.pickerPending = nil
	}
	changed := false
	exit := false
	switchTarget := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			ac.picker.Down()
			changed = true
		case 'k':
			ac.picker.Up()
			changed = true
		case '\r', '\n':
			switchTarget = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routePickerEscape(ac.picker, tail)
				if ok {
					i += consumed - 1
					changed = true
					continue
				}
				if len(tail) == 1 {
					d.retainPickerESCLocked(ac)
					break
				}
				if isPickerEscapePrefix(tail) {
					ac.pickerPending = append(ac.pickerPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	var target picker.Target
	var ok bool
	if switchTarget {
		target, ok = ac.picker.Selected()
	}
	ac.pickerMu.Unlock()

	if changed {
		d.registerPreviewForSelection(ac)
	}
	if exit {
		d.closePicker(ac)
	}
	if switchTarget && ok {
		d.switchToTarget(sess, ac, target)
		return
	}
	if exit || changed {
		d.paint(sess, ac, true)
	}
}

func (d *Daemon) retainPickerESCLocked(ac *attachedClient) {
	ac.pickerPending = append(ac.pickerPending[:0], keys.ESC)
	ac.pickerESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		ac.pickerMu.Lock()
		if ac.pickerESC.timer != timer || len(ac.pickerPending) != 1 || ac.pickerPending[0] != keys.ESC || ac.picker == nil {
			ac.pickerMu.Unlock()
			return
		}
		ac.pickerPending = nil
		ac.pickerESC.timer = nil
		ac.pickerESC.done = nil
		ac.picker = nil
		ac.pickerMu.Unlock()

		d.unregisterPreview(ac)
		if sess := ac.currentSession(); sess != nil {
			d.paint(sess, ac, true)
		}
	})
}

func (d *Daemon) stopPickerPendingTimerLocked(ac *attachedClient) {
	ac.pickerESC.stop()
}

func routePickerEscape(m *picker.Model, data []byte) (int, bool) {
	if len(data) >= 3 && (data[1] == '[' || data[1] == 'O') {
		switch data[2] {
		case 'A':
			m.Up()
			return 3, true
		case 'B':
			m.Down()
			return 3, true
		}
	}
	return 0, false
}

func isPickerEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && (data[1] == '[' || data[1] == 'O')
}

func (d *Daemon) registerPreviewForSelection(ac *attachedClient) {
	ac.pickerMu.Lock()
	var target picker.Target
	var ok bool
	if ac.picker != nil {
		target, ok = ac.picker.Selected()
	}
	ac.pickerMu.Unlock()

	var next *tab
	if ok {
		next = d.tabByTarget(target)
	}

	ac.pickerMu.Lock()
	old := ac.pickerPreview
	if ac.picker == nil {
		next = nil
	}
	ac.pickerPreview = next
	ac.pickerMu.Unlock()

	if old != nil && old != next {
		old.mu.Lock()
		if old.previewClient == ac {
			old.previewClient = nil
		}
		old.mu.Unlock()
	}
	if next != nil {
		next.mu.Lock()
		next.previewClient = ac
		next.mu.Unlock()

		ac.pickerMu.Lock()
		keep := ac.pickerPreview == next
		ac.pickerMu.Unlock()
		if !keep {
			next.mu.Lock()
			if next.previewClient == ac {
				next.previewClient = nil
			}
			next.mu.Unlock()
		}
	}
}

func (d *Daemon) unregisterPreview(ac *attachedClient) {
	ac.pickerMu.Lock()
	old := ac.pickerPreview
	ac.pickerPreview = nil
	ac.pickerMu.Unlock()

	if old != nil {
		old.mu.Lock()
		if old.previewClient == ac {
			old.previewClient = nil
		}
		old.mu.Unlock()
	}
}

func (d *Daemon) clearDestroyedTabPreview(tb *tab) {
	tb.mu.Lock()
	previewer := tb.previewClient
	tb.mu.Unlock()
	if previewer == nil {
		return
	}
	cleared := false
	previewer.pickerMu.Lock()
	if previewer.pickerPreview == tb {
		previewer.pickerPreview = nil
		cleared = true
	}
	previewer.pickerMu.Unlock()
	tb.mu.Lock()
	if tb.previewClient == previewer {
		tb.previewClient = nil
	}
	tb.mu.Unlock()
	if cleared {
		if sess := previewer.currentSession(); sess != nil {
			d.paint(sess, previewer, true)
		}
	}
}

func (d *Daemon) closePicker(ac *attachedClient) {
	ac.pickerMu.Lock()
	ac.picker = nil
	ac.pickerPending = nil
	d.stopPickerPendingTimerLocked(ac)
	ac.pickerMu.Unlock()
	d.unregisterPreview(ac)
}

func (d *Daemon) tabByTarget(target picker.Target) *tab {
	d.mu.Lock()
	sess := d.sessions[target.Session]
	d.mu.Unlock()
	if sess == nil {
		return nil
	}
	sess.mu.Lock()
	defer sess.mu.Unlock()
	if target.TabIndex < 0 || target.TabIndex >= len(sess.tabs) {
		return nil
	}
	return sess.tabs[target.TabIndex]
}

func (d *Daemon) sessionByID(id domain.SessionID) *session {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[id]
}

func (d *Daemon) switchToTarget(sess *session, ac *attachedClient, target picker.Target) {
	targetSess := d.sessionByID(target.Session)
	if targetSess == nil {
		d.paint(sess, ac, true)
		return
	}
	if targetSess == sess {
		sess.switchTab(target.TabIndex)
		d.paint(sess, ac, true)
		return
	}
	old := d.stealClientForTarget(sess, ac, targetSess, target)
	if ac.currentSession() != targetSess {
		d.paint(sess, ac, true)
		return
	}
	if old != nil && old != ac {
		d.unregisterPreview(old)
		old.setSession(nil)
		d.notifyDetachedAsync(old, ports.ReasonDetach)
	}
	d.firstPaint(targetSess, ac, ac.size)
}

func (d *Daemon) stealClientForTarget(from *session, ac *attachedClient, targetSess *session, target picker.Target) *attachedClient {
	d.mu.Lock()
	if d.sessions[target.Session] != targetSess {
		d.mu.Unlock()
		return nil
	}
	from.mu.Lock()
	if from.client != ac {
		from.mu.Unlock()
		d.mu.Unlock()
		return nil
	}
	from.client = nil
	ac.setSession(nil)
	from.mu.Unlock()

	targetSess.mu.Lock()
	old := targetSess.client
	targetSess.client = ac
	if target.TabIndex >= 0 && target.TabIndex < len(targetSess.tabs) {
		targetSess.active = target.TabIndex
	}
	targetSess.mu.Unlock()
	ac.setSession(targetSess)
	d.mu.Unlock()
	return old
}

func composePickerClientFrame(model *picker.Model, preview picker.Preview, base renderer.Frame) (renderer.Frame, []renderer.Damage) {
	inner := pickerModal.Composite(base, renderer.DefaultStyle())
	modalFrame := model.Render(domain.Size{Cols: inner.Width, Rows: inner.Height}, preview)
	for y := range min(inner.Height, modalFrame.Height) {
		for x := range min(inner.Width, modalFrame.Width) {
			base.Set(inner.X+x, inner.Y+y, modalFrame.At(x, y))
		}
	}
	return base, []renderer.Damage{renderer.FullRedraw()}
}

func snapshotPickerPreview(tb *tab) picker.Preview {
	if tb == nil {
		return picker.Preview{}
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	return pickerPreviewFromLockedTab(tb)
}

func pickerPreviewFromLockedTab(tb *tab) picker.Preview {
	rows := make([][]renderer.Cell, tb.screen.Frame.Height)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), tb.screen.Frame.Row(y)...)
	}
	return picker.Preview{Rows: rows, Width: tb.screen.Frame.Width, Height: tb.screen.Frame.Height}
}
