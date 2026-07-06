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
// Locking: a pane's screen/scrollback and per-client renderer shadow are
// guarded by pane.mu/tab.mu as appropriate; the attached-client pointer by
// session.mu; the registry by Daemon.mu. When more than one is held the order
// is always attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader only ever takes pane.mu, so it never blocks on a slow client.
package daemon

import (
	"sort"
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var pickerModal = ui.Modal{WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8, Title: " Sessions "}

func (d *Daemon) enterPicker(sess *session, ac *attachedClient) {
	views, curTab := d.pickerViews(sess)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = picker.New(views, sess.id, curTab)
	ac.overlays.pickerPending = nil
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
	d.paint(sess, ac, true)
}

func (d *Daemon) pickerViews(cur *session) ([]picker.SessionView, int) {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	stopped := make([]stoppedSession, 0, len(d.stopped))
	for _, s := range d.stopped {
		if !s.purging {
			stopped = append(stopped, s)
		}
	}
	d.mu.Unlock()
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].name < sessions[j].name })
	sort.Slice(stopped, func(i, j int) bool { return stopped[i].name < stopped[j].name })

	views := make([]picker.SessionView, 0, len(sessions)+len(stopped))
	curTab := 0
	for _, s := range sessions {
		s.mu.Lock()
		view := picker.SessionView{ID: s.id, Name: s.name, Active: s.active, Tabs: make([]string, len(s.tabs))}
		sessionAttention := false
		for i, tb := range s.tabs {
			label := strconv.Itoa(i + 1)
			if tb.attention {
				label = attentionSuffix(label)
				sessionAttention = true
			}
			view.Tabs[i] = label
		}
		if sessionAttention {
			view.Name = attentionSuffix(view.Name)
		}
		if s == cur {
			curTab = s.active
		}
		s.mu.Unlock()
		views = append(views, view)
	}
	for _, s := range stopped {
		views = append(views, picker.SessionView{
			ID:      domain.SessionID("stopped:" + s.name),
			Name:    s.name,
			Tabs:    []string{""},
			Stopped: true,
		})
	}
	return views, curTab
}

func attentionSuffix(label string) string {
	return label + " " + string(attentionGlyph)
}

func (d *Daemon) handlePickerInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	if ac.overlays.picker == nil {
		ac.overlays.pickerPending = nil
		d.stopPickerPendingTimerLocked(ac)
		ac.overlays.pickerMu.Unlock()
		return
	}
	if len(ac.overlays.pickerPending) > 0 {
		d.stopPickerPendingTimerLocked(ac)
		combined := make([]byte, 0, len(ac.overlays.pickerPending)+len(data))
		combined = append(combined, ac.overlays.pickerPending...)
		combined = append(combined, data...)
		data = combined
		ac.overlays.pickerPending = nil
	}
	changed := false
	exit := false
	switchTarget := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'x':
			target, ok := ac.overlays.picker.Selected()
			ac.overlays.pickerMu.Unlock()
			if ok {
				d.killPickerTarget(target)
			}
			d.refreshPicker(ac)
			d.paint(sess, ac, true)
			return
		case 'j':
			ac.overlays.picker.Down()
			changed = true
		case 'k':
			ac.overlays.picker.Up()
			changed = true
		case '\r', '\n':
			switchTarget = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routePickerEscape(ac.overlays.picker, tail)
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
					ac.overlays.pickerPending = append(ac.overlays.pickerPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	var target picker.Target
	var ok bool
	if switchTarget {
		target, ok = ac.overlays.picker.Selected()
	}
	ac.overlays.pickerMu.Unlock()

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
	ac.overlays.pickerPending = append(ac.overlays.pickerPending[:0], keys.ESC)
	ac.overlays.pickerESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		ac.overlays.pickerMu.Lock()
		if ac.overlays.pickerESC.timer != timer || len(ac.overlays.pickerPending) != 1 || ac.overlays.pickerPending[0] != keys.ESC || ac.overlays.picker == nil {
			ac.overlays.pickerMu.Unlock()
			return
		}
		ac.overlays.pickerPending = nil
		ac.overlays.pickerESC.timer = nil
		ac.overlays.pickerESC.done = nil
		ac.overlays.picker = nil
		ac.overlays.pickerMu.Unlock()

		d.unregisterPreview(ac)
		if sess := ac.currentSession(); sess != nil {
			d.paint(sess, ac, true)
		}
	})
}

func (d *Daemon) stopPickerPendingTimerLocked(ac *attachedClient) {
	ac.overlays.pickerESC.stop()
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
	ac.overlays.pickerMu.Lock()
	var target picker.Target
	var ok bool
	if ac.overlays.picker != nil {
		target, ok = ac.overlays.picker.Selected()
	}
	ac.overlays.pickerMu.Unlock()

	var next *tab
	if ok {
		next = d.tabByTarget(target)
	}

	ac.overlays.pickerMu.Lock()
	old := ac.overlays.pickerPreview
	if ac.overlays.picker == nil {
		next = nil
	}
	ac.overlays.pickerPreview = next
	ac.overlays.pickerMu.Unlock()

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

		ac.overlays.pickerMu.Lock()
		keep := ac.overlays.pickerPreview == next
		ac.overlays.pickerMu.Unlock()
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
	ac.overlays.pickerMu.Lock()
	old := ac.overlays.pickerPreview
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()

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
	previewer.overlays.pickerMu.Lock()
	if previewer.overlays.pickerPreview == tb {
		previewer.overlays.pickerPreview = nil
		cleared = true
	}
	previewer.overlays.pickerMu.Unlock()
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
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = nil
	ac.overlays.pickerPending = nil
	d.stopPickerPendingTimerLocked(ac)
	ac.overlays.pickerMu.Unlock()
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
	d.clearHistoryNav(ac)
	if target.Stopped {
		d.resumeStoppedAndSwitch(sess, ac, target)
		return
	}
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
	d.touchMRU(targetSess)
	ac.setSession(targetSess)
	d.mu.Unlock()
	return old
}

func (d *Daemon) resumeStoppedAndSwitch(from *session, ac *attachedClient, target picker.Target) {
	d.mu.Lock()
	stopped, ok := d.stopped[target.Name]
	if !ok || stopped.purging {
		d.mu.Unlock()
		d.paint(from, ac, true)
		return
	}
	from.mu.Lock()
	if from.client != ac {
		from.mu.Unlock()
		d.mu.Unlock()
		return
	}
	cwd := d.dirOrHome(stopped.cwd)
	targetSess, err := d.createSessionLocked(target.Name, false, cwd, ac.size)
	if err != nil {
		from.mu.Unlock()
		d.mu.Unlock()
		d.log.Warn("resuming stopped session failed", "err", err, "session", target.Name)
		d.paint(from, ac, true)
		return
	}
	from.client = nil
	ac.setSession(nil)
	from.mu.Unlock()
	targetSess.mu.Lock()
	targetSess.client = ac
	targetSess.mu.Unlock()
	d.touchMRU(targetSess)
	ac.setSession(targetSess)
	d.mu.Unlock()
	d.firstPaint(targetSess, ac, ac.size)
}

func (d *Daemon) killPickerTarget(target picker.Target) {
	if target.Stopped {
		d.mu.Lock()
		stopped, ok := d.stopped[target.Name]
		if ok && !stopped.purging {
			if err := d.persist.Delete(target.Name); err != nil {
				d.mu.Unlock()
				d.log.Warn("deleting persisted stopped session failed", "err", err, "session", target.Name)
				return
			}
			if cur, ok := d.stopped[target.Name]; ok && cur == stopped {
				delete(d.stopped, target.Name)
			}
		}
		d.mu.Unlock()
		return
	}
	d.mu.Lock()
	targetSess := d.sessions[target.Session]
	d.mu.Unlock()
	if targetSess != nil {
		_ = d.killSession(targetSess, ports.ReasonSessionKilled, true)
	}
}

func (d *Daemon) refreshPicker(ac *attachedClient) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	views, curTab := d.pickerViews(sess)
	ac.overlays.pickerMu.Lock()
	if ac.overlays.picker != nil {
		ac.overlays.picker = picker.New(views, sess.id, curTab)
	}
	ac.overlays.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
}

func composePickerClientFrame(model *picker.Model, preview picker.Preview, base renderer.Frame, styles ...themeStyles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveThemeStyles(styles)
	return composeModalClientFrame(base, pickerModal, styleSet, styleSet.selection, func(size domain.Size, styles ...renderer.Style) renderer.Frame {
		return model.Render(size, preview, styles...)
	})
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
	p := tb.focusedPane()
	if p == nil {
		return picker.Preview{}
	}
	if tb.tree == nil || tb.tree.Root == nil || tb.tree.Root.Kind == layout.Leaf {
		p.mu.Lock()
		defer p.mu.Unlock()
		return pickerPreviewFromLockedPane(p)
	}

	area := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	if area.Width <= 0 || area.Height <= 0 {
		p.mu.Lock()
		defer p.mu.Unlock()
		return pickerPreviewFromLockedPane(p)
	}
	frame, _ := composeTabFrame(tb, area, themeui.Theme{})
	return pickerPreviewFromFrame(frame)
}

func pickerPreviewFromLockedPane(p *pane) picker.Preview {
	return pickerPreviewFromFrame(p.screen.Frame)
}

func pickerPreviewFromFrame(frame renderer.Frame) picker.Preview {
	rows := make([][]renderer.Cell, frame.Height)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), frame.Row(y)...)
	}
	return picker.Preview{Rows: rows, Width: frame.Width, Height: frame.Height}
}
