// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"sort"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

var pickerModal = ui.Modal{WidthPct: 80, HeightPct: 80, MinWidth: 24, MinHeight: 8, Title: " Sessions ", Anchor: domain.AnchorCenter, Margins: ui.Margins{}}

func (d *Daemon) enterPicker(sess *session, ac *attachedClient) {
	views, curTab := d.pickerViews(sess)
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = picker.New(views, sess.id, curTab)
	ac.overlays.pickerPending = nil
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	d.registerPreviewForSelection(ac)
	d.invalidateRender(sess, ac, true, "picker.go")
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

	for _, s := range sessions {
		d.refreshSessionFocusedTitles(s)
	}

	includeTerminalTitle := d.currentTabsConfig().TerminalTitle
	views := make([]picker.SessionView, 0, len(sessions)+len(stopped))
	curTab := 0
	for _, s := range sessions {
		s.mu.Lock()
		view := picker.SessionView{ID: s.id, Name: s.name, Active: s.active, Tabs: make([]picker.TabEntry, len(s.tabs))}
		sessionAttention := false
		for i, tb := range s.tabs {
			name := tabDisplayName(tb, i)
			view.Tabs[i] = picker.TabEntry{
				Name:      name,
				Detail:    tabTitleDetail(name, tb.focusedPaneTitle(includeTerminalTitle)),
				Attention: tb.attention,
			}
			if tb.attention {
				sessionAttention = true
			}
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
			Tabs:    []picker.TabEntry{{}},
			Stopped: true,
		})
	}
	return views, curTab
}

func attentionSuffix(label string) string {
	return label + " " + string(ui.AttentionGlyph)
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
			d.invalidateRender(sess, ac, true, "picker.go")
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
		d.invalidateRender(sess, ac, true, "picker.go")
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
			d.invalidateRender(sess, ac, true, "picker.go")
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
	previous := ac.overlays.pickerPreviewSession
	ac.overlays.pickerPreviewSession = nil
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	if previous != nil {
		if rc := previous.renderCoordinator(); rc != nil {
			rc.teardownPreviewFor(ac)
		}
	}
	if !ok {
		return
	}
	next := d.tabByTarget(target)
	targetSess := d.sessionByID(target.Session)
	if next == nil || targetSess == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	// Selection may have changed while the target was resolved.
	if ac.overlays.picker == nil {
		ac.overlays.pickerMu.Unlock()
		return
	}
	ac.overlays.pickerPreview = next
	ac.overlays.pickerPreviewSession = targetSess
	ac.overlays.pickerMu.Unlock()
	// A same-session preview is already invalidated by its own coordinator.
	// Cross-session previews subscribe to the target's producer wake instead
	// of starting a direct paint/timer path.
	if targetSess == ac.currentSession() {
		return
	}
	rc := d.attachCoordinator(targetSess, nil, nil, false)
	rc.subscribePreviewFor(ac, func(renderWake) {
		ac.overlays.pickerMu.Lock()
		valid := ac.overlays.pickerPreviewSession == targetSess
		ac.overlays.pickerMu.Unlock()
		if valid {
			if owner := ac.currentSession(); owner != nil {
				d.invalidateRender(owner, ac, false, "picker preview")
			}
		}
	})
}

func (d *Daemon) unregisterPreview(ac *attachedClient) {
	if ac == nil || ac.overlays == nil {
		return
	}
	ac.overlays.pickerMu.Lock()
	previous := ac.overlays.pickerPreviewSession
	ac.overlays.pickerPreviewSession = nil
	ac.overlays.pickerPreview = nil
	ac.overlays.pickerMu.Unlock()
	if previous != nil {
		if rc := previous.renderCoordinator(); rc != nil {
			rc.teardownPreviewFor(ac)
		}
	}
}

// clearDestroyedTabPreview clears coordinator-owned picker state for clients
// that were previewing a removed tab. Tabs do not route render ownership.
func (d *Daemon) clearDestroyedTabPreview(tb *tab) {
	if d == nil || d.sessions == nil {
		return
	}
	d.mu.Lock()
	var clients []*attachedClient
	for _, sess := range d.sessions {
		sess.mu.Lock()
		if sess.client != nil {
			clients = append(clients, sess.client)
		}
		sess.mu.Unlock()
	}
	d.mu.Unlock()
	for _, ac := range clients {
		if ac == nil || ac.overlays == nil {
			continue
		}
		ac.overlays.pickerMu.Lock()
		cleared := ac.overlays.pickerPreview == tb
		ac.overlays.pickerMu.Unlock()
		if cleared {
			d.unregisterPreview(ac)
			if sess := ac.currentSession(); sess != nil {
				d.invalidateRender(sess, ac, true, "picker.go")
			}
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

func (d *Daemon) switchToTarget(sess *session, ac *attachedClient, target picker.Target) bool {
	if target.Stopped {
		return d.resumeStoppedAndSwitch(sess, ac, target)
	}
	targetSess := d.sessionByID(target.Session)
	if targetSess == nil {
		d.invalidateRender(sess, ac, true, "picker.go")
		return false
	}
	if targetSess == sess {
		if sess.switchTab(target.TabIndex) {
			d.activateTab(sess, sess.activeTab())
		}
		d.invalidateRender(sess, ac, true, "picker.go")
		return true
	}
	old := d.stealClientForTarget(sess, ac, targetSess, target)
	if ac.currentSession() != targetSess {
		d.invalidateRender(sess, ac, true, "picker.go")
		return false
	}
	if old != nil && old != ac {
		d.unregisterPreview(old)
		old.clearPreviousSession()
		old.setSession(nil)
		d.notifyDetachedAsync(old, ports.ReasonDetach)
	}
	d.firstPaint(targetSess, ac, ac.size)
	return true
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
	term := from.terminal
	from.client = nil
	ac.setSession(nil)
	from.mu.Unlock()

	targetSess.mu.Lock()
	old := targetSess.client
	targetSess.terminal = term
	if target.TabIndex >= 0 && target.TabIndex < len(targetSess.tabs) {
		targetSess.active = target.TabIndex
	}
	targetSess.mu.Unlock()

	// Do not expose targetSess.client until the target coordinator has claimed
	// this attachment and the output dependency chain has been rebased.
	d.handoffCoordinator(from, targetSess, old, ac)
	ac.setSession(targetSess)
	targetSess.mu.Lock()
	targetSess.client = ac
	targetSess.mu.Unlock()
	d.touchMRU(targetSess)
	ac.recordPreviousSession(from)
	d.mu.Unlock()
	if old != nil && old != ac {
		// Displacement is a detach lifecycle transition too. Cancel the
		// attachment-owned PR #71 timer only after releasing daemon/session
		// locks, preserving sendMu > daemon/session lock ordering.
		old.cancelResizePaint()
	}
	return old
}

func (d *Daemon) resumeStoppedAndSwitch(from *session, ac *attachedClient, target picker.Target) bool {
	d.mu.Lock()
	stopped, ok := d.stopped[target.Name]
	if !ok || stopped.purging {
		d.mu.Unlock()
		d.invalidateRender(from, ac, true, "picker.go")
		return false
	}
	from.mu.Lock()
	if from.client != ac {
		from.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	term := from.terminal
	cwd := d.dirOrHome(stopped.cwd)
	targetSess, err := d.createSessionLocked(target.Name, false, cwd, ac.size, term, stopped.tabNames)
	if err != nil {
		from.mu.Unlock()
		d.mu.Unlock()
		d.log.Warn("resuming stopped session failed", "err", err, "session", target.Name)
		d.invalidateRender(from, ac, true, "picker.go")
		return false
	}
	from.client = nil
	ac.setSession(nil)
	from.mu.Unlock()

	d.handoffCoordinator(from, targetSess, nil, ac)
	ac.setSession(targetSess)
	targetSess.mu.Lock()
	targetSess.client = ac
	targetSess.mu.Unlock()
	d.touchMRU(targetSess)
	ac.recordPreviousSession(from)
	d.mu.Unlock()
	d.firstPaint(targetSess, ac, ac.size)
	return true
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
			delete(d.stopped, target.Name)
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

func snapshotPickerPreview(tb *tab) picker.Preview {
	if tb == nil {
		return picker.Preview{}
	}
	tb.mu.Lock()
	layoutSnap := solveTabLayoutLocked(tb)
	if !layoutSnap.ok {
		p := tb.focusedPane()
		if p == nil {
			tb.mu.Unlock()
			return picker.Preview{}
		}
		p.mu.Lock()
		preview := pickerPreviewFromLockedPane(p)
		p.mu.Unlock()
		tb.mu.Unlock()
		return preview
	}
	state := capturedRenderState{layout: capturedTabLayout{
		root: tb.tree.Clone().Root, area: layoutSnap.area, focus: layoutSnap.focus,
		placements:  append([]layout.Placement(nil), layoutSnap.placements...),
		fingerprint: layoutSnap.fingerprint, valid: true,
	}}
	for _, placement := range layoutSnap.placements {
		p := tb.panes[placement.ID]
		if p == nil {
			continue
		}
		visible := placement.Content
		if placement.Collapsed {
			visible = domain.Rect{}
		}
		p.mu.Lock()
		captured := capturePaneRenderStateLocked(p, visible, damageCapturePreview)
		p.mu.Unlock()
		captured.placement = placement
		captured.focused = placement.ID == layoutSnap.focus
		state.panes = append(state.panes, captured)
	}
	tb.mu.Unlock()
	return pickerPreviewFromCapturedRender(state)
}

func pickerPreviewFromCapturedRender(state capturedRenderState) picker.Preview {
	composed := composeFrame(state, composeCacheInput{}).frame
	if composed.Height < 2 {
		return pickerPreviewFromFrame(composed)
	}
	rows := make([][]renderer.Cell, composed.Height-2)
	for y := range rows {
		rows[y] = append([]renderer.Cell(nil), composed.Row(y+1)...)
	}
	return picker.Preview{Rows: rows, Width: composed.Width, Height: len(rows)}
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
