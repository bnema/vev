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
	"bytes"
	"strconv"

	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/mouse"
)

func (d *Daemon) handleSequencedInput(sess *session, ac *attachedClient, _ uint64, data []byte) {
	// Do not acknowledge client-side echo prediction here: input has only been
	// accepted/routed, not necessarily echoed by the PTY and incorporated into a
	// rendered screen state. Until prediction is implemented against rendered
	// output state, EchoAck must remain conservative.
	d.handleInput(sess, ac, data)
}

func (d *Daemon) handleInput(_ *session, ac *attachedClient, data []byte) {
	ac.initOverlays()
	ac.mouseScan.Scan(data,
		func(ev mouse.Event) { d.handleMouse(ac, ev) },
		func(b []byte) {
			if ac.overlays.HandleInput(d, b) {
				return
			}
			ac.keys.Route(b)
		},
	)
}

func (d *Daemon) handleMouse(ac *attachedClient, ev mouse.Event) {
	ac.initOverlays()
	rt := ac.overlays
	if rt.promptActive() || rt.paletteActive() || rt.pickerActive() {
		return
	}
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	if rt.copyActive() {
		if rt.copySearchActive() {
			return
		}
		copyPane := copyTargetPane(rt)
		tb.mu.Lock()
		floatingPane, geometry, floatingVisible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
		floatingCopy := floatingVisible && copyPane == floatingPane
		tb.mu.Unlock()
		if floatingCopy {
			contentRow := ev.Row - 1
			if !pointInRect(ev.Col, contentRow, geometry.Inner) {
				return
			}
			// Match the multi-pane translation: hand copyMouse a zero-based
			// popup row so a click selects exactly the row under the pointer.
			ev.Row = contentRow
			ev = translateMouseEvent(ev, geometry.Inner.X, geometry.Inner.Y)
			switch ev.Button {
			case mouse.Left:
				d.copyMouse(sess, ac, ev)
			case mouse.WheelUp:
				d.copyWheel(sess, ac, -3)
			case mouse.WheelDown:
				d.copyWheel(sess, ac, 3)
			}
			return
		}
		if ev.Button == mouse.Left {
			contentRow := ev.Row - 1
			tb.mu.Lock()
			p := tb.focusedPane()
			var pl layout.Placement
			var ok bool
			multi := len(tb.panes) > 1
			if p != nil {
				pl, ok = focusedPlacementLocked(tb)
			}
			if ok && multi && !pointInRect(ev.Col, contentRow, pl.Content) && ev.Type == mouse.Press {
				if target, hit := hitTestPlacementLocked(tb, ev.Col, contentRow); hit {
					oldFocus := tb.tree.Focus
					focusPlacementLocked(tb, target.ID)
					d.applyLayoutLocked(tb)
					tb.mu.Unlock()
					d.exitCopyMode(ac)
					if target.ID != oldFocus {
						d.refreshPaneTitleOnFocus(sess, target.ID)
					}
					d.paint(sess, ac, true)
					return
				}
			}
			tb.mu.Unlock()
			if ok && multi {
				clampedCol := clampInt(ev.Col, pl.Content.X, pl.Content.X+pl.Content.Width-1)
				clampedRow := clampInt(contentRow, pl.Content.Y, pl.Content.Y+pl.Content.Height-1)
				ev.Col = clampedCol
				ev.Row = clampedRow
				ev = translateMouseEvent(ev, pl.Content.X, pl.Content.Y)
			}
		}
		switch ev.Button {
		case mouse.Left:
			d.copyMouse(sess, ac, ev)
		case mouse.WheelUp:
			d.copyWheel(sess, ac, -3)
		case mouse.WheelDown:
			d.copyWheel(sess, ac, 3)
		}
		return
	}

	tb.mu.Lock()
	contentRow := ev.Row - 1
	floating, floatingGeometry, floatingVisible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
	if floatingVisible {
		if !pointInRect(ev.Col, contentRow, floatingGeometry.Inner) {
			tb.mu.Unlock()
			return
		}
		tb.mu.Unlock()
		ev = translateMouseEvent(ev, floatingGeometry.Inner.X, floatingGeometry.Inner.Y)
		d.handleTerminalMouse(sess, ac, floating, ev, true, true)
		return
	}
	pl, hit := hitTestPlacementLocked(tb, ev.Col, contentRow)
	multi := len(tb.panes) > 1
	focusedID := layout.PaneID("")
	if tb.tree != nil {
		focusedID = tb.tree.Focus
	}
	if hit && pointInRect(ev.Col, contentRow, pl.TitleBar) {
		if !isMouseFocusPress(ev) {
			tb.mu.Unlock()
			return
		}
		oldFocus := focusedID
		focusPlacementLocked(tb, pl.ID)
		d.applyLayoutLocked(tb)
		tb.mu.Unlock()
		if pl.ID != oldFocus {
			d.exitCopyMode(ac)
			d.refreshPaneTitleOnFocus(sess, pl.ID)
		}
		d.paint(sess, ac, true)
		return
	}
	var p *pane
	translated := false
	hoveredFocused := true
	if hit && !pl.Collapsed && pointInRect(ev.Col, contentRow, pl.Content) {
		oldFocus := focusedID
		if isMouseFocusPress(ev) {
			focusPlacementLocked(tb, pl.ID)
			d.applyLayoutLocked(tb)
		}
		p = tb.panes[pl.ID]
		hoveredFocused = pl.ID == oldFocus
		tb.mu.Unlock()
		if p == nil {
			return
		}
		if isMouseFocusPress(ev) && pl.ID != oldFocus {
			d.exitCopyMode(ac)
			d.refreshPaneTitleOnFocus(sess, pl.ID)
			d.paint(sess, ac, true)
		}
		if multi {
			ev = translateMouseEvent(ev, pl.Content.X, pl.Content.Y)
			translated = true
		}
	} else {
		if multi {
			tb.mu.Unlock()
			return
		}
		p = tb.focusedPane()
		tb.mu.Unlock()
		if p == nil {
			return
		}
	}
	d.handleTerminalMouse(sess, ac, p, ev, translated, hoveredFocused)
}

func (d *Daemon) handleTerminalMouse(sess *session, ac *attachedClient, p *pane, ev mouse.Event, translated, hoveredFocused bool) {
	if p == nil {
		return
	}
	rt := ac.overlays
	p.mu.Lock()
	childRows := p.screen.Frame.Height
	mouseMode, mouseSGR := p.screen.MouseMode()
	altScreen := p.screen.AltScreenActive()
	scrollbackRows := 0
	if p.scrollback != nil {
		scrollbackRows = p.scrollback.Len()
	}
	p.mu.Unlock()

	if mouseMode != 0 {
		if !mouseSGR || ev.Row == 0 || ev.Row > childRows {
			return
		}
		if translated {
			d.writeToPane(sess, p, ev.Raw)
		} else {
			d.writeToPane(sess, p, sgrRowOffset(ev.Raw, -1))
		}
		return
	}

	switch ev.Button {
	case mouse.Left:
		switch ev.Type {
		case mouse.Press:
			if altScreen || ev.Row >= childRows {
				rt.copyMu.Lock()
				rt.normalMousePressValid = false
				rt.copyMu.Unlock()
				return
			}
			rt.copyMu.Lock()
			rt.normalMousePressRow = ev.Row
			rt.normalMousePressTop = scrollbackRows
			rt.normalMousePressValid = true
			rt.copyMu.Unlock()
		case mouse.Motion:
			if altScreen || ev.Row >= childRows {
				return
			}
			rt.copyMu.Lock()
			pressValid := rt.normalMousePressValid
			pressRow := rt.normalMousePressRow
			pressTop := rt.normalMousePressTop
			rt.copyMu.Unlock()
			if !pressValid {
				return
			}

			p.mu.Lock()
			document := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
			rt.copyMu.Lock()
			mode := scopy.NewMode(document)
			mode.StartSelectionAt(document, pressTop+pressRow)
			mode.ExtendTo(document, document.Len()-document.Height+ev.Row)
			rt.copyMode = mode
			rt.copySnapshot = &document
			rt.copyPane = p
			rt.copyPressRow = pressTop + pressRow
			rt.copyPressRowValid = true
			rt.copyDragging = true
			rt.normalMousePressValid = false
			rt.copyMu.Unlock()
			p.mu.Unlock()
			d.paint(sess, ac, true)
		case mouse.Release:
			rt.copyMu.Lock()
			rt.normalMousePressValid = false
			rt.copyMu.Unlock()
		}
	case mouse.WheelUp:
		if altScreen {
			d.writeToPane(sess, p, []byte("\x1b[A\x1b[A\x1b[A"))
			return
		}
		if !hoveredFocused {
			return
		}
		d.enterCopyMode(sess, ac)
		d.copyWheel(sess, ac, -3)
	case mouse.WheelDown:
		if altScreen {
			d.writeToPane(sess, p, []byte("\x1b[B\x1b[B\x1b[B"))
		}
	}
}

func isMouseFocusPress(ev mouse.Event) bool {
	return ev.Type == mouse.Press && (ev.Button == mouse.Left || ev.Button == mouse.Middle || ev.Button == mouse.Right)
}

func clampInt(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func (d *Daemon) writeToPane(sess *session, p *pane, data []byte) {
	if p == nil || p.pty == nil {
		return
	}
	if _, err := p.pty.Write(data); err != nil {
		name := ""
		if sess != nil {
			sess.mu.Lock()
			name = sess.name
			sess.mu.Unlock()
		}
		d.log.Error("pty write failed", "err", err, "session", name)
	}
}

func sgrRowOffset(raw []byte, delta int) []byte {
	return sgrOffset(raw, 0, delta)
}

func sgrOffset(raw []byte, colDelta, rowDelta int) []byte {
	if len(raw) < len("\x1b[<0;1;1M") {
		return raw
	}
	end := len(raw) - 1
	if raw[0] != '\x1b' || raw[1] != '[' || raw[2] != '<' || (raw[end] != 'M' && raw[end] != 'm') {
		return raw
	}

	parts := bytes.Split(raw[3:end], []byte(";"))
	if len(parts) != 3 {
		return raw
	}
	cx, err := strconv.Atoi(string(parts[1]))
	if err != nil {
		return raw
	}
	cy, err := strconv.Atoi(string(parts[2]))
	if err != nil {
		return raw
	}
	cx += colDelta
	cy += rowDelta
	if cx < 1 || cy < 1 {
		return raw
	}

	out := make([]byte, 0, len(raw)+4)
	out = append(out, raw[:3]...)
	out = append(out, parts[0]...)
	out = append(out, ';')
	out = strconv.AppendInt(out, int64(cx), 10)
	out = append(out, ';')
	out = strconv.AppendInt(out, int64(cy), 10)
	out = append(out, raw[end])
	return out
}

type daemonKeyHandler struct {
	d  *Daemon
	ac *attachedClient
}

func (h daemonKeyHandler) Forward(data []byte) {
	sess := h.ac.currentSession()
	if sess == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.terminalTargetLocked()
	tb.mu.Unlock()
	h.d.writeToPane(sess, p, data)
}

func (h daemonKeyHandler) Action(action keys.Action) {
	sess := h.ac.currentSession()
	if sess == nil {
		return
	}
	switch action {
	case keys.ActionOpenPalette:
		h.d.enterPalette(sess, h.ac)
	case keys.ActionJumpAttention:
		h.d.jumpAttention(sess, h.ac)
	case keys.ActionToggleFloatingPane:
		if err := h.d.toggleFloating(sess, h.ac); err != nil {
			h.d.log.Warn("toggle floating pane failed", "err", err)
		}
	case keys.ActionFocusPaneLeft:
		_ = h.d.focusDir(sess, h.ac, layout.Left)
	case keys.ActionFocusPaneRight:
		_ = h.d.focusDir(sess, h.ac, layout.Right)
	case keys.ActionFocusPaneUp:
		_ = h.d.focusDir(sess, h.ac, layout.Up)
	case keys.ActionFocusPaneDown:
		_ = h.d.focusDir(sess, h.ac, layout.Down)
	case keys.ActionSwitchTab1, keys.ActionSwitchTab2, keys.ActionSwitchTab3,
		keys.ActionSwitchTab4, keys.ActionSwitchTab5, keys.ActionSwitchTab6,
		keys.ActionSwitchTab7, keys.ActionSwitchTab8, keys.ActionSwitchTab9:
		idx := int(action - keys.ActionSwitchTab1)
		if sess.switchTab(idx) {
			h.d.activateTab(sess, sess.activeTab())
			h.d.paint(sess, h.ac, true)
		}
	}
}
