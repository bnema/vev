// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/bnema/vev/internal/domain"
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
	frameEvent := ev
	ac.initOverlays()
	rt := ac.overlays
	if rt.promptActive() || rt.paletteActive() || rt.pickerActive() || rt.noticesActive() || rt.resizeModeActive() {
		return
	}
	sess := ac.currentSession()
	if sess == nil {
		invalidateRejectedLeftPointer(rt, ev)
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		invalidateRejectedLeftPointer(rt, ev)
		return
	}

	if d.handleCopyMouse(sess, ac, tb, ev) {
		return
	}
	// A fresh drag is pinned to its press geometry. Handle later events before
	// normal terminal routing so crossing a split cannot retarget its document.
	if ev.Button == mouse.Left && ev.Type != mouse.Press && d.handleFreshCopyPointer(sess, ac, ev) {
		return
	}

	tb.mu.Lock()
	contentRow := ev.Row - 1
	floating, floatingGeometry, floatingVisible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
	if floatingVisible {
		if !pointInRect(ev.Col, contentRow, floatingGeometry.Inner) {
			tb.mu.Unlock()
			invalidateRejectedLeftPointer(rt, ev)
			return
		}
		tb.mu.Unlock()
		if ev.Button == mouse.Left && ev.Type == mouse.Press && d.handleFreshCopyPress(sess, ac, tb, floating, frameEvent) {
			return
		}
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
			invalidateRejectedLeftPointer(rt, ev)
			return
		}
		oldFocus := focusedID
		layoutChanged := focusPlacementLocked(tb, pl.ID)
		tb.mu.Unlock()
		if layoutChanged {
			d.applyTabLayout(sess, tb)
		}
		// A title bar never routes to terminal content. Clear any pre-existing
		// left-button candidate before handling the focus result, including when
		// this press leaves the same pane focused.
		invalidateRejectedLeftPointer(rt, ev)
		if pl.ID != oldFocus {
			d.exitCopyMode(ac)
			d.refreshPaneTitleOnFocus(sess, pl.ID)
		}
		d.invalidateRender(sess, ac, true, "input.go")
		return
	}
	var p *pane
	translated := false
	hoveredFocused := true
	if hit && !pl.Collapsed && pointInRect(ev.Col, contentRow, pl.Content) {
		oldFocus := focusedID
		layoutChanged := false
		if isMouseFocusPress(ev) {
			layoutChanged = focusPlacementLocked(tb, pl.ID)
		}
		p = tb.panes[pl.ID]
		hoveredFocused = pl.ID == oldFocus
		tb.mu.Unlock()
		if layoutChanged {
			d.applyTabLayout(sess, tb)
		}
		if p == nil {
			invalidateRejectedLeftPointer(rt, ev)
			return
		}
		if isMouseFocusPress(ev) && pl.ID != oldFocus {
			d.exitCopyMode(ac)
			d.refreshPaneTitleOnFocus(sess, pl.ID)
			d.invalidateRender(sess, ac, true, "input.go")
		}
		if multi {
			ev = translateMouseEvent(ev, pl.Content.X, pl.Content.Y)
			translated = true
		}
	} else {
		if multi {
			tb.mu.Unlock()
			invalidateRejectedLeftPointer(rt, ev)
			return
		}
		p = tb.focusedPane()
		tb.mu.Unlock()
		if p == nil {
			invalidateRejectedLeftPointer(rt, ev)
			return
		}
	}
	if ev.Button == mouse.Left && ev.Type == mouse.Press && d.handleFreshCopyPress(sess, ac, tb, p, frameEvent) {
		return
	}
	d.handleTerminalMouse(sess, ac, p, ev, translated, hoveredFocused)
}

func (d *Daemon) handleTerminalMouse(sess *session, ac *attachedClient, p *pane, ev mouse.Event, translated, hoveredFocused bool) {
	if p == nil {
		return
	}
	p.mu.Lock()
	childRows := p.screen.Frame.Height
	mouseMode, mouseSGR := p.screen.MouseMode()
	altScreen := p.screen.AltScreenActive()
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
		d.notify(sess, domain.NoticeError, domain.NoticeInputDropped,
			"input not delivered to pane", err)
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
	d       *Daemon
	ac      *attachedClient
	actions daemonActionRunner
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
	runResizeAction := func(request daemonActionRequest) {
		request.target = resolveDaemonActionTarget(sess)
		runner := h.actions
		if runner == nil {
			runner = daemonActions{d: h.d}
		}
		if err := runner.Run(request); err != nil {
			if errors.Is(err, errDaemonActionNoChange) {
				return
			}
			h.d.reportError(sess, resizeUserError(err))
			return
		}
		if h.actions == nil {
			finishDaemonActionForClient(h.d, request, h.ac, "input.go")
		}
	}
	switch action {
	case keys.ActionOpenPalette:
		h.d.enterPalette(sess, h.ac)
	case keys.ActionJumpAttention:
		if err := h.d.jumpAttention(sess, h.ac); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionToggleFloatingPane:
		if err := h.d.toggleFloating(sess, h.ac); err != nil {
			h.d.log.Warn("toggle floating pane failed", "err", err)
			h.d.reportError(sess, err)
		}
	case keys.ActionFocusPaneLeft:
		if err := h.d.focusDir(sess, h.ac, layout.Left); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionFocusPaneRight:
		if err := h.d.focusDir(sess, h.ac, layout.Right); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionFocusPaneUp:
		if err := h.d.focusDir(sess, h.ac, layout.Up); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionFocusPaneDown:
		if err := h.d.focusDir(sess, h.ac, layout.Down); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionGrowPaneWidth:
		runResizeAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Width, delta: resizeStepCols})
	case keys.ActionShrinkPaneWidth:
		runResizeAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Width, delta: -resizeStepCols})
	case keys.ActionGrowPaneHeight:
		runResizeAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Height, delta: resizeStepRows})
	case keys.ActionShrinkPaneHeight:
		runResizeAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Height, delta: -resizeStepRows})
	case keys.ActionEqualizePanes:
		runResizeAction(daemonActionRequest{kind: daemonActionEqualizePanes})
	case keys.ActionSwitchTab1, keys.ActionSwitchTab2, keys.ActionSwitchTab3,
		keys.ActionSwitchTab4, keys.ActionSwitchTab5, keys.ActionSwitchTab6,
		keys.ActionSwitchTab7, keys.ActionSwitchTab8, keys.ActionSwitchTab9:
		idx := int(action - keys.ActionSwitchTab1)
		if sess.switchTab(idx) {
			h.d.activateTab(sess, sess.activeTab())
			h.d.invalidateRender(sess, h.ac, true, "input.go")
		}
	}
}
