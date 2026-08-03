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

func (d *Daemon) handleSequencedInputForRole(token attachmentRoleToken, _ uint64, data []byte) {
	if !token.activeEffect() {
		return
	}
	d.handleInputForRole(token, data)
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

func (d *Daemon) handleInputForRole(token attachmentRoleToken, data []byte) {
	ac := token.ac
	ac.initOverlays()
	if proxy, ok := token.sess.(*proxySession); ok {
		handler := proxyKeyHandler{d: d, proxy: proxy, ac: ac, roleToken: token}
		ac.mouseScan.Scan(data,
			func(ev mouse.Event) {
				if token.activeEffect() && !proxyMouseOwnedLocally(ac.overlays) {
					handler.Mouse(ev)
				}
			},
			func(b []byte) {
				if !token.activeEffect() || ac.overlays.HandleInput(d, b, token.effect) || !token.activeEffect() {
					return
				}
				ac.keys.RouteWithHandler(b, handler)
			},
		)
		return
	}
	ac.mouseScan.Scan(data,
		func(ev mouse.Event) {
			if token.activeEffect() {
				d.handleMouse(ac, ev)
			}
		},
		func(b []byte) {
			if !token.activeEffect() {
				return
			}
			if ac.overlays.HandleInput(d, b, token.effect) {
				return
			}
			if !token.activeEffect() {
				return
			}
			ac.keys.RouteWithHandler(b, daemonKeyHandler{d: d, ac: ac, roleToken: token})
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
	tb := sess.tabForAttachment(ac)
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
	contentRow := ev.Row - clientTopBarRows
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
			d.writeToPane(sess, p, sgrRowOffset(ev.Raw, -clientTopBarRows))
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
	d         *Daemon
	ac        *attachedClient
	actions   daemonActionRunner
	roleToken attachmentRoleToken
}

// acquireRoleEffect preserves a synchronous frame's existing admission and
// gives delayed router callbacks a fresh ticket for the exact captured role.
// Direct/headless callers without a role token retain their existing behavior.
func (h daemonKeyHandler) acquireRoleEffect() (*session, *roleEffectTicket, bool) {
	if h.roleToken.ac != nil {
		if effect := h.roleToken.effect; effect != nil && !effect.ended.Load() {
			sess, _ := localSession(h.roleToken.sess)
			return sess, effect, false
		}
		effect, admitted := h.roleToken.ac.beginRoleEffect(h.roleToken)
		if h.d.afterDelayedKeyEffectAttempt != nil {
			h.d.afterDelayedKeyEffectAttempt(admitted)
		}
		if !admitted {
			return nil, nil, false
		}
		if h.d.afterRoleEffectAdmitted != nil {
			token := h.roleToken
			token.effect = effect
			h.d.afterRoleEffectAdmitted(token)
		}
		sess, ok := localSession(h.roleToken.sess)
		if !ok {
			effect.End()
			return nil, nil, false
		}
		return sess, effect, true
	}
	sess := h.ac.currentSession()
	if sess == nil {
		return nil, nil, false
	}
	return sess, nil, false
}

func (h daemonKeyHandler) Forward(data []byte) {
	sess, effect, owned := h.acquireRoleEffect()
	if sess == nil {
		return
	}
	if owned {
		defer effect.End()
	}
	tb := sess.tabForAttachment(h.ac)
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.terminalTargetLocked()
	tb.mu.Unlock()
	h.d.writeToPane(sess, p, data)
}

func (h daemonKeyHandler) Action(action keys.Action, _ []byte) {
	sess, effect, owned := h.acquireRoleEffect()
	if sess == nil {
		return
	}
	if owned {
		defer effect.End()
	}
	runAction := func(request daemonActionRequest) {
		request.target = resolveDaemonActionTargetForAttachment(sess, h.ac)
		runner := h.actions
		if runner == nil {
			runner = daemonActions{d: h.d}
		}
		if err := runner.Run(request); err != nil {
			if errors.Is(err, errDaemonActionNoChange) {
				return
			}
			var userErr *domain.UserError
			if !errors.As(err, &userErr) {
				err = resizeUserError(err)
			}
			h.d.reportError(sess, err)
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
		if !proxiedJumpSearchesOtherSessions(h.ac.proxied) {
			if idx, ok := oldestAttentionTab(sess); ok && sess.switchAttachmentTab(h.ac, idx) {
				h.d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(h.ac), false, h.ac, nil)
				h.d.invalidateRender(sess, h.ac, true, "input.go")
			}
			return
		}
		if effect == nil {
			if err := h.d.jumpAttention(sess, h.ac); err != nil {
				h.d.reportError(sess, err)
			}
			return
		}
		effect.bindActionEnd(h.d, "jump-attention")
		token := h.roleToken
		token.effect = effect
		if err := h.d.jumpAttentionForRole(sess, h.ac, token); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionToggleFloatingPane:
		if err := h.d.toggleFloating(sess, h.ac); err != nil {
			h.d.log.Warn("toggle floating pane failed", "err", err)
			h.d.reportError(sess, err)
		}
	case keys.ActionFocusPaneLeft:
		h.focus(sess, layout.Left, effect)
	case keys.ActionFocusPaneRight:
		h.focus(sess, layout.Right, effect)
	case keys.ActionFocusPaneUp:
		h.focus(sess, layout.Up, effect)
	case keys.ActionFocusPaneDown:
		h.focus(sess, layout.Down, effect)
	case keys.ActionGrowPaneWidth:
		runAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Width, delta: resizeStepCols})
	case keys.ActionShrinkPaneWidth:
		runAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Width, delta: -resizeStepCols})
	case keys.ActionGrowPaneHeight:
		runAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Height, delta: resizeStepRows})
	case keys.ActionShrinkPaneHeight:
		runAction(daemonActionRequest{kind: daemonActionResizePane, axis: layout.Height, delta: -resizeStepRows})
	case keys.ActionEqualizePanes:
		runAction(daemonActionRequest{kind: daemonActionEqualizePanes})
	case keys.ActionConsumeOrExpelPaneLeft:
		runAction(daemonActionRequest{kind: daemonActionConsumeOrExpelPane, direction: layout.Left})
	case keys.ActionConsumeOrExpelPaneRight:
		runAction(daemonActionRequest{kind: daemonActionConsumeOrExpelPane, direction: layout.Right})
	case keys.ActionSwitchTab1, keys.ActionSwitchTab2, keys.ActionSwitchTab3,
		keys.ActionSwitchTab4, keys.ActionSwitchTab5, keys.ActionSwitchTab6,
		keys.ActionSwitchTab7, keys.ActionSwitchTab8, keys.ActionSwitchTab9:
		idx := int(action - keys.ActionSwitchTab1)
		if sess.switchAttachmentTab(h.ac, idx) {
			h.d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(h.ac), false, h.ac, nil)
			h.d.invalidateRender(sess, h.ac, true, "input.go")
		}
	}
}

func (h daemonKeyHandler) focus(sess *session, dir layout.Direction, effect *roleEffectTicket) {
	var err error
	if h.ac.proxied {
		err = h.d.focusDirProxied(sess, h.ac, dir)
	} else {
		err = h.d.focusDir(sess, h.ac, dir, effect)
	}
	if err != nil && !errors.Is(err, errAttachmentTransition) && !errors.Is(err, errNoNeighbor) {
		h.d.reportError(sess, err)
	}
}
