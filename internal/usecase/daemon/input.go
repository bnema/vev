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

func (d *Daemon) handleSequencedInputForAttachment(token attachmentConnectionToken, _ uint64, data []byte) {
	if !token.attachmentEffectCurrent() {
		return
	}
	d.handleInputForAttachment(token, data)
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

func (d *Daemon) handleInputForAttachment(token attachmentConnectionToken, data []byte) {
	ac := token.ac
	ac.initOverlays()
	if proxy, ok := token.sess.(*proxySession); ok {
		handler := proxyKeyHandler{d: d, proxy: proxy, ac: ac, connectionToken: token}
		ac.mouseScan.Scan(data,
			func(ev mouse.Event) {
				if token.attachmentEffectCurrent() && !proxyMouseOwnedLocally(ac.overlays) {
					handler.Mouse(ev)
				}
			},
			func(b []byte) {
				if !token.attachmentEffectCurrent() || ac.overlays.HandleInput(d, b, token.effect) || !token.attachmentEffectCurrent() {
					return
				}
				ac.keys.RouteWithHandler(b, handler)
			},
		)
		return
	}
	ac.mouseScan.Scan(data,
		func(ev mouse.Event) {
			if token.attachmentEffectCurrent() {
				d.handleMouse(ac, ev)
			}
		},
		func(b []byte) {
			if !token.attachmentEffectCurrent() {
				return
			}
			if ac.overlays.HandleInput(d, b, token.effect) {
				return
			}
			if !token.attachmentEffectCurrent() {
				return
			}
			ac.keys.RouteWithHandler(b, daemonKeyHandler{d: d, ac: ac, connectionToken: token})
		},
	)
}

func (d *Daemon) handleMouse(ac *attachedClient, ev mouse.Event) {
	d.handleMouseMutation(ac, ev)
}

func (d *Daemon) handleMouseMutation(ac *attachedClient, ev mouse.Event) {
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

	// Copy-mode publication deliberately revalidates outside the dispatch lock:
	// a concurrent release or pane close must be able to invalidate its candidate.
	var routed mouseRoute
	_ = sess.runMutation(func() error {
		routed = d.routeMouseMutation(sess, ac, ev)
		return nil
	})
	if routed.handled {
		return
	}
	if routed.p == nil {
		invalidateRejectedLeftPointer(rt, ev)
		return
	}
	if ev.Button == mouse.Left && ev.Type == mouse.Press && d.handleFreshCopyPress(sess, ac, routed.tb, routed.p, frameEvent) {
		return
	}
	d.handleTerminalMouse(sess, ac, routed.p, routed.event, routed.translated, routed.hoveredFocused)
}

type mouseRoute struct {
	tb             *tab
	p              *pane
	event          mouse.Event
	translated     bool
	hoveredFocused bool
	handled        bool
}

func (d *Daemon) routeMouseMutation(sess *session, ac *attachedClient, ev mouse.Event) mouseRoute {
	route := mouseRoute{event: ev, hoveredFocused: true}
	tb := sess.tabForAttachment(ac)
	if tb == nil {
		route.handled = true
		return route
	}
	route.tb = tb
	contentRow := ev.Row - clientTopBarRows
	tb.mu.Lock()
	floating, floatingGeometry, floatingVisible := tb.visibleFloatingSnapshotLocked(d.currentFloatingConfig())
	if floatingVisible {
		if !pointInRect(ev.Col, contentRow, floatingGeometry.Inner) {
			tb.mu.Unlock()
			route.handled = true
			return route
		}
		tb.mu.Unlock()
		route.p = floating
		route.event = translateMouseEvent(ev, floatingGeometry.Inner.X, floatingGeometry.Inner.Y)
		route.translated = true
		return route
	}
	pl, hit := hitTestPlacementLocked(tb, ev.Col, contentRow)
	multi := len(tb.panes) > 1
	focusedID := mouseFocusedPaneIDLocked(ac, tb)
	if hit && pointInRect(ev.Col, contentRow, pl.TitleBar) {
		if !isMouseFocusPress(ev) {
			tb.mu.Unlock()
			route.handled = true
			return route
		}
		tb.mu.Unlock()
		_, focused := d.focusMousePane(sess, ac, tb, pl.ID)
		if focused == nil {
			route.handled = true
			return route
		}
		invalidateRejectedLeftPointer(ac.overlays, ev)
		if pl.ID != focusedID {
			d.exitCopyMode(ac)
			d.refreshPaneTitleOnFocus(sess, pl.ID)
		}
		d.invalidateRender(sess, ac, true, "input.go")
		route.handled = true
		return route
	}
	if hit && !pl.Collapsed && pointInRect(ev.Col, contentRow, pl.Content) {
		oldFocus := focusedID
		shouldFocus := isMouseFocusPress(ev)
		p := tb.panes[pl.ID]
		hoveredFocused := pl.ID == oldFocus
		tb.mu.Unlock()
		if shouldFocus {
			_, p = d.focusMousePane(sess, ac, tb, pl.ID)
		}
		if p == nil {
			route.handled = true
			return route
		}
		if shouldFocus && pl.ID != oldFocus {
			d.exitCopyMode(ac)
			d.refreshPaneTitleOnFocus(sess, pl.ID)
			d.invalidateRender(sess, ac, true, "input.go")
		}
		route.p = p
		route.hoveredFocused = hoveredFocused
		if multi {
			route.event = translateMouseEvent(ev, pl.Content.X, pl.Content.Y)
			route.translated = true
		}
		return route
	}
	if multi {
		tb.mu.Unlock()
		route.handled = true
		return route
	}
	tb.mu.Unlock()
	_, route.p = sess.paneForAttachment(ac)
	return route
}

func mouseFocusedPaneIDLocked(ac *attachedClient, tb *tab) layout.PaneID {
	if ac != nil && tb != nil {
		view := ac.viewSnapshot()
		if domain.TabStableID(tb.stableID) == view.tabID {
			for _, p := range tb.panes {
				if p != nil && domain.PaneStableID(p.stableID) == view.paneID {
					return p.id
				}
			}
		}
	}
	if tb == nil || tb.tree == nil {
		return ""
	}
	return tb.tree.Focus
}

// focusMousePane runs inside the caller's session mutation boundary.
func (d *Daemon) focusMousePane(sess *session, ac *attachedClient, tb *tab, id layout.PaneID) (bool, *pane) {
	var changed bool
	var focused *pane
	tb.mu.Lock()
	if tb.tree != nil {
		focused = tb.panes[id]
		if focused != nil {
			changed = focusPlacementLocked(tb, id)
		}
	}
	tb.mu.Unlock()
	if focused == nil {
		return false, nil
	}
	sess.mu.Lock()
	sess.setAttachmentPaneLocked(ac, tb, focused)
	sess.mu.Unlock()
	if changed {
		d.applyTabLayout(sess, tb)
	}
	return changed, focused
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
		_ = sess.runMutation(func() error {
			if translated {
				d.writeToPane(sess, p, ev.Raw)
			} else {
				d.writeToPane(sess, p, sgrRowOffset(ev.Raw, -clientTopBarRows))
			}
			return nil
		})
		return
	}

	switch ev.Button {
	case mouse.WheelUp:
		if altScreen {
			_ = sess.runMutation(func() error {
				d.writeToPane(sess, p, []byte("\x1b[A\x1b[A\x1b[A"))
				return nil
			})
			return
		}
		if !hoveredFocused {
			return
		}
		d.enterCopyMode(sess, ac)
		d.copyWheel(sess, ac, -3)
	case mouse.WheelDown:
		if altScreen {
			_ = sess.runMutation(func() error {
				d.writeToPane(sess, p, []byte("\x1b[B\x1b[B\x1b[B"))
				return nil
			})
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
	d               *Daemon
	ac              *attachedClient
	actions         daemonActionRunner
	connectionToken attachmentConnectionToken
}

// acquireAttachmentEffect preserves a synchronous frame's existing admission and
// gives delayed router callbacks a fresh ticket for the exact captured attachment.
// Direct/headless callers without an attachment token retain their existing behavior.
func (h daemonKeyHandler) acquireAttachmentEffect() (*session, *attachmentEffectTicket, bool) {
	if h.connectionToken.ac != nil {
		if effect := h.connectionToken.effect; effect != nil && !effect.ended.Load() {
			sess, _ := localSession(h.connectionToken.sess)
			return sess, effect, false
		}
		effect, admitted := h.connectionToken.ac.beginAttachmentEffect(h.connectionToken)
		if h.d.afterDelayedKeyEffectAttempt != nil {
			h.d.afterDelayedKeyEffectAttempt(admitted)
		}
		if !admitted {
			return nil, nil, false
		}
		if h.d.afterAttachmentEffectAdmitted != nil {
			token := h.connectionToken
			token.effect = effect
			h.d.afterAttachmentEffectAdmitted(token)
		}
		sess, ok := localSession(h.connectionToken.sess)
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
	sess, effect, owned := h.acquireAttachmentEffect()
	if sess == nil {
		return
	}
	if owned {
		defer effect.End()
	}
	_ = sess.runMutation(func() error {
		tb := sess.tabForAttachment(h.ac)
		if tb == nil {
			return nil
		}
		tb.mu.Lock()
		p := tb.terminalTargetLocked()
		tb.mu.Unlock()
		h.d.writeToPane(sess, p, data)
		return nil
	})
}

func (h daemonKeyHandler) Action(action keys.Action, _ []byte) {
	sess, effect, owned := h.acquireAttachmentEffect()
	if sess == nil {
		return
	}
	if owned {
		defer effect.End()
	}
	runAction := func(request daemonActionRequest) {
		request.effect = effect
		runner := h.actions
		if runner == nil {
			runner = daemonActions{d: h.d}
		}
		if err := sess.runMutation(func() error {
			request.target = resolveDaemonActionTargetForAttachment(sess, h.ac)
			return runner.Run(request)
		}); err != nil {
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
			_ = sess.runMutation(func() error {
				if idx, ok := oldestAttentionTab(sess); ok && sess.switchAttachmentTabForDispatch(h.ac, idx) {
					h.d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(h.ac), false, h.ac, nil)
					h.d.invalidateRender(sess, h.ac, true, "input.go")
				}
				return nil
			})
			return
		}
		if effect == nil {
			if err := h.d.jumpAttention(sess, h.ac); err != nil {
				h.d.reportError(sess, err)
			}
			return
		}
		effect.bindActionEnd(h.d, "jump-attention")
		token := h.connectionToken
		token.effect = effect
		if err := h.d.jumpAttentionForAttachment(sess, h.ac, token); err != nil {
			h.d.reportError(sess, err)
		}
	case keys.ActionToggleFloatingPane:
		if err := sess.runMutation(func() error { return h.d.toggleFloating(sess, h.ac) }); err != nil {
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
		_ = sess.runMutation(func() error {
			if sess.switchAttachmentTabForDispatch(h.ac, idx) {
				h.d.activateTabAfterResizeForLease(sess, sess.tabForAttachment(h.ac), false, h.ac, nil)
				h.d.invalidateRender(sess, h.ac, true, "input.go")
			}
			return nil
		})
	}
}

func (h daemonKeyHandler) focus(sess *session, dir layout.Direction, effect *attachmentEffectTicket) {
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
