// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"bytes"
	"strconv"

	"github.com/bnema/vev/internal/domain"
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
		invalidateRejectedLeftPointer(rt, ev)
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		invalidateRejectedLeftPointer(rt, ev)
		return
	}

	if rt.copyActive() {
		if rt.copySearchActive() {
			return
		}
		switch ev.Button {
		case mouse.WheelUp:
			d.copyWheel(sess, ac, -3)
		case mouse.WheelDown:
			d.copyWheel(sess, ac, 3)
		case mouse.Left:
			d.handleActiveCopyMouse(sess, ac, tb, ev)
		}
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
		focusPlacementLocked(tb, pl.ID)
		d.applyLayoutLocked(tb)
		tb.mu.Unlock()
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
		if isMouseFocusPress(ev) {
			focusPlacementLocked(tb, pl.ID)
			d.applyLayoutLocked(tb)
		}
		p = tb.panes[pl.ID]
		hoveredFocused = pl.ID == oldFocus
		tb.mu.Unlock()
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
	d.handleTerminalMouse(sess, ac, p, ev, translated, hoveredFocused)
}

// invalidateRejectedLeftPointer clears a stale drag before returning from an
// event that cannot be routed to terminal content. It never runs while tab or
// pane locks are held, preserving the copyMu -> tab/pane lock prohibition.
func invalidateRejectedLeftPointer(rt *overlayRuntime, ev mouse.Event) {
	if rt == nil || ev.Button != mouse.Left || (ev.Type != mouse.Press && ev.Type != mouse.Release) {
		return
	}
	rt.copyMu.Lock()
	rt.invalidateCopyPointerLocked(true)
	rt.copyMu.Unlock()
}

// handleActiveCopyMouse keeps every drag tied to the pane/document captured at
// press time. Geometry is resolved after copyMu is released to preserve the
// daemon's copyMu -> tab/pane lock prohibition.
func (d *Daemon) handleActiveCopyMouse(sess *session, ac *attachedClient, tb *tab, ev mouse.Event) {
	rt := ac.overlays
	rt.copyMu.Lock()
	mode, target, document := rt.copyMode, rt.copyPane, rt.copyDocument
	pointer := rt.copyPointer
	rt.copyMu.Unlock()
	if mode == nil || target == nil || document == nil {
		return
	}
	cfg := d.currentFloatingConfig()
	// Once pressed, use the committed rectangle captured by that press. A
	// later floating/layout change must not turn an in-flight drag into a hit
	// on a different pane.
	if ev.Type != mouse.Press && pointer.valid && pointer.pane == target && pointer.document == document {
		mapped, ok := mapCopyMouse(ev, pointer.geometry, mode.ViewportTop, document, ev.Type == mouse.Motion)
		if ev.Type == mouse.Release && !ok {
			rt.copyMu.Lock()
			rt.invalidateCopyPointerLocked(true)
			rt.copyMu.Unlock()
			return
		}
		if ok {
			d.copyMouse(sess, ac, ev, mapped)
		}
		return
	}
	tb.mu.Lock()
	geometry, ok := copyMouseGeometryForPaneLocked(tb, cfg, target)
	if !ok && ev.Type == mouse.Press {
		// Preserve the established press-on-another-pane behavior: copy mode
		// exits rather than retargeting its immutable document.
		if hit, hitOK := hitTestCopyMouseGeometryLocked(tb, cfg, ev.Col, ev.Row); hitOK && hit.pane != target {
			if tb.tree != nil {
				focusPlacementLocked(tb, hit.pane.id)
				d.applyLayoutLocked(tb)
			}
		}
	}
	tb.mu.Unlock()
	if !ok {
		if ev.Type == mouse.Release || ev.Type == mouse.Press {
			rt.copyMu.Lock()
			rt.invalidateCopyPointerLocked(true)
			rt.copyMu.Unlock()
		}
		return
	}
	clamp := ev.Type == mouse.Motion || ev.Type == mouse.Release
	mapped, mappedOK := mapCopyMouse(ev, geometry, mode.ViewportTop, document, clamp)
	if !mappedOK {
		if ev.Type == mouse.Press {
			otherPane := false
			tb.mu.Lock()
			if hit, hitOK := hitTestCopyMouseGeometryLocked(tb, cfg, ev.Col, ev.Row); hitOK && hit.pane != target {
				otherPane = true
				if tb.tree != nil {
					focusPlacementLocked(tb, hit.pane.id)
					d.applyLayoutLocked(tb)
				}
			}
			tb.mu.Unlock()
			if otherPane {
				d.exitCopyMode(ac)
			}
		}
		if ev.Type == mouse.Release || ev.Type == mouse.Press {
			rt.copyMu.Lock()
			rt.invalidateCopyPointerLocked(true)
			rt.copyMu.Unlock()
		}
		return
	}
	if ev.Type == mouse.Press {
		// Keep the resolved frame geometry with the pointer installed by copyMouse.
		rt.copyMu.Lock()
		if rt.copyMode == mode && rt.copyPane == target && rt.copyDocument == document {
			rt.beginCopyPointerLocked(copyPointerState{pane: target, document: document, geometry: geometry, press: mapped.pos})
		}
		rt.copyMu.Unlock()
	}
	d.copyMouse(sess, ac, ev, mapped)
}

func (d *Daemon) handleTerminalMouse(sess *session, ac *attachedClient, p *pane, ev mouse.Event, translated, hoveredFocused bool) {
	if p == nil {
		return
	}
	rt := ac.overlays
	p.mu.Lock()
	childRows := p.screen.Frame.Height
	childCols := p.screen.Frame.Width
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
	case mouse.Left:
		// At this point ev is pane-local in X and retains its client-frame Y,
		// so this synthetic geometry crosses the top-bar boundary once.
		geometry := copyMouseGeometry{pane: p, content: domain.Rect{X: 0, Y: clientTopBarRows, Width: childCols, Height: childRows}}
		switch ev.Type {
		case mouse.Press:
			if altScreen {
				rt.copyMu.Lock()
				rt.invalidateCopyPointerLocked(true)
				rt.copyMu.Unlock()
				return
			}
			p.mu.Lock()
			snapshot := scopy.NewSnapshot(p.history, p.screen.Frame)
			p.mu.Unlock()
			document := scopy.NewDocument(snapshot, d.currentCopyConfig().WordSeparators)
			mapped, ok := mapCopyMouse(ev, geometry, max(document.Len()-document.Height(), 0), document, false)
			if !ok {
				rt.copyMu.Lock()
				rt.invalidateCopyPointerLocked(true)
				rt.copyMu.Unlock()
				return
			}
			rt.copyMu.Lock()
			rt.beginCopyPointerLocked(copyPointerState{pane: p, document: document, geometry: geometry, press: mapped.pos})
			rt.copyMu.Unlock()
		case mouse.Motion:
			if altScreen {
				return
			}
			rt.copyMu.Lock()
			pointer := rt.copyPointer
			rt.copyMu.Unlock()
			if !pointer.valid || pointer.pane != p {
				return
			}
			mapped, ok := mapCopyMouse(ev, pointer.geometry, max(pointer.document.Len()-pointer.document.Height(), 0), pointer.document, true)
			if !ok {
				return
			}
			tb := sess.activeTab()
			if !d.publishCopyMode(sess, ac, tb, p, pointer.document, func(mode *scopy.Mode) {
				mode.StartCharacterSelection(pointer.press)
				mode.ExtendCharacterSelection(mapped.pos)
			}, func(runtime *overlayRuntime, mode *scopy.Mode) {
				if runtime.copyPointerEpoch != pointer.epoch || runtime.copyPane != pointer.pane || mode.Document() != pointer.document {
					return
				}
				pointer.dragging = true
				runtime.copyPointer = pointer
			}) {
				return
			}
			d.invalidateRender(sess, ac, true, "input.go")
		case mouse.Release:
			rt.copyMu.Lock()
			rt.invalidateCopyPointerLocked(true)
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
			h.d.invalidateRender(sess, h.ac, true, "input.go")
		}
	}
}
