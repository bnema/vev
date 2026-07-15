// Package daemon holds vev's server-side session multiplexer use case.
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
	frameEvent := ev
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
		focusPlacementLocked(tb, pl.ID)
		d.applyLayoutLocked(tb)
		tb.mu.Unlock()
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
	if ev.Button == mouse.Left && ev.Type == mouse.Press && d.handleFreshCopyPress(sess, ac, tb, p, frameEvent) {
		return
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
	rt.invalidateCopyPointerLocked()
	rt.copyMu.Unlock()
}

// handleFreshCopyPress is the only normal-screen press path that creates a
// selection candidate. Its geometry comes from the same frame-absolute hit
// test used by active copy mode, never from pane-local synthetic rectangles.
func (d *Daemon) handleFreshCopyPress(_ *session, ac *attachedClient, tb *tab, routed *pane, ev mouse.Event) bool {
	if routed == nil {
		return false
	}
	tb.mu.Lock()
	geometry, ok := hitTestCopyMouseGeometryLocked(tb, d.currentFloatingConfig(), ev.Col, ev.Row)
	tb.mu.Unlock()
	if !ok || geometry.pane != routed {
		ac.overlays.copyMu.Lock()
		ac.overlays.invalidateCopyPointerLocked()
		ac.overlays.copyMu.Unlock()
		return true
	}
	geometry.pane.mu.Lock()
	mouseMode, _ := geometry.pane.screen.MouseMode()
	altScreen := geometry.pane.screen.AltScreenActive()
	snapshot := scopy.NewSnapshot(geometry.pane.history, geometry.pane.screen.Frame)
	geometry.pane.mu.Unlock()
	if mouseMode != 0 || altScreen {
		return false // child forwarding retains its existing raw-byte path.
	}
	document := scopy.NewDocument(snapshot, d.currentCopyConfig().WordSeparators)
	mapped, ok := mapCopyMouse(ev, geometry, max(document.Len()-document.Height(), 0), document, false)
	if !ok {
		ac.overlays.copyMu.Lock()
		ac.overlays.invalidateCopyPointerLocked()
		ac.overlays.copyMu.Unlock()
		return true
	}
	ac.overlays.copyMu.Lock()
	if ac.overlays.copyMode == nil && ac.overlays.copyCandidate == nil {
		ac.overlays.beginCopyPointerLocked(copyPointerState{pane: geometry.pane, document: document, geometry: geometry, press: mapped.pos})
	}
	ac.overlays.copyMu.Unlock()
	return true
}

// handleFreshCopyPointer maps a pre-copy drag against its press-owned document
// and geometry. It is intentionally independent of the currently hovered pane.
func (d *Daemon) handleFreshCopyPointer(sess *session, ac *attachedClient, ev mouse.Event) bool {
	rt := ac.overlays
	rt.copyMu.Lock()
	pointer := rt.copyPointer
	epoch := rt.copyPointerEpoch
	active := rt.copyMode != nil || rt.copyCandidate != nil
	rt.copyMu.Unlock()
	if !pointer.valid {
		if active && ev.Type == mouse.Release {
			rt.copyMu.Lock()
			if rt.copyPointerEpoch == epoch {
				rt.invalidateCopyPointerLocked()
			}
			rt.copyMu.Unlock()
			return true
		}
		return false
	}
	if active {
		if ev.Type == mouse.Release {
			rt.copyMu.Lock()
			if rt.copyPointerEpoch == epoch {
				rt.invalidateCopyPointerLocked()
			}
			rt.copyMu.Unlock()
			return true
		}
		return false
	}
	pointer.pane.mu.Lock()
	mouseMode, _ := pointer.pane.screen.MouseMode()
	altScreen := pointer.pane.screen.AltScreenActive()
	pointer.pane.mu.Unlock()
	if mouseMode != 0 || altScreen {
		rt.copyMu.Lock()
		if rt.copyPointerEpoch == epoch {
			rt.invalidateCopyPointerLocked()
		}
		rt.copyMu.Unlock()
		return false // preserve child forwarding when it enabled mouse reporting.
	}
	mapped, ok := mapCopyMouse(ev, pointer.geometry, max(pointer.document.Len()-pointer.document.Height(), 0), pointer.document, ev.Type == mouse.Motion)
	if ev.Type == mouse.Release {
		rt.copyMu.Lock()
		if rt.copyPointerEpoch == epoch {
			rt.invalidateCopyPointerLocked()
		}
		rt.copyMu.Unlock()
		return true
	}
	if !ok {
		return true
	}
	if !d.publishCopyMode(sess, ac, sess.activeTab(), pointer.pane, pointer.document, func(mode *scopy.Mode) {
		mode.StartCharacterSelection(pointer.press)
		mode.ExtendCharacterSelection(mapped.pos)
	}, func(runtime *overlayRuntime, mode *scopy.Mode) {
		if runtime.copyPointerEpoch != epoch || runtime.copyPane != pointer.pane || mode.Document() != pointer.document {
			return
		}
		pointer.dragging = true
		runtime.copyPointer = pointer
	}) {
		return true
	}
	d.invalidateRender(sess, ac, true, "input.go")
	return true
}

type copyMouseInputSnapshot struct {
	mode        *scopy.Mode
	pane        *pane
	document    *scopy.Document
	viewportTop int
	pointer     copyPointerState
	epoch       uint64
}

// snapshotCopyMouseInput captures every mutable copy-mode value needed for a
// mapping while copyMu is held. Mapping must never inspect Mode afterwards.
func snapshotCopyMouseInput(rt *overlayRuntime) (copyMouseInputSnapshot, bool) {
	if rt == nil {
		return copyMouseInputSnapshot{}, false
	}
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	if rt.copyMode == nil || rt.copyPane == nil || rt.copyDocument == nil {
		return copyMouseInputSnapshot{}, false
	}
	return copyMouseInputSnapshot{
		mode:        rt.copyMode,
		pane:        rt.copyPane,
		document:    rt.copyDocument,
		viewportTop: rt.copyMode.ViewportTop,
		pointer:     rt.copyPointer,
		epoch:       rt.copyPointerEpoch,
	}, true
}

// handleActiveCopyMouse keeps every drag tied to the pane/document captured at
// press time. Geometry is resolved after copyMu is released to preserve the
// daemon's copyMu -> tab/pane lock prohibition.
func (d *Daemon) handleActiveCopyMouse(sess *session, ac *attachedClient, tb *tab, ev mouse.Event) {
	rt := ac.overlays
	snapshot, active := snapshotCopyMouseInput(rt)
	if !active {
		return
	}
	cfg := d.currentFloatingConfig()
	// Once pressed, use the committed rectangle captured by that press. A
	// later floating/layout change must not turn an in-flight drag into a hit
	// on a different pane.
	if ev.Type != mouse.Press && snapshot.pointer.valid && snapshot.pointer.pane == snapshot.pane && snapshot.pointer.document == snapshot.document {
		if d.beforeCopyMouseMap != nil {
			d.beforeCopyMouseMap()
		}
		mapped, ok := mapCopyMouse(ev, snapshot.pointer.geometry, snapshot.viewportTop, snapshot.document, ev.Type == mouse.Motion)
		if ev.Type == mouse.Release && !ok {
			rt.copyMu.Lock()
			rt.invalidateCopyPointerLocked()
			rt.copyMu.Unlock()
			return
		}
		if ok {
			d.copyMouse(sess, ac, ev, mapped, snapshot, snapshot.pointer.geometry)
		}
		return
	}
	tb.mu.Lock()
	geometry, ok := copyMouseGeometryForPaneLocked(tb, cfg, snapshot.pane)
	if !ok && ev.Type == mouse.Press {
		// Preserve the established press-on-another-pane behavior: copy mode
		// exits rather than retargeting its immutable document.
		if hit, hitOK := hitTestCopyMouseGeometryLocked(tb, cfg, ev.Col, ev.Row); hitOK && hit.pane != snapshot.pane && tb.tree != nil {
			focusPlacementLocked(tb, hit.pane.id)
			d.applyLayoutLocked(tb)
		}
	}
	tb.mu.Unlock()
	if !ok {
		if ev.Type == mouse.Release || ev.Type == mouse.Press {
			rt.copyMu.Lock()
			rt.invalidateCopyPointerLocked()
			rt.copyMu.Unlock()
		}
		return
	}
	clamp := ev.Type == mouse.Motion || ev.Type == mouse.Release
	if d.beforeCopyMouseMap != nil {
		d.beforeCopyMouseMap()
	}
	mapped, mappedOK := mapCopyMouse(ev, geometry, snapshot.viewportTop, snapshot.document, clamp)
	if !mappedOK {
		if ev.Type == mouse.Press {
			otherPane := false
			tb.mu.Lock()
			if hit, hitOK := hitTestCopyMouseGeometryLocked(tb, cfg, ev.Col, ev.Row); hitOK && hit.pane != snapshot.pane {
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
			rt.invalidateCopyPointerLocked()
			rt.copyMu.Unlock()
		}
		return
	}
	d.copyMouse(sess, ac, ev, mapped, snapshot, geometry)
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
