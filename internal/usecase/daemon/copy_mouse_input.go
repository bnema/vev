// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/mouse"
)

// handleCopyMouse owns copy-mode mouse routing. A release is lifecycle input
// even while visual search owns all other copy-mode mouse input.
func (d *Daemon) handleCopyMouse(sess *session, ac *attachedClient, tb *tab, ev mouse.Event) bool {
	rt := ac.overlays
	if !rt.copyActive() {
		return false
	}
	if rt.copySearchActive() {
		// Search owns ordinary mouse input, but a release completes an
		// in-flight drag. Route it through the active mapper so its endpoint is
		// clamped against the press-owned geometry before clearing the pointer.
		if ev.Button == mouse.Left && ev.Type == mouse.Release {
			d.handleActiveCopyMouse(sess, ac, tb, ev)
		}
		return true
	}
	if ev.Button == mouse.Left && ev.Type == mouse.Release {
		d.handleActiveCopyMouse(sess, ac, tb, ev)
		return true
	}
	switch ev.Button {
	case mouse.WheelUp:
		d.copyWheel(sess, ac, -3)
	case mouse.WheelDown:
		d.copyWheel(sess, ac, 3)
	case mouse.Left:
		d.handleActiveCopyMouse(sess, ac, tb, ev)
	}
	return true
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

// handleFreshCopyPress is the only normal-screen press path that creates a
// selection candidate. Its geometry comes from the same frame-absolute hit
// test used by active copy mode, never from pane-local synthetic rectangles.
func (d *Daemon) handleFreshCopyPress(sess *session, ac *attachedClient, tb *tab, routed *pane, ev mouse.Event) bool {
	if routed == nil {
		return false
	}
	tb.mu.Lock()
	geometry, ok := hitTestCopyMouseGeometryLocked(tb, d.currentFloatingConfig(), ev.Col, ev.Row)
	tb.mu.Unlock()
	if !ok || geometry.pane != routed {
		ac.overlays.copyMu.Lock()
		ac.overlays.invalidateCopyPointerLocked(true)
		ac.overlays.copyMu.Unlock()
		return true
	}
	geometry.pane.mu.Lock()
	mouseMode, _ := geometry.pane.screen.MouseMode()
	altScreen := geometry.pane.screen.AltScreenActive()
	snapshot := scopy.NewSnapshot(geometry.pane.history, geometry.pane.screen.Frame, geometry.pane.screen.LineBounds())
	geometry.pane.mu.Unlock()
	if mouseMode != 0 || altScreen {
		return false // child forwarding retains its existing raw-byte path.
	}
	document := scopy.NewDocument(snapshot, d.currentCopyConfig().WordSeparators)
	mapped, ok := mapCopyMouse(ev, geometry, max(document.Len()-document.Height(), 0), document, false)
	if !ok {
		ac.overlays.copyMu.Lock()
		ac.overlays.invalidateCopyPointerLocked(true)
		ac.overlays.copyMu.Unlock()
		return true
	}
	rt := ac.overlays
	rt.copyMu.Lock()
	if rt.copyMode != nil || rt.copyCandidate != nil {
		rt.copyMu.Unlock()
		return true
	}
	rt.beginCopyPointerLocked(copyPointerState{pane: geometry.pane, document: document, geometry: geometry, press: mapped.pos})
	pointer := rt.copyPointer
	now := d.clock.Now()
	doubleClick := d.isCopyDoubleClickLocked(rt, geometry.pane, mapped.pos, now)
	if doubleClick {
		rt.copyClick = copyClickCandidate{}
	} else {
		rt.copyClick = copyClickCandidate{valid: true, pane: geometry.pane, pos: mapped.pos, at: now}
	}
	rt.copyMu.Unlock()
	if !doubleClick {
		return true
	}

	selectedWord := false
	if d.publishCopyMode(sess, ac, tb, geometry.pane, document, func(mode *scopy.Mode) {
		mode.SetPosition(mapped.pos)
		selectedWord = mode.SelectWordAt(mapped.pos)
	}, func(runtime *overlayRuntime, mode *scopy.Mode) {
		if runtime.copyPointerEpoch != pointer.epoch || runtime.copyPane != pointer.pane || mode.Document() != pointer.document {
			return
		}
		pointer.wordDrag = selectedWord
		runtime.copyPointer = pointer
	}) {
		d.invalidateRender(sess, ac, true, "input.go")
	}
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
				rt.invalidateCopyPointerLocked(false)
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
				rt.invalidateCopyPointerLocked(false)
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
			rt.invalidateCopyPointerLocked(true)
		}
		rt.copyMu.Unlock()
		return false // preserve child forwarding when it enabled mouse reporting.
	}
	mapped, ok := mapCopyMouse(ev, pointer.geometry, max(pointer.document.Len()-pointer.document.Height(), 0), pointer.document, ev.Type == mouse.Motion)
	if ev.Type == mouse.Motion {
		rt.copyMu.Lock()
		if rt.copyPointerEpoch == epoch && rt.copyClick.valid && rt.copyClick.pane == pointer.pane {
			rt.copyClick.dragged = true
		}
		rt.copyMu.Unlock()
	}
	if ev.Type == mouse.Release {
		rt.copyMu.Lock()
		if rt.copyPointerEpoch == epoch {
			rt.invalidateCopyPointerLocked(false)
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

// invalidateSnapshotCopyPointer clears only the pointer captured with the
// input snapshot. Mapping runs outside copyMu, so a newer press can otherwise
// be erased by an older rejected event.
func invalidateSnapshotCopyPointer(rt *overlayRuntime, snapshot copyMouseInputSnapshot) {
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	if rt.copyPointerEpoch != snapshot.epoch || rt.copyMode != snapshot.mode ||
		rt.copyPane != snapshot.pane || rt.copyDocument != snapshot.document {
		return
	}
	rt.invalidateCopyPointerLocked(true)
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
	if d.beforeCopyMouseMap != nil {
		d.beforeCopyMouseMap()
	}
	cfg := d.currentFloatingConfig()
	// Once pressed, use the committed rectangle captured by that press. A
	// later floating/layout change must not turn an in-flight drag into a hit
	// on a different pane.
	if ev.Type != mouse.Press && snapshot.pointer.valid && snapshot.pointer.pane == snapshot.pane && snapshot.pointer.document == snapshot.document {
		mapped, ok := mapCopyMouse(ev, snapshot.pointer.geometry, snapshot.viewportTop, snapshot.document, ev.Type == mouse.Motion || ev.Type == mouse.Release)
		if ev.Type == mouse.Release && !ok {
			invalidateSnapshotCopyPointer(rt, snapshot)
			return
		}
		if ok {
			d.copyMouse(sess, ac, ev, mapped, snapshot, snapshot.pointer.geometry)
		}
		return
	}
	tb.mu.Lock()
	geometry, ok := copyMouseGeometryForPaneLocked(tb, cfg, snapshot.pane)
	otherPane := false
	layoutChanged := false
	if ev.Type == mouse.Press {
		// A new press is allowed to change focus, but never the document owned
		// by the active copy mode. Do all tab work first; exitCopyMode takes
		// copyMu only after tb.mu is released.
		if hit, hitOK := hitTestCopyMouseGeometryLocked(tb, cfg, ev.Col, ev.Row); hitOK && hit.pane != snapshot.pane {
			otherPane = true
			if tb.tree != nil {
				layoutChanged = focusPlacementLocked(tb, hit.pane.id)
			}
		}
	}
	tb.mu.Unlock()
	if layoutChanged {
		d.applyTabLayout(sess, tb)
	}
	if otherPane {
		d.exitCopyMode(ac)
		return
	}
	if !ok {
		if ev.Type == mouse.Release || ev.Type == mouse.Press {
			invalidateSnapshotCopyPointer(rt, snapshot)
		}
		return
	}
	clamp := ev.Type == mouse.Motion || ev.Type == mouse.Release
	mapped, mappedOK := mapCopyMouse(ev, geometry, snapshot.viewportTop, snapshot.document, clamp)
	if !mappedOK {
		if ev.Type == mouse.Release || ev.Type == mouse.Press {
			invalidateSnapshotCopyPointer(rt, snapshot)
		}
		return
	}
	d.copyMouse(sess, ac, ev, mapped, snapshot, geometry)
}
