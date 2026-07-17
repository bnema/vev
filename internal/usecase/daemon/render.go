// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"bytes"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// completedSynchronizedUpdate reports a full DEC 2026 batch contained in one
// PTY read. The VT's final state is inactive in this case, so it cannot be
// inferred from before/after SyncUpdateActive alone.
func completedSynchronizedUpdate(data []byte) bool {
	_, tail, found := bytes.Cut(data, []byte(renderer.SyncStartCSI))
	return found && bytes.Contains(tail, []byte(renderer.SyncEndCSI))
}

// paneRenderable reports dynamic composition visibility. Callers must not
// hold coordinator locks: this acquires daemon/session/tab/pane-adjacent locks.
func (d *Daemon) paneRenderable(sess *session, tb *tab, p *pane) bool {
	if sess == nil || tb == nil || p == nil {
		return false
	}
	sess.mu.Lock()
	active := sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == tb
	attached := sess.client != nil
	sess.mu.Unlock()

	// The normal attached render path needs no cross-session picker lookup.
	// Only inactive or headless tabs can be renderable as a picker preview.
	if (!active || !attached) && !d.tabIsPickerPreview(tb) {
		return false
	}
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.floating.pane == p {
		return active && attached && tb.floating.state == floatingVisible
	}
	placements, ok := solvedPlacementsLocked(tb)
	if !ok {
		return false
	}
	for _, placement := range placements {
		if placement.ID == p.id && !placement.Collapsed && placement.Content.Width > 0 && placement.Content.Height > 0 {
			return true
		}
	}
	return false
}

// tabIsPickerPreview reports whether any attached client currently composes
// tb as a picker preview. It snapshots daemon ownership before taking overlay
// locks, preserving the Daemon -> session -> tab -> pane lock order.
func (d *Daemon) tabIsPickerPreview(tb *tab) bool {
	if d == nil || tb == nil {
		return false
	}
	d.mu.Lock()
	sessions := make([]*session, 0, len(d.sessions))
	for _, sess := range d.sessions {
		sessions = append(sessions, sess)
	}
	d.mu.Unlock()
	for _, sess := range sessions {
		sess.mu.Lock()
		ac := sess.client
		sess.mu.Unlock()
		if ac == nil || ac.overlays == nil {
			continue
		}
		ac.overlays.pickerMu.Lock()
		preview := ac.overlays.pickerPreview == tb
		ac.overlays.pickerMu.Unlock()
		if preview {
			return true
		}
	}
	return false
}

func (d *Daemon) ptyReader(sess *session, tb *tab, p *pane) {
	defer d.sessWg.Done()
	if p == nil {
		return
	}
	buf := make([]byte, ptyReadBufSize)
	p.mu.Lock()
	p.screen.OnResponse = func(b []byte) { p.ptyResponses = append(p.ptyResponses, b...) }
	p.screen.OnBell = func() { p.ptyAttention = true }
	p.screen.OnNotify = func(string, string) { p.ptyAttention = true }
	p.screen.OnProgress = func(bool) { p.ptyAttention = true }
	p.screen.OnClipboard = func(b64 string) { p.ptyClipboards = append(p.ptyClipboards, b64) }
	p.mu.Unlock()
	for {
		n, err := p.pty.Read(buf)
		if n > 0 {
			d.processPTYData(sess, tb, p, buf[:n], true)
		}
		if err != nil {
			if p.onExit != nil {
				p.onExit()
			} else {
				d.reapPane(sess, tb, p)
			}
			return
		}
	}
}

// processPTYData is the sole VT parsing path. While a resize apply owns the
// pane, reader calls append exact bytes and returns immediately; it never waits
// for PTY.Resize or loses a short read.
func (d *Daemon) processPTYData(sess *session, tb *tab, p *pane, data []byte, bufferDuringApply bool) {
	rc := sess.renderCoordinator()
	p.mu.Lock()
	if bufferDuringApply && p.resizeApplying {
		p.resizePending = append(p.resizePending, data...)
		p.mu.Unlock()
		return
	}
	wasSyncing := p.screen.SyncUpdateActive()
	p.screen.Write(data)
	p.refreshTerminalTitleLocked()
	isSyncing := p.screen.SyncUpdateActive()
	completeSyncRead := !wasSyncing && !isSyncing && completedSynchronizedUpdate(data)
	var syncGen uint64
	syncEnded := false
	if wasSyncing != isSyncing {
		if isSyncing {
			syncGen = sess.syncGen.Add(1)
			p.syncGen = syncGen
			if rc != nil {
				rc.noteSyncBeginWithRenderability(p, syncGen, func() bool { return d.paneRenderable(sess, tb, p) }, func() {
					p.mu.Lock()
					if p.syncGen == syncGen && p.screen.SyncUpdateActive() {
						p.screen.ForceSyncEnd()
					}
					p.mu.Unlock()
				})
			}
		} else {
			syncGen = p.syncGen
			syncEnded = rc != nil && rc.removeSyncEnd(p, syncGen, true)
		}
	}
	p.mu.Unlock()
	renderable := d.paneRenderable(sess, tb, p)
	markSnapshotDirty(sess)
	d.flushPTYEffects(sess, tb, p)
	if rc != nil {
		if wasSyncing != isSyncing {
			if isSyncing {
				if renderable {
					rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
				}
			} else if syncEnded && renderable {
				rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "render.go"})
			}
		}
		if completeSyncRead && renderable {
			rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "render.go"})
		} else if renderable && !isSyncing && wasSyncing == isSyncing {
			rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
		}
	}
}

// flushPTYEffects preserves the normal response/title/attention/clipboard
// behavior for both direct reads and resize-buffer replay.
func (d *Daemon) flushPTYEffects(sess *session, tb *tab, p *pane) {
	p.mu.Lock()
	attention := p.ptyAttention
	p.ptyAttention = false
	responses := append([]byte(nil), p.ptyResponses...)
	p.ptyResponses = p.ptyResponses[:0]
	clipboards := append([]string(nil), p.ptyClipboards...)
	p.ptyClipboards = p.ptyClipboards[:0]
	p.mu.Unlock()
	if attention {
		d.noteAttention(sess, tb)
	}
	for _, b64 := range clipboards {
		d.forwardClipboardAsync(sess, b64)
	}
	if len(responses) > 0 {
		if _, err := p.pty.Write(responses); err != nil {
			d.log.Warn("pty response write failed", "err", err, "session", sess.name)
		}
	}
}

// paint draws the composed client frame (active tab plus status bar) and
// sends the resulting bytes. The renderer shadow is reset on explicit invalidations
// such as switch/create/close/rename/resize so the repaint is complete.
func (d *Daemon) observeRuntime(kind ports.RuntimeMarkKind, bytes uint64, valid bool) {
	if d != nil && d.runtimeObserver != nil {
		d.runtimeObserver.ObserveRuntime(ports.NewRuntimeMark("daemon", kind, bytes, valid))
	}
}

// runtimeMarkBatch captures immutable runtime marks during a render transaction.
// paint owns attachment sendMu while it captures, composes, and emits; JSONL
// observer I/O must therefore happen only after that ownership is released.
type runtimeMarkBatch struct {
	observer ports.RuntimeObserver
	marks    []ports.RuntimeMark
}

func (d *Daemon) newRuntimeMarkBatch() runtimeMarkBatch {
	if d == nil {
		return runtimeMarkBatch{}
	}
	return runtimeMarkBatch{observer: d.runtimeObserver}
}

func (b *runtimeMarkBatch) span(start, end ports.RuntimeMarkKind, bytes uint64) func(uint64, bool) {
	if b == nil || b.observer == nil {
		return func(uint64, bool) {}
	}
	correlation := ports.NewRuntimeCorrelation()
	b.marks = append(b.marks, ports.NewRuntimeMarkWithCorrelation("daemon", correlation, start, bytes, true))
	return func(endBytes uint64, valid bool) {
		b.marks = append(b.marks, ports.NewRuntimeMarkWithCorrelation("daemon", correlation, end, endBytes, valid))
	}
}

func (b *runtimeMarkBatch) flush() {
	if b == nil || b.observer == nil {
		return
	}
	for _, mark := range b.marks {
		b.observer.ObserveRuntime(mark)
	}
}

func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool, lease *attachmentLease) {
	marks := d.newRuntimeMarkBatch()
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	ac.sendMu.Lock()
	// sendMu is the attachment ownership boundary. Check the session's
	// published identity while holding it so a deadline captured before an
	// attach/replace cannot emit on either the old or new output chain.
	sess.mu.Lock()
	owned := sess.client == ac
	sess.mu.Unlock()
	if !owned || ac.currentSession() != sess {
		ac.sendMu.Unlock()
		return
	}
	if lease != nil {
		rc := sess.renderCoordinator()
		if rc == nil || lease.attachment != ac || !rc.leaseCurrent(lease, true) {
			ac.sendMu.Unlock()
			return
		}
	}
	// Capacity is checked before any destructive capture. The coordinator is
	// the normal gate, but this ownership check also protects direct resize and
	// test paint paths from consuming damage that cannot be emitted.
	if ac.output != nil && ac.output.atCapacity() {
		// The coordinator owns the blocked interval; this guard only protects
		// direct test/resize paint calls from destructive capture.
		ac.sendMu.Unlock()
		return
	}
	// Composition owns attachment sendMu; initialize its lazy overlay state
	// under that same ownership so concurrent fallback paints cannot observe a
	// partially published runtime.
	ac.initOverlays()
	overlays := ac.overlays.SnapshotForRender()
	repaintAttachedClients := false
	defer func() {
		overlays.Unlock()
		// Every return below has released sendMu before reaching this defer.
		// Emit the captured sequence before any follow-up repaint can introduce
		// another boundary, while no attachment, session, tab, or pane lock is held.
		marks.flush()
		if repaintAttachedClients {
			d.repaintAllAttachedClients()
		}
	}()
	preview := snapshotPickerPreview(nil)
	if overlays.previewTab != tb {
		preview = snapshotPickerPreview(overlays.previewTab)
	}
	d.refreshSessionFocusedTitles(sess)
	// Contextual ranks are completely captured under paletteMu. Compose them
	// without reading the live MRU, whose order may have changed mid-interaction.
	bars := d.barStateForPaletteHints(sess, overlays.copyFeedback, overlays.paletteHints, overlays.paletteRecent)
	bars.theme = ac.getTheme()
	attentionVisible := pulseVisible(bars.attentionFrame)
	repaintAttachedClients = sess.ackAttention(tb, attentionVisible)

	paletteCfg := d.currentPaletteConfig()
	floatingCfg := d.currentFloatingConfig()
	// Title refresh may inspect process state, so it remains before the capture
	// boundary and outside tab/pane ownership locks.
	tb.mu.Lock()
	titleIDs := ac.renderScratch.titleIDs[:0]
	if tb.tree != nil {
		titleIDs = appendStackPaneIDs(titleIDs, tb.tree.Root)
	}
	ac.renderScratch.titleIDs = titleIDs
	floating := tb.floating.pane
	hasFloating := tb.floating.state == floatingVisible && floating != nil
	tb.mu.Unlock()
	for _, id := range titleIDs {
		d.refreshPaneTitle(sess, id)
	}
	if hasFloating {
		d.refreshPaneDisplayTitle(sess, floating, false)
	}

	capturedOverlays := capturedOverlayRenderState{
		copyActive: overlays.copyActive, copySearchActive: overlays.copySearchModel != nil,
		pickerActive: overlays.pickerActive, paletteActive: overlays.paletteActive, promptActive: overlays.promptActive,
		copyFeedback: overlays.copyFeedback,
	}
	endCapture := marks.span(ports.RuntimeCaptureStart, ports.RuntimeCaptureEnd, 0)
	state, ok := capturePrimaryRenderState(sess, ac, primaryCaptureRequest{
		bars:        bars,
		overlays:    capturedOverlays,
		preview:     preview,
		floatingCfg: floatingCfg,
		reset:       reset,
		lease:       lease,
	})
	endCapture(0, ok)
	if !ok {
		ac.sendMu.Unlock()
		return
	}
	if ac.renderStages.capture != nil {
		ac.renderStages.capture()
	}
	if overlays.pickerActive && overlays.previewTab == tb {
		previewState := *state
		previewState.overlays = capturedOverlayRenderState{}
		previewState.reset = true
		state.preview = pickerPreviewFromCapturedRender(previewState)
	}
	captureOverlayLayers(state, overlays, paletteCfg)
	endCompose := marks.span(ports.RuntimeComposeStart, ports.RuntimeComposeEnd, 0)
	composed := composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
	endCompose(0, true)
	if ac.renderStages.compose != nil {
		ac.renderStages.compose()
	}
	d.emitFrame(sess, ac, state, composed, &marks)
}

func resolveStyles(styles []themeui.Styles) themeui.Styles {
	if len(styles) > 0 {
		return styles[0]
	}
	return themeui.NewStyles(themeui.Theme{})
}

type barCache struct {
	top    []renderer.Cell
	bottom []renderer.Cell
}

func (c *barCache) Reset() {
	c.top = nil
	c.bottom = nil
}

func offsetPlacement(pl layout.Placement, dx, dy int) layout.Placement {
	pl.Content.X += dx
	pl.Content.Y += dy
	pl.TitleBar.X += dx
	pl.TitleBar.Y += dy
	return pl
}

func blitPaneFrame(dst renderer.Frame, r domain.Rect, src renderer.Frame, dim bool, dimmer themeui.Dimmer) {
	rows := min(r.Height, src.Height)
	cols := min(r.Width, src.Width)
	for y := range rows {
		for x := range cols {
			cell := src.At(x, y)
			if dim {
				cell.Style = dimmer.Dim(cell.Style)
			}
			dst.Set(r.X+x, r.Y+y, cell)
		}
	}
}

func drawDividers(frame renderer.Frame, n *layout.Node, r domain.Rect, style renderer.Style) {
	if n == nil || n.Kind != layout.Split || len(n.Children) <= 1 {
		return
	}
	count := len(n.Children)
	if n.Dir == layout.Horizontal {
		usable := r.Width - (count - 1)
		base, rem := usable/count, usable%count
		x := r.X
		for i, child := range n.Children {
			w := base
			if i < rem {
				w++
			}
			drawDividers(frame, child, domain.Rect{X: x, Y: r.Y, Width: w, Height: r.Height}, style)
			x += w
			if i < count-1 {
				for y := r.Y; y < r.Y+r.Height; y++ {
					frame.Set(x, y, renderer.Cell{Rune: '│', Style: style})
				}
				x++
			}
		}
		return
	}
	usable := r.Height - (count - 1)
	base, rem := usable/count, usable%count
	y := r.Y
	for i, child := range n.Children {
		h := base
		if i < rem {
			h++
		}
		drawDividers(frame, child, domain.Rect{X: r.X, Y: y, Width: r.Width, Height: h}, style)
		y += h
		if i < count-1 {
			for x := r.X; x < r.X+r.Width; x++ {
				frame.Set(x, y, renderer.Cell{Rune: '─', Style: style})
			}
			y++
		}
	}
}

func (c *barCache) capture(top, bottom []renderer.Cell) {
	c.top = cloneCells(c.top, top)
	c.bottom = cloneCells(c.bottom, bottom)
}

func cloneCells(dst, src []renderer.Cell) []renderer.Cell {
	if cap(dst) < len(src) {
		dst = make([]renderer.Cell, len(src))
	} else {
		dst = dst[:len(src)]
	}
	copy(dst, src)
	return dst
}

func sameCells(a, b []renderer.Cell) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !a[i].Equal(b[i]) {
			return false
		}
	}
	return true
}

func offsetDamage(in []renderer.Damage) []renderer.Damage {
	return translateDamage(in, 0, 1)
}

func translateDamage(in []renderer.Damage, dx, dy int) []renderer.Damage {
	out := make([]renderer.Damage, len(in))
	for i, d := range in {
		out[i] = d
		if d.Kind != renderer.DamageFullRedraw {
			out[i].X += dx
			out[i].Y += dy
		}
	}
	return out
}

func translatePaneDamage(d renderer.Damage, content domain.Rect, area domain.Rect) []renderer.Damage {
	if d.Kind == renderer.DamageFullRedraw {
		return []renderer.Damage{d}
	}
	if d.Kind == renderer.DamageScrollUp && (content.X != 0 || content.Width != area.Width) {
		return []renderer.Damage{{Kind: renderer.DamageText, X: content.X, Y: content.Y, Width: content.Width, Height: content.Height}}
	}
	d.X += content.X
	d.Y += content.Y
	return []renderer.Damage{d}
}
