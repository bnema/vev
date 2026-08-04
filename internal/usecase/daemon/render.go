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
	active := sess.attachmentViewsTabLocked(tb)
	attached := len(sess.attachments) != 0
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
	sessions := localSessionsSnapshot(d.sessions)
	d.mu.Unlock()
	for _, sess := range sessions {
		for _, ac := range sess.snapshotAttachments() {
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
	}
	return false
}

// ptyReader is retained as a compatibility entry point for focused tests. The
// production goroutine calls readPanePTY, whose closure owns only the pane.
func (d *Daemon) ptyReader(sess *session, tb *tab, p *pane) {
	if p != nil && p.ownerSnapshot() == nil && sess != nil && tb != nil {
		publishPaneOwner(p, sess, tb, 0)
	}
	d.readPanePTY(p)
}

func (d *Daemon) readPanePTY(p *pane) {
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
			d.processPanePTYData(p, buf[:n], true)
		}
		if err != nil {
			lease := p.effectLease()
			if sessionName, current := paneEffectSessionName(lease); current {
				d.log.Info("pane pty closed", "err", err, "session", sessionName)
			} else {
				d.log.Info("pane pty closed", "err", err)
			}
			if p.onExit != nil {
				p.onExit()
			} else {
				d.reapPaneOwner(p)
			}
			return
		}
	}
}

type panePTYEffects struct {
	lease             paneEffectLease
	renderCoordinator *renderCoordinator
	wasSyncing        bool
	isSyncing         bool
	completeSyncRead  bool
	syncEnded         bool
	attention         bool
	responses         []byte
	clipboards        []string
	syncCleanup       syncTimerCleanup
}

// processPTYData retains the old call shape for resize replay and focused tests,
// but routing is always derived from the pane owner captured under pane.mu.
func (d *Daemon) processPTYData(_ *session, _ *tab, p *pane, data []byte, bufferDuringApply bool) {
	d.processPanePTYData(p, data, bufferDuringApply)
}

// processPanePTYData is the sole VT parsing path. While a resize apply owns the
// pane, reader calls append exact bytes and returns immediately; it never waits
// for PTY.Resize or loses a short read.
func (d *Daemon) processPanePTYData(p *pane, data []byte, bufferDuringApply bool) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if bufferDuringApply && p.resizeApplying {
		p.resizePending = append(p.resizePending, data...)
		p.mu.Unlock()
		return
	}
	effects := panePTYEffects{lease: p.effectLeaseLocked()}
	owner := effects.lease.owner
	if owner != nil && owner.session != nil {
		effects.renderCoordinator = owner.session.renderCoordinator()
	}
	effects.wasSyncing = p.screen.SyncUpdateActive()
	p.screen.Write(data)
	p.refreshTerminalTitleLocked()
	effects.isSyncing = p.screen.SyncUpdateActive()
	effects.completeSyncRead = !effects.wasSyncing && !effects.isSyncing && completedSynchronizedUpdate(data)
	if effects.wasSyncing != effects.isSyncing && owner != nil && owner.session != nil {
		if effects.isSyncing {
			syncGen := owner.session.syncGen.Add(1)
			p.syncGen = syncGen
			if effects.renderCoordinator != nil {
				lease := effects.lease
				effects.syncCleanup = effects.renderCoordinator.beginSyncBatchWithRenderability(p, syncGen, func() bool {
					return lease.Current() && d.paneRenderable(owner.session, owner.tab, p)
				}, func() {
					p.mu.Lock()
					if p.owner.Load() == lease.owner && p.syncGen == syncGen && p.screen.SyncUpdateActive() {
						p.screen.ForceSyncEnd()
					}
					p.mu.Unlock()
				})
			}
		} else {
			effects.syncEnded = effects.renderCoordinator != nil && effects.renderCoordinator.removeSyncEnd(p, p.syncGen, true)
		}
	}
	effects.attention = p.ptyAttention
	p.ptyAttention = false
	effects.responses = append([]byte(nil), p.ptyResponses...)
	p.ptyResponses = p.ptyResponses[:0]
	effects.clipboards = append([]string(nil), p.ptyClipboards...)
	p.ptyClipboards = p.ptyClipboards[:0]
	p.mu.Unlock()
	effects.syncCleanup.finish()
	d.applyPanePTYEffects(p, effects)
}

// applyPanePTYEffects emits only effects still bound to the owner generation
// that parsed them. Each owner-sensitive boundary revalidates independently so
// ownership publication can retire the remaining effects without rerouting
// bytes that belonged to the old owner.
func (d *Daemon) applyPanePTYEffects(p *pane, effects panePTYEffects) {
	owner := effects.lease.owner
	if owner == nil || owner.session == nil || owner.tab == nil {
		return
	}
	if effects.lease.Current() {
		markSnapshotDirty(owner.session)
	}
	if effects.attention && effects.lease.Current() {
		d.noteAttention(owner.session, owner.tab)
	}
	for _, b64 := range effects.clipboards {
		if effects.lease.Current() {
			d.forwardClipboardAsync(effects.lease, b64)
		}
	}
	if len(effects.responses) > 0 && effects.lease.Current() {
		if _, err := p.pty.Write(effects.responses); err != nil {
			if sessionName, current := paneEffectSessionName(effects.lease); current {
				d.log.Warn("pty response write failed", "err", err, "session", sessionName)
			}
		}
	}
	if effects.renderCoordinator == nil || !effects.lease.Current() {
		return
	}
	renderable := d.paneRenderable(owner.session, owner.tab, p)
	if !effects.lease.Current() {
		return
	}
	if effects.wasSyncing != effects.isSyncing {
		if effects.isSyncing {
			if renderable {
				effects.renderCoordinator.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
			}
		} else if effects.syncEnded && renderable {
			effects.renderCoordinator.invalidate(renderInvalidation{class: invalidateUrgent, producer: "render.go"})
		}
	}
	if effects.completeSyncRead && renderable {
		effects.renderCoordinator.invalidate(renderInvalidation{class: invalidateUrgent, producer: "render.go"})
	} else if renderable && !effects.isSyncing && effects.wasSyncing == effects.isSyncing {
		effects.renderCoordinator.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
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
	observer         ports.RuntimeObserver
	marks            []ports.RuntimeMark
	attachmentEffect *attachmentEffectTicket
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

func (d *Daemon) paint(entry attachmentSession, ac *attachedClient, reset bool, lease *attachmentLease) {
	// Local sessions retain their tab/PTY preparation below. Proxy sessions
	// enter the same attachment pipeline but supply their VT snapshot through
	// attachmentSession.captureRenderState.
	sess, local := localSession(entry)
	marks := d.newRuntimeMarkBatch()
	if lease != nil {
		token := attachmentToken(entry, ac, ac.transport())
		token.lease = lease
		ticket, admitted := ac.beginAttachmentEffect(token)
		if !admitted {
			return
		}
		marks.attachmentEffect = ticket
		defer ticket.End()
	}
	var tb *tab
	if local {
		tb = sess.tabForAttachment(ac)
		if tb == nil {
			return
		}
	}

	ac.sendMu.Lock()
	// sendMu is the attachment ownership boundary. Check the session's
	// published identity while holding it so a deadline captured before an
	// attach/replace cannot emit on either the old or new output chain.
	entry.core().mu.Lock()
	_, owned := entry.core().attachments[ac]
	entry.core().mu.Unlock()
	if !owned || ac.currentAttachmentSession() != entry {
		ac.sendMu.Unlock()
		return
	}
	if lease != nil {
		rc := attachmentRenderCoordinator(entry)
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
	if local && overlays.previewTab != tb {
		preview = snapshotPickerPreview(overlays.previewTab)
	}
	if local {
		d.refreshSessionFocusedTitles(sess)
	}
	// Contextual ranks are completely captured under paletteMu. Compose them
	// without reading the live MRU, whose order may have changed mid-interaction.
	statusFeedback := overlays.statusFeedback
	if statusFeedback == "" && overlays.resizeActive {
		statusFeedback = "resize: h/j/k/l or arrows · = equalize · q/esc/enter exit"
	}
	bars := d.barStateForAttachmentPaletteHintsFor(entry, ac, statusFeedback, overlays.paletteHints, overlays.paletteRecent)
	applied := ac.getAppliedTheme()
	bars.theme = applied.Raw
	if local {
		attentionVisible := pulseVisible(bars.attentionFrame)
		repaintAttachedClients = sess.ackAttention(tb, ac, attentionVisible)
	}

	paletteCfg := d.currentPaletteConfig()
	floatingCfg := d.currentFloatingConfig()
	// Title refresh may inspect process state, so it remains before the capture
	// boundary and outside tab/pane ownership locks.
	if local {
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
			d.refreshPaneTitle(sess, id, tb)
		}
		if hasFloating {
			d.refreshPaneDisplayTitle(sess, floating, false)
		}
	}

	capturedOverlays := capturedOverlayRenderState{
		copyActive: overlays.copyActive, copySearchActive: overlays.copySearchModel != nil,
		pickerActive: overlays.pickerActive, paletteActive: overlays.paletteActive, promptActive: overlays.promptActive,
		resizeActive: overlays.resizeActive, statusFeedback: statusFeedback,
	}
	endCapture := marks.span(ports.RuntimeCaptureStart, ports.RuntimeCaptureEnd, 0)
	state, ok := entry.captureRenderState(ac, renderCaptureRequest{
		bars:            bars,
		overlays:        capturedOverlays,
		preview:         preview,
		floatingCfg:     floatingCfg,
		styles:          applied.Resolved.Styles,
		styleGeneration: applied.Generation,
		reset:           reset,
		lease:           lease,
	})
	endCapture(0, ok)
	if !ok {
		ac.sendMu.Unlock()
		return
	}
	if ac.renderStages.capture != nil {
		ac.renderStages.capture()
	}
	if local && overlays.pickerActive && overlays.previewTab == tb {
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
	d.emitFrame(entry, ac, state, composed, &marks)
}

var fallbackChromeStyles = themeui.Resolve(themeui.Theme{}, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}).Styles

func resolveStyles(styles []themeui.Styles) themeui.Styles {
	if len(styles) > 0 {
		return styles[0]
	}
	return fallbackChromeStyles
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
	startX, startY := max(r.X, 0), max(r.Y, 0)
	endX, endY := min(r.X+r.Width, dst.Width), min(r.Y+r.Height, dst.Height)
	for y := startY; y < endY; y++ {
		sy := y - r.Y
		if sy < 0 || sy >= src.Height {
			continue
		}
		for x := startX; x < endX; x++ {
			sx := x - r.X
			if sx < 0 || sx >= src.Width {
				continue
			}
			cell := src.At(sx, sy)
			if dim {
				cell.Style = dimmer.Dim(cell.Style)
			}
			dst.Set(x, y, cell)
		}
	}
}

// drawDividers paints each precomputed divider gap onto frame, offsetting by
// dy (composeFrame's content area starts one row below the top status bar).
// The dividers themselves come from layout.SolveWithDividers, computed once
// per capture alongside pane placements, rather than recomputed here.
func drawDividers(frame renderer.Frame, dividers []layout.Divider, dy int, style renderer.Style) {
	for _, d := range dividers {
		glyph := rune('│')
		if d.Dir == layout.Vertical {
			glyph = '─'
		}
		y0 := max(d.Rect.Y+dy, 0)
		y1 := min(d.Rect.Y+dy+d.Rect.Height, frame.Height)
		x0 := max(d.Rect.X, 0)
		x1 := min(d.Rect.X+d.Rect.Width, frame.Width)
		for y := y0; y < y1; y++ {
			for x := x0; x < x1; x++ {
				frame.Set(x, y, renderer.Cell{Rune: glyph, Style: style})
			}
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
