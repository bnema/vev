// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"bytes"

	"github.com/bnema/vev/internal/domain"
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

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// paneRenderable reports dynamic composition visibility. Callers must not
// hold coordinator locks: this acquires daemon/session/tab/pane-adjacent locks.
func (d *Daemon) paneRenderable(sess *session, tb *tab, p *pane) bool {
	if sess == nil || tb == nil || p == nil {
		return false
	}
	preview := d.tabIsPickerPreview(tb)
	sess.mu.Lock()
	active := sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == tb
	attached := sess.client != nil
	sess.mu.Unlock()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	if tb.floating.pane == p {
		return active && attached && tb.floating.state == floatingVisible
	}
	if !((active && attached) || preview) {
		return false
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
	var resp []byte
	var clipboards []string
	attentionCh := make(chan struct{}, 1)
	p.mu.Lock()
	p.screen.OnResponse = func(b []byte) { resp = append(resp, b...) }
	p.screen.OnBell = func() { signal(attentionCh) }
	p.screen.OnNotify = func(string, string) { signal(attentionCh) }
	p.screen.OnProgress = func(bool) { signal(attentionCh) }
	p.screen.OnClipboard = func(b64 string) { clipboards = append(clipboards, b64) }
	p.mu.Unlock()
	for {
		n, err := p.pty.Read(buf)
		if n > 0 {
			data := buf[:n]
			rc := sess.renderCoordinator()
			p.mu.Lock()
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
					// Publish the gate while the authoritative screen mutation is
					// still protected. A concurrent deadline therefore cannot
					// compose this partial DEC 2026 payload.
					if rc != nil {
						rc.noteSyncBeginWithRenderability(p, syncGen, func() bool {
							return d.paneRenderable(sess, tb, p)
						}, func() {
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
			select {
			case <-attentionCh:
				d.noteAttention(sess, tb)
			default:
			}
			if len(clipboards) > 0 {
				for _, b64 := range clipboards {
					d.forwardClipboardAsync(sess, b64)
				}
				clipboards = clipboards[:0]
			}
			if len(resp) > 0 {
				if _, writeErr := p.pty.Write(resp); writeErr != nil {
					d.log.Warn("pty response write failed", "err", writeErr, "session", sess.name)
				}
				resp = resp[:0]
			}
			if rc != nil {
				if wasSyncing != isSyncing {
					if isSyncing {
						// The accumulated synchronized batch is the pending render only
						// while this pane can currently reach a composition target.
						if renderable {
							rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
						}
					} else if syncEnded && renderable {
						// State removal happened under pane.mu. Publish a new urgent
						// completion after unlock: detach/replace may have cleared the
						// pending batch work, so fireCurrent alone could not wake a
						// headless picker preview or replacement attachment.
						rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "render.go"})
					}
				}
				if completeSyncRead && renderable {
					// The entire batch completed in this read. Its final screen state
					// is ready now, so bypass the bulk debounce without arming a
					// watchdog for a batch that no longer exists.
					rc.invalidate(renderInvalidation{class: invalidateUrgent, producer: "render.go"})
				} else if renderable && !isSyncing && wasSyncing == isSyncing {
					rc.invalidate(renderInvalidation{class: invalidateOutput, producer: "render.go"})
				}
			}
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

// paint draws the composed client frame (active tab plus status bar) and
// sends the resulting bytes. The renderer shadow is reset on explicit invalidations
// such as switch/create/close/rename/resize so the repaint is complete.
func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool, epochs ...uint64) {
	attachmentEpoch := uint64(0)
	if len(epochs) != 0 {
		attachmentEpoch = epochs[0]
	}
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
	if !owned || ac.currentSession() != sess || (attachmentEpoch != 0 && (ac.coordinatorEpoch.Load() != attachmentEpoch || ac.coordinatorReadyEpoch.Load() != attachmentEpoch)) {
		ac.sendMu.Unlock()
		return
	}
	// Capacity is checked before any destructive capture. The coordinator is
	// the normal gate, but this ownership check also protects direct resize and
	// test paint paths from consuming damage that cannot be emitted.
	if ac.output != nil && ac.output.atCapacity() {
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
		if repaintAttachedClients {
			d.repaintAllAttachedClients()
		}
	}()
	defer overlays.Unlock()
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
	state, ok := captureRenderState(sess, ac, bars, capturedOverlays, preview, floatingCfg, reset, damageCaptureConsume)
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
	composed := composeFrame(*state, ac.pipelineCache, ac.pipelineScratch)
	if ac.renderStages.compose != nil {
		ac.renderStages.compose()
	}
	d.emitFrame(sess, ac, state, composed)
}

type themeStyles struct {
	statusBar   renderer.Style
	accent      renderer.Style
	border      renderer.Style
	selection   renderer.Style
	copyStatus  renderer.Style
	paletteDesc renderer.Style

	// Top bar tab label segments: name (bold) and parenthesized pane-title
	// (muted), one pair per base style. No-ops to statusBar/accent on
	// non-truecolor themes.
	tabName        renderer.Style
	tabNameActive  renderer.Style
	tabTitle       renderer.Style
	tabTitleActive renderer.Style

	// Session picker row segments. Detail reuses paletteDesc (both are
	// themeui.MutedTextStyle(t)); pickerName/pickerSelection* are picker-only.
	pickerName           renderer.Style
	pickerSelectionName  renderer.Style
	pickerSelectionMuted renderer.Style
}

func newThemeStyles(t themeui.Theme) themeStyles {
	statusBar := themeui.StatusBarStyle(t)
	accent := themeui.AccentStyle(t)
	selection := themeui.SelectionStyle(t)
	return themeStyles{
		statusBar:   statusBar,
		accent:      accent,
		border:      themeui.BorderStyle(t),
		selection:   selection,
		copyStatus:  selection,
		paletteDesc: themeui.MutedTextStyle(t),

		tabName:        themeui.EmphasisStyle(statusBar, t),
		tabNameActive:  themeui.EmphasisStyle(accent, t),
		tabTitle:       themeui.MutedVariantStyle(statusBar, t),
		tabTitleActive: themeui.MutedVariantStyle(accent, t),

		pickerName:           themeui.EmphasisStyle(renderer.DefaultStyle(), t),
		pickerSelectionName:  themeui.EmphasisStyle(selection, t),
		pickerSelectionMuted: themeui.MutedVariantStyle(selection, t),
	}
}

func resolveThemeStyles(styles []themeStyles) themeStyles {
	if len(styles) > 0 {
		return styles[0]
	}
	return newThemeStyles(themeui.Theme{})
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

func blitPaneFrame(dst renderer.Frame, r domain.Rect, src renderer.Frame, dim bool, theme themeui.Theme) {
	rows := min(r.Height, src.Height)
	cols := min(r.Width, src.Width)
	for y := range rows {
		for x := range cols {
			cell := src.At(x, y)
			if dim {
				cell.Style = themeui.DimStyle(cell.Style, theme)
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
