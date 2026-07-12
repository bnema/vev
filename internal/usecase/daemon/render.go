// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
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

// clearNonRenderablePaneDamage is the single transitional S1 owner for VT
// damage that cannot reach a composition target. S2 replaces this visibility
// decision with pipeline ownership; do not paint or mutate renderer shadows
// here.
func (d *Daemon) clearNonRenderablePaneDamage(sess *session, tb *tab, p *pane) bool {
	renderable := d.paneRenderable(sess, tb, p)
	if !renderable && p != nil {
		p.mu.Lock()
		p.screen.ClearDamage()
		p.mu.Unlock()
	}
	return renderable
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
			renderable := d.clearNonRenderablePaneDamage(sess, tb, p)
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
// copyTargetRectLocked maps a captured copy source into the already-composed
// client frame. The caller holds tb.mu, preserving the layout/floating
// snapshot while the rectangle is chosen.
func copyTargetRectLocked(layoutSnap tabLayoutSnapshot, contentArea domain.Rect, p, floating *pane, hasFloating bool, floatingFrameGeometry floatingGeometry) domain.Rect {
	if p == nil {
		return domain.Rect{}
	}
	if hasFloating && p == floating {
		return floatingFrameGeometry.Inner
	}
	for _, placement := range layoutSnap.placements {
		if placement.ID == p.id && !placement.Collapsed {
			r := placement.Content
			r.X += contentArea.X
			r.Y += contentArea.Y
			return r
		}
	}
	p.mu.Lock()
	width, height := p.screen.Frame.Width, p.screen.Frame.Height
	p.mu.Unlock()
	return domain.Rect{X: contentArea.X, Y: contentArea.Y, Width: min(width, contentArea.Width), Height: min(height, contentArea.Height)}
}

func titleBarPaneIDs(placements []layout.Placement, ok bool) []layout.PaneID {
	if !ok {
		return nil
	}
	ids := make([]layout.PaneID, 0, len(placements))
	for _, pl := range placements {
		if pl.TitleBar.Height > 0 {
			ids = append(ids, pl.ID)
		}
	}
	return ids
}

func (d *Daemon) scheduleResizePaintLocked(sess *session, ac *attachedClient) {
	ac.resizePaint.stop()
	ac.resizePaintGeneration++
	generation := ac.resizePaintGeneration
	ac.resizePaintPending = true
	ac.resizePaint.retain(d.clock, maxDebounceInterval, func(ports.Timer) {
		d.invalidateForResizeGeneration(sess, ac, generation)
	})
}

// invalidateForResizeGeneration preserves PR #71's attachment-owned timer,
// generation rejection, and cancellation while transferring only its eventual
// render request to the coordinator.
func (d *Daemon) invalidateForResizeGeneration(sess *session, ac *attachedClient, generation uint64) {
	ac.sendMu.Lock()
	if ac.currentSession() != sess || !ac.resizePaintPending || ac.resizePaintGeneration != generation {
		ac.sendMu.Unlock()
		return
	}
	ac.resizePaint.stop()
	ac.resizePaintPending = false
	ac.sendMu.Unlock()
	d.invalidateRender(sess, ac, true, "render.go")
}

func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool) {
	d.paintForResizeGeneration(sess, ac, reset, 0, 0)
}

// paintCoordinatorWake composes a coordinator wake only for its captured
// attachment incarnation. The epoch is checked after sendMu is acquired.
func (d *Daemon) paintCoordinatorWake(sess *session, w renderWake) {
	d.paintForResizeGeneration(sess, w.attachment, w.reset, 0, w.attachmentEpoch)
}

func (d *Daemon) paintForResizeGeneration(sess *session, ac *attachedClient, reset bool, resizeGeneration, attachmentEpoch uint64) {
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
	if resizeGeneration != 0 && (!ac.resizePaintPending || ac.resizePaintGeneration != resizeGeneration) {
		ac.sendMu.Unlock()
		return
	}
	// Composition owns attachment sendMu; initialize its lazy overlay state
	// under that same ownership so concurrent fallback paints cannot observe a
	// partially published runtime.
	ac.initOverlays()
	if ac.resizePaintPending {
		ac.resizePaint.stop()
		ac.resizePaintPending = false
		reset = true
	}
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

	styles := newThemeStyles(ac.getTheme())
	paletteCfg := d.currentPaletteConfig()
	tb.mu.Lock()
	layoutSnap := solveTabLayoutLocked(tb)
	titleIDs := titleBarPaneIDs(layoutSnap.placements, layoutSnap.ok)
	floating := tb.floating.pane
	hasFloating := tb.floating.state == floatingVisible && floating != nil
	tb.mu.Unlock()
	for _, id := range titleIDs {
		d.refreshPaneTitle(sess, id)
	}
	if hasFloating {
		d.refreshPaneDisplayTitle(sess, floating, false)
	}

	tb.mu.Lock()
	if !layoutSnap.matchesLocked(tb) {
		layoutSnap = solveTabLayoutLocked(tb)
	}
	p := tb.focusedPane()
	if p == nil {
		tb.mu.Unlock()
		ac.sendMu.Unlock()
		return
	}
	// Recheck the slot after refreshing its title outside the tab lock. One
	// paint uses one immutable floating config snapshot across composition,
	// copy targeting, and cursor placement.
	floating = tb.floating.pane
	hasFloating = tb.floating.state == floatingVisible && floating != nil
	floatingCfg := d.currentFloatingConfig()
	overlayActive := overlays.copyActive || overlays.copySearchModel != nil || overlays.pickerActive || overlays.paletteActive || overlays.promptActive
	if reset || overlayActive {
		ac.bars.Reset()
		ac.composed.invalidate()
	}
	if reset || overlays.pickerActive || overlays.paletteActive || overlays.promptActive {
		ac.lastCursor.valid = false
	}
	var frame renderer.Frame
	var damage []renderer.Damage
	if overlayActive {
		frame, damage = composeClientFrameWithLayoutCachedConsumeDamage(bars, tb, reset, layoutSnap, &ac.bars, nil)
	} else {
		frame, damage = composeClientFrameWithLayoutCachedConsumeDamage(bars, tb, reset, layoutSnap, &ac.bars, &ac.composed)
	}
	contentArea := domain.Rect{Y: 1, Width: frame.Width, Height: max(0, frame.Height-2)}
	floatingFrameGeometry := floatingGeometry{}
	if hasFloating {
		desiredFloatingGeometry := calculateContentFloatingGeometry(domain.Size{Cols: contentArea.Width, Rows: contentArea.Height}, floatingCfg)
		frame, damage, floatingFrameGeometry = composeFloatingFrame(frame, damage, floating, tb.floating.generation, contentArea, desiredFloatingGeometry, layoutSnap, bars.theme, &ac.composed, reset || overlayActive)
	}
	if overlays.copyActive {
		copyPane := overlays.copyPane
		if copyPane == nil {
			copyPane = p
		}
		copyTarget := copyTargetRectLocked(layoutSnap, contentArea, copyPane, floating, hasFloating, floatingFrameGeometry)
		frame, damage = composeCopyClientFrame(overlays.copyMode, overlays.copySnapshot, copyTarget, frame, bars)
	}
	// A palette above normal/copy content dims that composed content. When a
	// floating pane is present its own backdrop already dims normal pane
	// contents; applying the palette backdrop here would also dim the popup.
	if overlays.paletteActive && !hasFloating {
		(overlayBackdrop{DimPaneContents: true}).apply(frame, contentArea, layoutSnap, bars.theme)
	}
	if overlays.copySearchModel != nil {
		frame, damage = composeCopySearchClientFrame(overlays.copySearchModel, frame, styles)
	}
	if overlays.pickerActive {
		if overlays.previewTab == tb {
			if layoutSnap.ok && tb.tree != nil && tb.tree.Root != nil && tb.tree.Root.Kind != layout.Leaf {
				previewFrame, _ := composeTabFrameWithLayout(tb, layoutSnap.area, themeui.Theme{}, layoutSnap)
				preview = pickerPreviewFromFrame(previewFrame)
			} else {
				preview = pickerPreviewFromLockedTab(tb)
			}
		}
		frame, damage = composePickerClientFrame(overlays.pickerModel, preview, frame, styles)
	}
	if overlays.paletteActive {
		guidance := ""
		if overlays.paletteHints != nil {
			guidance = overlays.paletteHints.Feedback
		}
		frame, damage = composePaletteClientFrame(overlays.paletteModel, frame, paletteCfg, guidance, styles)
	}
	if overlays.promptActive {
		frame, damage = composePromptClientFrame(overlays.promptModel, frame, styles)
	}
	overlays.Unlock()
	cursorPane := p
	cursorContent, cursorVisible := focusedPaneContentRect(layoutSnap, p.id)
	cursorContent.X += contentArea.X
	cursorContent.Y += contentArea.Y
	if hasFloating {
		cursorPane = floating
		cursorContent, cursorVisible = floatingFrameGeometry.Inner, floatingFrameGeometry.Inner.Width > 0 && floatingFrameGeometry.Inner.Height > 0
	}
	cursorPane.mu.Lock()
	desiredCursor := desiredCursorOut(cursorPane.screen, cursorContent, !cursorVisible || overlays.copyActive || overlays.copySearchModel != nil || overlays.pickerActive || overlays.paletteActive || overlays.promptActive)
	cursorPane.mu.Unlock()
	ac.sess.mu.Lock()
	if ac.sess.v != sess {
		ac.sess.mu.Unlock()
		tb.mu.Unlock()
		ac.sendMu.Unlock()
		return
	}
	prepared, err := ac.output.prepare(frame, damage, reset || overlayActive)
	var data []byte
	if err == nil {
		data = prepared.data
	}
	var cursorTail []byte
	if err == nil {
		cursorTail = ac.encodeCursorTail(desiredCursor, len(data) > 0)
	}
	tb.mu.Unlock()

	var serr error
	var sendTr ports.Transport
	if err == nil {
		data = append(data, cursorTail...)
		if len(data) > 0 {
			sendTr = ac.transport()
			if sendTr == nil {
				serr = errors.New("client transport is nil")
			} else {
				send := sendTr.Send
				if async, ok := sendTr.(ports.AsyncTransport); ok {
					send = async.SendAsync
				}
				serr = prepared.send(data, ac.echoAck.Load(), send)
			}
		}
	}
	ac.sess.mu.Unlock()
	ac.sendMu.Unlock()

	if err != nil {
		d.log.Error("render draw failed", "err", err, "session", sess.name)
		return
	}
	if serr != nil {
		d.detachOnSendError(sess, ac, sendTr)
	}
}

func focusedPaneContentRect(layoutSnap tabLayoutSnapshot, id layout.PaneID) (domain.Rect, bool) {
	if !layoutSnap.ok {
		return layoutSnap.area, layoutSnap.area.Width > 0 && layoutSnap.area.Height > 0
	}
	for _, pl := range layoutSnap.placements {
		if pl.ID == id && !pl.Collapsed && pl.Content.Width > 0 && pl.Content.Height > 0 {
			return pl.Content, true
		}
	}
	return domain.Rect{}, false
}

// desiredCursorOut computes the terminal cursor state that should be shown to
// the client for the current pane placement and overlay mode.
func desiredCursorOut(s *vt.Screen, content domain.Rect, hide bool) cursorOut {
	if hide || !s.CursorVisible() {
		return cursorOut{hidden: true}
	}
	style, ok := s.CursorStyle()
	if !ok {
		style = 1
	}
	return cursorOut{row: content.Y + s.CursorRow(), col: content.X + s.CursorCol(), style: style, hasStyle: true}
}

func (ac *attachedClient) encodeCursorTail(desired cursorOut, force bool) []byte {
	changed := force || !ac.lastCursor.valid || ac.lastCursor.hidden != desired.hidden || ac.lastCursor.row != desired.row || ac.lastCursor.col != desired.col || ac.lastCursor.style != desired.style || ac.lastCursor.hasStyle != desired.hasStyle
	if !changed {
		return nil
	}
	prev := ac.lastCursor
	ac.lastCursor = desired
	ac.lastCursor.valid = true
	if desired.hidden {
		return []byte("\x1b[?25l")
	}
	var b []byte
	b = append(b, "\x1b["...)
	b = strconv.AppendInt(b, int64(desired.row+1), 10)
	b = append(b, ';')
	b = strconv.AppendInt(b, int64(desired.col+1), 10)
	b = append(b, 'H')
	if !prev.valid || prev.hidden || prev.style != desired.style || prev.hasStyle != desired.hasStyle {
		b = append(b, "\x1b["...)
		b = strconv.AppendInt(b, int64(desired.style), 10)
		b = append(b, " q"...)
	}
	b = append(b, "\x1b[?25h"...)
	return b
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

type composedFrameCache struct {
	frame                   renderer.Frame
	valid                   bool
	layoutSnap              tabLayoutSnapshot
	titleGenerations        map[layout.PaneID]uint64
	floating                *pane
	floatingFrame           renderer.Frame
	floatingGeneration      uint64
	floatingGeometry        floatingGeometry
	floatingTitleGeneration uint64
}

func (c *composedFrameCache) invalidate() {
	c.valid = false
	c.floating = nil
	c.floatingGeneration = 0
	c.floatingGeometry = floatingGeometry{}
	c.floatingTitleGeneration = 0
}

func composeClientFrame(sess *session, tb *tab, full bool, rightStatus string, caches ...*barCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithState(barState{status: sess.statusSegments(true), copyFeedback: rightStatus}, tb, full, caches...)
}

func composeClientFrameWithState(bars barState, tb *tab, full bool, caches ...*barCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithLayout(bars, tb, full, solveTabLayoutLocked(tb), caches...)
}

func composeClientFrameWithLayout(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, caches ...*barCache) (renderer.Frame, []renderer.Damage) {
	var cache *barCache
	if len(caches) > 0 {
		cache = caches[0]
	}
	return composeClientFrameWithLayoutCached(bars, tb, full, layoutSnap, cache, nil)
}

func composeClientFrameWithLayoutCached(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, cache *barCache, composed *composedFrameCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithLayoutCachedOptions(bars, tb, full, layoutSnap, cache, composed, false)
}

func composeClientFrameWithLayoutCachedConsumeDamage(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, cache *barCache, composed *composedFrameCache) (renderer.Frame, []renderer.Damage) {
	return composeClientFrameWithLayoutCachedOptions(bars, tb, full, layoutSnap, cache, composed, true)
}

func composeClientFrameWithLayoutCachedOptions(bars barState, tb *tab, full bool, layoutSnap tabLayoutSnapshot, cache *barCache, composed *composedFrameCache, consumeDamage bool) (renderer.Frame, []renderer.Damage) {
	p := tb.focusedPane()
	styles := newThemeStyles(bars.theme)
	if p == nil {
		width, screenRows := tb.size.Cols, tb.size.Rows
		if width <= 0 || screenRows <= 0 {
			return renderer.NewFrame(0, 0), nil
		}
		return renderer.NewFrame(width, screenRows+2), nil
	}
	p.mu.Lock()
	width, screenRows := p.screen.Frame.Width, p.screen.Frame.Height
	p.mu.Unlock()
	if tb.size.Valid() {
		width, screenRows = tb.size.Cols, tb.size.Rows
	}
	cacheValid := composed != nil && composed.valid && composed.frame.Width == width && composed.frame.Height == screenRows+2
	layoutSame := composed == nil || (cacheValid && sameTabLayoutSnapshot(composed.layoutSnap, layoutSnap))
	var frame renderer.Frame
	if cacheValid {
		frame = composed.frame
	} else {
		frame = renderer.NewFrame(width, screenRows+2)
		if composed != nil {
			full = true
		}
	}
	if !layoutSame {
		full = true
	}
	contentArea := domain.Rect{Y: 1, Width: width, Height: screenRows}
	if cacheValid && !layoutSame {
		clearFrameRect(frame, contentArea)
	}
	topBar := frame.Row(0)
	drawTopBarSnapshot(topBar, bars.status, bars.attentionFrame, bars.topRight, styles)
	var titleGenerations map[layout.PaneID]uint64
	if composed != nil {
		if composed.titleGenerations == nil {
			composed.titleGenerations = make(map[layout.PaneID]uint64)
		}
		titleGenerations = composed.titleGenerations
	}
	contentDamage := composeTabFrameIntoWithLayoutOptions(tb, frame, contentArea, bars.theme, layoutSnap, cacheValid && layoutSame, consumeDamage, titleGenerations)
	bottomBar := frame.Row(screenRows + 1)
	drawStatusBarState(bottomBar, bars, styles)
	if composed != nil {
		composed.frame = frame
		composed.valid = true
		composed.layoutSnap = layoutSnap
	}
	if full {
		if cache != nil {
			cache.capture(topBar, bottomBar)
		}
		return frame, []renderer.Damage{renderer.FullRedraw()}
	}
	damage := translateDamage(contentDamage, 0, 1)
	if cache == nil || !sameCells(cache.top, topBar) {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: width, Height: 1})
	}
	if cache == nil || !sameCells(cache.bottom, bottomBar) {
		damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: screenRows + 1, Width: width, Height: 1})
	}
	if cache != nil {
		cache.capture(topBar, bottomBar)
	}
	return frame, damage
}

func composeTabFrame(tb *tab, area domain.Rect, theme themeui.Theme) (renderer.Frame, []renderer.Damage) {
	return composeTabFrameWithLayout(tb, area, theme, tabLayoutSnapshot{})
}

func composeTabFrameWithLayout(tb *tab, area domain.Rect, theme themeui.Theme, layoutSnap tabLayoutSnapshot) (renderer.Frame, []renderer.Damage) {
	frame := renderer.NewFrame(area.Width, area.Height)
	damage := composeTabFrameIntoWithLayout(tb, frame, area, theme, layoutSnap, false)
	return frame, damage
}

func composeTabFrameIntoWithLayout(tb *tab, frame renderer.Frame, area domain.Rect, theme themeui.Theme, layoutSnap tabLayoutSnapshot, cacheValid bool) []renderer.Damage {
	return composeTabFrameIntoWithLayoutOptions(tb, frame, area, theme, layoutSnap, cacheValid, false, nil)
}

func composeTabFrameIntoWithLayoutOptions(tb *tab, frame renderer.Frame, area domain.Rect, theme themeui.Theme, layoutSnap tabLayoutSnapshot, cacheValid bool, consumeDamage bool, titleGenerations map[layout.PaneID]uint64) []renderer.Damage {
	contentArea := domain.Rect{Width: area.Width, Height: area.Height}
	root := tb.tree.Root
	placements, ok := layoutSnap.placements, layoutSnap.ok && layoutSnap.root == root && layoutSnap.area == contentArea
	if !ok {
		placements, ok = layout.Solve(root, contentArea)
	}
	var fallback *pane
	if !ok {
		fallback = tb.focusedPane()
		if fallback == nil {
			return nil
		}
		placements = []layout.Placement{{ID: fallback.id, Content: contentArea}}
	}
	// A valid cache has the same layout, so its title IDs remain valid. Reset
	// the existing cache only when rebuilding after layout or frame churn.
	if titleGenerations != nil && !cacheValid {
		clear(titleGenerations)
	}
	if ok && !cacheValid {
		drawDividers(frame, root, area, themeui.DimStyle(newThemeStyles(theme).border, theme))
	}
	var damage []renderer.Damage
	for _, pl := range placements {
		p := tb.panes[pl.ID]
		if p == nil && fallback != nil && pl.ID == fallback.id {
			p = fallback
		}
		if p == nil {
			continue
		}
		focused := tb.tree.Focus == pl.ID
		pl = offsetPlacement(pl, area.X, area.Y)
		if pl.TitleBar.Height > 0 {
			generation := drawPaneTitleBar(frame, pl, p, focused, theme)
			if cacheValid && (titleGenerations == nil || titleGenerations[pl.ID] != generation) {
				titleDamage := pl.TitleBar
				titleDamage.X -= area.X
				titleDamage.Y -= area.Y
				damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: titleDamage.X, Y: titleDamage.Y, Width: titleDamage.Width, Height: titleDamage.Height})
			}
			if titleGenerations != nil {
				titleGenerations[pl.ID] = generation
			}
		}
		if pl.Collapsed || pl.Content.Width <= 0 || pl.Content.Height <= 0 {
			if consumeDamage {
				p.mu.Lock()
				p.screen.ClearDamage()
				p.mu.Unlock()
			}
			continue
		}
		p.mu.Lock()
		paneDamage := p.screen.Damage()
		if !cacheValid || len(paneDamage) > 0 {
			blitPaneFrame(frame, pl.Content, p.screen.Frame, !focused, theme)
		}
		for _, d := range paneDamage {
			localContent := pl.Content
			localContent.X -= area.X
			localContent.Y -= area.Y
			damage = append(damage, translatePaneDamage(d, localContent, contentArea)...)
		}
		if consumeDamage {
			p.screen.ClearDamage()
		}
		p.mu.Unlock()
	}
	return damage
}

func sameTabLayoutSnapshot(a, b tabLayoutSnapshot) bool {
	return a.ok == b.ok && a.root == b.root && a.fingerprint == b.fingerprint && a.area == b.area && a.focus == b.focus
}

func offsetPlacement(pl layout.Placement, dx, dy int) layout.Placement {
	pl.Content.X += dx
	pl.Content.Y += dy
	pl.TitleBar.X += dx
	pl.TitleBar.Y += dy
	return pl
}

func clearFrameRect(frame renderer.Frame, r domain.Rect) {
	blank := renderer.BlankCell()
	for y := r.Y; y < r.Y+r.Height && y < frame.Height; y++ {
		for x := r.X; x < r.X+r.Width && x < frame.Width; x++ {
			frame.Set(x, y, blank)
		}
	}
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

func drawPaneTitleBar(frame renderer.Frame, pl layout.Placement, p *pane, focused bool, theme themeui.Theme) uint64 {
	styles := newThemeStyles(theme)
	style := styles.border
	if focused {
		style = styles.statusBar
	} else {
		style = themeui.DimStyle(style, theme)
	}
	for x := pl.TitleBar.X; x < pl.TitleBar.X+pl.TitleBar.Width && x < frame.Width; x++ {
		frame.Set(x, pl.TitleBar.Y, renderer.Cell{Rune: ' ', Style: style})
	}
	p.mu.Lock()
	title := p.displayTitleLocked()
	generation := p.title.generation
	p.mu.Unlock()
	ui.DrawText(frame, pl.TitleBar.X, pl.TitleBar.Y, pl.TitleBar.X+pl.TitleBar.Width, title, style)
	return generation
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
