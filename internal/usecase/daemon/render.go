// Package daemon holds vev's server-side session multiplexer use case: the
// accept loop, the ephemeral/named session registry, the per-tab PTY reader
// and VT screen, and the per-client debounced render scheduler.
//
// Concurrency model (sessions own one or more PTY-backed tabs):
//
//   - Serve runs the accept loop. Each accepted connection is handled by its
//     own goroutine (handleConn): it reads the first frame and routes it to a
//     session create/attach, a list, or a kill.
//   - Per session there are exactly two long-lived goroutines: the PTY reader
//     (drains child output into the VT screen and pokes a cap-1 dirty channel)
//     and the render scheduler (debounces dirties and paints the attached
//     client). Both are tied to the session context and unwind when the
//     session is killed (pty.Close unblocks the reader; ctx cancel stops the
//     scheduler).
//   - The daemon exits (Serve returns) when the last session is removed, or
//     when the parent context is cancelled (graceful shutdown notifies any
//     attached clients with ReasonServerShutdown).
//
// Locking: a pane's screen/scrollback and per-client renderer shadow are
// guarded by pane.mu/tab.mu as appropriate; the attached-client pointer by
// session.mu; the registry by Daemon.mu. When more than one is held the order
// is always attachedClient.sendMu > Daemon.mu > session.mu > tab.mu > pane.mu.
// The PTY reader only ever takes pane.mu, so it never blocks on a slow client.
package daemon

import (
	"bytes"
	"errors"
	"strconv"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
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
			p.mu.Lock()
			wasSyncing := p.screen.SyncUpdateActive()
			p.screen.Write(data)
			p.refreshTerminalTitleLocked()
			isSyncing := p.screen.SyncUpdateActive()
			p.mu.Unlock()
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
			if wasSyncing != isSyncing {
				p.mu.Lock()
				p.syncGen++
				gen := p.syncGen
				p.mu.Unlock()
				if isSyncing {
					go d.syncWatchdog(p, gen)
				}
			}
			if (wasSyncing && !isSyncing) || (!isSyncing && syncUpdateEndIn(data)) {
				signal(p.flush)
				continue
			}
			if isSyncing {
				continue
			}
			signal(p.dirty)
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

// scheduler debounces dirty signals. The first dirty opens a short tab;
// sustained floods progressively widen that tab, while isolated updates
// return to the minimum delay for interactive latency.
func (d *Daemon) scheduler(sess *session, tb *tab, p *pane) {
	defer d.sessWg.Done()
	if p == nil {
		return
	}
	delay := minDebounceInterval
	lastRender := d.clock.Now()
outer:
	for {
		select {
		case <-sess.ctx.Done():
			return
		case <-paneDone(p):
			return
		case <-p.flush:
			d.render(sess, tb, p)
			lastRender = d.clock.Now()
			continue
		case <-p.dirty:
			if d.clock.Now().Sub(lastRender) >= maxDebounceInterval {
				delay = minDebounceInterval
			}
		}

		coalesced := 0
		timer := d.clock.NewTimer(delay)
	absorb:
		for {
			select {
			case <-sess.ctx.Done():
				timer.Stop()
				return
			case <-paneDone(p):
				timer.Stop()
				return
			case <-p.flush:
				if !timer.Stop() {
					select {
					case <-timer.C():
					default:
					}
				}
				d.render(sess, tb, p)
				lastRender = d.clock.Now()
				continue outer
			case <-p.dirty:
				coalesced++
			case <-timer.C():
				break absorb
			}
		}
		delay = nextDebounceDelay(delay, coalesced)
		d.render(sess, tb, p)
		lastRender = d.clock.Now()
	}
}

func nextDebounceDelay(delay time.Duration, coalesced int) time.Duration {
	if coalesced == 0 {
		return minDebounceInterval
	}
	if delay >= maxDebounceInterval {
		return maxDebounceInterval
	}
	delay += debounceStep
	if delay > maxDebounceInterval {
		return maxDebounceInterval
	}
	return delay
}

func (d *Daemon) syncWatchdog(p *pane, gen uint64) {
	timer := d.clock.NewTimer(maxSyncUpdateDuration)
	select {
	case <-paneDone(p):
		timer.Stop()
		return
	case <-timer.C():
	}

	p.mu.Lock()
	if p.syncGen != gen || !p.screen.SyncUpdateActive() {
		p.mu.Unlock()
		return
	}
	p.screen.ForceSyncEnd()
	p.mu.Unlock()
	signal(p.flush)
}

func syncUpdateEndIn(data []byte) bool {
	return bytes.Contains(data, []byte("\x1b[?2026l"))
}

// render paints the current client, or (when detached) just clears accumulated
// damage so it never grows unbounded while headless.
func (d *Daemon) render(sess *session, tb *tab, p *pane) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.screen.SyncUpdateActive() {
		p.mu.Unlock()
		return
	}
	p.mu.Unlock()

	// A retained floating pane continues to drain its PTY while hidden. Its
	// scheduler must not route through picker preview (or the active client),
	// but it must consume damage so a long-hidden command cannot retain it.
	tb.mu.Lock()
	hiddenFloating := tb.floating.pane == p && tb.floating.state != floatingVisible
	previewer := tb.previewClient
	tb.mu.Unlock()
	if hiddenFloating {
		p.mu.Lock()
		p.screen.ClearDamage()
		p.mu.Unlock()
		return
	}

	sess.mu.Lock()
	ac := sess.client
	active := sess.active >= 0 && sess.active < len(sess.tabs) && sess.tabs[sess.active] == tb
	sess.mu.Unlock()

	if previewer != nil {
		if previewSess := previewer.currentSession(); previewSess != nil {
			d.paint(previewSess, previewer, false)
		}
		if ac != nil && active && ac != previewer {
			d.paint(sess, ac, false)
			return
		}
		p.mu.Lock()
		p.screen.ClearDamage()
		p.mu.Unlock()
		return
	}

	if ac == nil || !active {
		p.mu.Lock()
		p.screen.ClearDamage()
		p.mu.Unlock()
		return
	}
	d.refreshBarScriptsIfDue(sess, d.clock.Now(), false)
	d.paint(sess, ac, false)
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

func (d *Daemon) paint(sess *session, ac *attachedClient, reset bool) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	ac.initOverlays()
	ac.sendMu.Lock()
	if ac.currentSession() != sess {
		ac.sendMu.Unlock()
		return
	}
	// Ack-gated coalescing: while the client is at least maxUnackedOutputStates
	// behind, skip composing another diff and mark the paint deferred. The
	// MsgOutputAck handler flushes one cumulative paint once acks catch up. A
	// deferred reset stays sticky so the cumulative paint is dependency-free.
	if ac.output.deferIfAtCapacity(reset) {
		ac.sendMu.Unlock()
		return
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
	bars := d.barStateForClient(sess, ac, overlays.copyFeedback)
	bars.theme = ac.getTheme()
	_, attentionVisible := pulseStyle(bars.attentionFrame)
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
		frame, damage = composeCopyClientFrame(overlays.copyMode, copyPane, copyTarget, frame, bars)
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
		frame, damage = composePaletteClientFrame(overlays.paletteModel, frame, paletteCfg, styles)
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
}

func newThemeStyles(t themeui.Theme) themeStyles {
	return themeStyles{
		statusBar:   themeui.StatusBarStyle(t),
		accent:      themeui.AccentStyle(t),
		border:      themeui.BorderStyle(t),
		selection:   themeui.SelectionStyle(t),
		copyStatus:  themeui.SelectionStyle(t),
		paletteDesc: themeui.MutedTextStyle(t),
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
	return composeClientFrameWithState(barState{status: sess.statusSegments(), copyFeedback: rightStatus}, tb, full, caches...)
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
