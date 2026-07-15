package daemon

import (
	"errors"
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/pkg/renderer"
)

// composeCacheInput is an attachment-local value snapshot. composeFrame never
// retains or accesses the attachment that owns it.
// renderStageHooks are optional observability hooks. They are invoked only at
// completed production boundaries: capture and compose after their result is
// available, and emit after prepare plus transport success (including a
// successful no-byte emission). Failed attempts count for capture/compose but
// never for emit.
type renderStageHooks struct {
	capture func()
	compose func()
	emit    func()
}

type composeCacheInput struct {
	valid                   bool
	frame                   renderer.Frame
	layoutFingerprint       string
	titleGenerations        map[layout.PaneID]uint64
	damage                  []renderer.Damage
	floatingVisible         bool
	floatingGeneration      uint64
	floatingGeometry        floatingGeometry
	floatingTitleGeneration uint64
	bars                    barCache
	theme                   themeui.Theme
}

type composedRenderFrame struct {
	frame  renderer.Frame
	damage []renderer.Damage
	cursor cursorOut
	cache  composeCacheInput
	reset  bool
}

// composeFrame is pure with respect to daemon ownership: it consumes only the
// capture, the last committed cache, and an independent attachment-owned
// scratch cache. It returns a replacement cache without mutating committed.
const inactivePaneForegroundDimming = 55

func composeFrame(state capturedRenderState, in composeCacheInput, scratchIn ...composeCacheInput) composedRenderFrame {
	scratch := composeCacheInput{}
	if len(scratchIn) > 0 {
		scratch = scratchIn[0]
	}
	width, rows := state.layout.area.Width, state.layout.area.Height
	if width <= 0 || rows < 0 {
		return composedRenderFrame{frame: renderer.NewFrame(0, 0), cursor: cursorOut{hidden: true}, reset: state.reset}
	}
	// Compose into the alternate attachment-owned buffer. The committed frame is
	// copied before incremental drawing so prepare/send failures cannot change it.
	canReuseFrame := scratch.frame.Width == width && scratch.frame.Height == rows+2
	frame := scratch.frame
	if !canReuseFrame {
		frame = renderer.NewFrame(width, rows+2)
	}
	if in.frame.Width == width && in.frame.Height == rows+2 {
		for y := 0; y < frame.Height; y++ {
			copy(frame.Row(y), in.frame.Row(y))
		}
	}
	styles := newThemeStyles(state.theme)
	defaultDimmer := themeui.NewDimmer(state.theme)
	inactivePaneDimmer := themeui.NewDimmer(state.theme, themeui.WithForegroundDimming(inactivePaneForegroundDimming))
	drawTopBarSnapshot(frame.Row(0), state.bars.status, state.bars.attentionFrame, state.bars.topRight, styles)
	drawStatusBarState(frame.Row(rows+1), state.bars, styles)
	content := domain.Rect{Y: 1, Width: width, Height: rows}
	if state.layout.valid && state.layout.root != nil {
		drawDividers(frame, state.layout.root, content, defaultDimmer.Dim(styles.border))
	}

	full := state.reset || !in.valid || in.frame.Width != width || in.frame.Height != rows+2 || in.layoutFingerprint != state.layout.fingerprint || in.theme != state.theme || in.floatingVisible != state.floating.visible
	titles := scratch.titleGenerations
	if titles == nil {
		titles = make(map[layout.PaneID]uint64, len(state.panes))
	}
	clear(titles)
	damage := scratch.damage[:0]
	for _, pane := range state.panes {
		pl := offsetPlacement(pane.placement, 0, 1)
		if pl.TitleBar.Height > 0 {
			drawCapturedPaneTitleBar(frame, pl, pane.title, pane.focused, styles, defaultDimmer)
			if !full && in.titleGenerations[pane.id] != pane.titleGeneration {
				damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: pl.TitleBar.X, Y: pl.TitleBar.Y, Width: pl.TitleBar.Width, Height: pl.TitleBar.Height})
			}
			titles[pane.id] = pane.titleGeneration
		}
		if !pl.Collapsed && pl.Content.Width > 0 && pl.Content.Height > 0 && (full || len(pane.damage) > 0) {
			blitPaneFrame(frame, pl.Content, pane.frame, !pane.focused, inactivePaneDimmer)
		}
		for _, d := range pane.damage {
			if d.Kind != renderer.DamageFullRedraw {
				d.Y++
			}
			damage = append(damage, d)
		}
	}
	// Keep the committed cache unadorned: floating composition clones this
	// base, so closing or moving a popup cannot retain dimmed/bordered cells.
	baseFrame := frame
	if state.floating.visible {
		var floatingDamage []renderer.Damage
		frame, floatingDamage = composeCapturedFloatingFrame(floatingComposeInput{
			baseFrame:  baseFrame,
			baseDamage: damage,
			floating:   state.floating,
			content:    content,
			layout:     state.layout,
			theme:      state.theme,
			cache:      in,
			full:       full || state.overlays.active(),
		})
		damage = floatingDamage
	}
	if !full {
		if !sameCells(in.bars.top, frame.Row(0)) {
			damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: width, Height: 1})
		}
		if !sameCells(in.bars.bottom, frame.Row(rows+1)) {
			damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: rows + 1, Width: width, Height: 1})
		}
	}
	if state.overlays.active() {
		// Without a floating frame, overlay composition would otherwise mutate
		// the base cache in place. Floating composition already owns a clone.
		if !state.floating.visible {
			baseFrame = frame.Clone()
		}
		frame, damage = composeCapturedOverlays(state, frame, damage, content)
	}
	if full || state.overlays.active() {
		damage = []renderer.Damage{renderer.FullRedraw()}
	}
	cursorInputs := state.cursor
	cursorInputs.hiddenByOverlay = cursorInputs.hiddenByOverlay || state.overlays.active()
	cursor := desiredCapturedCursor(cursorInputs)
	outCache := composeCacheInput{valid: !state.overlays.active(), frame: baseFrame, layoutFingerprint: state.layout.fingerprint, theme: state.theme, titleGenerations: titles, damage: damage, floatingVisible: state.floating.visible, floatingGeneration: state.floating.generation, floatingGeometry: state.floating.geometry.translate(content.X, content.Y), floatingTitleGeneration: state.floating.titleGeneration, bars: scratch.bars}
	outCache.bars.capture(baseFrame.Row(0), baseFrame.Row(rows+1))
	return composedRenderFrame{frame: frame, damage: damage, cursor: cursor, cache: outCache, reset: state.reset || state.overlays.active()}
}

func drawCapturedPaneTitleBar(frame renderer.Frame, pl layout.Placement, title string, focused bool, styles themeStyles, dimmer themeui.Dimmer) {
	style := styles.border
	if focused {
		style = styles.statusBar
	} else {
		style = dimmer.Dim(style)
	}
	for x := pl.TitleBar.X; x < pl.TitleBar.X+pl.TitleBar.Width && x < frame.Width; x++ {
		frame.Set(x, pl.TitleBar.Y, renderer.Cell{Rune: ' ', Style: style})
	}
	ui.DrawText(frame, pl.TitleBar.X, pl.TitleBar.Y, pl.TitleBar.X+pl.TitleBar.Width, title, style)
}

func desiredCapturedCursor(c capturedCursorInputs) cursorOut {
	if !c.renderable || c.hiddenByOverlay || !c.visible {
		return cursorOut{hidden: true}
	}
	style := c.style
	if !c.hasStyle {
		style = 1
	}
	return cursorOut{row: c.content.Y + 1 + c.row, col: c.content.X + c.col, style: style, hasStyle: true}
}

func captureOverlayLayers(state *capturedRenderState, snap *overlayRenderSnapshot, paletteCfg domain.PaletteConfig) {
	if state == nil || snap == nil {
		return
	}
	o := &state.overlays
	o.copyActive, o.copySearchActive, o.pickerActive, o.paletteActive, o.promptActive = snap.copyActive, snap.copySearchModel != nil, snap.pickerActive, snap.paletteActive, snap.promptActive
	o.copyMode, o.copySnapshot = snap.copyMode, snap.copySnapshot
	if snap.copyPane != nil {
		o.copyPaneID = snap.copyPane.id
	}
	styles := newThemeStyles(state.theme)
	size := domain.Size{Cols: state.layout.area.Width, Rows: state.layout.area.Height + 2}
	if snap.copySearchModel != nil {
		o.copySearch.modal = copySearchModal
		o.copySearch.inner = snap.copySearchModel.Render(rectSize(copySearchModal.Inner(size)), styles.selection)
	}
	if snap.pickerActive && snap.pickerModel != nil {
		o.picker.modal = pickerModal
		renderStyles := picker.RenderStyles{Selection: styles.selection, SelectionName: styles.pickerSelectionName, SelectionMuted: styles.pickerSelectionMuted, Name: styles.pickerName, Detail: styles.paletteDesc, Base: renderer.DefaultStyle(), Separator: styles.pickerSeparator}
		o.picker.inner = snap.pickerModel.Render(rectSize(pickerModal.Inner(size)), state.preview, renderStyles)
	}
	if snap.paletteActive && snap.paletteModel != nil {
		o.palette.modal = paletteModalFor(size, paletteCfg)
		guidance := ""
		if snap.paletteHints != nil {
			guidance = snap.paletteHints.Feedback
		}
		o.paletteGuidance = snap.paletteFeedback
		o.palette.inner = snap.paletteModel.Render(rectSize(o.palette.modal.Inner(size)), palette.RenderOptions{Styles: palette.RenderStyles{Selection: styles.selection, Description: styles.paletteDesc}, Guidance: guidance, Feedback: snap.paletteFeedback})
	}
	if snap.promptActive && snap.promptModel != nil {
		o.prompt.modal = promptModalFor(snap.promptModel.Title())
		o.prompt.inner = snap.promptModel.Render(rectSize(o.prompt.modal.Inner(size)), styles.accent)
	}
	state.cursor.hiddenByOverlay = o.active()
}

func composeCapturedOverlays(state capturedRenderState, frame renderer.Frame, damage []renderer.Damage, content domain.Rect) (renderer.Frame, []renderer.Damage) {
	o := state.overlays
	if o.copyActive {
		target := domain.Rect{}
		if state.floating.visible && (o.copyPaneID == "" || o.copyPaneID == state.floating.pane.id) {
			target = state.floating.geometry.translate(content.X, content.Y).Inner
		}
		if target.Width == 0 {
			id := o.copyPaneID
			if id == "" {
				id = state.layout.focus
			}
			for _, p := range state.panes {
				if p.id == id && !p.placement.Collapsed {
					target = p.placement.Content
					target.Y += content.Y
					break
				}
			}
		}
		frame, damage = composeCopyClientFrame(o.copyMode, o.copySnapshot, target, frame, state.bars)
	}
	layoutSnapshot := tabLayoutSnapshot{placements: state.layout.placements, area: state.layout.area, focus: state.layout.focus, ok: state.layout.valid}
	if o.paletteActive && !state.floating.visible {
		(overlayBackdrop{DimPaneContents: true}).apply(frame, content, layoutSnapshot, state.theme)
	}
	for _, modal := range []capturedModal{o.copySearch, o.picker, o.palette, o.prompt} {
		if modal.inner.Width == 0 && modal.inner.Height == 0 {
			continue
		}
		inner := modal.modal.Composite(frame, newThemeStyles(state.theme).border)
		for y := range min(inner.Height, modal.inner.Height) {
			copy(frame.Row(inner.Y + y)[inner.X:inner.X+min(inner.Width, modal.inner.Width)], modal.inner.Row(y)[:min(inner.Width, modal.inner.Width)])
		}
		damage = []renderer.Damage{renderer.FullRedraw()}
	}
	return frame, damage
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

// commitDamageReceipts acknowledges only snapshots that reached the client.
// sendMu serializes attachment transactions; panes are locked individually so
// no pane lock is held while another is acquired. A stale generation is
// intentionally acknowledged too: Screen conservatively leaves FullRedraw for
// the next transaction.
func commitDamageReceipts(receipts []damageReceipt) {
	for _, receipt := range receipts {
		if receipt.pane == nil {
			continue
		}
		receipt.pane.mu.Lock()
		receipt.pane.screen.AcknowledgeDamage(receipt.generation)
		receipt.pane.mu.Unlock()
	}
}

// emitFrame is the sole side-effecting half of the pipeline. The caller holds
// sendMu for the complete capture/compose/emit transaction.
func (d *Daemon) emitFrame(sess *session, ac *attachedClient, state *capturedRenderState, composed composedRenderFrame, batches ...*runtimeMarkBatch) bool {
	var ownedMarks runtimeMarkBatch
	var marks *runtimeMarkBatch
	if len(batches) != 0 {
		marks = batches[0]
	} else {
		ownedMarks = d.newRuntimeMarkBatch()
		marks = &ownedMarks
		// Direct test callers retain the same guarantee as paint: observer I/O
		// follows emitFrame's release of attachment ownership.
		defer marks.flush()
	}
	if sess == nil || ac == nil || state == nil {
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	sess.mu.Lock()
	owned := sess.client == ac
	sess.mu.Unlock()
	if !owned || state.attachment != ac || ac.currentSession() != sess {
		ac.sendMu.Unlock()
		return false
	}
	if state.lease != nil {
		rc := sess.renderCoordinator()
		if rc == nil || state.lease.attachment != ac || !rc.leaseCurrent(state.lease, true) {
			ac.sendMu.Unlock()
			return false
		}
	}
	ac.sess.mu.Lock()
	if ac.sess.v != sess {
		ac.sess.mu.Unlock()
		ac.sendMu.Unlock()
		return false
	}
	endDiff := marks.span(ports.RuntimeDiffStart, ports.RuntimeDiffEnd, 0)
	prepared, err := ac.output.prepare(composed.frame, composed.damage, composed.reset)
	endDiff(0, err == nil)
	if err != nil {
		ac.sess.mu.Unlock()
		ac.sendMu.Unlock()
		d.log.Error("render draw failed", "err", err, "session", sess.name)
		return true
	}
	data := append([]byte(nil), prepared.data...)
	data = append(data, ac.encodeCursorTail(composed.cursor, len(data) > 0)...)
	var sendTr ports.Transport
	var sendErr error
	if len(data) > 0 {
		sendTr = ac.transport()
		endEmit := marks.span(ports.RuntimeEmitStart, ports.RuntimeEmitEnd, uint64(len(data)))
		if sendTr == nil {
			sendErr = errors.New("client transport is nil")
		} else {
			send := sendTr.Send
			if async, ok := sendTr.(ports.AsyncTransport); ok {
				send = async.SendAsync
			}
			sendErr = prepared.send(data, ac.echoAck.Load(), send)
		}
		endEmit(uint64(len(data)), sendErr == nil)
	}
	if sendErr == nil {
		// Publish only after output preparation and transport emission both
		// succeed. Receipt acknowledgement follows after releasing the attachment
		// session guard, while sendMu still owns this transaction.
		ac.pipelineScratch = ac.pipelineCache
		ac.pipelineCache = composed.cache
	}
	ac.sess.mu.Unlock()
	if sendErr == nil {
		// A successful no-byte emission also commits: its renderer shadow still
		// represents the captured frame. Lock panes only under sendMu and with no
		// session guard held.
		commitDamageReceipts(state.receipts)
		if ac.renderStages.emit != nil {
			ac.renderStages.emit()
		}
	}
	ac.sendMu.Unlock()
	if sendErr != nil {
		d.detachOnSendError(sess, ac, sendTr)
	}
	return true
}
