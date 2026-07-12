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
type composeCacheInput struct {
	valid                   bool
	frame                   renderer.Frame
	layoutFingerprint       string
	titleGenerations        map[layout.PaneID]uint64
	floatingGeneration      uint64
	floatingGeometry        floatingGeometry
	floatingTitleGeneration uint64
	bars                    barCache
}

type composedRenderFrame struct {
	frame  renderer.Frame
	damage []renderer.Damage
	cursor cursorOut
	cache  composeCacheInput
	reset  bool
}

// composeFrame is pure with respect to daemon ownership: it consumes only the
// capture and an attachment-local cache value, and returns a replacement cache.
func composeFrame(state capturedRenderState, in composeCacheInput) composedRenderFrame {
	width, rows := state.layout.area.Width, state.layout.area.Height
	if width <= 0 || rows < 0 {
		return composedRenderFrame{frame: renderer.NewFrame(0, 0), cursor: cursorOut{hidden: true}, reset: state.reset}
	}
	frame := renderer.NewFrame(width, rows+2)
	styles := newThemeStyles(state.theme)
	drawTopBarSnapshot(frame.Row(0), state.bars.status, state.bars.attentionFrame, state.bars.topRight, styles)
	drawStatusBarState(frame.Row(rows+1), state.bars, styles)
	content := domain.Rect{Y: 1, Width: width, Height: rows}
	if state.layout.valid && state.layout.root != nil {
		drawDividers(frame, state.layout.root, content, themeui.DimStyle(styles.border, state.theme))
	}

	full := state.reset || !in.valid || in.frame.Width != width || in.frame.Height != rows+2 || in.layoutFingerprint != state.layout.fingerprint
	titles := make(map[layout.PaneID]uint64, len(state.panes))
	var damage []renderer.Damage
	for _, pane := range state.panes {
		pl := offsetPlacement(pane.placement, 0, 1)
		if pl.TitleBar.Height > 0 {
			drawCapturedPaneTitleBar(frame, pl, pane.title, pane.focused, state.theme)
			if !full && in.titleGenerations[pane.id] != pane.titleGeneration {
				damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: pl.TitleBar.X, Y: pl.TitleBar.Y, Width: pl.TitleBar.Width, Height: pl.TitleBar.Height})
			}
			titles[pane.id] = pane.titleGeneration
		}
		if !pl.Collapsed && pl.Content.Width > 0 && pl.Content.Height > 0 {
			blitPaneFrame(frame, pl.Content, pane.frame, !pane.focused, state.theme)
		}
		for _, d := range pane.damage {
			if d.Kind != renderer.DamageFullRedraw {
				d.Y++
			}
			damage = append(damage, d)
		}
	}
	if state.floating.visible {
		var floatingDamage []renderer.Damage
		frame, floatingDamage = composeCapturedFloatingFrame(frame, damage, state.floating, content, state.layout, state.theme, in, full || state.overlays.active())
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
	baseFrame := frame.Clone()
	frame, damage = composeCapturedOverlays(state, frame, damage, content)
	if full || state.overlays.active() {
		damage = []renderer.Damage{renderer.FullRedraw()}
	}
	cursorInputs := state.cursor
	cursorInputs.hiddenByOverlay = cursorInputs.hiddenByOverlay || state.overlays.active()
	cursor := desiredCapturedCursor(cursorInputs)
	outCache := composeCacheInput{valid: !state.overlays.active(), frame: baseFrame, layoutFingerprint: state.layout.fingerprint, titleGenerations: titles, floatingGeneration: state.floating.generation, floatingGeometry: state.floating.geometry.translate(content.X, content.Y), floatingTitleGeneration: state.floating.titleGeneration}
	outCache.bars.capture(baseFrame.Row(0), baseFrame.Row(rows+1))
	return composedRenderFrame{frame: frame, damage: damage, cursor: cursor, cache: outCache, reset: state.reset || state.overlays.active()}
}

func drawCapturedPaneTitleBar(frame renderer.Frame, pl layout.Placement, title string, focused bool, theme themeui.Theme) {
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
		renderStyles := picker.RenderStyles{Selection: styles.selection, SelectionName: styles.pickerSelectionName, SelectionMuted: styles.pickerSelectionMuted, Name: styles.pickerName, Detail: styles.paletteDesc, Base: renderer.DefaultStyle()}
		o.picker.inner = snap.pickerModel.Render(rectSize(pickerModal.Inner(size)), state.preview, renderStyles)
	}
	if snap.paletteActive && snap.paletteModel != nil {
		o.palette.modal = paletteModalFor(size, paletteCfg)
		guidance := ""
		if snap.paletteHints != nil {
			guidance = snap.paletteHints.Feedback
		}
		o.paletteGuidance = guidance
		o.palette.inner = snap.paletteModel.Render(rectSize(o.palette.modal.Inner(size)), palette.RenderOptions{Styles: palette.RenderStyles{Selection: styles.selection, Description: styles.paletteDesc}, Guidance: guidance})
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
	legacy := tabLayoutSnapshot{placements: state.layout.placements, area: state.layout.area, focus: state.layout.focus, ok: state.layout.valid}
	if o.paletteActive && !state.floating.visible {
		(overlayBackdrop{DimPaneContents: true}).apply(frame, content, legacy, state.theme)
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

// emitFrame is the sole side-effecting half of the pipeline. The caller holds
// sendMu for the complete capture/compose/emit transaction.
func (d *Daemon) emitFrame(sess *session, ac *attachedClient, state *capturedRenderState, composed composedRenderFrame) bool {
	if sess == nil || ac == nil || state == nil {
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	sess.mu.Lock()
	owned := sess.client == ac
	sess.mu.Unlock()
	if !owned || state.attachment != ac || state.attachmentEpoch != ac.coordinatorEpoch.Load() || ac.currentSession() != sess {
		ac.sendMu.Unlock()
		return false
	}
	ac.sess.mu.Lock()
	if ac.sess.v != sess {
		ac.sess.mu.Unlock()
		ac.sendMu.Unlock()
		return false
	}
	prepared, err := ac.output.prepare(composed.frame, composed.damage, composed.reset)
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
		if sendTr == nil {
			sendErr = errors.New("client transport is nil")
		} else {
			send := sendTr.Send
			if async, ok := sendTr.(ports.AsyncTransport); ok {
				send = async.SendAsync
			}
			sendErr = prepared.send(data, ac.echoAck.Load(), send)
		}
	}
	if sendErr == nil {
		ac.pipelineCache = composed.cache
	}
	ac.sess.mu.Unlock()
	ac.sendMu.Unlock()
	if sendErr != nil {
		d.detachOnSendError(sess, ac, sendTr)
	}
	return true
}
