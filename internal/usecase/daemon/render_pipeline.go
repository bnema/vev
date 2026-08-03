package daemon

import (
	"errors"
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/notices"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/bnema/vev/internal/usecase/prompt"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/internal/usecase/visualsearch"
	"github.com/bnema/vev/pkg/renderer"
)

// composeCacheInput is an attachment-local value snapshot. composeFrame never
// retains or accesses the attachment that owns it.
// renderStageHooks are optional observability hooks. capture and compose run
// after their result is available, and emit after prepare plus transport success
// (including a successful no-byte emission); those hooks run while sendMu is
// held. Failed attempts count for capture/compose but never for emit.
// handoffRebase marks the boundary immediately before a handoff acquires sendMu
// to rebase attachment-owned output state, so it intentionally runs unlocked.
type renderStageHooks struct {
	capture       func()
	compose       func()
	emit          func()
	handoffRebase func()
}

type composeCacheInput struct {
	valid                   bool
	frame                   renderer.Frame
	layoutFingerprint       string
	titleGenerations        map[layout.PaneID]uint64
	damage                  []renderer.Damage
	floatingVisible         bool
	floatingFocused         bool
	floatingGeneration      uint64
	floatingGeometry        floatingGeometry
	floatingTitleGeneration uint64
	bars                    barCache
	toastFootprints         []domain.Rect
	theme                   themeui.Theme
	styleGeneration         uint64
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
	contentY := 1
	frameHeight := rows + tabChromeRows
	if state.contentOnly {
		contentY = 0
		frameHeight = rows
	}
	canReuseFrame := scratch.frame.Width == width && scratch.frame.Height == frameHeight
	frame := scratch.frame
	if !canReuseFrame {
		frame = renderer.NewFrame(width, frameHeight)
	}
	if in.frame.Width == width && in.frame.Height == frameHeight {
		for y := 0; y < frame.Height; y++ {
			copy(frame.Row(y), in.frame.Row(y))
		}
	}
	styles := state.styles
	if styles == (themeui.Styles{}) {
		// Direct/test-only composition has no applied snapshot; production
		// capture always supplies immutable styles.
		styles = fallbackChromeStyles
	}
	state.styles = styles
	defaultDimmer := themeui.NewDimmer(state.theme)
	neutralBorder := styles.NeutralBorder
	inactivePaneDimmer := themeui.NewDimmer(state.theme, themeui.WithForegroundDimming(inactivePaneForegroundDimming))
	if !state.contentOnly {
		drawTopBarSnapshot(frame.Row(0), state.bars.status, state.bars.attentionFrame, state.bars.topRight, styles)
		drawStatusBarState(frame.Row(rows+1), state.bars, styles)
	}
	content := domain.Rect{Y: contentY, Width: width, Height: rows}
	if state.layout.valid {
		drawDividers(frame, state.layout.dividers, content.Y, defaultDimmer.Dim(neutralBorder))
	}

	full := state.reset || !in.valid || in.frame.Width != width || in.frame.Height != frameHeight || in.layoutFingerprint != state.layout.fingerprint || in.theme != state.theme || in.styleGeneration != state.styleGeneration || in.floatingVisible != state.floating.visible
	titles := scratch.titleGenerations
	if titles == nil {
		titles = make(map[layout.PaneID]uint64, len(state.panes))
	}
	clear(titles)
	damage := scratch.damage[:0]
	for _, pane := range state.panes {
		pl := offsetPlacement(pane.placement, 0, contentY)
		if pl.TitleBar.Height > 0 {
			drawCapturedPaneTitleBar(frame, pl, pane.title, pane.focused, styles, neutralBorder, defaultDimmer)
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
				d.Y += contentY
			}
			damage = append(damage, d)
		}
	}
	// Keep the committed cache unadorned. Toasts are composed first so floating
	// terminals and scoped modals dim them with the rest of the complete frame.
	baseFrame := frame
	overlaysActive := state.overlays.active()
	toastsVisible := len(state.overlays.notices) > 0 || state.overlays.noticeOverflow > 0
	var toastFootprints []domain.Rect
	if toastsVisible {
		frame = baseFrame.Clone()
		toastFootprints = composeCapturedNotices(state.overlays, frame, state.styles)
	}
	if state.floating.visible {
		var floatingDamage []renderer.Damage
		frame, floatingDamage = composeCapturedFloatingFrame(floatingComposeInput{
			baseFrame:    frame,
			baseDamage:   damage,
			floating:     state.floating,
			content:      content,
			theme:        state.theme,
			borderMuted:  styles.BorderMuted,
			borderActive: styles.BorderActive,
			cache:        in,
			full:         full || overlaysActive,
		})
		damage = floatingDamage
	}
	if overlaysActive {
		if !toastsVisible && !state.floating.visible {
			frame = baseFrame.Clone()
		}
		frame, damage = composeCapturedCopyMode(state, frame, damage, content)
		frame, damage = composeCapturedOverlays(state, frame, damage)
	}
	if !state.contentOnly && !full {
		if !sameCells(in.bars.top, baseFrame.Row(0)) {
			damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: 0, Width: width, Height: 1})
		}
		if !sameCells(in.bars.bottom, baseFrame.Row(rows+1)) {
			damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: 0, Y: rows + 1, Width: width, Height: 1})
		}
	}
	// The cache stays toast-free. The terminal shadow, in contrast, includes
	// toasts, so every render damages both the last and current toast coverage.
	// This restores cells exposed by dismissal and redraws a stable toast over
	// any underlying pane update without promoting either case to a full frame.
	if full || overlaysActive {
		damage = []renderer.Damage{renderer.FullRedraw()}
	} else {
		damage = appendToastDamage(damage, in.toastFootprints)
		damage = appendToastDamage(damage, toastFootprints)
	}
	cursorInputs := state.cursor
	cursorInputs.hiddenByOverlay = cursorInputs.hiddenByOverlay || overlaysActive
	cursor := desiredCapturedCursor(cursorInputs, contentY)
	outCache := composeCacheInput{valid: !overlaysActive, frame: baseFrame, layoutFingerprint: state.layout.fingerprint, theme: state.theme, styleGeneration: state.styleGeneration, titleGenerations: titles, damage: damage, toastFootprints: append(scratch.toastFootprints[:0], toastFootprints...), floatingVisible: state.floating.visible, floatingFocused: state.floating.focused, floatingGeneration: state.floating.generation, floatingGeometry: state.floating.geometry.translate(content.X, content.Y), floatingTitleGeneration: state.floating.titleGeneration, bars: scratch.bars}
	if !state.contentOnly {
		outCache.bars.capture(baseFrame.Row(0), baseFrame.Row(rows+1))
	} else {
		outCache.bars.Reset()
	}
	return composedRenderFrame{frame: frame, damage: damage, cursor: cursor, cache: outCache, reset: state.reset || state.overlays.active()}
}

func drawCapturedPaneTitleBar(frame renderer.Frame, pl layout.Placement, title string, focused bool, styles themeui.Styles, neutralBorder renderer.Style, dimmer themeui.Dimmer) {
	style := styles.StatusBar
	if !focused {
		style = dimmer.Dim(neutralBorder)
	}
	for x := pl.TitleBar.X; x < pl.TitleBar.X+pl.TitleBar.Width && x < frame.Width; x++ {
		frame.Set(x, pl.TitleBar.Y, renderer.Cell{Rune: ' ', Style: style})
	}
	ui.DrawText(frame, pl.TitleBar.X, pl.TitleBar.Y, pl.TitleBar.X+pl.TitleBar.Width, title, style)
}

func desiredCapturedCursor(c capturedCursorInputs, contentY int) cursorOut {
	if !c.renderable || c.hiddenByOverlay || !c.visible {
		return cursorOut{hidden: true}
	}
	style := c.style
	if !c.hasStyle {
		style = 1
	}
	return cursorOut{valid: true, row: c.content.Y + contentY + c.row, col: c.content.X + c.col, style: style, hasStyle: true}
}

func captureOverlayLayers(state *capturedRenderState, snap *overlayRenderSnapshot, paletteCfg domain.PaletteConfig) {
	if state == nil || snap == nil {
		return
	}
	o := &state.overlays
	o.copyActive, o.copySearchActive, o.pickerActive, o.paletteActive, o.promptActive = snap.copyActive, snap.copySearchModel != nil, snap.pickerActive, snap.paletteActive, snap.promptActive
	o.noticesOverlayActive = snap.noticesOverlayActive
	o.copyMode = snap.copyMode
	o.notices, o.noticeOverflow = snap.notices, snap.noticeOverflow
	if snap.copyPane != nil {
		o.copyPaneID = snap.copyPane.id
	}
	styles := state.styles
	if styles == (themeui.Styles{}) {
		styles = fallbackChromeStyles
	}
	rows := state.layout.area.Height
	if !state.contentOnly {
		rows += tabChromeRows
	}
	size := domain.Size{Cols: state.layout.area.Width, Rows: rows}
	if snap.copySearchModel != nil {
		presentation := copySearchModal.Resolve(size)
		o.copySearch = capturedModal{active: true, title: copySearchModal.Title, presentation: presentation, focused: true}
		o.copySearch.inner = snap.copySearchModel.RenderStyled(rectSize(presentation.Inner), visualsearch.RenderStyles{Base: styles.PromptBase, Selection: styles.SearchSelection})
	}
	if snap.pickerActive && snap.pickerModel != nil {
		presentation := pickerModal.Resolve(size)
		title := snap.pickerTitle
		if title == "" {
			title = pickerModal.Title
		}
		o.picker = capturedModal{active: true, title: title, presentation: presentation, focused: true}
		stoppedStyle := styles.PickerName
		stoppedStyle.Attrs |= renderer.AttrDim
		stoppedStyle.Italic = true
		renderStyles := picker.RenderStyles{Background: styles.PickerBase, Selection: styles.PickerSelection, SelectionName: styles.PickerSelectionName, SelectionMuted: styles.PickerSelectionMuted, Name: styles.PickerName, Detail: styles.PickerDescription, Base: styles.PickerBase, Separator: styles.PickerSeparator, Stopped: stoppedStyle}
		o.picker.inner = snap.pickerModel.Render(rectSize(presentation.Inner), state.preview, renderStyles)
	}
	if snap.noticesOverlayActive && snap.noticesOverlayModel != nil {
		presentation := noticesModal.Resolve(size)
		o.noticesOverlay = capturedModal{active: true, title: noticesModal.Title, presentation: presentation, focused: true}
		renderStyles := notices.RenderStyles{Background: styles.PickerBase, Base: styles.PickerBase, Selection: styles.PickerSelection, Text: styles.PickerBase, SelectionText: styles.PickerSelectionName, Muted: styles.PickerDescription, SelectionMuted: styles.PickerSelectionMuted}
		o.noticesOverlay.inner = snap.noticesOverlayModel.Render(rectSize(presentation.Inner), renderStyles)
	}
	if snap.paletteActive && snap.paletteModel != nil {
		modal := paletteModalFor(size, paletteCfg)
		presentation := modal.Resolve(size)
		o.palette = capturedModal{active: true, title: modal.Title, presentation: presentation, focused: true}
		guidance := ""
		if snap.paletteHints != nil {
			guidance = snap.paletteHints.Feedback
		}
		o.paletteGuidance = snap.paletteFeedback
		o.palette.inner = snap.paletteModel.Render(rectSize(presentation.Inner), palette.RenderOptions{Styles: palette.RenderStyles{Base: styles.PickerBase, Row: styles.PickerBase, Selection: styles.PickerSelection, Description: styles.PickerDescription, SelectionDescription: styles.PickerSelectionMuted}, Guidance: guidance, Feedback: snap.paletteFeedback})
	}
	if snap.promptActive && snap.promptModel != nil {
		modal := promptModalFor(snap.promptModel.Title())
		presentation := modal.Resolve(size)
		o.prompt = capturedModal{active: true, title: modal.Title, presentation: presentation, focused: true}
		o.prompt.inner = snap.promptModel.RenderStyled(rectSize(presentation.Inner), prompt.RenderStyles{Base: styles.PromptBase, Selection: styles.SurfaceActive})
	}
	state.cursor.hiddenByOverlay = o.active()
}

func composeCapturedCopyMode(state capturedRenderState, frame renderer.Frame, damage []renderer.Damage, content domain.Rect) (renderer.Frame, []renderer.Damage) {
	o := state.overlays
	if !o.copyActive {
		return frame, damage
	}
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
	return composeCopyClientFrame(o.copyMode, target, frame, state.styles)
}

func composeCapturedOverlays(state capturedRenderState, frame renderer.Frame, damage []renderer.Damage) (renderer.Frame, []renderer.Damage) {
	o := state.overlays
	// Paint in reverse keyboard priority so the same layer that owns input is
	// visually topmost: prompt > palette > picker > notices > copy search.
	for _, modal := range []capturedModal{o.copySearch, o.noticesOverlay, o.picker, o.palette, o.prompt} {
		if !modal.active {
			continue
		}
		applyOverlayBackdrop(frame, state.theme)
		border := state.styles.BorderMuted
		if modal.focused {
			border = state.styles.BorderActive
		}
		(ui.Modal{Title: modal.title}).CompositePresentation(frame, modal.presentation, border, state.styles.PickerBase)
		copyModalInner(frame, modal.presentation.Inner, modal.inner)
		damage = []renderer.Damage{renderer.FullRedraw()}
	}
	return frame, damage
}

// copyModalInner copies a captured model frame into its resolved destination.
// Both source and destination are clipped so degenerate presentations remain
// safe immutable composition inputs.
func copyModalInner(dst renderer.Frame, target domain.Rect, src renderer.Frame) {
	left := max(target.X, 0)
	top := max(target.Y, 0)
	right := min(target.X+target.Width, dst.Width)
	bottom := min(target.Y+target.Height, dst.Height)
	if left >= right || top >= bottom {
		return
	}

	sourceX := left - target.X
	sourceY := top - target.Y
	width := min(right-left, src.Width-sourceX)
	height := min(bottom-top, src.Height-sourceY)
	if width <= 0 || height <= 0 {
		return
	}
	for y := range height {
		copy(dst.Row(top + y)[left:left+width], src.Row(sourceY + y)[sourceX:sourceX+width])
	}
}

func composeCapturedNotices(overlays capturedOverlayRenderState, frame renderer.Frame, styles themeui.Styles) []domain.Rect {
	if len(overlays.notices) == 0 && overlays.noticeOverflow == 0 {
		return nil
	}
	views := make([]ui.NoticeView, len(overlays.notices))
	for i, n := range overlays.notices {
		views[i] = ui.NoticeView{Severity: n.Severity, Title: n.Code.String(), Message: n.Message, Count: n.Count}
	}
	return ui.ComposeNotices(frame, views, overlays.noticeOverflow, noticeStylesFrom(styles))
}

func appendToastDamage(damage []renderer.Damage, footprints []domain.Rect) []renderer.Damage {
	for _, footprint := range footprints {
		if footprint.Width > 0 && footprint.Height > 0 {
			damage = append(damage, renderer.Damage{Kind: renderer.DamageText, X: footprint.X, Y: footprint.Y, Width: footprint.Width, Height: footprint.Height})
		}
	}
	return damage
}

// noticeStylesFrom picks the toast box color per severity from the theme's
// chrome roles. Warn uses the dedicated BorderWarn role so it reads as
// distinct from Info instead of sharing BorderMuted with it.
func noticeStylesFrom(styles themeui.Styles) ui.NoticeStyles {
	return ui.NoticeStyles{
		Text:     styles.PickerBase,
		BoxError: styles.BorderActive,
		BoxWarn:  styles.BorderWarn,
		BoxInfo:  styles.BorderMuted,
	}
}

type cursorCandidate struct {
	data []byte
	next cursorOut
}

func (ac *attachedClient) prepareCursorTail(desired cursorOut, force bool) cursorCandidate {
	candidate := cursorCandidate{next: desired}
	candidate.next.valid = true
	prev := ac.lastCursor
	changed := force || !prev.valid || prev.hidden != desired.hidden || prev.row != desired.row || prev.col != desired.col || prev.style != desired.style || prev.hasStyle != desired.hasStyle
	if !changed {
		return candidate
	}
	if desired.hidden {
		candidate.data = []byte("\x1b[?25l")
		return candidate
	}
	candidate.data = append(candidate.data, "\x1b["...)
	candidate.data = strconv.AppendInt(candidate.data, int64(desired.row+1), 10)
	candidate.data = append(candidate.data, ';')
	candidate.data = strconv.AppendInt(candidate.data, int64(desired.col+1), 10)
	candidate.data = append(candidate.data, 'H')
	if !prev.valid || prev.hidden || prev.style != desired.style || prev.hasStyle != desired.hasStyle {
		candidate.data = append(candidate.data, "\x1b["...)
		candidate.data = strconv.AppendInt(candidate.data, int64(desired.style), 10)
		candidate.data = append(candidate.data, " q"...)
	}
	candidate.data = append(candidate.data, "\x1b[?25h"...)
	return candidate
}

// commitDamageReceipts acknowledges only snapshots that reached the client.
// sendMu serializes attachment transactions; panes are locked individually so
// no pane lock is held while another is acquired. A stale generation is
// intentionally acknowledged too: Screen conservatively leaves FullRedraw for
// the next transaction.
func commitDamageReceipts(receipts []damageReceipt) {
	for _, receipt := range receipts {
		if receipt.pane != nil {
			receipt.pane.mu.Lock()
			receipt.pane.screen.AcknowledgeDamage(receipt.generation)
			receipt.pane.mu.Unlock()
			continue
		}
		if receipt.proxy != nil {
			receipt.proxy.mu.Lock()
			if receipt.proxy.screen != nil && receipt.proxy.screen == receipt.proxyScreen {
				receipt.proxy.screen.AcknowledgeDamage(receipt.generation)
			}
			receipt.proxy.mu.Unlock()
		}
	}
}

// emitFrame is the sole side-effecting half of the pipeline. The caller holds
// sendMu for the complete capture/compose/emit transaction.
func (d *Daemon) emitFrame(entry attachmentSession, ac *attachedClient, state *capturedRenderState, composed composedRenderFrame, batches ...*runtimeMarkBatch) bool {
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
	if entry == nil || entry.core() == nil || ac == nil || state == nil {
		if ac != nil {
			ac.sendMu.Unlock()
		}
		return false
	}
	entry.core().mu.Lock()
	_, owned := entry.core().attachments[ac]
	entry.core().mu.Unlock()
	if !owned || state.attachment != ac || ac.currentAttachmentSession() != entry {
		ac.sendMu.Unlock()
		return false
	}
	if state.lease != nil {
		rc := attachmentRenderCoordinator(entry)
		if rc == nil || state.lease.attachment != ac || !rc.leaseCurrent(state.lease, true) {
			ac.sendMu.Unlock()
			return false
		}
	}
	endDiff := marks.span(ports.RuntimeDiffStart, ports.RuntimeDiffEnd, 0)
	var (
		preparedANSI   *preparedOutput
		preparedScreen *preparedStructuredOutput
		err            error
		cursor         cursorCandidate
		data           []byte
	)
	if ac.proxied {
		screenOutput := ac.ensureScreenOutput()
		if screenOutput == nil {
			err = errors.New("proxied attachment has no structured output stream")
		} else {
			preparedScreen, err = screenOutput.prepare(composed.frame, composed.damage, composed.cursor, composed.reset, ac.echoAck.Load())
			if err == nil {
				data = preparedScreen.data
			}
		}
	} else {
		preparedANSI, err = ac.output.prepare(composed.frame, composed.damage, composed.reset)
		if err == nil {
			cursor = ac.prepareCursorTail(composed.cursor, len(preparedANSI.data) > 0)
			data = append([]byte(nil), preparedANSI.data...)
			data = append(data, cursor.data...)
		}
	}
	endDiff(0, err == nil)
	if err != nil {
		ac.discardProxyCapture()
		ac.sendMu.Unlock()
		d.log.Error("render draw failed", "err", err, "session", entry.core().name)
		// Without a coordinator reportError repaints synchronously. Suppress only
		// that nested notice repaint; leave the guard before returning so a later,
		// independent failed transaction can still notify the user.
		if sess, ok := localSession(entry); ok {
			if sess.renderCoordinator() == nil {
				if !ac.prepareFailureFallback.CompareAndSwap(false, true) {
					return true
				}
				d.reportError(sess, domain.UserErr(domain.NoticeInternal, "display update failed", err))
				ac.prepareFailureFallback.Store(false)
				return true
			}
			d.reportError(sess, domain.UserErr(domain.NoticeInternal, "display update failed", err))
		}
		// Notices are session-scoped, but the repaint is not: a proxy attachment
		// has no local session to report through and still needs its chrome
		// redrawn after a failed transaction.
		d.invalidateRender(entry, ac, true, "render_pipeline.go:prepare-failed")
		return true
	}
	var sendTr ports.Transport
	var sendErr error
	// Metadata precedes any output bytes and is published on its own when the
	// terminal frame is empty, so a proxied client never keeps a stale snapshot.
	if len(data) > 0 || ac.proxied {
		sendTransport := ac.transportSnapshot()
		sendTr = sendTransport.transport
		if ac.proxied {
			if sess, ok := localSession(entry); ok {
				sendErr = ac.sendSessionMetaIfChanged(sess, sendTransport, marks.attachmentEffect)
			} else if proxy, ok := entry.(*proxySession); ok {
				meta, metaOK := proxy.sessionMetaSnapshot()
				if !metaOK {
					sendErr = errSessionMetaUnavailable
				} else {
					sendErr = ac.sendSessionMetaSnapshot(meta, sendTransport, marks.attachmentEffect)
				}
			}
		}
		if len(data) > 0 {
			endEmit := marks.span(ports.RuntimeEmitStart, ports.RuntimeEmitEnd, uint64(len(data)))
			if sendErr == nil && sendTr == nil {
				sendErr = errors.New("client transport is nil")
			}
			if sendErr == nil {
				send := sendTr.Send
				if async, ok := sendTr.(ports.AsyncTransport); ok {
					send = async.SendAsync
				}
				interruptible := marks.attachmentEffect != nil && marks.attachmentEffect.beginTransportSend(sendTransport)
				if ac.proxied {
					sendErr = preparedScreen.send(send)
				} else {
					sendErr = preparedANSI.send(data, ac.echoAck.Load(), send)
				}
				if interruptible {
					if sendErr != nil {
						marks.attachmentEffect.reportTransportFailure(sendTransport)
					}
					marks.attachmentEffect.endTransportSend()
				}
			}
			endEmit(uint64(len(data)), sendErr == nil)
			if sendErr == nil && ac.proxied {
				kind := ports.RuntimeScreenDelta
				if preparedScreen.update.Kind == ports.ScreenUpdateSnapshot {
					kind = ports.RuntimeScreenSnapshot
				}
				marks.diagnostic(kind, uint64(len(preparedScreen.data)), uint64(len(preparedScreen.update.Spans)))
			}
		}
	}
	if sendErr == nil {
		if len(data) == 0 {
			if ac.proxied {
				preparedScreen.commitNoSend()
			} else {
				preparedANSI.commitNoSend()
			}
		}
		// Publish only after output preparation and transport emission both
		// succeed. A cross-session transition may publish concurrently, but its
		// mandatory first-paint rebase waits for sendMu and therefore follows this
		// completed output transaction.
		if ac.proxied {
			ac.lastCursor = composed.cursor
		} else {
			ac.lastCursor = cursor.next
		}
		ac.pipelineScratch = ac.pipelineCache
		ac.pipelineCache = composed.cache

		// A successful no-byte emission also commits: its renderer shadow still
		// represents the captured frame. Lock panes only under sendMu and with no
		// session guard held.
		commitDamageReceipts(state.receipts)
		if ac.renderStages.emit != nil {
			ac.renderStages.emit()
		}
	}
	if sendErr != nil {
		ac.discardProxyCapture()
	}
	ac.sendMu.Unlock()
	if sendErr != nil {
		// A transport failure may invalidate the role gate. Release this render's
		// admission first. Detachment freezes the gate and therefore cannot mutate
		// ownership until any enclosing admitted operation has also ended.
		if marks.attachmentEffect == nil {
			if sess, ok := localSession(entry); ok {
				d.detachOnSendError(sess, ac, sendTr)
			} else if proxy, ok := entry.(*proxySession); ok {
				d.detachProxyOnSendError(proxy, ac, sendTr)
			}
		} else {
			// Capture the exact admitted capability, including its coordinator
			// lease, before End permits a replacement publication. Reserve cleanup
			// accounting before End so terminal Wait cannot race a later Add.
			token := marks.attachmentEffect.connectionToken()
			launchCleanup := d.reserveAttachmentSendErrorCleanup(token, sendTr)
			marks.attachmentEffect.End()
			launchCleanup()
		}
	}
	return true
}
