// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"strconv"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/internal/usecase/ui"
	"github.com/bnema/vev/internal/usecase/visualsearch"
	"github.com/bnema/vev/pkg/renderer"
)

var copySearchModal = ui.Modal{WidthPct: 100, MinWidth: 32, FixedHeight: 11, Title: " Search ", Anchor: domain.AnchorBottom, Margins: ui.Margins{Bottom: 1}}

func copyTargetPane(rt *overlayRuntime) *pane {
	if rt == nil {
		return nil
	}
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	if rt.copyMode == nil {
		return nil
	}
	return rt.copyPane
}

func (d *Daemon) copyWheel(sess *session, ac *attachedClient, delta int) {
	rt := ac.overlays
	rt.copyMu.Lock()
	if rt.copyMode == nil || rt.copyPane == nil || rt.copyDocument == nil {
		rt.copyMu.Unlock()
		return
	}
	if delta > 0 && rt.copyMode.AtBottom() {
		rt.clearCopyModeLocked()
		rt.copyMu.Unlock()
		d.invalidateRender(sess, ac, true, "copymode.go")
		return
	}
	rt.copyMode.MoveRows(delta)
	exit := delta > 0 && rt.copyMode.AtBottom()
	if exit {
		rt.clearCopyModeLocked()
	}
	rt.copyMu.Unlock()
	d.invalidateRender(sess, ac, true, "copymode.go")
}
func (d *Daemon) enterCopyMode(sess *session, ac *attachedClient) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.terminalTargetLocked()
	tb.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	snapshot := scopy.NewSnapshot(p.history, p.screen.Frame)
	p.mu.Unlock()
	cfg := d.currentCopyConfig()
	document := scopy.NewDocument(snapshot, cfg.WordSeparators)
	if !d.publishCopyMode(sess, ac, tb, p, document, nil, nil) {
		return
	}
	d.invalidateRender(sess, ac, true, "copymode.go")
}

// publishCopyMode serializes overlay publication after pane membership is
// revalidated. Document is the sole immutable copy payload.
func (d *Daemon) publishCopyMode(sess *session, ac *attachedClient, tb *tab, p *pane, document *scopy.Document, prepare func(*scopy.Mode), activate func(*overlayRuntime, *scopy.Mode)) bool {
	if sess == nil || ac == nil || tb == nil || p == nil || document == nil {
		return false
	}
	mode := scopy.NewMode(document)
	if prepare != nil {
		prepare(mode)
	}
	rt := ac.overlays
	rt.copyMu.Lock()
	d.stopCopyPendingTimerLocked(ac)
	rt.copyPending = nil
	rt.copyMode = nil
	rt.copyCandidate = mode
	rt.copyDocument = document
	rt.copyPane = p
	rt.copySearch = nil
	rt.copySearchPending = nil
	rt.copyPressRowValid = false
	rt.copyDragging = false
	rt.normalMousePressValid = false
	rt.copyMu.Unlock()
	valid := sess.activeTab() == tb
	if valid {
		tb.mu.Lock()
		valid = tb.panes[p.id] == p || (tb.floating.state == floatingVisible && tb.floating.pane == p)
		tb.mu.Unlock()
	}
	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	if rt.copyCandidate != mode || rt.copyPane != p || rt.copyDocument != document {
		return false
	}
	if !valid {
		rt.clearCopyModeLocked()
		return false
	}
	rt.copyCandidate = nil
	rt.copyMode = mode
	if activate != nil {
		activate(rt, mode)
	}
	return true
}

// copyMouse remains row-compatible until centralized pointer mapping supplies columns.
func (d *Daemon) copyMouse(sess *session, ac *attachedClient, ev mouse.Event) {
	rt := ac.overlays
	if ev.Button != mouse.Left {
		return
	}
	rt.copyMu.Lock()
	if rt.copyMode == nil || rt.copyPane == nil || rt.copyDocument == nil {
		rt.copyMu.Unlock()
		return
	}
	document := rt.copyDocument
	if ev.Row >= document.Height() {
		if ev.Type == mouse.Press {
			rt.copyPressRowValid = false
			rt.copyDragging = false
		}
		rt.copyMu.Unlock()
		return
	}
	pos := scopy.Pos{Row: rt.copyMode.ViewportTop + ev.Row, Col: 0}
	changed := false
	switch ev.Type {
	case mouse.Press:
		if rt.copyMode.SetPosition(pos) {
			changed = true
		}
		rt.copyPressRow = rt.copyMode.Cursor().Row
		rt.copyPressRowValid = true
		rt.copyDragging = false
	case mouse.Motion:
		if rt.copyPressRowValid {
			if !rt.copyDragging {
				rt.copyMode.SetPosition(scopy.Pos{Row: rt.copyPressRow, Col: 0})
				rt.copyMode.ToggleLineSelection()
				rt.copyDragging = true
			}
			changed = rt.copyMode.MoveRows(pos.Row-rt.copyMode.Cursor().Row) || changed
		}
	case mouse.Release:
		rt.copyPressRowValid = false
		rt.copyDragging = false
	}
	rt.copyMu.Unlock()
	if changed {
		d.invalidateRender(sess, ac, true, "copymode.go")
	}
}

func (d *Daemon) handleCopyInput(ac *attachedClient, data []byte) {
	rt := ac.overlays
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	rt.copyMu.Lock()
	if rt.copyMode == nil || rt.copyPane == nil || rt.copyDocument == nil {
		rt.copyPending = nil
		d.stopCopyPendingTimerLocked(ac)
		rt.copyMu.Unlock()
		return
	}
	if len(rt.copyPending) > 0 {
		d.stopCopyPendingTimerLocked(ac)
		data = append(append([]byte(nil), rt.copyPending...), data...)
		rt.copyPending = nil
	}
	if rt.copySearch != nil {
		changed, close, accepted := d.routeCopySearchInputLocked(rt, data)
		rt.copyMu.Unlock()
		if changed || close || accepted {
			d.invalidateRender(sess, ac, true, "copymode.go")
		}
		return
	}
	changed, copyOut, exit := false, false, false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'h':
			rt.copyMode.Left()
			changed = true
		case 'j':
			rt.copyMode.Down()
			changed = true
		case 'k':
			rt.copyMode.Up()
			changed = true
		case 'l':
			rt.copyMode.Right()
			changed = true
		case 'w':
			rt.copyMode.WordNext()
			changed = true
		case 'b':
			rt.copyMode.WordBackward()
			changed = true
		case 'e':
			rt.copyMode.WordEnd()
			changed = true
		case 'g':
			rt.copyMode.Top()
			changed = true
		case 'G':
			rt.copyMode.Bottom()
			changed = true
		case ' ', 'v':
			rt.copyMode.ToggleLineSelection()
			changed = true
		case '/':
			rt.copySearch = visualsearch.New(rt.copyMode.Document().Snapshot())
			rt.copySearchPending = nil
			changed = true
			if i+1 < len(data) {
				a, b, c := d.routeCopySearchInputLocked(rt, data[i+1:])
				changed = changed || a || c
				_ = b
			}
			i = len(data)
		case 'n':
			changed = rt.copyMode.NextSearchMatch(1) || changed
		case 'N':
			changed = rt.copyMode.NextSearchMatch(-1) || changed
		case '\r', '\n', 'y':
			copyOut = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routeCopyEscape(rt.copyMode, tail)
				if ok {
					i += consumed - 1
					changed = true
					continue
				}
				if len(tail) == 1 {
					d.retainCopyESCLocked(ac)
					break
				}
				if isCopyEscapePrefix(tail) {
					rt.copyPending = append(rt.copyPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	text := ""
	if copyOut {
		text = rt.copyMode.SelectedText()
	}
	if exit {
		d.stopCopyPendingTimerLocked(ac)
		rt.clearCopyModeLocked()
	}
	rt.copyMu.Unlock()
	if copyOut && text != "" {
		chunks := scopy.OSC52(text)
		for _, chunk := range chunks {
			failed, err := d.boundedSendOutputErrTransport(ac, chunk)
			if err != nil {
				d.detachOnSendError(sess, ac, failed)
				return
			}
		}
		rt.copyMu.Lock()
		if len(chunks) > 0 {
			rt.copyFeedback = "copied " + strconv.Itoa(len([]rune(text))) + " chars to clipboard"
		} else {
			rt.copyFeedback = "selection too large to copy"
		}
		rt.copyMu.Unlock()
	}
	if exit || changed {
		d.invalidateRender(sess, ac, true, "copymode.go")
	}
}
func (d *Daemon) routeCopySearchInputLocked(rt *overlayRuntime, data []byte) (changed, closeSearch, accepted bool) {
	routeOverlayBytes(data, &rt.copySearchPending, overlayEvents{rune: func(r rune) { rt.copySearch.Insert(r); changed = true }, backspace: func() { rt.copySearch.Backspace(); changed = true }, enter: func() {
		if _, ok := rt.copySearch.Selected(); ok {
			accepted = rt.copyMode.SetSearchMatches(rt.copySearch.Query(), rt.copySearch.Matches(), rt.copySearch.SelectedIndex())
			closeSearch = accepted
		}
	}, cancel: func() { closeSearch = true }, up: func() { rt.copySearch.Up(); changed = true }, down: func() { rt.copySearch.Down(); changed = true }})
	if changed && !closeSearch {
		d.previewCopySearchSelectionLocked(rt)
	}
	if closeSearch {
		rt.copySearch = nil
		rt.copySearchPending = nil
	}
	return
}
func (d *Daemon) previewCopySearchSelectionLocked(rt *overlayRuntime) {
	if rt == nil || rt.copyMode == nil || rt.copySearch == nil {
		return
	}
	if match, ok := rt.copySearch.Selected(); ok {
		rt.copyMode.SetPosition(scopy.Pos{Row: match.Row, Col: match.Start})
	}
}
func (d *Daemon) retainCopyESCLocked(ac *attachedClient) {
	rt := ac.overlays
	mode := rt.copyMode
	rt.copyPending = append(rt.copyPending[:0], keys.ESC)
	rt.copyESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		rt.copyMu.Lock()
		if rt.copyESC.timer != timer {
			rt.copyMu.Unlock()
			return
		}
		rt.copyPending = nil
		rt.copyESC.timer = nil
		rt.copyESC.done = nil
		if rt.copyMode != mode || rt.copyMode == nil {
			rt.copyMu.Unlock()
			return
		}
		rt.clearCopyModeLocked()
		rt.copyMu.Unlock()
		if sess := ac.currentSession(); sess != nil {
			d.invalidateRender(sess, ac, true, "copymode.go")
		}
	})
}
func (d *Daemon) stopCopyPendingTimerLocked(ac *attachedClient) { ac.overlays.copyESC.stop() }
func routeCopyEscape(m *scopy.Mode, data []byte) (int, bool) {
	if len(data) >= 3 && (data[1] == '[' || data[1] == 'O') {
		switch data[2] {
		case 'A':
			m.Up()
			return 3, true
		case 'B':
			m.Down()
			return 3, true
		}
	}
	if len(data) >= 4 && data[1] == '[' && data[3] == '~' {
		switch data[2] {
		case '5':
			m.Page(-1)
			return 4, true
		case '6':
			m.Page(1)
			return 4, true
		}
	}
	return 0, false
}
func isCopyEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && (data[1] == '[' || data[1] == 'O') || len(data) == 3 && data[0] == 0x1b && data[1] == '[' && (data[2] == '5' || data[2] == '6')
}
func composeCopyClientFrame(mode *scopy.Mode, target domain.Rect, frame renderer.Frame, bars barState) (renderer.Frame, []renderer.Damage) {
	if mode == nil || target.Width <= 0 || target.Height <= 0 || frame.Width <= 0 || frame.Height <= 0 {
		return frame, nil
	}
	styles := newThemeStyles(bars.theme)
	copyFrame := mode.Render(styles.copyStatus, styles.selection)
	bodyRows := max(copyFrame.Height-1, 0)
	for y := 0; y < target.Height && y < bodyRows && target.Y+y < frame.Height-1; y++ {
		dstX := max(target.X, 0)
		srcX := max(-target.X, 0)
		width := min(target.Width-srcX, copyFrame.Width-srcX)
		width = min(width, frame.Width-dstX)
		if width > 0 && target.Y+y >= 0 {
			copy(frame.Row(target.Y + y)[dstX:dstX+width], copyFrame.Row(y)[srcX:srcX+width])
		}
	}
	statusY := frame.Height - 1
	for x := range frame.Row(statusY) {
		frame.Row(statusY)[x] = renderer.BlankCell()
	}
	copy(frame.Row(statusY), copyFrame.Row(copyFrame.Height - 1)[:min(frame.Width, copyFrame.Width)])
	return frame, []renderer.Damage{renderer.FullRedraw()}
}
