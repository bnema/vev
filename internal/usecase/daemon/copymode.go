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
	if rt.copyMode == nil || rt.copyPane == nil || rt.copySnapshot == nil {
		rt.copyMu.Unlock()
		return
	}
	document := rt.copySnapshot
	if delta > 0 && rt.copyMode.AtBottom(*document) {
		rt.clearCopyModeLocked()
		rt.copyMu.Unlock()
		d.invalidateRender(sess, ac, true, "copymode.go")
		return
	}
	rt.copyMode.Move(*document, delta)
	exit := delta > 0 && rt.copyMode.AtBottom(*document)
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
	// Capture the live pane under pane.mu; all subsequent copy interaction uses
	// the immutable document while holding only copyMu.
	p.mu.Lock()
	document := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
	p.mu.Unlock()
	if !d.publishCopyMode(sess, ac, tb, p, document, nil) {
		return
	}
	d.invalidateRender(sess, ac, true, "copymode.go")
}

// publishCopyMode installs a non-renderable candidate, validates its captured
// target without overlapping copyMu with session or tab locks, then atomically
// activates that same candidate. A tab transition, pane close, or newer
// publication invalidates the candidate before it can render or be yanked.
func (d *Daemon) publishCopyMode(sess *session, ac *attachedClient, tb *tab, p *pane, document scopy.Snapshot, prepare func(*scopy.Mode)) bool {
	if sess == nil || ac == nil || tb == nil || p == nil {
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
	rt.copySnapshot = &document
	rt.copyPane = p
	rt.copySearch = nil
	rt.copySearchPending = nil
	rt.copyPressRowValid = false
	rt.copyDragging = false
	rt.normalMousePressValid = false
	rt.copyMu.Unlock()

	active := sess.activeTab()
	valid := active == tb
	if valid {
		tb.mu.Lock()
		valid = tb.panes[p.id] == p || (tb.floating.state == floatingVisible && tb.floating.pane == p)
		tb.mu.Unlock()
	}

	rt.copyMu.Lock()
	defer rt.copyMu.Unlock()
	if rt.copyCandidate != mode || rt.copyPane != p || rt.copySnapshot != &document {
		return false
	}
	if !valid {
		rt.clearCopyModeLocked()
		return false
	}
	rt.copyCandidate = nil
	rt.copyMode = mode
	return true
}

func (d *Daemon) copyMouse(sess *session, ac *attachedClient, ev mouse.Event) {
	rt := ac.overlays
	if ev.Button != mouse.Left {
		return
	}
	rt.copyMu.Lock()
	if rt.copyMode == nil || rt.copyPane == nil || rt.copySnapshot == nil {
		rt.copyMu.Unlock()
		return
	}
	document := rt.copySnapshot
	if ev.Row >= document.Height {
		if ev.Type == mouse.Press {
			rt.copyPressRowValid = false
			rt.copyDragging = false
		}
		rt.copyMu.Unlock()
		return
	}
	absRow := rt.copyMode.ViewportTop + ev.Row
	changed := false
	switch ev.Type {
	case mouse.Press:
		rt.copyMode.SetCursor(*document, absRow)
		rt.copyPressRow = rt.copyMode.Cursor
		rt.copyPressRowValid = true
		rt.copyDragging = false
		changed = true
	case mouse.Motion:
		if !rt.copyPressRowValid {
			break
		}
		if !rt.copyDragging {
			rt.copyMode.StartSelectionAt(*document, rt.copyPressRow)
			rt.copyDragging = true
		}
		rt.copyMode.ExtendTo(*document, absRow)
		changed = true
	case mouse.Release:
		// Button release intentionally has no visual effect.
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
	if rt.copyMode == nil || rt.copyPane == nil || rt.copySnapshot == nil {
		rt.copyPending = nil
		d.stopCopyPendingTimerLocked(ac)
		rt.copyMu.Unlock()
		return
	}
	if len(rt.copyPending) > 0 {
		d.stopCopyPendingTimerLocked(ac)
		combined := make([]byte, 0, len(rt.copyPending)+len(data))
		combined = append(combined, rt.copyPending...)
		combined = append(combined, data...)
		data = combined
		rt.copyPending = nil
	}
	document := rt.copySnapshot
	if rt.copySearch != nil {
		changed, closeSearch, accepted := d.routeCopySearchInputLocked(rt, *document, data)
		rt.copyMu.Unlock()
		if changed || closeSearch || accepted {
			d.invalidateRender(sess, ac, true, "copymode.go")
		}
		return
	}
	changed := false
	copyOut := false
	exit := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			rt.copyMode.Move(*document, 1)
			changed = true
		case 'k':
			rt.copyMode.Move(*document, -1)
			changed = true
		case 'g':
			rt.copyMode.Top(*document)
			changed = true
		case 'G':
			rt.copyMode.Bottom(*document)
			changed = true
		case ' ', 'v':
			rt.copyMode.ToggleSelection()
			changed = true
		case '/':
			rt.copySearch = visualsearch.New(*document)
			rt.copySearchPending = nil
			changed = true
			if i+1 < len(data) {
				searchChanged, _, accepted := d.routeCopySearchInputLocked(rt, *document, data[i+1:])
				changed = changed || searchChanged || accepted
			}
			i = len(data)
		case 'n':
			changed = rt.copyMode.NextSearchMatch(*document, 1) || changed
		case 'N':
			changed = rt.copyMode.NextSearchMatch(*document, -1) || changed
		case '\r', '\n', 'y':
			copyOut = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routeCopyEscape(rt.copyMode, *document, tail)
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
		text = rt.copyMode.SelectedText(*document)
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
	if exit {
		d.invalidateRender(sess, ac, true, "copymode.go")
		return
	}
	if changed {
		d.invalidateRender(sess, ac, true, "copymode.go")
	}
}

func (d *Daemon) routeCopySearchInputLocked(rt *overlayRuntime, snap scopy.Snapshot, data []byte) (changed bool, closeSearch bool, accepted bool) {
	routeOverlayBytes(data, &rt.copySearchPending, overlayEvents{
		rune: func(r rune) {
			rt.copySearch.Insert(r)
			changed = true
		},
		backspace: func() {
			rt.copySearch.Backspace()
			changed = true
		},
		enter: func() {
			if _, ok := rt.copySearch.Selected(); ok {
				searchSnap := rt.copySearch.Snapshot()
				accepted = rt.copyMode.SetSearchMatches(searchSnap, rt.copySearch.Query(), rt.copySearch.Matches(), rt.copySearch.SelectedIndex())
				closeSearch = accepted
			}
		},
		cancel: func() { closeSearch = true },
		up: func() {
			rt.copySearch.Up()
			changed = true
		},
		down: func() {
			rt.copySearch.Down()
			changed = true
		},
	})
	if changed && !closeSearch {
		d.previewCopySearchSelectionLocked(rt, snap)
	}
	if closeSearch {
		rt.copySearch = nil
		rt.copySearchPending = nil
	}
	return changed, closeSearch, accepted
}

func (d *Daemon) previewCopySearchSelectionLocked(rt *overlayRuntime, snap scopy.Snapshot) {
	if rt == nil || rt.copyMode == nil || rt.copySearch == nil {
		return
	}
	match, ok := rt.copySearch.Selected()
	if !ok {
		return
	}
	rt.copyMode.SetCursor(snap, match.Row)
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

func (d *Daemon) stopCopyPendingTimerLocked(ac *attachedClient) {
	rt := ac.overlays
	rt.copyESC.stop()
}

func routeCopyEscape(m *scopy.Mode, snap scopy.Snapshot, data []byte) (int, bool) {
	if len(data) >= 3 && (data[1] == '[' || data[1] == 'O') {
		switch data[2] {
		case 'A':
			m.Move(snap, -1)
			return 3, true
		case 'B':
			m.Move(snap, 1)
			return 3, true
		}
	}
	if len(data) >= 4 && data[1] == '[' && data[3] == '~' {
		switch data[2] {
		case '5':
			m.Page(snap, -1)
			return 4, true
		case '6':
			m.Page(snap, 1)
			return 4, true
		}
	}
	return 0, false
}

func isCopyEscapePrefix(data []byte) bool {
	return len(data) == 2 && data[0] == 0x1b && (data[1] == '[' || data[1] == 'O') ||
		len(data) == 3 && data[0] == 0x1b && data[1] == '[' && (data[2] == '5' || data[2] == '6')
}

func composeCopySearchClientFrame(model *visualsearch.Model, base renderer.Frame, styles ...themeStyles) (renderer.Frame, []renderer.Damage) {
	styleSet := resolveThemeStyles(styles)
	return composeModalClientFrame(base, copySearchModal, styleSet, styleSet.selection, model.Render)
}

func composeCopyClientFrame(mode *scopy.Mode, document *scopy.Snapshot, target domain.Rect, frame renderer.Frame, bars barState) (renderer.Frame, []renderer.Damage) {
	if mode == nil || document == nil || target.Width <= 0 || target.Height <= 0 || frame.Width <= 0 || frame.Height <= 0 {
		return frame, nil
	}
	styles := newThemeStyles(bars.theme)
	copyFrame := mode.Render(*document, styles.copyStatus, styles.selection)
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
	statusRow := frame.Row(statusY)
	blank := renderer.BlankCell()
	for x := range statusRow {
		statusRow[x] = blank
	}
	copy(frame.Row(statusY), copyFrame.Row(copyFrame.Height - 1)[:min(frame.Width, copyFrame.Width)])
	return frame, []renderer.Damage{renderer.FullRedraw()}
}
