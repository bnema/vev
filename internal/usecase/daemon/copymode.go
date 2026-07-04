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
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/mouse"
	"github.com/bnema/vev/pkg/renderer"
)

func (d *Daemon) copyWheel(sess *session, ac *attachedClient, delta int) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	ac.copyMu.Lock()
	if ac.copyMode == nil {
		ac.copyMu.Unlock()
		p.mu.Unlock()
		return
	}
	snap := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
	if delta > 0 && ac.copyMode.AtBottom(snap) {
		ac.copyMode = nil
		ac.copyMu.Unlock()
		p.mu.Unlock()
		d.paint(sess, ac, true)
		return
	}
	ac.copyMode.Move(snap, delta)
	exit := delta > 0 && ac.copyMode.AtBottom(snap)
	if exit {
		ac.copyMode = nil
	}
	ac.copyMu.Unlock()
	p.mu.Unlock()

	d.paint(sess, ac, true)
}

func (d *Daemon) enterCopyMode(sess *session, ac *attachedClient) {
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	snap := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
	p.mu.Unlock()
	ac.copyMu.Lock()
	ac.copyMode = scopy.NewMode(snap)
	ac.copyPressRowValid = false
	ac.copyDragging = false
	ac.normalMousePressValid = false
	ac.copyMu.Unlock()
	d.paint(sess, ac, true)
}

func (d *Daemon) copyMouse(sess *session, ac *attachedClient, ev mouse.Event) {
	if ev.Button != mouse.Left {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	if ev.Row >= p.screen.Frame.Height {
		if ev.Type == mouse.Press {
			ac.copyMu.Lock()
			ac.copyPressRowValid = false
			ac.copyDragging = false
			ac.copyMu.Unlock()
		}
		p.mu.Unlock()
		return
	}
	ac.copyMu.Lock()
	if ac.copyMode == nil {
		ac.copyMu.Unlock()
		p.mu.Unlock()
		return
	}
	snap := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
	absRow := ac.copyMode.ViewportTop + ev.Row
	changed := false
	switch ev.Type {
	case mouse.Press:
		ac.copyMode.SetCursor(snap, absRow)
		ac.copyPressRow = ac.copyMode.Cursor
		ac.copyPressRowValid = true
		ac.copyDragging = false
		changed = true
	case mouse.Motion:
		if !ac.copyPressRowValid {
			break
		}
		if !ac.copyDragging {
			ac.copyMode.StartSelectionAt(snap, ac.copyPressRow)
			ac.copyDragging = true
		}
		ac.copyMode.ExtendTo(snap, absRow)
		changed = true
	case mouse.Release:
		// Button release intentionally has no visual effect.
	}
	ac.copyMu.Unlock()
	p.mu.Unlock()

	if changed {
		d.paint(sess, ac, true)
	}
}

func (d *Daemon) handleCopyInput(ac *attachedClient, data []byte) {
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	tb.mu.Lock()
	p := tb.focusedPane()
	tb.mu.Unlock()
	if p == nil {
		return
	}
	p.mu.Lock()
	ac.copyMu.Lock()
	if ac.copyMode == nil {
		ac.copyPending = nil
		d.stopCopyPendingTimerLocked(ac)
		ac.copyMu.Unlock()
		p.mu.Unlock()
		return
	}
	if len(ac.copyPending) > 0 {
		d.stopCopyPendingTimerLocked(ac)
		combined := make([]byte, 0, len(ac.copyPending)+len(data))
		combined = append(combined, ac.copyPending...)
		combined = append(combined, data...)
		data = combined
		ac.copyPending = nil
	}
	snap := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
	changed := false
	copyOut := false
	exit := false
	for i := 0; i < len(data); i++ {
		switch data[i] {
		case 'j':
			ac.copyMode.Move(snap, 1)
			changed = true
		case 'k':
			ac.copyMode.Move(snap, -1)
			changed = true
		case 'g':
			ac.copyMode.Top(snap)
			changed = true
		case 'G':
			ac.copyMode.Bottom(snap)
			changed = true
		case ' ', 'v':
			ac.copyMode.ToggleSelection()
			changed = true
		case '\r', '\n', 'y':
			copyOut = true
			exit = true
		case 'q', 0x03, 0x1b:
			if data[i] == 0x1b {
				tail := data[i:]
				consumed, ok := routeCopyEscape(ac.copyMode, snap, tail)
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
					ac.copyPending = append(ac.copyPending[:0], tail...)
					break
				}
			}
			exit = true
		}
	}
	text := ""
	if copyOut {
		text = ac.copyMode.SelectedText(snap)
	}
	if exit {
		d.stopCopyPendingTimerLocked(ac)
		ac.copyMode = nil
	}
	ac.copyMu.Unlock()
	p.mu.Unlock()

	if copyOut && text != "" {
		chunks := scopy.OSC52(text)
		for _, chunk := range chunks {
			if err := d.boundedSendErr(ac, frameOutput(chunk)); err != nil {
				d.detachOnSendError(sess, ac)
				return
			}
		}
		ac.copyMu.Lock()
		if len(chunks) > 0 {
			ac.copyFeedback = "copied " + strconv.Itoa(len([]rune(text))) + " chars to clipboard"
		} else {
			ac.copyFeedback = "selection too large to copy"
		}
		ac.copyMu.Unlock()
	}
	if exit {
		d.paint(sess, ac, true)
		return
	}
	if changed {
		d.paint(sess, ac, true)
	}
}

func (d *Daemon) retainCopyESCLocked(ac *attachedClient) {
	mode := ac.copyMode
	ac.copyPending = append(ac.copyPending[:0], keys.ESC)
	ac.copyESC.retain(d.clock, keys.ESCDelay, func(timer ports.Timer) {
		ac.copyMu.Lock()
		if ac.copyESC.timer != timer {
			ac.copyMu.Unlock()
			return
		}
		ac.copyPending = nil
		ac.copyESC.timer = nil
		ac.copyESC.done = nil
		if ac.copyMode != mode || ac.copyMode == nil {
			ac.copyMu.Unlock()
			return
		}
		ac.copyMode = nil
		ac.copyMu.Unlock()

		if sess := ac.currentSession(); sess != nil {
			d.paint(sess, ac, true)
		}
	})
}

func (d *Daemon) stopCopyPendingTimerLocked(ac *attachedClient) {
	ac.copyESC.stop()
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

func composeCopyClientFrame(mode *scopy.Mode, tb *tab, bars barState) (renderer.Frame, []renderer.Damage) {
	styles := newThemeStyles(bars.theme)
	p := tb.focusedPane()
	if p == nil {
		return renderer.NewFrame(0, 0), nil
	}
	p.mu.Lock()
	paneFrame := p.screen.Frame
	paneWidth, paneRows := paneFrame.Width, paneFrame.Height
	snap := scopy.NewSnapshot(p.scrollback, paneFrame)
	p.mu.Unlock()
	copyFrame := mode.Render(snap, styles.copyStatus, styles.selection)
	pl, ok := focusedPlacementLocked(tb)
	if !ok {
		pl = layout.Placement{Content: domain.Rect{Width: paneWidth, Height: paneRows}}
	}
	width, screenRows := paneWidth, paneRows
	if len(tb.panes) > 1 && tb.size.Valid() {
		width, screenRows = tb.size.Cols, tb.size.Rows
	}
	frame := renderer.NewFrame(width, screenRows+2)
	drawTopBarSnapshot(frame.Row(0), bars.status, bars.attentionFrame, styles)
	base, _ := composeTabFrame(tb, domain.Rect{Width: width, Height: screenRows}, bars.theme)
	for y := range screenRows {
		copy(frame.Row(y+1), base.Row(y))
	}
	for y := 0; y < pl.Content.Height && y < copyFrame.Height-1; y++ {
		copy(frame.Row(pl.Content.Y + 1 + y)[pl.Content.X:pl.Content.X+min(pl.Content.Width, copyFrame.Width)], copyFrame.Row(y)[:min(pl.Content.Width, copyFrame.Width)])
	}
	statusY := screenRows + 1
	copy(frame.Row(statusY), copyFrame.Row(copyFrame.Height - 1)[:min(width, copyFrame.Width)])
	return frame, []renderer.Damage{renderer.FullRedraw()}
}
