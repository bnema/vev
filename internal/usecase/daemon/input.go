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
// Locking: a session's screen and per-client renderer shadow are both guarded
// by tab.mu; the attached-client pointer by session.mu; the registry by
// Daemon.mu. When more than one is held the order is always
// Daemon.mu > session.mu, and (for the transport) attachedClient.sendMu >
// tab.mu — the PTY reader only ever takes tab.mu, so it never blocks on
// a slow client.
package daemon

import (
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/mouse"
)

func (d *Daemon) handleInput(_ *session, ac *attachedClient, data []byte) {
	ac.mouseScan.Scan(data,
		func(ev mouse.Event) { d.handleMouse(ac, ev) },
		func(b []byte) {
			if ac.promptActive() {
				d.handlePromptInput(ac, b)
				return
			}
			if ac.paletteActive() {
				d.handlePaletteInput(ac, b)
				return
			}
			if ac.pickerActive() {
				d.handlePickerInput(ac, b)
				return
			}
			if ac.copyModeActive() {
				d.handleCopyInput(ac, b)
				return
			}
			ac.keys.Route(b)
		},
	)
}

func (d *Daemon) handleMouse(ac *attachedClient, ev mouse.Event) {
	if ac.promptActive() || ac.paletteActive() || ac.pickerActive() {
		return
	}
	sess := ac.currentSession()
	if sess == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}

	if ac.copyModeActive() {
		switch ev.Button {
		case mouse.Left:
			d.copyMouse(sess, ac, ev)
		case mouse.WheelUp:
			d.copyWheel(sess, ac, -3)
		case mouse.WheelDown:
			d.copyWheel(sess, ac, 3)
		}
		return
	}

	tb.mu.Lock()
	childRows := tb.screen.Frame.Height
	mouseMode, mouseSGR := tb.screen.MouseMode()
	altScreen := tb.screen.AltScreenActive()
	scrollbackRows := 0
	if tb.scrollback != nil {
		scrollbackRows = tb.scrollback.Len()
	}
	tb.mu.Unlock()

	if mouseMode != 0 {
		if !mouseSGR || ev.Row >= childRows {
			return
		}
		daemonKeyHandler{d: d, ac: ac}.Forward(ev.Raw)
		return
	}

	switch ev.Button {
	case mouse.Left:
		if altScreen || ev.Row >= childRows {
			return
		}
		switch ev.Type {
		case mouse.Press:
			ac.copyMu.Lock()
			ac.normalMousePressRow = ev.Row
			ac.normalMousePressTop = scrollbackRows
			ac.normalMousePressValid = true
			ac.copyMu.Unlock()
		case mouse.Motion:
			ac.copyMu.Lock()
			pressValid := ac.normalMousePressValid
			pressRow := ac.normalMousePressRow
			pressTop := ac.normalMousePressTop
			ac.copyMu.Unlock()
			if !pressValid {
				return
			}

			tb.mu.Lock()
			snap := scopy.NewSnapshot(tb.scrollback, tb.screen.Frame)
			tb.mu.Unlock()

			ac.copyMu.Lock()
			mode := scopy.NewMode(snap)
			mode.StartSelectionAt(snap, pressTop+pressRow)
			mode.ExtendTo(snap, pressTop+ev.Row)
			ac.copyMode = mode
			ac.copyPressRow = pressTop + pressRow
			ac.copyPressRowValid = true
			ac.copyDragging = true
			ac.normalMousePressValid = false
			ac.copyMu.Unlock()
			d.paint(sess, ac, true)
		case mouse.Release:
			ac.copyMu.Lock()
			ac.normalMousePressValid = false
			ac.copyMu.Unlock()
		}
	case mouse.WheelUp:
		if altScreen {
			daemonKeyHandler{d: d, ac: ac}.Forward([]byte("\x1b[A\x1b[A\x1b[A"))
			return
		}
		d.enterCopyMode(sess, ac)
		d.copyWheel(sess, ac, -3)
	case mouse.WheelDown:
		if altScreen {
			daemonKeyHandler{d: d, ac: ac}.Forward([]byte("\x1b[B\x1b[B\x1b[B"))
		}
	}
}

type daemonKeyHandler struct {
	d  *Daemon
	ac *attachedClient
}

func (h daemonKeyHandler) Forward(data []byte) {
	sess := h.ac.currentSession()
	if sess == nil {
		return
	}
	tb := sess.activeTab()
	if tb == nil {
		return
	}
	if _, err := tb.pty.Write(data); err != nil {
		h.d.log.Error("pty write failed", "err", err, "session", sess.name)
	}
}

func (h daemonKeyHandler) Action(action keys.Action) {
	sess := h.ac.currentSession()
	if sess == nil {
		return
	}
	switch action {
	case keys.ActionOpenPalette:
		h.d.enterPalette(sess, h.ac)
	case keys.ActionSwitchTab1, keys.ActionSwitchTab2, keys.ActionSwitchTab3,
		keys.ActionSwitchTab4, keys.ActionSwitchTab5, keys.ActionSwitchTab6,
		keys.ActionSwitchTab7, keys.ActionSwitchTab8, keys.ActionSwitchTab9:
		idx := int(action - keys.ActionSwitchTab1)
		if sess.switchTab(idx) {
			h.d.paint(sess, h.ac, true)
		}
	}
}
