package daemon

import (
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// Terminal focus.
//
// Each attachment carries the focus its client's terminal window last
// reported. It is a lock-free read, so any daemon feature can ask whether a
// user may be looking at an attachment. An attachment that never reports keeps
// unknown focus, which counts as possibly seen, so terminals without focus
// reporting behave exactly as before.
//
// Attention uses it today: only an attachment that may be seen acknowledges a
// bell, so a vev window left open on another workspace no longer clears it for
// every other client. When that window gains focus again, the tab it shows is
// acknowledged by its next paint.

// terminalFocus returns the focus the client last reported.
func (ac *attachedClient) terminalFocus() domain.TerminalFocus {
	if ac == nil {
		return domain.TerminalFocusUnknown
	}
	return domain.TerminalFocus(ac.focus.Load())
}

// setTerminalFocus records a reported focus and reports whether it changed.
func (ac *attachedClient) setTerminalFocus(focus domain.TerminalFocus) bool {
	return domain.TerminalFocus(ac.focus.Swap(uint32(focus))) != focus
}

// applyTerminalFocusForAttachment records a focus report. Gaining focus
// repaints the attachment, whose paint acknowledges the visible tab's bell,
// and the bars of every client, whose bells may clear with it.
func (d *Daemon) applyTerminalFocusForAttachment(effect *attachmentEffect, message protocol.TerminalFocus) {
	if message.Validate() != nil || effect == nil || effect.sess == nil || effect.ac == nil || effect.ac.terminalFocus() == message.Focus {
		return
	}
	if !message.Focus.MaySee() {
		effect.ac.setTerminalFocus(message.Focus)
		return
	}
	// Like a bell rung on a seen tab, the bell a focused window finds on its
	// tab is drawn for one pulse before a paint acknowledges it. Mark it before
	// the focus lets any paint acknowledge.
	effect.sess.markVisibleAttention(effect.ac)
	effect.ac.setTerminalFocus(message.Focus)
	d.invalidateRender(effect.sess, effect.ac, false, "terminal_focus.go")
}

// markVisibleAttention asks for one visible bell pulse on the tab ac shows,
// when that tab is ringing.
func (s *session) markVisibleAttention(ac *attachedClient) {
	view := ac.viewSnapshot()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, tb := range s.tabs {
		if tb != nil && tb.attention && domain.TabStableID(tb.stableID) == view.tabID {
			tb.attentionVisiblePaint = true
			return
		}
	}
}
