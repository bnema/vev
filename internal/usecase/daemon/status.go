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
	"strconv"

	"github.com/bnema/vev/pkg/renderer"
)

func drawStatus(row []renderer.Cell, sess *session, rightText string, styles ...themeStyles) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
	styleSet := resolveThemeStyles(styles)
	status := sess.statusSegments()
	x := 0
	writeStatusText(row, &x, " "+status.session+" ", styleSet.statusBar)
	for _, w := range status.tabs {
		style := styleSet.statusBar
		if w.active {
			style = styleSet.accent
		}
		writeStatusText(row, &x, " "+w.name+" ", style)
	}
	if rightText == "" {
		return
	}
	style := styleSet.statusBar
	rightWidth := len([]rune(rightText)) + 1
	if len(row)-x-1 < rightWidth {
		return
	}
	x = len(row) - rightWidth
	writeStatusText(row, &x, " "+rightText, style)
}

type statusSnapshot struct {
	session string
	tabs    []statusTab
}

type statusTab struct {
	name   string
	active bool
}

func (s *session) statusSegments() statusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := s.name
	if s.ephemeral {
		name += "*"
	}
	snap := statusSnapshot{session: name, tabs: make([]statusTab, len(s.tabs))}
	for i := range s.tabs {
		name := strconv.Itoa(i + 1)
		snap.tabs[i] = statusTab{name: name, active: i == s.active}
	}
	return snap
}

func writeStatusText(row []renderer.Cell, x *int, text string, style renderer.Style) {
	for _, r := range text {
		if *x >= len(row) {
			return
		}
		row[*x] = renderer.Cell{Rune: r, Style: style}
		(*x)++
	}
}
