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
	"sort"
	"strconv"
	"strings"
	"time"

	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	attentionGlyph     = ''
	pulseFrameCount    = 30
	pulseFrameInterval = 120 * time.Millisecond
)

func pulseStyle(frame int) (renderer.Style, bool) {
	f := frame % pulseFrameCount
	if f == 0 {
		return renderer.DefaultStyle(), false
	}
	peak := pulseFrameCount / 2
	distance := f - peak
	if distance < 0 {
		distance = -distance
	}
	intensity := 1 - float64(distance)/float64(peak)
	style := renderer.DefaultStyle()
	style.Bold = true
	style.Foreground = 244 + int(intensity*11)
	return style, true
}

func drawTopBarSnapshot(row []renderer.Cell, status statusSnapshot, frame int, styles themeStyles) {
	clearStatusRow(row)
	x := 0
	for _, w := range status.tabs {
		style := styles.statusBar
		if w.active {
			style = styles.accent
		}
		writeStatusText(row, &x, " "+w.name, style)
		if w.attention {
			writeStatusText(row, &x, " ", style)
			writeBell(row, &x, frame)
		}
		writeStatusText(row, &x, " ", style)
	}
}

func drawStatusBarState(row []renderer.Cell, state barState, styles themeStyles) {
	clearStatusRow(row)
	leftText := " " + state.status.session + " "
	x := 0
	writeStatusText(row, &x, leftText, styles.statusBar)

	reservedLeft := len([]rune(leftText))
	if state.copyFeedback != "" {
		drawRightPlainText(row, state.copyFeedback, reservedLeft, styles.statusBar)
		return
	}
	rightText := state.attentionStackText(len(row), reservedLeft)
	if rightText == "" {
		return
	}
	rightWidth := len([]rune(rightText)) + 1
	x = len(row) - rightWidth
	writeStatusText(row, &x, " ", styles.statusBar)
	writeAttentionText(row, &x, rightText, state.attentionFrame, styles.statusBar)
}

func drawRightPlainText(row []renderer.Cell, text string, reservedLeft int, style renderer.Style) {
	if len([]rune(text))+1+reservedLeft > len(row) {
		return
	}
	x := len(row) - len([]rune(text)) - 1
	writeStatusText(row, &x, " "+text, style)
}

func clearStatusRow(row []renderer.Cell) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
}

type barState struct {
	status         statusSnapshot
	copyFeedback   string
	otherAttention []string
	attentionFrame int
	// theme is the client's terminal theme, if reported. Its zero value
	// (Theme{}, Known: false) is a valid "no theme" default that resolves to
	// the pre-theme fallback styles (see newThemeStyles / theme.usable).
	theme themeui.Theme
}

type statusSnapshot struct {
	session string
	tabs    []statusTab
}

type statusTab struct {
	name      string
	active    bool
	attention bool
}

func (s *session) statusSegments() statusSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	name := s.name
	if s.ephemeral {
		name += "*"
	}
	snap := statusSnapshot{session: name, tabs: make([]statusTab, len(s.tabs))}
	for i, tb := range s.tabs {
		name := strconv.Itoa(i + 1)
		snap.tabs[i] = statusTab{name: name, active: i == s.active, attention: tb.attention}
	}
	return snap
}

func (d *Daemon) barStateFor(cur *session, copyFeedback string) barState {
	state := barState{copyFeedback: copyFeedback}
	if d != nil {
		state.attentionFrame = d.attentionFrame()
	}
	if cur != nil {
		state.status = cur.statusSegments()
	}
	if d == nil || copyFeedback != "" {
		return state
	}
	type attentionSession struct {
		name string
		at   time.Time
	}
	d.mu.Lock()
	attention := make([]attentionSession, 0, len(d.sessions))
	for _, sess := range d.sessions {
		if sess == cur {
			continue
		}
		sess.mu.Lock()
		var oldest time.Time
		found := false
		for _, tb := range sess.tabs {
			if !tb.attention {
				continue
			}
			if !found || tb.attentionAt.Before(oldest) {
				oldest = tb.attentionAt
				found = true
			}
		}
		if found {
			attention = append(attention, attentionSession{name: sess.name, at: oldest})
		}
		sess.mu.Unlock()
	}
	d.mu.Unlock()
	sort.SliceStable(attention, func(i, j int) bool { return attention[i].at.Before(attention[j].at) })
	state.otherAttention = make([]string, len(attention))
	for i := range attention {
		state.otherAttention[i] = attention[i].name
	}
	return state
}

func (s barState) attentionStackText(width, reservedLeft int) string {
	if len(s.otherAttention) == 0 {
		return ""
	}
	parts := make([]string, len(s.otherAttention))
	for i, name := range s.otherAttention {
		parts[i] = string(attentionGlyph) + " " + name
	}
	full := strings.Join(parts, "  ")
	if len([]rune(full))+1+reservedLeft <= width {
		return full
	}
	collapsed := string(attentionGlyph) + " ×" + strconv.Itoa(len(s.otherAttention))
	if len([]rune(collapsed))+1+reservedLeft <= width {
		return collapsed
	}
	return ""
}

func writeAttentionText(row []renderer.Cell, x *int, text string, frame int, style renderer.Style) {
	for _, r := range text {
		if r == attentionGlyph {
			writeBell(row, x, frame)
			continue
		}
		writeStatusText(row, x, string(r), style)
	}
}

func writeBell(row []renderer.Cell, x *int, frame int) {
	style, visible := pulseStyle(frame)
	if !visible {
		writeStatusText(row, x, " ", renderer.DefaultStyle())
		return
	}
	writeStatusText(row, x, string(attentionGlyph), style)
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
