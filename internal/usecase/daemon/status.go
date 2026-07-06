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
	"time"

	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

const (
	attentionGlyph     = ''
	maxMRUSessions     = 9
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

func drawTopBarSnapshot(row []renderer.Cell, status statusSnapshot, frame int, topRight string, styles themeStyles) {
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
	drawRightPlainText(row, topRight, x, styles.statusBar)
}

func drawStatusBarState(row []renderer.Cell, state barState, styles themeStyles) {
	clearStatusRow(row)
	x := 0
	writeStatusText(row, &x, " "+state.status.session+" ", styles.accent)

	rightText := composeBottomRightText(state.bottomRight, state.copyFeedback)
	fittedMRU := fitMRU(state.mru, len(row), x, rightText)
	for i, sess := range fittedMRU {
		style := mruStyle(styles.statusBar, state.theme, i, len(fittedMRU))
		name := sess.name
		if sess.ephemeral {
			name += "*"
		}
		writeStatusText(row, &x, " "+name, style)
		if sess.attention {
			writeStatusText(row, &x, " ", style)
			writeBell(row, &x, state.attentionFrame)
		}
		writeStatusText(row, &x, " ", style)
	}
	drawRightPlainText(row, rightText, x, styles.statusBar)
}

func composeBottomRightText(scriptText, copyFeedback string) string {
	if scriptText == "" {
		return copyFeedback
	}
	if copyFeedback == "" {
		return scriptText
	}
	return scriptText + " " + copyFeedback
}

func drawRightPlainText(row []renderer.Cell, text string, reservedLeft int, style renderer.Style) {
	textWidth := statusTextWidth(text)
	if text == "" || textWidth+1+reservedLeft > len(row) {
		return
	}
	x := len(row) - textWidth - 1
	writeStatusText(row, &x, " "+text, style)
}

func statusTextWidth(text string) int {
	width := 0
	for _, r := range text {
		width += renderer.RuneWidth(r)
	}
	return width
}

func clearStatusRow(row []renderer.Cell) {
	for i := range row {
		row[i] = renderer.BlankCell()
	}
}

type barState struct {
	status         statusSnapshot
	topRight       string
	bottomRight    string
	copyFeedback   string
	mru            []mruSession
	attentionFrame int
	// theme is the client's terminal theme, if reported. Its zero value
	// (Theme{}, Known: false) is a valid "no theme" default that resolves to
	// the pre-theme fallback styles (see newThemeStyles / theme.usable).
	theme themeui.Theme
}

type mruSession struct {
	id        domain.SessionID
	name      string
	ephemeral bool
	attention bool
	// mruAt orders entries in barStateFor (freshest first); drawing ignores it.
	mruAt uint64
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
	if d != nil {
		state.topRight, state.bottomRight = d.barScriptSnapshot(cur)
	}
	if d == nil {
		return state
	}
	d.mu.Lock()
	mru := make([]mruSession, 0, len(d.sessions))
	for _, sess := range d.sessions {
		if sess == cur {
			continue
		}
		at := sess.mruAt.Load()
		sess.mu.Lock()
		entry := mruSession{id: sess.id, name: sess.name, ephemeral: sess.ephemeral, mruAt: at}
		for _, tb := range sess.tabs {
			if tb.attention {
				entry.attention = true
				break
			}
		}
		sess.mu.Unlock()
		mru = append(mru, entry)
	}
	d.mu.Unlock()
	sort.SliceStable(mru, func(i, j int) bool {
		if mru[i].mruAt == mru[j].mruAt {
			return mru[i].name < mru[j].name
		}
		return mru[i].mruAt > mru[j].mruAt
	})
	if len(mru) > maxMRUSessions {
		mru = mru[:maxMRUSessions]
	}
	state.mru = mru
	return state
}

func fitMRU(entries []mruSession, rowLen, leftUsed int, feedback string) []mruSession {
	// With no feedback, keep one blank trailing cell; with feedback, reserve
	// its " text" width plus a one-cell gap so drawRightPlainText always fits.
	copyReserve := 1
	if feedback != "" {
		copyReserve = statusTextWidth(feedback) + 2
	}
	physicalBudget := rowLen - leftUsed - copyReserve
	if physicalBudget <= 0 || len(entries) == 0 {
		return nil
	}
	cost := func(e mruSession) int {
		n := 2 + len([]rune(e.name))
		if e.ephemeral {
			n++
		}
		if e.attention {
			n += 2
		}
		return n
	}

	budget := physicalBudget
	if feedback == "" {
		budget -= mruFutureRightReserve(rowLen)
		// Keep at least one recent session when it physically fits; the reserved
		// right side is only a budget preference, not a reason to hide all recents.
		if firstCost := cost(entries[0]); firstCost <= physicalBudget && budget < firstCost {
			budget = firstCost
		}
	}
	if budget <= 0 {
		return nil
	}
	used := 0
	for i, e := range entries {
		used += cost(e)
		if used > budget {
			return entries[:i]
		}
	}
	return entries
}

const (
	mruReserveMinRow  = 40
	mruReserveDivisor = 4
	mruReserveMin     = 12
	mruReserveMax     = 24
)

func mruFutureRightReserve(rowLen int) int {
	if rowLen < mruReserveMinRow {
		return 0
	}
	reserve := rowLen / mruReserveDivisor
	if reserve < mruReserveMin {
		return mruReserveMin
	}
	if reserve > mruReserveMax {
		return mruReserveMax
	}
	return reserve
}

func mruStyle(base renderer.Style, t themeui.Theme, i, count int) renderer.Style {
	if count <= 1 || !base.HasForegroundRGB || !base.HasBackgroundRGB || !t.HasBG {
		return base
	}
	amount := (float64(i) / float64(count-1)) * 0.6
	base.ForegroundRGB = themeui.Blend(base.ForegroundRGB, t.Background, amount)
	base.BackgroundRGB = themeui.Blend(base.BackgroundRGB, t.Background, amount)
	return base
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
		width := renderer.RuneWidth(r)
		if width == 0 {
			continue
		}
		if *x >= len(row) || *x+width > len(row) {
			return
		}
		row[*x] = renderer.Cell{Rune: r, Style: style}
		(*x)++
		if width == 2 {
			row[*x] = renderer.Cell{Style: style, Continuation: true}
			(*x)++
		}
	}
}
