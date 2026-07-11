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
// session.mu; the registry by Daemon.mu. History-navigation display state is
// guarded by attachedClient.historyNavMu. When more than one is held the order
// is always attachedClient.sendMu > attachedClient.historyNavMu > Daemon.mu >
// session.mu > tab.mu > pane.mu. The PTY reader only ever takes pane.mu, so it
// never blocks on a slow client.
package daemon

import (
	"sort"
	"strconv"
	"time"

	"github.com/bnema/vev/internal/domain"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
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
	labels := fitTabLabels(status.tabs, len(row), topRight)
	for i, w := range status.tabs {
		style := styles.statusBar
		if w.active {
			style = styles.accent
		}
		writeStatusText(row, &x, " "+labels[i], style)
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
	rightText := composeBottomRightText(state.bottomRight, state.copyFeedback)
	if len(state.history) > 0 {
		drawHistoryNavSessions(row, &x, state.history, rightText, state.attentionFrame, styles)
		drawRightPlainText(row, rightText, x, styles.statusBar)
		return
	}

	writeStatusText(row, &x, " "+state.status.session+" ", styles.accent)

	fittedMRU := fitMRU(state.mru, len(row), x, rightText)
	for i, sess := range fittedMRU {
		style := mruStyle(styles.statusBar, state.theme, i, len(fittedMRU))
		drawStatusSessionEntry(row, &x, sess.name, sess.ephemeral, sess.attention, style, state.attentionFrame)
	}
	drawRightPlainText(row, rightText, x, styles.statusBar)
}

func drawHistoryNavSessions(row []renderer.Cell, x *int, entries []historyNavSession, rightText string, attentionFrame int, styles themeStyles) {
	for _, sess := range fitHistoryNav(entries, len(row), *x, rightText) {
		style := styles.statusBar
		if sess.active {
			style = styles.accent
		}
		drawStatusSessionEntry(row, x, sess.name, sess.ephemeral, sess.attention, style, attentionFrame)
	}
}

func drawStatusSessionEntry(row []renderer.Cell, x *int, name string, ephemeral, attention bool, style renderer.Style, attentionFrame int) {
	if ephemeral {
		name += "*"
	}
	writeStatusText(row, x, " "+name, style)
	if attention {
		writeStatusText(row, x, " ", style)
		writeBell(row, x, attentionFrame)
	}
	writeStatusText(row, x, " ", style)
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
	history        []historyNavSession
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

type historyNavSession struct {
	id        domain.SessionID
	name      string
	ephemeral bool
	attention bool
	active    bool
}

type statusSnapshot struct {
	session string
	tabs    []statusTab
}

type statusTab struct {
	name      string // tab display name (degradation target)
	paneTitle string // formatPaneTitle output of the focused pane
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
		name := tabDisplayName(tb, i)
		active := i == s.active
		attention := tb.attention && (!active || tb.attentionVisiblePaint)
		snap.tabs[i] = statusTab{name: name, paneTitle: tb.focusedPaneTitleLocked(), active: active, attention: attention}
	}
	return snap
}

func tabDisplayName(tb *tab, index int) string {
	if tb.name != "" {
		return tb.name
	}
	return strconv.Itoa(index + 1)
}

func (d *Daemon) barStateForClient(cur *session, ac *attachedClient, copyFeedback string) barState {
	state := d.barStateFor(cur, copyFeedback)
	state.history = d.historyNavBarState(ac)
	return state
}

func (d *Daemon) historyNavBarState(ac *attachedClient) []historyNavSession {
	if d == nil || ac == nil {
		return nil
	}
	ac.historyNavMu.Lock()
	if !ac.historyNav.active() {
		ac.historyNavMu.Unlock()
		return nil
	}
	ids := append([]domain.SessionID(nil), ac.historyNav.ids...)
	activeIndex := ac.historyNav.index
	ac.historyNavMu.Unlock()

	d.mu.Lock()
	out := make([]historyNavSession, 0, len(ids))
	valid := true
	for i, id := range ids {
		sess := d.sessions[id]
		if sess == nil {
			valid = false
			break
		}
		sess.mu.Lock()
		entry := historyNavSession{id: sess.id, name: sess.name, ephemeral: sess.ephemeral, active: i == activeIndex}
		for _, tb := range sess.tabs {
			if tb.attention {
				entry.attention = true
				break
			}
		}
		sess.mu.Unlock()
		out = append(out, entry)
	}
	d.mu.Unlock()
	if !valid {
		d.clearHistoryNav(ac)
		return nil
	}
	return out
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

func fitHistoryNav(entries []historyNavSession, rowLen, leftUsed int, feedback string) []historyNavSession {
	copyReserve := 1
	if feedback != "" {
		copyReserve = statusTextWidth(feedback) + 2
	}
	budget := rowLen - leftUsed - copyReserve
	if budget <= 0 || len(entries) == 0 {
		return nil
	}
	cost := func(e historyNavSession) int {
		name := e.name
		if e.ephemeral {
			name += "*"
		}
		n := 2 + statusTextWidth(name)
		if e.attention {
			n += 1 + renderer.RuneWidth(attentionGlyph)
		}
		return n
	}
	used := 0
	for _, e := range entries {
		used += cost(e)
	}
	if used <= budget {
		return entries
	}
	active := 0
	for i, e := range entries {
		if e.active {
			active = i
			break
		}
	}
	if cost(entries[active]) > budget {
		return nil
	}
	start, end := active, active+1
	used = cost(entries[active])
	for {
		expanded := false
		if start > 0 {
			c := cost(entries[start-1])
			if used+c <= budget {
				start--
				used += c
				expanded = true
			}
		}
		if end < len(entries) {
			c := cost(entries[end])
			if used+c <= budget {
				end++
				used += c
				expanded = true
			}
		}
		if !expanded {
			break
		}
	}
	return entries[start:end]
}

// fitTabLabels returns, per tab, the text drawn between its surrounding
// spaces (attention glyph handled by the caller), guaranteeing all tabs fit.
func fitTabLabels(tabs []statusTab, rowLen int, rightText string) []string {
	reserve := 1
	if rightText != "" {
		reserve = statusTextWidth(rightText) + 2
	}
	budget := rowLen - reserve

	full := make([]string, len(tabs))
	overhead := make([]int, len(tabs))
	widths := make([]int, len(tabs))
	total := 0
	for i, t := range tabs {
		full[i] = composeTabTitle(t.name, t.paneTitle)
		o := 2
		if t.attention {
			o += 1 + renderer.RuneWidth(attentionGlyph)
		}
		overhead[i] = o
		widths[i] = statusTextWidth(full[i])
		total += o + widths[i]
	}
	labels := make([]string, len(tabs))
	if total <= budget {
		copy(labels, full)
		return labels
	}

	// Overflow: greedy water-filling in ascending width order (shortest tabs
	// first) so unused budget from short tabs flows to longer ones, while
	// output order (labels indexed by original tab index) stays stable.
	order := make([]int, len(tabs))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		return widths[order[a]] < widths[order[b]]
	})

	remaining := budget
	remainingTabs := len(tabs)
	for _, i := range order {
		share := remaining / remainingTabs
		textBudget := share - overhead[i]
		switch {
		case textBudget <= 0:
			labels[i] = ""
		case widths[i] <= textBudget:
			labels[i] = full[i]
		case textBudget >= statusTextWidth(tabs[i].name)+4:
			labels[i] = ui.TruncateText(full[i], textBudget)
		default:
			labels[i] = ui.TruncateText(tabs[i].name, textBudget)
		}
		consumed := overhead[i] + min(widths[i], max(textBudget, 0))
		remaining -= consumed
		remainingTabs--
	}
	return labels
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
		name := e.name
		if e.ephemeral {
			name += "*"
		}
		n := 2 + statusTextWidth(name)
		if e.attention {
			n += 1 + renderer.RuneWidth(attentionGlyph)
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
