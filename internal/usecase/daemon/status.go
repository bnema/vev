// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"sort"
	"strconv"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/palette"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

const (
	maxMRUSessions     = 9
	pulseFrameCount    = 30
	pulseFrameInterval = 120 * time.Millisecond
)

// pulseVisible reports whether the attention bell glyph is showing (as
// opposed to its blank beat) at frame.
func pulseVisible(frame int) bool {
	return frame%pulseFrameCount != 0
}

// pulseStyle returns the style for the attention bell glyph at frame, built
// on top of base so the bell always keeps the caller's background (the tab's
// accent/statusBar, or a faded MRU entry's blended colors) instead of
// punching a default-background hole in a themed bar. On the invisible beat
// it returns (base, false) unchanged.
func pulseStyle(frame int, base renderer.Style) (renderer.Style, bool) {
	if !pulseVisible(frame) {
		return base, false
	}
	f := frame % pulseFrameCount
	peak := pulseFrameCount / 2
	distance := f - peak
	if distance < 0 {
		distance = -distance
	}
	intensity := 1 - float64(distance)/float64(peak)
	return themeui.PulseColor(base, intensity), true
}

func drawTopBarSnapshot(row []renderer.Cell, status statusSnapshot, frame int, topRight string, styles themeui.Styles) {
	clearStatusRow(row, styles.SurfaceBar)
	x := 0
	labels := fitTabLabels(status.tabs, len(row), topRight)
	for i, w := range status.tabs {
		baseStyle := styles.SurfaceInactive
		nameStyle := styles.TabInactive
		titleStyle := styles.TabInactiveTitle
		if w.active {
			baseStyle = styles.SurfaceActive
			nameStyle = styles.TabActive
			titleStyle = styles.TabActiveTitle
		}
		label := labels[i]
		writeStatusText(row, &x, " "+label.text[:label.nameLen], nameStyle)
		if w.attention {
			writeStatusText(row, &x, " ", baseStyle)
			writeBell(row, &x, frame, baseStyle)
		}
		writeStatusText(row, &x, label.text[label.nameLen:], titleStyle)
		writeStatusText(row, &x, " ", baseStyle)
	}
	drawRightPlainText(row, topRight, x, styles.SurfaceBar)
}

func drawStatusBarState(row []renderer.Cell, state barState, styles themeui.Styles) {
	clearStatusRow(row, styles.SurfaceBar)
	x := 0
	rightText := composeBottomRightText(state.bottomRight, state.statusFeedback)
	writeStatusText(row, &x, " "+state.status.session+" ", styles.SurfaceActive)
	if state.rankedRecent != nil {
		for _, sess := range fitRankedRecent(state.rankedRecent, len(row), x, rightText) {
			style := styles.SurfaceInactive // contextual ranks deliberately do not fade.
			if sess.selected {
				style = styles.SurfaceActive
			}
			drawRankedStatusSessionEntry(row, &x, sess, style, state.attentionFrame)
		}
	} else {
		fittedMRU := fitMRU(state.mru, len(row), x, rightText)
		for i, sess := range fittedMRU {
			style := styles.MRUStyle(i, len(fittedMRU))
			drawStatusSessionEntry(row, &x, sess, style, state.attentionFrame)
		}
	}
	drawRightPlainText(row, rightText, x, styles.SurfaceBar)
}

type rankedRecent struct {
	rank      int
	name      string
	kind      recentRouteKind
	ephemeral bool
	attention bool
	selected  bool
}

func drawRankedStatusSessionEntry(row []renderer.Cell, x *int, sess rankedRecent, style renderer.Style, attentionFrame int) {
	prefixStyle := style
	prefixStyle.Bold = true
	writeStatusText(row, x, " "+strconv.Itoa(sess.rank)+":", prefixStyle)
	writeStatusText(row, x, sess.name, style)
	if sess.attention {
		writeStatusText(row, x, " ", style)
		writeBell(row, x, attentionFrame, style)
	}
	writeStatusText(row, x, " ", style)
}

func drawStatusSessionEntry(row []renderer.Cell, x *int, entry recentRouteDisplay, style renderer.Style, attentionFrame int) {
	writeStatusText(row, x, " "+entry.name, style)
	if entry.attention {
		writeStatusText(row, x, " ", style)
		writeBell(row, x, attentionFrame, style)
	}
	writeStatusText(row, x, " ", style)
}

func composeBottomRightText(scriptText, statusFeedback string) string {
	if scriptText == "" {
		return statusFeedback
	}
	if statusFeedback == "" {
		return scriptText
	}
	return scriptText + " " + statusFeedback
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

func clearStatusRow(row []renderer.Cell, style renderer.Style) {
	blank := renderer.Cell{Rune: ' ', Style: style}
	for i := range row {
		row[i] = blank
	}
}

type barState struct {
	status         statusSnapshot
	topRight       string
	bottomRight    string
	statusFeedback string
	mru            []recentRouteDisplay
	rankedRecent   []rankedRecent
	attentionFrame int
	// theme is the client's terminal theme, if reported. Its zero value
	// (Theme{}, Known: false) is a valid "no theme" default that resolves to
	// the pre-theme fallback styles (see themeui.NewStyles / theme.usable).
	theme themeui.Theme
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

func (s *session) statusSegments(includeTerminalTitle bool) statusSnapshot {
	return s.statusSegmentsFor(nil, includeTerminalTitle)
}

func (s *session) statusSegmentsFor(ac *attachedClient, includeTerminalTitle bool) statusSnapshot {
	var routeSnapshot protocol.RecentRouteSnapshot
	if ac != nil {
		routeSnapshot = ac.routeSnapshotCopy()
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	presentation := recentRoutePresentation{name: s.name, ephemeral: s.ephemeral}
	if active, ok := activeRouteEntryForLifecycle(routeSnapshot, s.incarnation); ok {
		presentation.hostLabel = domain.RemoteDisplayOrigin(active.HostLabel)
		if active.Kind == protocol.RouteKindRemote {
			presentation.kind = recentRouteRemote
		}
	}
	name := formatRecentRouteName(presentation, false)
	snap := statusSnapshot{session: name, tabs: make([]statusTab, len(s.tabs))}
	activeIndex := 0
	if ac != nil {
		view := ac.viewSnapshot()
		for i, tb := range s.tabs {
			if domain.TabStableID(tb.stableID) == view.tabID {
				activeIndex = i
				break
			}
		}
	}
	for i, tb := range s.tabs {
		name := tabDisplayName(tb, i)
		active := i == activeIndex
		attention := tb.attention && (!active || tb.attentionVisiblePaint)
		snap.tabs[i] = statusTab{name: name, paneTitle: tb.focusedPaneTitle(includeTerminalTitle), active: active, attention: attention}
	}
	return snap
}

func tabDisplayName(tb *tab, index int) string {
	if tb.name != "" {
		return tb.name
	}
	return strconv.Itoa(index + 1)
}

func rankedRecentForHintsWithSnapshot(hints *palette.ContextualHints, snapshot protocol.RecentRouteSnapshot) []rankedRecent {
	if hints == nil || hints.Kind != command.ContextHintRecentSessions {
		return nil
	}
	var formatted []recentRouteDisplay
	if snapshot.Generation != 0 {
		formatted = formatRecentRouteSnapshot(snapshot)
	}
	entries := make([]rankedRecent, 0, len(hints.Recent))
	for i, hint := range hints.Recent {
		display := recentRouteDisplay{name: hint.Name}
		// The hint and route snapshot are immutable interaction data. The
		// formatted snapshot is authoritative when available, keeping ranked
		// JRS labels identical to normal MRU labels without a live lookup.
		if i < len(formatted) {
			display = formatted[i]
		}
		entries = append(entries, rankedRecent{
			rank:      hint.Rank,
			name:      display.name,
			kind:      display.kind,
			ephemeral: display.ephemeral,
			attention: display.attention,
			selected:  hint.Rank == hints.SelectedRank,
		})
	}
	return entries
}

// barStateForPaletteHints selects snapshot-only contextual composition when a
// palette interaction has captured recent-route hints.
func (d *Daemon) barStateForPaletteHints(cur *session, statusFeedback string, hints *palette.ContextualHints) barState {
	return d.barStateForAttachmentPaletteHints(cur, statusFeedback, hints)
}

// barStateForAttachmentPaletteHints composes daemon-owned chrome for an
// attachment. Bar scripts remain local-session-only because
// their existing execution contract depends on local tabs and PTYs.
func (d *Daemon) barStateForAttachmentPaletteHints(cur *session, statusFeedback string, hints *palette.ContextualHints) barState {
	return d.barStateForAttachmentPaletteHintsFor(cur, nil, statusFeedback, hints, protocol.RecentRouteSnapshot{})
}

func (d *Daemon) barStateForAttachmentPaletteHintsFor(cur *session, ac *attachedClient, statusFeedback string, hints *palette.ContextualHints, capturedRouteSnapshot protocol.RecentRouteSnapshot) barState {
	routeSnapshot := capturedRouteSnapshot
	if routeSnapshot.Generation == 0 && ac != nil {
		routeSnapshot = ac.routeSnapshotCopy()
	}
	ranked := rankedRecentForHintsWithSnapshot(hints, routeSnapshot)
	state := barState{statusFeedback: statusFeedback}
	if d != nil {
		state.attentionFrame = d.attentionFrame()
	}
	if cur != nil {
		includeTerminalTitle := true
		if d != nil {
			includeTerminalTitle = d.currentTabsConfig().TerminalTitle
		}
		state.status = cur.statusSegmentsFor(ac, includeTerminalTitle)
	}
	if cur != nil && d != nil {
		state.topRight, state.bottomRight = d.barScriptSnapshot(cur)
	}
	if ranked != nil {
		state.rankedRecent = ranked
	} else if routeSnapshot.Generation != 0 {
		state.mru = d.formatRecentRouteSnapshotForAttachment(ac, routeSnapshot)
	}
	return state
}

func (d *Daemon) barStateFor(cur *session, statusFeedback string) barState {
	return d.barStateBase(cur, statusFeedback)
}

func (d *Daemon) barStateBase(cur *session, statusFeedback string) barState {
	return d.barStateBaseFor(cur, nil, statusFeedback)
}

func (d *Daemon) barStateBaseFor(cur *session, ac *attachedClient, statusFeedback string) barState {
	state := barState{statusFeedback: statusFeedback}
	if d != nil {
		state.attentionFrame = d.attentionFrame()
	}
	if cur != nil {
		includeTerminalTitle := true
		if d != nil {
			includeTerminalTitle = d.currentTabsConfig().TerminalTitle
		}
		state.status = cur.statusSegmentsFor(ac, includeTerminalTitle)
	}
	if d != nil {
		state.topRight, state.bottomRight = d.barScriptSnapshot(cur)
	}
	return state
}

// fittedTabLabel is one tab's drawable label; text[:nameLen] is the tab-name
// segment, text[nameLen:] the pane-title segment (possibly empty). nameLen is
// always a byte offset landing on a rune boundary, since it is derived from
// the byte length of the (possibly truncated) name prefix.
type fittedTabLabel struct {
	text    string
	nameLen int
}

// fitTabLabels returns, per tab, the text drawn between its surrounding
// spaces (attention glyph handled by the caller), guaranteeing all tabs fit.
func fitTabLabels(tabs []statusTab, rowLen int, rightText string) []fittedTabLabel {
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
			o += 1 + renderer.RuneWidth(ui.AttentionGlyph)
		}
		overhead[i] = o
		widths[i] = statusTextWidth(full[i])
		total += o + widths[i]
	}
	labels := make([]fittedTabLabel, len(tabs))
	if total <= budget {
		for i, t := range tabs {
			labels[i] = fittedTabLabel{text: full[i], nameLen: len(t.name)}
		}
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
			labels[i] = fittedTabLabel{}
		case widths[i] <= textBudget:
			labels[i] = fittedTabLabel{text: full[i], nameLen: len(tabs[i].name)}
		case textBudget >= statusTextWidth(tabs[i].name)+4:
			labels[i] = fittedTabLabel{text: ui.TruncateText(full[i], textBudget), nameLen: len(tabs[i].name)}
		default:
			text := ui.TruncateText(tabs[i].name, textBudget)
			labels[i] = fittedTabLabel{text: text, nameLen: len(text)}
		}
		consumed := overhead[i] + min(widths[i], max(textBudget, 0))
		remaining -= consumed
		remainingTabs--
	}
	return labels
}

func recentRouteDisplayWidth(entry recentRouteDisplay) int {
	width := 2 + statusTextWidth(entry.name)
	if entry.attention {
		width += 1 + renderer.RuneWidth(ui.AttentionGlyph)
	}
	return width
}

func fitWholeRecentEntries[T any](entries []T, budget int, cost func(T) int) []T {
	if budget <= 0 || len(entries) == 0 {
		return nil
	}
	used := 0
	for i, entry := range entries {
		used += cost(entry)
		if used > budget {
			return entries[:i]
		}
	}
	return entries
}

func fitRankedRecent(entries []rankedRecent, rowLen, leftUsed int, feedback string) []rankedRecent {
	reserve := 1
	if feedback != "" {
		reserve = statusTextWidth(feedback) + 2
	}
	budget := rowLen - leftUsed - reserve
	return fitWholeRecentEntries(entries, budget, func(entry rankedRecent) int {
		return recentRouteDisplayWidth(recentRouteDisplay{
			name:      entry.name,
			kind:      entry.kind,
			ephemeral: entry.ephemeral,
			attention: entry.attention,
		}) + statusTextWidth(strconv.Itoa(entry.rank)+":")
	})
}

func fitMRU(entries []recentRouteDisplay, rowLen, leftUsed int, feedback string) []recentRouteDisplay {
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

	budget := physicalBudget
	if feedback == "" {
		budget -= mruFutureRightReserve(rowLen)
		// Keep at least one recent session when it physically fits; the reserved
		// right side is only a budget preference, not a reason to hide all recents.
		if firstCost := recentRouteDisplayWidth(entries[0]); firstCost <= physicalBudget && budget < firstCost {
			budget = firstCost
		}
	}
	return fitWholeRecentEntries(entries, budget, recentRouteDisplayWidth)
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

// writeBell draws the attention glyph on its visible beat, or a plain cell in
// base's style on its blank beat, so the bell never leaves a bare
// default-background hole in a themed bar.
func writeBell(row []renderer.Cell, x *int, frame int, base renderer.Style) {
	style, visible := pulseStyle(frame, base)
	if !visible {
		writeStatusText(row, x, " ", base)
		return
	}
	writeStatusText(row, x, string(ui.AttentionGlyph), style)
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
