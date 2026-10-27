// Package daemon holds vev's server-side session multiplexer use case.
package daemon

import (
	"sort"
	"strconv"
	"time"

	renderer "bnema.dev/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/agentactivity"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/palette"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

const (
	// maxMRUSessions is how many history entries Ctrl+1..9 reach.
	maxMRUSessions     = 9
	pulseFrameCount    = 30
	pulseFrameInterval = 120 * time.Millisecond
)

// statusActivity is what the tab's attention cell shows, by priority.
type statusActivity uint8

const (
	activityNone statusActivity = iota
	// activityDone is the pulsing green check for an unseen completed agent
	// turn.
	activityDone
	// activityWorking is the agent spinner.
	activityWorking
	// activityBell is the pulsing bell: attention or a Blocked agent.
	activityBell
)

// tabActivity selects one cell for a tab: bell/Blocked > Done > Working,
// following agentactivity.Indicator. A Blocked agent keeps the bell pulsing
// while it lasts, even after its attention event was acknowledged. Done is
// never shown for the tab an attachment that may be seen displays, unless it
// comes from a pane that tab does not draw (the caller passes seen false);
// that tab shows its Working spinner instead while another of its agents
// runs.
func tabActivity(bell bool, indicator agentactivity.Indicator, active, seen bool) statusActivity {
	return tabActivityWith(bell, indicator, false, active, seen)
}

// tabActivityWith is tabActivity for a tab whose Done may hide a running
// agent: working reports that one of its panes is Working.
func tabActivityWith(bell bool, indicator agentactivity.Indicator, working, active, seen bool) statusActivity {
	switch {
	case bell || indicator == agentactivity.IndicatorBlocked:
		return activityBell
	case indicator == agentactivity.IndicatorDone && (!active || !seen):
		return activityDone
	case indicator == agentactivity.IndicatorWorking, indicator == agentactivity.IndicatorDone && working:
		return activityWorking
	}
	return activityNone
}

// pulseVisible reports whether the attention bell glyph is showing (as
// opposed to its blank beat) at frame.
func pulseVisible(frame int) bool {
	return frame%pulseFrameCount != 0
}

// pulseIntensity returns the pulse strength at frame, 0 at the blank beat
// and 1 at the peak, and whether the glyph is showing.
func pulseIntensity(frame int) (float64, bool) {
	if !pulseVisible(frame) {
		return 0, false
	}
	f := frame % pulseFrameCount
	peak := pulseFrameCount / 2
	distance := f - peak
	if distance < 0 {
		distance = -distance
	}
	return 1 - float64(distance)/float64(peak), true
}

// pulseStyleFor returns the style for the attention bell glyph at frame, built
// on top of base so the bell always keeps the caller's background (the tab's
// accent/statusBar, or a faded MRU entry's blended colors) instead of
// punching a default-background hole in a themed bar. On the invisible beat
// it returns (base, false) unchanged. reduced selects the attribute-only pulse
// for chrome built by the reduced-color policy.
func pulseStyleFor(frame int, base renderer.Style, reduced bool) (renderer.Style, bool) {
	intensity, visible := pulseIntensity(frame)
	if !visible {
		return base, false
	}
	if reduced {
		return themeui.PulseAttribute(base), true
	}
	return themeui.PulseColor(base, intensity), true
}

// donePulseStyleFor is pulseStyleFor for the agent Done cell: the same beat,
// fading the done color (themeui.Styles.AgentDone) from base's background.
func donePulseStyleFor(frame int, base, done renderer.Style, reduced bool) (renderer.Style, bool) {
	intensity, visible := pulseIntensity(frame)
	if !visible {
		return base, false
	}
	if reduced {
		return themeui.PulseDoneAttribute(base, done), true
	}
	return themeui.PulseDone(base, done, intensity), true
}

// drawTopBarActivity draws the tab bar. frame drives the bell pulse; tick
// drives the spinner, whose frames may be empty (no spinner).
func drawTopBarActivity(row []renderer.Cell, status statusSnapshot, frame int, tick uint64, spinner []rune, glyphs ui.GlyphSet, topRight string, styles themeui.Styles) {
	clearStatusRow(row, styles.SurfaceBar)
	x := 0
	labels := fitTabLabels(status.tabs, len(row), topRight, len(spinner) > 0)
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
		if activity := w.cellActivity(len(spinner) > 0); activity != activityNone {
			writeStatusText(row, &x, " ", baseStyle)
			writeActivityCell(row, &x, activity, frame, tick, spinner, glyphs, baseStyle, styles.AgentDone, styles.Reduced)
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
	cell := sessionCellPaint{frame: state.attentionFrame, tick: state.animTick, spinner: state.spinner, glyphs: state.glyphs, done: styles.AgentDone, reduced: styles.Reduced}
	if state.rankedRecent != nil {
		for _, sess := range fitRankedRecent(state.rankedRecent, len(row), x, rightText) {
			style := styles.SurfaceInactive // contextual ranks deliberately do not fade.
			if sess.selected {
				style = styles.SurfaceActive
			}
			drawRankedStatusSessionEntry(row, &x, sess, style, cell)
		}
	} else {
		fittedMRU := fitMRU(state.mru, len(row), x, rightText, state.showRanks)
		for i, sess := range fittedMRU {
			style := styles.MRUStyle(i, len(fittedMRU))
			if state.showRanks && i < maxMRUSessions {
				drawRankedStatusSessionEntry(row, &x, rankedRecent{rank: i + 1, name: sess.name, attention: sess.attention, activity: sess.activity}, style, cell)
				continue
			}
			drawStatusSessionEntry(row, &x, sess, style, cell)
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
	// activity is the session's agent cell, resolved by sessionEntryActivity.
	activity statusActivity
	selected bool
}

// sessionCellPaint carries what a session entry's attention cell needs: the
// bell pulse frame, the spinner tick and frames, and the attachment's glyphs.
type sessionCellPaint struct {
	frame   int
	tick    uint64
	spinner []rune
	glyphs  ui.GlyphSet
	// done is the agent Done color (themeui.Styles.AgentDone).
	done    renderer.Style
	reduced bool
}

// sessionEntryActivity selects a session entry's agent cell from the
// aggregate indicator of its tabs. Working draws nothing without a spinner.
// Done shows until the agent's tab itself is viewed: seeing the entry does
// not acknowledge it.
func sessionEntryActivity(indicator agentactivity.Indicator, spinner bool) statusActivity {
	activity := tabActivity(false, indicator, false, false)
	if activity == activityWorking && !spinner {
		return activityNone
	}
	return activity
}

// entryCell returns the cell a session entry draws: a plain bell always
// shows, otherwise the resolved agent activity.
func entryCell(attention bool, activity statusActivity) statusActivity {
	if attention {
		return activityBell
	}
	return activity
}

func drawRankedStatusSessionEntry(row []renderer.Cell, x *int, sess rankedRecent, style renderer.Style, cell sessionCellPaint) {
	prefixStyle := style
	prefixStyle.Bold = true
	writeStatusText(row, x, " "+strconv.Itoa(sess.rank)+":", prefixStyle)
	writeStatusText(row, x, sess.name, style)
	writeSessionCell(row, x, entryCell(sess.attention, sess.activity), style, cell)
	writeStatusText(row, x, " ", style)
}

func drawStatusSessionEntry(row []renderer.Cell, x *int, entry recentRouteDisplay, style renderer.Style, cell sessionCellPaint) {
	writeStatusText(row, x, " "+entry.name, style)
	writeSessionCell(row, x, entryCell(entry.attention, entry.activity), style, cell)
	writeStatusText(row, x, " ", style)
}

// writeSessionCell draws " " and the cell after a session name, or nothing.
func writeSessionCell(row []renderer.Cell, x *int, activity statusActivity, style renderer.Style, cell sessionCellPaint) {
	if activity == activityNone {
		return
	}
	writeStatusText(row, x, " ", style)
	writeActivityCell(row, x, activity, cell.frame, cell.tick, cell.spinner, cell.glyphs, style, cell.done, cell.reduced)
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
	// showRanks prefixes each mru entry with its Ctrl+N rank while the
	// client holds Ctrl.
	showRanks      bool
	attentionFrame int
	// animTick is the monotonic animator tick driving the spinner, and
	// spinner the resolved frames (empty: no spinner).
	animTick uint64
	spinner  []rune
	// glyphs is the attachment's resolved ui.glyphs set; the zero value
	// draws Nerd Font glyphs.
	glyphs ui.GlyphSet
	// theme is the client's terminal theme, if reported. Its zero value
	// (Theme{}, Known: false) is a valid "no theme" default that resolves to
	// the pre-theme fallback styles (see themeui.NewStyles / theme.usable).
	theme themeui.Theme
}

type statusSnapshot struct {
	session string
	tabs    []statusTab
	// activeAgentDoneGen is the agent activity generation of the active tab
	// when it carries an unseen completion and the attachment may be seen, or
	// 0. A paint built from this snapshot acknowledges exactly that
	// generation; the scheduler keeps the Done of panes the tab does not draw.
	activeAgentDoneGen uint64
}

type statusTab struct {
	name      string // tab display name (degradation target)
	paneTitle string // formatPaneTitle output of the focused pane
	active    bool
	attention bool
	// activity is the cell selected by tabActivity; attention alone (as
	// built by tests and session-entry callers) still draws the bell.
	activity statusActivity
}

// cellActivity returns the drawn cell. Working draws nothing without a
// spinner.
func (t statusTab) cellActivity(spinner bool) statusActivity {
	activity := t.activity
	if t.attention {
		activity = activityBell
	}
	if activity == activityWorking && !spinner {
		return activityNone
	}
	return activity
}

func (s *session) statusSegments(includeTerminalTitle bool) statusSnapshot {
	return s.statusSegmentsFor(nil, includeTerminalTitle)
}

func (s *session) statusSegmentsFor(ac *attachedClient, includeTerminalTitle bool) statusSnapshot {
	return s.statusSegmentsWithAgents(ac, includeTerminalTitle, true)
}

// statusSegmentsWithAgents builds the tab bar snapshot. agents false ignores
// published agent activity (agents.activity = off) before the scheduler has
// cleared it.
func (s *session) statusSegmentsWithAgents(ac *attachedClient, includeTerminalTitle, agents bool) statusSnapshot {
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
	name := formatRecentRouteName(presentation)
	snap := statusSnapshot{session: name, tabs: make([]statusTab, len(s.tabs))}
	activeIndex := 0
	// An unfocused client leaves the bell of its own tab pending, so it shows
	// it like a background tab's bell.
	seen := ac.terminalFocus().MaySee()
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
		attention := tb.attention && (!active || !seen || tb.attentionVisiblePaint)
		indicator := agentactivity.IndicatorNone
		working := false
		// A Done on a pane the tab does not draw (a hidden floating pane
		// or a collapsed stack member) is unseen even in the viewed tab.
		doneSeen := seen
		if agents {
			if state := tb.agentState.Load(); state != nil {
				indicator = state.indicator
				working = state.working
				doneSeen = seen && !state.doneHidden
				if active && seen && state.done {
					snap.activeAgentDoneGen = state.gen
				}
			}
		}
		activity := tabActivityWith(attention, indicator, working, active, doneSeen)
		snap.tabs[i] = statusTab{name: name, paneTitle: tb.focusedPaneTitle(includeTerminalTitle), active: active, attention: attention, activity: activity}
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
	return rankedRecentForHints(hints, formatHistoryRoutes(snapshot))
}

// formatHistoryRoutes formats the status-bar history of snapshot, or nil
// before the client published one.
func formatHistoryRoutes(snapshot protocol.RecentRouteSnapshot) []recentRouteDisplay {
	if snapshot.Generation == 0 {
		return nil
	}
	return formatRecentRouteSnapshot(historyRouteSnapshot(snapshot))
}

func rankedRecentForHints(hints *palette.ContextualHints, formatted []recentRouteDisplay) []rankedRecent {
	if hints == nil || hints.Kind != command.ContextHintRecentSessions {
		return nil
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
			activity:  display.activity,
			selected:  hint.Rank == hints.SelectedRank,
		})
	}
	return entries
}

// resolveSessionActivity fills each entry's agent cell from the published
// per-session aggregate. A nil lookup leaves every entry without one.
func resolveSessionActivity(entries []recentRouteDisplay, lookup func(domain.SessionLifecycleID) agentactivity.Indicator, spinner bool) {
	if lookup == nil {
		return
	}
	for i := range entries {
		entries[i].activity = sessionEntryActivity(lookup(entries[i].lifecycle), spinner)
	}
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
	state := barState{statusFeedback: statusFeedback}
	d.fillBarAnimation(&state, ac)
	formatted := formatHistoryRoutes(routeSnapshot)
	resolveSessionActivity(formatted, d.agentSessionLookup(), len(state.spinner) > 0)
	ranked := rankedRecentForHints(hints, formatted)
	if cur != nil {
		includeTerminalTitle := true
		if d != nil {
			includeTerminalTitle = d.currentTabsConfig().TerminalTitle
		}
		state.status = cur.statusSegmentsWithAgents(ac, includeTerminalTitle, d == nil || d.currentRuntimeConfig().agents.Activity)
	}
	if cur != nil && d != nil {
		state.topRight, state.bottomRight = d.barScriptSnapshot(cur)
	}
	if ranked != nil {
		state.rankedRecent = ranked
	} else if routeSnapshot.Generation != 0 {
		state.mru = formatted
		state.showRanks = ac != nil && ac.ctrlHeld.Load()
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
	d.fillBarAnimation(&state, ac)
	if cur != nil {
		includeTerminalTitle := true
		if d != nil {
			includeTerminalTitle = d.currentTabsConfig().TerminalTitle
		}
		state.status = cur.statusSegmentsWithAgents(ac, includeTerminalTitle, d == nil || d.currentRuntimeConfig().agents.Activity)
	}
	if d != nil {
		state.topRight, state.bottomRight = d.barScriptSnapshot(cur)
	}
	return state
}

// fillBarAnimation snapshots the animator tick, ac's glyph set, and the
// spinner frames: an explicit agents.spinner, or the glyph set's spinner.
func (d *Daemon) fillBarAnimation(state *barState, ac *attachedClient) {
	if d == nil {
		state.glyphs = ui.NerdGlyphs()
		return
	}
	tick := d.animationTick()
	state.animTick = tick
	state.attentionFrame = int(tick % pulseFrameCount)
	state.glyphs = d.glyphsFor(ac)
	state.spinner = d.spinnerFrames(state.glyphs)
}

// spinnerFrames resolves the Working spinner for glyphs: an explicit
// agents.spinner, or the glyph set's spinner; nil when agent activity is off.
func (d *Daemon) spinnerFrames(glyphs ui.GlyphSet) []rune {
	agents := d.currentRuntimeConfig().agents
	if !agents.Activity {
		return nil
	}
	if agents.SpinnerSet {
		return agents.Spinner
	}
	return glyphs.Spinner
}

// glyphsFor resolves ui.glyphs for ac's outer terminal. Each attachment
// resolves its own set, like its color mode, because the bars are rendered
// for it on the daemon.
func (d *Daemon) glyphsFor(ac *attachedClient) ui.GlyphSet {
	console := ac != nil && ac.terminalCapabilities.Console
	return ui.ResolveGlyphs(d.currentRuntimeConfig().ui.Glyphs, console)
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
func fitTabLabels(tabs []statusTab, rowLen int, rightText string, spinner bool) []fittedTabLabel {
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
		if t.cellActivity(spinner) != activityNone {
			// Every activity glyph is one cell: the bell, the check, and
			// spinner frames of every glyph set or validated by the config
			// parser.
			o += 2
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
	if entryCell(entry.attention, entry.activity) != activityNone {
		// Every activity glyph of every glyph set is one cell.
		width += 2
	}
	return width
}

func fitWholeRecentEntries[T any](entries []T, budget int, cost func(int, T) int) []T {
	if budget <= 0 || len(entries) == 0 {
		return nil
	}
	used := 0
	for i, entry := range entries {
		used += cost(i, entry)
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
	return fitWholeRecentEntries(entries, budget, func(_ int, entry rankedRecent) int {
		return recentRouteDisplayWidth(recentRouteDisplay{
			name:      entry.name,
			kind:      entry.kind,
			ephemeral: entry.ephemeral,
			attention: entry.attention,
			activity:  entry.activity,
		}) + rankPrefixWidth(entry.rank)
	})
}

func rankPrefixWidth(rank int) int {
	return statusTextWidth(strconv.Itoa(rank) + ":")
}

// fitMRU keeps the leading entries that fit. ranked adds the "N:" rank prefix
// of each entry Ctrl+1..9 reaches to its width.
func fitMRU(entries []recentRouteDisplay, rowLen, leftUsed int, feedback string, ranked bool) []recentRouteDisplay {
	cost := func(i int, entry recentRouteDisplay) int {
		width := recentRouteDisplayWidth(entry)
		if ranked && i < maxMRUSessions {
			width += rankPrefixWidth(i + 1)
		}
		return width
	}
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
		if firstCost := cost(0, entries[0]); firstCost <= physicalBudget && budget < firstCost {
			budget = firstCost
		}
	}
	return fitWholeRecentEntries(entries, budget, cost)
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

// writeActivityCell draws one attention cell. The bell and the Done check
// pulse on the same beat; the spinner uses base. Every cell keeps base's
// background, and done supplies only the Done glyph's color.
func writeActivityCell(row []renderer.Cell, x *int, activity statusActivity, frame int, tick uint64, spinner []rune, glyphs ui.GlyphSet, base, done renderer.Style, reduced bool) {
	glyphs = glyphs.OrNerd()
	switch activity {
	case activityBell:
		writeBell(row, x, frame, glyphs.Attention, base, reduced)
	case activityWorking:
		writeStatusCell(row, x, spinnerFrame(spinner, tick), base)
	case activityDone:
		writeDone(row, x, frame, glyphs.Done, base, done, reduced)
	}
}

// spinnerFrame selects the frame for a monotonic animator tick.
func spinnerFrame(frames []rune, tick uint64) rune {
	if len(frames) == 0 {
		return ' '
	}
	return frames[tick%uint64(len(frames))]
}

// writeStatusCell draws r in exactly one cell; a rune of another width is
// replaced by a space so the reserved cell never overflows.
func writeStatusCell(row []renderer.Cell, x *int, r rune, style renderer.Style) {
	if renderer.RuneWidth(r) != 1 {
		r = ' '
	}
	writeStatusText(row, x, string(r), style)
}

// writeBell draws the attention glyph on its visible beat, or a plain cell in
// base's style on its blank beat, so the bell never leaves a bare
// default-background hole in a themed bar.
func writeBell(row []renderer.Cell, x *int, frame int, bell rune, base renderer.Style, reduced bool) {
	style, visible := pulseStyleFor(frame, base, reduced)
	if !visible {
		writeStatusText(row, x, " ", base)
		return
	}
	if bell == 0 {
		bell = ui.AttentionGlyph
	}
	writeStatusCell(row, x, bell, style)
}

// writeDone draws the Done check on its visible beat in the pulsing done
// color, or a plain cell in base's style on its blank beat.
func writeDone(row []renderer.Cell, x *int, frame int, check rune, base, done renderer.Style, reduced bool) {
	style, visible := donePulseStyleFor(frame, base, done, reduced)
	if !visible {
		writeStatusText(row, x, " ", base)
		return
	}
	writeStatusCell(row, x, check, style)
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
