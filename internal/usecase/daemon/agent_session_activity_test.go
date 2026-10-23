package daemon

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/agentactivity"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

// viewOtherSession makes ac's bottom bar list other's session entry.
func viewOtherSession(ac *attachedClient, other *session, attention bool) {
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 1,
		Entries: []protocol.RecentRouteEntry{{
			Key: 1, Generation: 1, Name: other.name, Kind: protocol.RouteKindLocal, Visited: true, Attention: attention,
			Target: protocol.ExactSessionTarget{LifecycleID: other.incarnation, SessionName: other.name},
		}},
	})
}

// sessionCellIndex is the cell after " work " and " other ".
var sessionCellIndex = statusTextWidth(" work  other ")

func TestSessionAgentAggregation(t *testing.T) {
	tests := []struct {
		name string
		tabs [2]*agentPaneStatus
		want agentactivity.Indicator
	}{
		{name: "nothing published", want: agentactivity.IndicatorNone},
		{name: "idle is absent", tabs: [2]*agentPaneStatus{{state: agentactivity.Idle}}, want: agentactivity.IndicatorNone},
		{name: "done", tabs: [2]*agentPaneStatus{{state: agentactivity.Idle, done: true}}, want: agentactivity.IndicatorDone},
		{name: "done beats working", tabs: [2]*agentPaneStatus{{state: agentactivity.Idle, done: true}, {state: agentactivity.Working}}, want: agentactivity.IndicatorDone},
		{name: "working beats idle", tabs: [2]*agentPaneStatus{{state: agentactivity.Working}, {state: agentactivity.Idle}}, want: agentactivity.IndicatorWorking},
		{name: "blocked beats working", tabs: [2]*agentPaneStatus{{state: agentactivity.Working}, {state: agentactivity.Blocked}}, want: agentactivity.IndicatorBlocked},
		{name: "blocked beats done", tabs: [2]*agentPaneStatus{{state: agentactivity.Blocked}, {state: agentactivity.Idle, done: true}}, want: agentactivity.IndicatorBlocked},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, _, _, releases := newManualTabSession(t, 2)
			defer releases[0]()
			defer releases[1]()
			for i, status := range tt.tabs {
				if status != nil {
					seedAgentStatus(d, sess, sess.tabs[i], sess.tabs[i].focusedPane(), *status)
				}
			}
			require.Equal(t, tt.want, d.agents.sessionIndicator(sess.incarnation))
		})
	}
}

func TestSessionEntryCellPriority(t *testing.T) {
	tests := []struct {
		name      string
		bell      bool
		indicator agentactivity.Indicator
		spinner   bool
		want      statusActivity
	}{
		{name: "nothing", spinner: true, want: activityNone},
		{name: "idle", indicator: agentactivity.IndicatorIdle, spinner: true, want: activityNone},
		{name: "done", indicator: agentactivity.IndicatorDone, spinner: true, want: activityDone},
		{name: "working", indicator: agentactivity.IndicatorWorking, spinner: true, want: activityWorking},
		{name: "working without spinner", indicator: agentactivity.IndicatorWorking, want: activityNone},
		{name: "blocked", indicator: agentactivity.IndicatorBlocked, want: activityBell},
		{name: "bell beats working", bell: true, indicator: agentactivity.IndicatorWorking, spinner: true, want: activityBell},
		{name: "bell beats done", bell: true, indicator: agentactivity.IndicatorDone, want: activityBell},
		{name: "plain bell", bell: true, want: activityBell},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, entryCell(tt.bell, sessionEntryActivity(tt.indicator, tt.spinner)))
		})
	}
}

func TestBottomBarShowsOtherSessionAgent(t *testing.T) {
	spinner := domain.DefaultAgentSpinner()
	tests := []struct {
		name      string
		status    agentPaneStatus
		attention bool
		want      rune
	}{
		{name: "working spinner", status: agentPaneStatus{state: agentactivity.Working}, want: spinner[1]},
		{name: "blocked bell", status: agentPaneStatus{state: agentactivity.Blocked}, want: ui.AttentionGlyph},
		{name: "done check", status: agentPaneStatus{state: agentactivity.Idle, done: true}, want: ui.DoneGlyph},
		{name: "plain bell beats spinner", status: agentPaneStatus{state: agentactivity.Working}, attention: true, want: ui.AttentionGlyph},
		{name: "idle draws nothing", status: agentPaneStatus{state: agentactivity.Idle}, want: ' '},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 1)
			defer releases[0]()
			other, _ := addAgentTestSession(t, d, "other")
			other.name = "other"
			viewOtherSession(ac, other, tt.attention)
			seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), tt.status)
			// Frame 1 is a visible bell beat; tick 1 the second spinner frame.
			d.setAttentionFrame(1)

			bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})
			row := make([]renderer.Cell, 60)
			drawStatusBarState(row, bars, resolveStyles(nil))
			require.Equal(t, " work  other", rowText(row)[:sessionCellIndex-1], rowText(row))
			require.Equal(t, tt.want, row[sessionCellIndex].Rune, rowText(row))
		})
	}
}

func TestRankedSessionEntryShowsAgent(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, _ := addAgentTestSession(t, d, "other")
	other.name = "other"
	viewOtherSession(ac, other, false)
	seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), agentPaneStatus{state: agentactivity.Idle, done: true})

	hints := recentRouteHints(ac.routeSnapshotCopy(), nil)
	d.setAttentionFrame(1) // a visible pulse beat
	bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", &hints, ac.routeSnapshotCopy())
	require.Len(t, bars.rankedRecent, 1)
	require.Equal(t, activityDone, bars.rankedRecent[0].activity)
	row := make([]renderer.Cell, 60)
	drawStatusBarState(row, bars, resolveStyles(nil))
	x := statusTextWidth(" work  1:other ")
	require.Equal(t, ui.DoneGlyph, row[x].Rune, rowText(row))
}

func TestFitMRUReservesSessionActivityCell(t *testing.T) {
	entries := []recentRouteDisplay{{name: "a", activity: activityWorking}, {name: "b"}}
	// " a X " is 5 cells, " b " 3; with feedback "x" reserving 3, 10 cells fit
	// only the first entry.
	require.Len(t, fitMRU(entries, 10, 0, "x", false), 1)
	require.Len(t, fitMRU([]recentRouteDisplay{{name: "a"}, {name: "b"}}, 10, 0, "x", false), 2)
	ranked := []rankedRecent{{rank: 1, name: "a", activity: activityDone}, {rank: 2, name: "b"}}
	require.Len(t, fitRankedRecent(ranked, 12, 0, ""), 1)
}

func TestAnimatorRepaintsAttachmentViewingAgentSession(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, otherSends := addAgentTestSession(t, d, "other")
	bystander, bystanderSends := addAgentTestSession(t, d, "bystander")
	viewOtherSession(ac, other, false)
	clk := newManualAttentionClock()
	d.clock = clk
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		d.attentionAnimator(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()

	seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), agentPaneStatus{state: agentactivity.Working})
	targets := d.animationTargets()
	require.False(t, targets.all)
	// Compare identities: formatting a live session would race the animator.
	got := map[*session]bool{}
	for _, target := range targets.sessions {
		got[target] = true
	}
	require.Len(t, got, 2)
	require.True(t, got[sess], "the viewer repaints")
	require.True(t, got[other], "the agent's session repaints")
	require.False(t, got[bystander], "the bystander does not")

	timer := clk.nextTimer(t)
	for want := uint64(1); want <= 2; want++ {
		_ = drainAllFrames(bystanderSends)
		timer.fire()
		_ = mustOutputData(t, sends)
		_ = mustOutputData(t, otherSends)
		require.Equal(t, want, d.animationTick())
	}
	require.Empty(t, drainAllFrames(bystanderSends), "an attachment that lists no animating session is not repainted")
}

func TestRepublishRepaintsOtherSessionViewers(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, _ := addAgentTestSession(t, d, "other")
	viewOtherSession(ac, other, false)
	rc := d.attachCoordinator(sess, nil, ac, true)

	tests := []struct {
		name    string
		status  agentPaneStatus
		repaint bool
	}{
		{name: "working repaints the viewer", status: agentPaneStatus{state: agentactivity.Working}, repaint: true},
		{name: "same aggregate does not", status: agentPaneStatus{state: agentactivity.Working}},
		{name: "blocked repaints", status: agentPaneStatus{state: agentactivity.Blocked}, repaint: true},
		{name: "idle clears the entry and repaints", status: agentPaneStatus{state: agentactivity.Idle}, repaint: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			before := rc.burstMetricsSnapshot().invalidations
			seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), tt.status)
			require.Equal(t, tt.repaint, rc.burstMetricsSnapshot().invalidations > before)
		})
	}
}

func TestSessionEntryDoneClearsOnlyWhenTabViewed(t *testing.T) {
	d, sess, ac, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, otherSends := addAgentTestSession(t, d, "other")
	other.name = "other"
	viewOtherSession(ac, other, false)
	tb := other.tabs[0]
	otherAC := testAttachment(other)
	// The agent's own client is unfocused, so nothing has seen the tab.
	otherAC.setTerminalFocus(domain.TerminalFocusUnfocused)
	seedAgentStatus(d, other, tb, tb.focusedPane(), agentPaneStatus{state: agentactivity.Idle, done: true})
	d.setAttentionFrame(1) // a visible pulse beat

	entryCellRune := func() rune {
		bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})
		row := make([]renderer.Cell, 60)
		drawStatusBarState(row, bars, resolveStyles(nil))
		return row[sessionCellIndex].Rune
	}

	// The viewer paints its bar, entry included: Done stays.
	_ = drainAllFrames(sends)
	d.paint(sess, ac, true, nil)
	require.Contains(t, string(mustOutputData(t, sends)), string(ui.DoneGlyph))
	d.agents.step(d.daemonNow())
	require.Equal(t, agentactivity.IndicatorDone, d.agents.sessionIndicator(other.incarnation))
	require.Equal(t, ui.DoneGlyph, entryCellRune())

	// An unfocused paint of the tab does not clear it either.
	_ = drainAllFrames(otherSends)
	d.paint(other, otherAC, true, nil)
	d.agents.step(d.daemonNow())
	require.Equal(t, ui.DoneGlyph, entryCellRune())

	// A paint that may be seen of the agent's tab clears it everywhere.
	otherAC.setTerminalFocus(domain.TerminalFocusFocused)
	d.paint(other, otherAC, true, nil)
	d.agents.step(d.daemonNow())
	require.Equal(t, agentactivity.IndicatorNone, d.agents.sessionIndicator(other.incarnation))
	require.Equal(t, ' ', entryCellRune())
}

func TestSessionEntryGlyphSets(t *testing.T) {
	reduced := themeui.ResolveForColor(themeui.Theme{}, domain.ThemeAccent{}, terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}).Styles
	want := map[domain.GlyphMode][3]rune{
		domain.GlyphsNerd:    {ui.AttentionGlyph, ui.DoneGlyph, '\U000f0a9f'},
		domain.GlyphsUnicode: {'•', '✓', '◓'},
		domain.GlyphsASCII:   {'!', '*', '/'},
	}
	cells := []struct {
		name     string
		activity statusActivity
		index    int
	}{
		{name: "bell", activity: activityBell, index: 0},
		{name: "done", activity: activityDone, index: 1},
		{name: "working", activity: activityWorking, index: 2},
	}
	for _, glyphs := range []ui.GlyphSet{ui.NerdGlyphs(), ui.UnicodeGlyphs(), ui.ASCIIGlyphs()} {
		for _, cell := range cells {
			for _, styles := range []themeui.Styles{resolveStyles(nil), reduced} {
				t.Run(fmt.Sprintf("%d/%s/reduced=%t", glyphs.Mode, cell.name, styles.Reduced), func(t *testing.T) {
					state := barState{
						status:         statusSnapshot{session: "work"},
						mru:            []recentRouteDisplay{{name: "other", activity: cell.activity}},
						attentionFrame: 1, animTick: 1, spinner: glyphs.Spinner, glyphs: glyphs,
					}
					row := make([]renderer.Cell, 60)
					drawStatusBarState(row, state, styles)
					got := row[sessionCellIndex]
					require.Equal(t, want[glyphs.Mode][cell.index], got.Rune, rowText(row))
					require.Equal(t, 1, renderer.RuneWidth(got.Rune))
					require.Equal(t, row[sessionCellIndex-1].Style.Background, got.Style.Background, "the cell keeps the entry background")
				})
			}
		}
	}
}

func TestPickerSessionLabelShowsAgent(t *testing.T) {
	spinner := domain.DefaultAgentSpinner()
	tests := []struct {
		name      string
		glyphs    ui.GlyphSet
		spinner   []rune
		attention bool
		indicator agentactivity.Indicator
		want      string
	}{
		{name: "idle", glyphs: ui.NerdGlyphs(), spinner: spinner, indicator: agentactivity.IndicatorIdle, want: "work"},
		{name: "working", glyphs: ui.NerdGlyphs(), spinner: spinner, indicator: agentactivity.IndicatorWorking, want: "work " + string(spinner[0])},
		{name: "working without spinner", glyphs: ui.NerdGlyphs(), indicator: agentactivity.IndicatorWorking, want: "work"},
		{name: "blocked", glyphs: ui.NerdGlyphs(), spinner: spinner, indicator: agentactivity.IndicatorBlocked, want: "work " + string(ui.AttentionGlyph)},
		{name: "done ascii", glyphs: ui.ASCIIGlyphs(), spinner: ui.ASCIIGlyphs().Spinner, indicator: agentactivity.IndicatorDone, want: "work *"},
		{name: "done nerd is the static bell-check", glyphs: ui.NerdGlyphs(), spinner: spinner, indicator: agentactivity.IndicatorDone, want: "work " + string(ui.DoneGlyph)},
		{name: "bell beats done", glyphs: ui.ASCIIGlyphs(), attention: true, indicator: agentactivity.IndicatorDone, want: "work !"},
		{name: "bell beats working", glyphs: ui.ASCIIGlyphs(), spinner: ui.ASCIIGlyphs().Spinner, attention: true, indicator: agentactivity.IndicatorWorking, want: "work !"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			view := sessionView{name: "work", tabs: []tabView{{id: "t1", name: "1", attention: tt.attention}}}
			got := view.pickerView(newPickerMarks(tt.glyphs, tt.spinner), tt.indicator)
			require.Equal(t, tt.want, got.Name)
		})
	}
}

func TestLocalPickerViewsCarryAgentMark(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, _ := addAgentTestSession(t, d, "other")
	other.name = "other"
	seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), agentPaneStatus{state: agentactivity.Blocked})
	views := d.localPickerViews(sess, ac)
	names := map[string]bool{}
	for _, view := range views.recentLive {
		names[view.Name] = true
	}
	require.True(t, names["other "+string(ui.AttentionGlyph)], "%v", names)
	require.True(t, names["work"], "%v", names)
}

func TestSessionEntryDonePulses(t *testing.T) {
	tests := []struct {
		name  string
		frame int
		want  rune
	}{
		{name: "blank beat", frame: 0, want: ' '},
		{name: "visible beat", frame: 1, want: ui.DoneGlyph},
		{name: "peak", frame: pulseFrameCount / 2, want: ui.DoneGlyph},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 1)
			defer releases[0]()
			other, _ := addAgentTestSession(t, d, "other")
			other.name = "other"
			viewOtherSession(ac, other, false)
			seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), agentPaneStatus{state: agentactivity.Idle, done: true})
			d.setAttentionFrame(tt.frame)

			styles := resolveStyles(nil)
			bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})
			row := make([]renderer.Cell, 60)
			drawStatusBarState(row, bars, styles)
			got := row[sessionCellIndex]
			require.Equal(t, tt.want, got.Rune, rowText(row))
			require.Equal(t, row[sessionCellIndex-1].Style.Background, got.Style.Background, "the cell keeps the entry background")
			if tt.want != ' ' {
				require.Equal(t, styles.AgentDone.Foreground, got.Style.Foreground, "indexed green without a theme")
				require.True(t, got.Style.Bold)
			}
		})
	}
}

func TestAnimatorTicksForDoneInListedSession(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, _ := addAgentTestSession(t, d, "other")
	bystander, _ := addAgentTestSession(t, d, "bystander")
	viewOtherSession(ac, other, false)
	seedAgentStatus(d, other, other.tabs[0], other.tabs[0].focusedPane(), agentPaneStatus{state: agentactivity.Idle, done: true})

	require.True(t, d.animatorActive(), "Done alone keeps the animator ticking")
	targets := d.animationTargets()
	require.False(t, targets.all)
	require.ElementsMatch(t, []*session{sess, other}, targets.sessions)
	require.NotContains(t, targets.sessions, bystander)
}

func TestSessionEntryDoneBeatsWorkingTabs(t *testing.T) {
	tests := []struct {
		name  string
		frame int
		want  rune
	}{
		{name: "visible beat shows the check", frame: pulseFrameCount / 2, want: ui.DoneGlyph},
		{name: "blank beat hides it", frame: 0, want: ' '},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _, releases := newManualTabSession(t, 1)
			defer releases[0]()
			other := newJumpTestSession(t, d, "other", 3)
			other.name = "other"
			viewOtherSession(ac, other, false)
			for i, status := range []agentPaneStatus{{state: agentactivity.Working}, {state: agentactivity.Idle, done: true}, {state: agentactivity.Working}} {
				seedAgentStatus(d, other, other.tabs[i], other.tabs[i].focusedPane(), status)
			}
			require.Equal(t, agentactivity.IndicatorDone, d.agents.sessionIndicator(other.incarnation))
			d.setAttentionFrame(tt.frame)

			styles := resolveStyles(nil)
			bars := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})
			row := make([]renderer.Cell, 60)
			drawStatusBarState(row, bars, styles)
			got := row[sessionCellIndex]
			require.Equal(t, tt.want, got.Rune, rowText(row))
			if tt.want != ' ' {
				require.Equal(t, styles.AgentDone.Foreground, got.Style.Foreground, "green Done")
				require.True(t, got.Style.Bold)
			}
		})
	}
}
