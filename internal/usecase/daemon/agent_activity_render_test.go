package daemon

import (
	"context"
	"fmt"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/terminalcap"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/agentactivity"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

func TestTabActivityPriority(t *testing.T) {
	tests := []struct {
		name      string
		bell      bool
		indicator agentactivity.Indicator
		working   bool
		active    bool
		seen      bool
		want      statusActivity
	}{
		{name: "nothing", want: activityNone},
		{name: "idle agent", indicator: agentactivity.IndicatorIdle, want: activityNone},
		{name: "bell beats working", bell: true, indicator: agentactivity.IndicatorWorking, want: activityBell},
		{name: "blocked pulses without a bell", indicator: agentactivity.IndicatorBlocked, active: true, seen: true, want: activityBell},
		{name: "working", indicator: agentactivity.IndicatorWorking, active: true, seen: true, want: activityWorking},
		{name: "done on background tab", indicator: agentactivity.IndicatorDone, want: activityDone},
		{name: "done hidden on seen active tab", indicator: agentactivity.IndicatorDone, active: true, seen: true, want: activityNone},
		{name: "done on active tab of unfocused window", indicator: agentactivity.IndicatorDone, active: true, want: activityDone},
		{name: "done beats working", indicator: agentactivity.IndicatorDone, working: true, want: activityDone},
		{name: "seen done falls back to working", indicator: agentactivity.IndicatorDone, working: true, active: true, seen: true, want: activityWorking},
		{name: "blocked beats done", indicator: agentactivity.IndicatorBlocked, working: true, want: activityBell},
		{name: "bell beats done", bell: true, indicator: agentactivity.IndicatorDone, want: activityBell},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				require.Equal(t, tt.want, tabActivityWith(tt.bell, tt.indicator, tt.working, tt.active, tt.seen))
			})
		})
	}
}

func TestStatusBarActivityCell(t *testing.T) {
	spinner := domain.DefaultAgentSpinner()
	tests := []struct {
		name    string
		tab     statusTab
		frame   int
		tick    uint64
		spinner []rune
		reduced bool
		want    rune
	}{
		{name: "bell visible beat", tab: statusTab{attention: true}, frame: 1, tick: 1, spinner: spinner, want: ui.AttentionGlyph},
		{name: "bell beats spinner", tab: statusTab{attention: true, activity: activityWorking}, frame: 1, tick: 1, spinner: spinner, want: ui.AttentionGlyph},
		{name: "blocked bell", tab: statusTab{activity: activityBell}, frame: 1, tick: 1, spinner: spinner, want: ui.AttentionGlyph},
		{name: "spinner frame follows tick", tab: statusTab{activity: activityWorking}, tick: 9, spinner: spinner, want: spinner[9%len(spinner)]},
		{name: "done check visible beat", tab: statusTab{activity: activityDone}, frame: 1, spinner: spinner, want: ui.DoneGlyph},
		{name: "done check blank beat", tab: statusTab{activity: activityDone}, frame: 0, spinner: spinner, want: ' '},
		{name: "spinner off keeps check", tab: statusTab{activity: activityDone}, frame: 1, want: ui.DoneGlyph},
		{name: "reduced color done", tab: statusTab{activity: activityDone}, frame: 1, reduced: true, want: ui.DoneGlyph},
		{name: "reduced color spinner", tab: statusTab{activity: activityWorking}, tick: 2, spinner: spinner, reduced: true, want: spinner[2]},
		{name: "circle slice frame", tab: statusTab{activity: activityWorking}, tick: 3, spinner: domain.DefaultAgentSpinner(), want: '\U000f0aa1'},
		{name: "explicit jump frame", tab: statusTab{activity: activityWorking}, tick: 1, spinner: []rune("⢄⢂⢁⡁⡈⡐⡠"), want: '⢂'},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				styles := resolveStyles(nil)
				if tt.reduced {
					styles = themeui.ResolveForColor(themeui.Theme{}, domain.ThemeAccent{}, terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}).Styles
				}
				tt.tab.name = "work"
				row := make([]renderer.Cell, 30)
				drawTopBarActivity(row, statusSnapshot{tabs: []statusTab{tt.tab}}, tt.frame, tt.tick, tt.spinner, ui.GlyphSet{}, "", styles)
				// " work " then the cell.
				require.Equal(t, tt.want, row[6].Rune, rowText(row))
				require.Equal(t, styles.SurfaceInactive.Background, row[6].Style.Background, "the cell keeps the tab background")
			})
		})
	}
}

func TestStatusBarDoneColor(t *testing.T) {
	green := renderer.RGB{R: 0x50, G: 0xc8, B: 0x78}
	theme := themeui.Theme{
		Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8}, Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a},
		HasFG: true, HasBG: true, Known: true, UsePalette: true, PaletteKnown: 1 << 2,
	}
	theme.Palette[2] = green
	resolve := func(mode terminalcap.ColorMode) themeui.Styles {
		return themeui.ResolveForColor(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto}, terminalcap.ColorCapabilities{Mode: mode, Source: terminalcap.SourceDeclared}).Styles
	}
	peak := pulseFrameCount / 2
	tests := []struct {
		name    string
		styles  themeui.Styles
		frame   int
		wantRGB *renderer.RGB
		wantFG  int
	}{
		{name: "truecolor peak is palette green", styles: resolve(terminalcap.TrueColor), frame: peak, wantRGB: &green},
		{name: "truecolor fades from the background", styles: resolve(terminalcap.TrueColor), frame: 1},
		{name: "no theme is indexed green", styles: resolveStyles(nil), frame: peak, wantFG: 2},
		{name: "reduced is ANSI green", styles: resolve(terminalcap.ANSI16), frame: peak, wantFG: 2},
		{name: "monochrome has no color", styles: resolve(terminalcap.Monochrome), frame: peak, wantFG: -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				row := make([]renderer.Cell, 30)
				drawTopBarActivity(row, statusSnapshot{tabs: []statusTab{{name: "work", activity: activityDone}}}, tt.frame, 0, nil, ui.GlyphSet{}, "", tt.styles)
				got := row[6].Style
				require.Equal(t, ui.DoneGlyph, row[6].Rune, rowText(row))
				require.True(t, got.Bold)
				require.Equal(t, tt.styles.SurfaceInactive.HasBackgroundRGB, got.HasBackgroundRGB)
				require.Equal(t, tt.styles.SurfaceInactive.BackgroundRGB, got.BackgroundRGB)
				switch {
				case tt.wantRGB != nil:
					require.True(t, got.HasForegroundRGB)
					require.Equal(t, *tt.wantRGB, got.ForegroundRGB)
				case tt.styles.AgentDone.HasForegroundRGB:
					want := themeui.Blend(tt.styles.SurfaceInactive.BackgroundRGB, green, 1-float64(pulseFrameCount/2-tt.frame)/float64(pulseFrameCount/2))
					require.Equal(t, want, got.ForegroundRGB)
					require.NotEqual(t, green, got.ForegroundRGB)
					require.NotEqual(t, themeui.PulseColor(tt.styles.SurfaceInactive, 1-float64(pulseFrameCount/2-tt.frame)/float64(pulseFrameCount/2)).ForegroundRGB, got.ForegroundRGB, "Done is not the bell color")
				default:
					require.False(t, got.HasForegroundRGB)
					require.Equal(t, tt.wantFG, got.Foreground)
				}
			})
		})
	}
}

func TestStatusBarWorkingWithoutSpinnerDrawsNothing(t *testing.T) {
	row := make([]renderer.Cell, 20)
	drawTopBarActivity(row, statusSnapshot{tabs: []statusTab{{name: "work", activity: activityWorking}}}, 0, 3, nil, ui.GlyphSet{}, "", resolveStyles(nil))
	require.Equal(t, " work", rowText(row)[:5])
	require.Equal(t, ' ', row[6].Rune)
	require.Equal(t, ' ', row[5].Rune)
}

func TestFitTabLabelsReservesActivityCell(t *testing.T) {
	tabs := []statusTab{{name: "alpha", activity: activityWorking}, {name: "beta", activity: activityDone}}
	withCell := fitTabLabels(tabs, 16, "", true)
	without := fitTabLabels([]statusTab{{name: "alpha"}, {name: "beta"}}, 16, "", true)
	width := func(labels []fittedTabLabel) int {
		return statusTextWidth(labels[0].text) + statusTextWidth(labels[1].text)
	}
	require.Equal(t, 9, width(without), "both names fit without activity cells")
	// Budget 15 minus two 2-cell activity reservations and the label gaps.
	require.LessOrEqual(t, width(withCell), 15-2*4)
	noSpinner := fitTabLabels(tabs[:1], 9, "", false)
	require.Equal(t, "alpha", noSpinner[0].text, "Working reserves nothing without a spinner")
}

func TestSpinnerFrameIsMonotonic(t *testing.T) {
	frames := domain.DefaultAgentSpinner()
	for tick := uint64(0); tick < 2*pulseFrameCount; tick++ {
		require.Equal(t, frames[tick%uint64(len(frames))], spinnerFrame(frames, tick))
	}
}

func TestAnimatorActivePredicate(t *testing.T) {
	tests := []struct {
		name    string
		status  agentPaneStatus
		spinner bool
		want    bool
	}{
		{name: "working with spinner", status: agentPaneStatus{state: agentactivity.Working}, spinner: true, want: true},
		{name: "working without spinner", status: agentPaneStatus{state: agentactivity.Working}, want: false},
		{name: "blocked", status: agentPaneStatus{state: agentactivity.Blocked}, want: true},
		{name: "done pulses", status: agentPaneStatus{state: agentactivity.Idle, done: true}, spinner: true, want: true},
		{name: "done pulses without spinner", status: agentPaneStatus{state: agentactivity.Idle, done: true}, want: true},
		{name: "idle", status: agentPaneStatus{state: agentactivity.Idle}, spinner: true, want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, _, _, releases := newManualTabSession(t, 1)
				defer releases[0]()
				if !tt.spinner {
					cfg := domain.Defaults()
					cfg.Agents.Spinner, cfg.Agents.SpinnerSet = []rune{}, true
					d.ApplyConfig(cfg)
				}
				seedAgentStatus(d, sess, sess.tabs[0], sess.tabs[0].focusedPane(), tt.status)
				require.Equal(t, tt.want, d.animatorActive())
			})
		})
	}
}

func TestAttentionAnimatorTicksForWorkingAgentAndParks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, _, sends, releases := newManualTabSession(t, 2)
		defer releases[0]()
		defer releases[1]()
		clk, clkTimers := newSignalClock(t, 8)
		d.clock = clk
		selectTestAttachmentTab(sess, 0)
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

		tb := sess.tabs[1]
		p := tb.focusedPane()
		seedAgentStatus(d, sess, tb, p, agentPaneStatus{state: agentactivity.Working})

		// The animator arms one timer and re-arms it with Reset. It advances
		// the tick before it repaints, so each repaint carries the next tick.
		timer := requireReceived(t, clkTimers, "animator never armed its pulse timer")
		for want := uint64(1); want <= 3; want++ {
			timer.fire()
			_ = mustOutputData(t, sends)
			require.Equal(t, want, d.animationTick())
		}

		// The park repaint follows the reset to frame 0.
		seedAgentStatus(d, sess, tb, p, agentPaneStatus{state: agentactivity.Idle})
		_ = mustOutputData(t, sends)
		require.Equal(t, uint64(0), d.animationTick())
		require.Empty(t, clkTimers, "an idle tab must leave the animator parked")
	})
}

func TestAttentionAnimatorPulsesDoneUntilAcknowledged(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		d, sess, ac, sends, releases := newManualTabSession(t, 2)
		defer releases[0]()
		defer releases[1]()
		clk, clkTimers := newSignalClock(t, 8)
		d.clock = clk
		selectTestAttachmentTab(sess, 0)
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

		// Only a Done check is shown: the animator must tick for it.
		tb := sess.tabs[1]
		seedAgentStatus(d, sess, tb, tb.focusedPane(), agentPaneStatus{state: agentactivity.Idle, done: true})
		timer := requireReceived(t, clkTimers, "animator never armed its pulse timer")
		// Frame 1 is the first visible beat: the check appears.
		timer.fire()
		require.Contains(t, string(mustOutputData(t, sends)), string(ui.DoneGlyph))
		// Indexed green does not fade, so the middle beats repaint nothing new;
		// the animator still ticks through them.
		for want := uint64(2); want < pulseFrameCount; want++ {
			timer.fire()
			synctest.Wait()
			require.Equal(t, want, d.animationTick())
		}
		_ = drainAllFrames(sends)
		// Frame 0 of the next cycle is the blank beat: the check disappears.
		timer.fire()
		require.NotContains(t, string(mustOutputData(t, sends)), string(ui.DoneGlyph))
		require.True(t, d.animatorActive(), "Done keeps the animator running")

		// Viewing the tab acknowledges the Done and parks the animator.
		selectTestAttachmentTab(sess, 1)
		ac.setTerminalFocus(domain.TerminalFocusFocused)
		timer.fire()
		_ = mustOutputData(t, sends)
		d.agents.step(d.daemonNow())
		require.Equal(t, agentactivity.IndicatorIdle, tb.agentIndicator())
		require.False(t, d.animatorActive())
		// The republish pokes the animator, which parks and resets the tick.
		synctest.Wait()
		require.Equal(t, uint64(0), d.animationTick())
	})
}

func TestAnimationTargetsRepaintOnlyAnimatingSessions(t *testing.T) {
	d, sess, _, sends, releases := newManualTabSession(t, 1)
	defer releases[0]()
	other, otherSends := addAgentTestSession(t, d, "other")
	tests := []struct {
		name      string
		seed      func()
		wantAll   bool
		wantSess  []*session
		wantPaint [2]bool
	}{
		{name: "nothing animates", wantPaint: [2]bool{false, false}},
		{name: "working agent repaints its session only", seed: func() {
			seedAgentStatus(d, sess, sess.tabs[0], sess.tabs[0].focusedPane(), agentPaneStatus{state: agentactivity.Working})
		}, wantSess: []*session{sess}, wantPaint: [2]bool{true, false}},
		{name: "a bell repaints every session", seed: func() {
			other.mu.Lock()
			other.tabs[0].attention = true
			other.mu.Unlock()
			// Pulse frame 0 draws the bell blank.
			d.setAttentionFrame(1)
		}, wantAll: true, wantPaint: [2]bool{true, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				if tt.seed != nil {
					tt.seed()
				}
				_ = drainAllFrames(sends)
				_ = drainAllFrames(otherSends)
				targets := d.animationTargets()
				require.Equal(t, tt.wantAll, targets.all)
				if !tt.wantAll {
					require.ElementsMatch(t, tt.wantSess, targets.sessions)
				}
				d.repaintAnimationTargets(targets)
				require.Equal(t, tt.wantPaint[0], len(drainAllFrames(sends)) > 0)
				require.Equal(t, tt.wantPaint[1], len(drainAllFrames(otherSends)) > 0)
			})
		})
	}
}

func TestApplyConfigAgentSpinnerLiveReload(t *testing.T) {
	tests := []struct {
		name     string
		agents   domain.AgentsConfig
		wantCell rune
	}{
		{name: "preset swap", agents: domain.AgentsConfig{Activity: true, Spinner: []rune(`|/-\`), SpinnerSet: true}, wantCell: '|'},
		{name: "spinner off", agents: domain.AgentsConfig{Activity: true, Spinner: []rune{}, SpinnerSet: true}, wantCell: ' '},
		{name: "unset follows the glyph set", agents: domain.AgentsConfig{Activity: true}, wantCell: domain.DefaultAgentSpinner()[0]},
		{name: "activity off hides the spinner", agents: domain.AgentsConfig{Activity: false, Spinner: domain.DefaultAgentSpinner(), SpinnerSet: true}, wantCell: ' '},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, sends, releases := newManualTabSession(t, 2)
				defer releases[0]()
				defer releases[1]()
				selectTestAttachmentTab(sess, 0)
				tb := sess.tabs[1]
				seedAgentStatus(d, sess, tb, tb.focusedPane(), agentPaneStatus{state: agentactivity.Working})
				cfg := domain.Defaults()
				cfg.Agents = tt.agents
				d.ApplyConfig(cfg)
				_ = mustOutputData(t, sends) // ApplyConfig repaints.

				bars := d.barStateBaseFor(sess, ac, "")
				for i := range bars.status.tabs {
					bars.status.tabs[i].paneTitle = ""
				}
				row := make([]renderer.Cell, 40)
				drawTopBarActivity(row, bars.status, bars.attentionFrame, 0, bars.spinner, bars.glyphs, "", resolveStyles(nil))
				// " 1 " then " 2", a gap, and the cell.
				x := statusTextWidth(" 1  2 ")
				require.Equal(t, tt.wantCell, row[x].Rune, rowText(row))
				require.Equal(t, tt.wantCell != ' ', d.animatorActive())
			})
		})
	}
}

func TestPaintAcknowledgesDoneOnShownTab(t *testing.T) {
	tests := []struct {
		name     string
		shownTab int
		focus    domain.TerminalFocus
		wantDone bool
	}{
		{name: "shown tab clears done", shownTab: 1, focus: domain.TerminalFocusFocused, wantDone: false},
		{name: "background tab keeps done", shownTab: 0, focus: domain.TerminalFocusFocused, wantDone: true},
		{name: "unfocused window keeps done", shownTab: 1, focus: domain.TerminalFocusUnfocused, wantDone: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, sends, releases := newManualTabSession(t, 2)
				defer releases[0]()
				defer releases[1]()
				tb := sess.tabs[1]
				seedAgentStatus(d, sess, tb, tb.focusedPane(), agentPaneStatus{state: agentactivity.Idle, done: true})
				selectTestAttachmentTab(sess, tt.shownTab)
				ac.setTerminalFocus(tt.focus)
				_ = drainAllFrames(sends)
				d.setAttentionFrame(1) // a visible pulse beat
				d.paint(sess, ac, true, nil)
				data := string(mustOutputData(t, sends))
				require.Equal(t, agentactivity.IndicatorDone, tb.agentIndicator(), "paint only requests the acknowledgement")
				d.agents.step(d.daemonNow())
				require.Equal(t, tt.wantDone, tb.agentIndicator() == agentactivity.IndicatorDone)
				if tt.shownTab == 0 {
					require.Contains(t, data, string(ui.DoneGlyph))
				}
			})
		})
	}
}

// addAgentTestSession registers a second one-tab session on d with its own
// attached client and returns it with that client's frames.
func addAgentTestSession(t *testing.T, d *Daemon, id domain.SessionID) (*session, chan wire.Envelope) {
	t.Helper()
	pty, release := newBlockingPTY(t)
	t.Cleanup(release)
	_, other, _, sends := newManualSessionWithPTYs(t, pty)
	other.id = id
	d.mu.Lock()
	d.sessions[id] = other
	d.mu.Unlock()
	return other, sends
}

func TestStatusBarGlyphSets(t *testing.T) {
	reduced := themeui.ResolveForColor(themeui.Theme{}, domain.ThemeAccent{}, terminalcap.ColorCapabilities{Mode: terminalcap.ANSI16, Source: terminalcap.SourceDeclared}).Styles
	sets := []ui.GlyphSet{ui.NerdGlyphs(), ui.UnicodeGlyphs(), ui.ASCIIGlyphs()}
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
	for _, glyphs := range sets {
		for _, cell := range cells {
			for _, styles := range []themeui.Styles{resolveStyles(nil), reduced} {
				t.Run(fmt.Sprintf("%d/%s/reduced=%t", glyphs.Mode, cell.name, styles.Reduced), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						row := make([]renderer.Cell, 30)
						status := statusSnapshot{tabs: []statusTab{{name: "work", activity: cell.activity}}}
						// Frame 1 is a visible bell beat; tick 1 the second spinner frame.
						drawTopBarActivity(row, status, 1, 1, glyphs.Spinner, glyphs, "", styles)
						require.Equal(t, want[glyphs.Mode][cell.index], row[6].Rune, rowText(row))
						require.Equal(t, 1, renderer.RuneWidth(row[6].Rune))
						require.Equal(t, ' ', row[7].Rune, "every glyph is one cell")
					})
				})
			}
		}
	}
}

func TestBarGlyphsResolvePerAttachment(t *testing.T) {
	tests := []struct {
		name    string
		mode    domain.GlyphMode
		term    string
		spinner []rune
		want    domain.GlyphMode
		wantSpn []rune
	}{
		{name: "auto linux console is ascii", term: "linux", want: domain.GlyphsASCII, wantSpn: []rune(`|/-\`)},
		{name: "auto vt100 is ascii", term: "vt100", want: domain.GlyphsASCII, wantSpn: []rune(`|/-\`)},
		{name: "auto xterm-256color is nerd", term: "xterm-256color", want: domain.GlyphsNerd, wantSpn: domain.DefaultAgentSpinner()},
		{name: "explicit unicode on xterm", mode: domain.GlyphsUnicode, term: "xterm-256color", want: domain.GlyphsUnicode, wantSpn: []rune("◐◓◑◒")},
		{name: "explicit nerd on the console", mode: domain.GlyphsNerd, term: "linux", want: domain.GlyphsNerd, wantSpn: domain.DefaultAgentSpinner()},
		{name: "explicit spinner wins over ascii", term: "linux", spinner: []rune("☱☲☴☲"), want: domain.GlyphsASCII, wantSpn: []rune("☱☲☴☲")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				d, sess, ac, _, releases := newManualTabSession(t, 1)
				defer releases[0]()
				cfg := domain.Defaults()
				cfg.UI.Glyphs = tt.mode
				if tt.spinner != nil {
					cfg.Agents.Spinner, cfg.Agents.SpinnerSet = tt.spinner, true
				}
				d.ApplyConfig(cfg)
				ac.terminalCapabilities = terminalcap.ResolveTerm(nil, tt.term, terminalcap.ColorCapabilities{})
				bars := d.barStateBaseFor(sess, ac, "")
				require.Equal(t, tt.want, bars.glyphs.Mode)
				require.Equal(t, tt.wantSpn, bars.spinner)
			})
		})
	}
}

func TestBarGlyphsDifferBetweenAttachments(t *testing.T) {
	d, sess, ac, _, releases := newManualTabSession(t, 1)
	defer releases[0]()
	console := &attachedClient{terminalCapabilities: terminalcap.ResolveTerm(nil, "linux", terminalcap.ColorCapabilities{})}
	require.Equal(t, domain.GlyphsNerd, d.barStateBaseFor(sess, ac, "").glyphs.Mode)
	require.Equal(t, domain.GlyphsASCII, d.barStateBaseFor(sess, console, "").glyphs.Mode)

	// Live reload applies to the next bar composition.
	cfg := domain.Defaults()
	cfg.UI.Glyphs = domain.GlyphsUnicode
	d.ApplyConfig(cfg)
	require.Equal(t, domain.GlyphsUnicode, d.barStateBaseFor(sess, ac, "").glyphs.Mode)
	require.Equal(t, domain.GlyphsUnicode, d.barStateBaseFor(sess, console, "").glyphs.Mode)
}

func TestPickerAttentionSuffixFollowsGlyphs(t *testing.T) {
	require.Equal(t, "work !", attentionSuffix("work", ui.ASCIIGlyphs().Attention))
	require.Equal(t, "work "+string(ui.AttentionGlyph), attentionSuffix("work", 0))
}
