package daemon

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/pkg/renderer"
)

// --- test doubles -----------------------------------------------------------

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestStatusCompositionGolden(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	sess.active = 1
	sess.name = "work"

	win := sess.activeTab()
	win.focusedPane().screen.Resize(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	win.focusedPane().screen.Write([]byte("hello"))

	frame, damage := composeClientFrame(sess, win, true, "")

	require.Equal(t, 12, frame.Width)
	require.Equal(t, 4, frame.Height)
	require.Equal(t, " 1  2       ", rowText(frame.Row(0)))
	require.Equal(t, "hello       ", rowText(frame.Row(1)))
	require.Equal(t, " work       ", rowText(frame.Row(3)))
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, damage)
	for i, c := range frame.Row(0) {
		if i >= len(" 1 ") && i < len(" 1  2 ") {
			require.True(t, c.Style.Inverse, "active tab segment cell %d should be inverse", i)
		}
	}
}

func TestStatusCompositionUsesTruecolorTheme(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	win := sess.activeTab()
	win.focusedPane().screen.Resize(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	ac.setTheme(themeui.Theme{
		Foreground: renderer.RGB{R: 220, G: 220, B: 220},
		Background: renderer.RGB{R: 10, G: 20, B: 30},
		HasFG:      true,
		HasBG:      true,
		TrueColor:  true,
		Known:      true,
	})

	bars := barState{status: sess.statusSegments(), theme: ac.getTheme()}
	frame, damage := composeClientFrameWithState(bars, win, true)
	out, err := renderer.New(renderer.Capabilities{}).Draw(frame, damage)

	require.NoError(t, err)
	require.Contains(t, string(out), ";48;2;")
}

func TestStatusApplyThemeStoresClientAndPropagatesScreens(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	msg := ports.Theme{HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3}, HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6}, SchemeKnown: true, Light: true}

	d.applyTheme(sess, ac, msg)

	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, ac.getTheme().Foreground)
	require.True(t, ac.getTheme().SchemeKnown)
	require.True(t, ac.getTheme().Light)
	assertSessionDefaultColors(t, sess, renderer.RGB{R: 1, G: 2, B: 3}, renderer.RGB{R: 4, G: 5, B: 6})
	assertSessionColorScheme(t, sess, true)
}

func TestApplyThemeForcedBuiltinThemePropagatesToChromeAndPanes(t *testing.T) {
	tests := []struct {
		name string
		mode domain.ThemeMode
		want themeui.Theme
	}{
		{name: "dark", mode: domain.ThemeDark, want: themeui.BuiltinDark},
		{name: "light", mode: domain.ThemeLight, want: themeui.BuiltinLight},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p1, releasePTY1 := newBlockingPTY(t)
			p2, releasePTY2 := newBlockingPTY(t)
			d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
			defer releasePTY1()
			defer releasePTY2()
			d.ApplyConfig(domain.Config{Theme: tc.mode})

			d.applyTheme(sess, ac, ports.Theme{
				HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
				HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
				TrueColor: true, SchemeKnown: true, Light: !tc.want.Light,
			})

			require.Equal(t, tc.want, ac.getTheme())
			assertSessionDefaultColors(t, sess, tc.want.Foreground, tc.want.Background)
			assertSessionColorScheme(t, sess, tc.want.Light)
		})
	}
}

func TestAttachClientAppliesForcedThemeBeforeMsgTheme(t *testing.T) {
	tests := []struct {
		name string
		mode domain.ThemeMode
		want themeui.Theme
	}{
		{name: "dark", mode: domain.ThemeDark, want: themeui.BuiltinDark},
		{name: "light", mode: domain.ThemeLight, want: themeui.BuiltinLight},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			p, releasePTY := newBlockingPTY(t)
			d, sess, _, _ := newManualSessionWithPTYs(t, p)
			defer releasePTY()
			d.ApplyConfig(domain.Config{Theme: tc.mode})
			for _, tb := range sess.tabs {
				tb.mu.Lock()
				panes := tb.panesSnapshot()
				tb.mu.Unlock()
				for _, p := range panes {
					p.mu.Lock()
					p.screen.SetDefaultColors(renderer.RGB{R: 1, G: 2, B: 3}, renderer.RGB{R: 4, G: 5, B: 6}, true)
					p.mu.Unlock()
				}
			}
			tr, _ := newCapturingTransport(t)

			ac, _ := d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})

			require.Equal(t, tc.want, ac.getTheme())
			assertSessionDefaultColors(t, sess, tc.want.Foreground, tc.want.Background)
			assertSessionColorScheme(t, sess, tc.want.Light)
		})
	}
}

func TestAutoThemeDetachClearsPaneColorScheme(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeAuto})
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true, Light: true,
	})
	assertSessionColorScheme(t, sess, true)

	d.clientGone(sess, ac, ac.transport(), true)

	assertSessionColorSchemeUnknown(t, sess)
}

func TestForcedThemeDetachPreservesBuiltinOnPanes(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark})
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true, Light: true,
	})

	d.clientGone(sess, ac, ac.transport(), true)

	assertSessionDefaultColors(t, sess, themeui.BuiltinDark.Foreground, themeui.BuiltinDark.Background)
	assertSessionColorScheme(t, sess, false)
}

func TestApplyThemeAutoUnknownDoesNotClobberPaneColorScheme(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true, Light: true,
	})
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 7, G: 8, B: 9},
		HasBackground: true, Background: renderer.RGB{R: 10, G: 11, B: 12},
		TrueColor: true,
	})

	assertSessionDefaultColors(t, sess, renderer.RGB{R: 7, G: 8, B: 9}, renderer.RGB{R: 10, G: 11, B: 12})
	assertSessionColorScheme(t, sess, true)
}

func assertSessionDefaultColors(t *testing.T, sess *session, fg, bg renderer.RGB) {
	t.Helper()
	for _, tb := range sess.tabs {
		var got []byte
		tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
		tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
		require.True(t, strings.Contains(string(got), formatOSCColor(fg)), string(got))
		require.True(t, strings.Contains(string(got), formatOSCColor(bg)), string(got))
	}
}

func assertSessionColorScheme(t *testing.T, sess *session, light bool) {
	t.Helper()
	want := "\x1b[?997;1n"
	if light {
		want = "\x1b[?997;2n"
	}
	for _, tb := range sess.tabs {
		var got []byte
		tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
		tb.focusedPane().screen.Write([]byte("\x1b[?996n"))
		require.Equal(t, want, string(got))
	}
}

func assertSessionColorSchemeUnknown(t *testing.T, sess *session) {
	t.Helper()
	for _, tb := range sess.tabs {
		var got []byte
		tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
		tb.focusedPane().screen.Write([]byte("\x1b[?996n"))
		require.Empty(t, got)
	}
}

func formatOSCColor(rgb renderer.RGB) string {
	return fmt.Sprintf("rgb:%02x%02x/%02x%02x/%02x%02x", rgb.R, rgb.R, rgb.G, rgb.G, rgb.B, rgb.B)
}

func TestAttachClientClearsStaleScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})
	tr, _ := newCapturingTransport(t)

	d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})

	var got []byte
	tb := sess.activeTab()
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got)
}

func TestClientGoneResetsScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})

	// Sanity: the tab answers OSC 10/11 while ac is attached.
	tb := sess.activeTab()
	var before []byte
	tb.focusedPane().screen.OnResponse = func(b []byte) { before = append(before, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.NotEmpty(t, before)

	d.clientGone(sess, ac, ac.transport(), true)

	var got []byte
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got, "OSC 10/11 queries must be swallowed once the client that reported these colors is gone")
}

func TestClientGoneResetDoesNotClobberNewlyAttachedClient(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})

	// Simulate the race window inside clientGone/detachOnSendError: the old
	// client has already been detached (detachIfCurrent succeeded), but
	// before resetScreenDefaultColors runs a new client attaches and applies
	// its own theme.
	require.True(t, sess.detachIfCurrent(ac))

	tr, _ := newCapturingTransport(t)
	newAC, _ := d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})
	d.applyTheme(sess, newAC, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 20, G: 21, B: 22},
		HasBackground: true, Background: renderer.RGB{R: 23, G: 24, B: 25},
	})

	// The late reset from the old client's detach path must not wipe the
	// new client's freshly applied colors.
	d.resetScreenDefaultColors(sess)

	tb := sess.activeTab()
	var got []byte
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.NotEmpty(t, got, "a newer client's screen default colors must survive a stale detach's reset")
}

func TestDetachOnSendErrorResetsScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 7, G: 8, B: 9},
		HasBackground: true, Background: renderer.RGB{R: 10, G: 11, B: 12},
	})

	d.detachOnSendError(sess, ac, ac.transport())

	tb := sess.activeTab()
	var got []byte
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got, "OSC 10/11 queries must be swallowed once the client that reported these colors is gone")
}

func TestApplyThemeIgnoresReplacedClient(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, old, _ := newManualSessionWithPTYs(t, p)
	defer release()
	tr, _ := newCapturingTransport(t)
	d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})

	d.applyTheme(sess, old, ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})

	var got []byte
	tb := sess.activeTab()
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got)
}

func TestStatusMarksEphemeralSession(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer release()
	sess.name = "0"
	sess.ephemeral = true
	win := sess.activeTab()
	win.focusedPane().screen.Resize(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}

	frame, _ := composeClientFrame(sess, win, true, "")

	require.Equal(t, " 1          ", rowText(frame.Row(0)))
	require.Equal(t, " 0*         ", rowText(frame.Row(3)))
}

func TestStatusCopyFeedbackRendersOnlyWhenFullyFits(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	sess.name = "work"
	win := sess.activeTab()
	win.focusedPane().screen.Resize(30, 2)
	win.size = domain.Size{Cols: 30, Rows: 2}

	frame, _ := composeClientFrame(sess, win, true, "ok")
	require.Equal(t, " work                       ok", rowText(frame.Row(3)))

	frame, _ = composeClientFrame(sess, win, true, "1234567890123456789")
	require.Equal(t, " work      1234567890123456789", rowText(frame.Row(3)))

	frame, _ = composeClientFrame(sess, win, true, "selection too large to copy")
	require.Equal(t, " work                         ", rowText(frame.Row(3)))
}

func TestAttentionConstants(t *testing.T) {
	require.Equal(t, '', attentionGlyph)
	require.Equal(t, 30, pulseFrameCount)
	require.Equal(t, 120*time.Millisecond, pulseFrameInterval)
}

func TestPulseStyleHidesAtFrameZeroAndRamps(t *testing.T) {
	_, visible := pulseStyle(0)
	require.False(t, visible)

	low, visible := pulseStyle(1)
	require.True(t, visible)
	peak, visible := pulseStyle(pulseFrameCount / 2)
	require.True(t, visible)
	require.Greater(t, peak.Foreground, low.Foreground)
}

func TestTopBarRendersAttentionBell(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	win := sess.activeTab()
	win.focusedPane().screen.Resize(18, 2)
	win.size = domain.Size{Cols: 18, Rows: 2}
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(10, 0)
	sess.mu.Unlock()

	frame, _ := composeClientFrameWithState(barState{status: sess.statusSegments(), attentionFrame: 1}, win, true)

	require.Contains(t, rowText(frame.Row(0)), "2 ")
	for _, c := range frame.Row(0) {
		if c.Rune == attentionGlyph {
			require.True(t, c.Style.Bold)
			return
		}
	}
	t.Fatalf("attention glyph not rendered in top bar: %q", rowText(frame.Row(0)))
}

func TestBarStateForMRUFreshestFirstCapCurrentExcludedAndAttention(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	sess.name = "current"
	sess.mruAt.Store(100)
	for i := 0; i < 10; i++ {
		other := &session{id: domain.SessionID("s" + strconv.Itoa(i)), name: "s" + strconv.Itoa(i), tabs: []*tab{{attention: i == 8}}}
		other.mruAt.Store(uint64(i + 1))
		d.sessions[other.id] = other
	}

	state := d.barStateFor(sess, "")

	require.Len(t, state.mru, 9)
	require.Equal(t, "s9", state.mru[0].name)
	require.Equal(t, "s1", state.mru[8].name)
	for _, got := range state.mru {
		require.NotEqual(t, "current", got.name)
	}
	require.True(t, state.mru[1].attention, "s8 attention should be carried into MRU state")
}

func TestBarStateForMRUZeroTimesUseDeterministicNameOrder(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	for _, name := range []string{"bravo", "alpha", "charlie"} {
		d.sessions[domain.SessionID(name)] = &session{id: domain.SessionID(name), name: name, tabs: []*tab{{}}}
	}

	state := d.barStateFor(sess, "")

	require.GreaterOrEqual(t, len(state.mru), 3)
	require.Equal(t, []string{"alpha", "bravo", "charlie"}, []string{state.mru[0].name, state.mru[1].name, state.mru[2].name})
}

func TestStatusBarRendersMRUNamesEphemeralAndInlineBell(t *testing.T) {
	state := barState{
		status:         statusSnapshot{session: "cur"},
		attentionFrame: 1,
		mru: []mruSession{
			{name: "fresh"},
			{name: "tmp", ephemeral: true, attention: true},
		},
	}
	row := make([]renderer.Cell, 24)

	drawStatusBarState(row, state, resolveThemeStyles(nil))

	require.Equal(t, " cur  fresh tmp*       ", rowText(row))
	for _, c := range row {
		if c.Rune == attentionGlyph {
			require.True(t, c.Style.Bold)
			return
		}
	}
	t.Fatalf("inline bell not rendered: %q", rowText(row))
}

func TestStatusBarMRUGradientTruecolorAndPlainFallback(t *testing.T) {
	theme := themeui.Theme{Foreground: renderer.RGB{R: 200, G: 200, B: 200}, Background: renderer.RGB{R: 20, G: 20, B: 20}, HasFG: true, HasBG: true, TrueColor: true, Known: true}
	styles := newThemeStyles(theme)
	state := barState{status: statusSnapshot{session: "c"}, theme: theme, mru: []mruSession{{name: "a"}, {name: "b"}, {name: "c"}}}
	row := make([]renderer.Cell, 16)

	drawStatusBarState(row, state, styles)

	first := row[3].Style.ForegroundRGB.R
	second := row[5].Style.ForegroundRGB.R
	third := row[7].Style.ForegroundRGB.R
	require.Equal(t, styles.statusBar.ForegroundRGB.R, first)
	require.Greater(t, first, second)
	require.Greater(t, second, third)

	plain := mruStyle(renderer.DefaultStyle(), themeui.Theme{}, 1, 3)
	require.False(t, plain.HasForegroundRGB)
}

func TestStatusBarNarrowRowsDropWholeOldestMRUEntries(t *testing.T) {
	state := barState{status: statusSnapshot{session: "cur"}, mru: []mruSession{{name: "fresh"}, {name: "middle"}, {name: "old"}}}
	row := make([]renderer.Cell, 19)

	drawStatusBarState(row, state, resolveThemeStyles(nil))

	text := rowText(row)
	require.Contains(t, text, "fresh")
	require.Contains(t, text, "middle")
	require.NotContains(t, text, "old")
}

func TestStatusBarCopyFeedbackFullyRenderedAlongsideMRU(t *testing.T) {
	state := barState{status: statusSnapshot{session: "cur"}, copyFeedback: "copied", mru: []mruSession{{name: "a"}, {name: "b"}, {name: "c"}}}
	row := make([]renderer.Cell, 20)

	drawStatusBarState(row, state, resolveThemeStyles(nil))

	text := rowText(row)
	require.Contains(t, text, " a")
	require.Contains(t, text, " b")
	require.Contains(t, text, " c")
	require.True(t, strings.HasSuffix(text, " copied"), text)
}

func TestStatusBarCopyFeedbackBoundaryWidths(t *testing.T) {
	state := barState{status: statusSnapshot{session: "cur"}, copyFeedback: "copied", mru: []mruSession{{name: "fresh"}}}
	tests := []struct {
		name string
		cols int
		want string
	}{
		{name: "just wide enough for feedback after dropping MRU", cols: 12, want: " cur  copied"},
		{name: "too narrow drops feedback instead of clipping", cols: 11, want: " cur       "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := make([]renderer.Cell, tt.cols)

			drawStatusBarState(row, state, resolveThemeStyles(nil))

			require.Equal(t, tt.want, rowText(row))
		})
	}
}

func TestStatusRepaintsOnCreateSwitchAndResize(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), stubClock{})
	tr, sends, releaseConn := newConn(t,
		mustHello(ports.IntentNew, "work", domain.Size{Cols: 20, Rows: 5}),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b1")),
		ports.Frame{Type: ports.MsgResize, Payload: ports.MarshalResize(ports.Resize{Size: domain.Size{Cols: 22, Rows: 6}})},
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, ports.MsgWelcome)
	first := awaitFrame(t, sends, ports.MsgOutput)
	palette := awaitFrame(t, sends, ports.MsgOutput)
	created := awaitFrame(t, sends, ports.MsgOutput)
	switched := awaitFrame(t, sends, ports.MsgOutput)
	resized := awaitFrame(t, sends, ports.MsgOutput)

	_ = palette
	for _, f := range []ports.Frame{first, created, switched, resized} {
		out, err := ports.UnmarshalOutput(f.Payload)
		require.NoError(t, err)
		require.Contains(t, string(out.Data), "work")
		require.Contains(t, string(out.Data), ";7m", "active status tab should be inverse-highlighted")
	}

	releaseConn()
	releasePTY1()
	releasePTY2()
	hg.Wait()
	d.sessWg.Wait()
}
