package daemon

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/palette"
	"github.com/bnema/vev/internal/usecase/picker"
	themeui "github.com/bnema/vev/internal/usecase/theme"
	"github.com/bnema/vev/internal/usecase/ui"
)

// stubClock returns timers whose channel never fires, so a scheduler under it
// blocks in its debounce loop until the session context is cancelled. Used by

func TestApplyConfigThemeRepaintInvalidatesComposedFrameCache(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeLight})

	win := testAttachmentTab(sess)
	left := win.focusedPane()
	right := newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 4})
	win.mu.Lock()
	win.size = domain.Size{Cols: 41, Rows: 4}
	win.tree.Root = &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(left.id), layout.NewLeaf(right.id)}}
	win.tree.Focus = left.id
	win.panes[right.id] = right
	win.mu.Unlock()
	left.mu.Lock()
	left.screen.Resize(20, 4)
	left.screen.Write([]byte("L"))
	left.mu.Unlock()
	right.mu.Lock()
	right.screen.Resize(20, 4)
	right.screen.Write([]byte("R"))
	right.mu.Unlock()

	d.paint(sess, ac, true, nil)
	ac.sendMu.Lock()
	require.True(t, ac.pipelineCache.valid)
	lightDimmedPane := ac.pipelineCache.frame.At(21, 1).Style
	lightDivider := ac.pipelineCache.frame.At(20, 1).Style
	ac.sendMu.Unlock()
	left.mu.Lock()
	left.screen.ClearDamage()
	left.mu.Unlock()
	right.mu.Lock()
	right.screen.ClearDamage()
	right.mu.Unlock()

	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark})

	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()
	require.True(t, ac.pipelineCache.valid, "reset=false config repaint should rebuild the composed cache")
	require.NotEqual(t, lightDimmedPane, ac.pipelineCache.frame.At(21, 1).Style, "dimmed pane style must not stay cached across theme reapply")
	require.NotEqual(t, lightDivider, ac.pipelineCache.frame.At(20, 1).Style, "divider style must not stay cached across theme reapply")
}

func TestStatusCompositionGolden(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	selectTestAttachmentTab(sess, 1)
	sess.name = "work"

	win := testAttachmentTab(sess)
	win.focusedPane().screen.Resize(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	win.focusedPane().screen.Write([]byte("hello"))

	frameState := capturedRenderState{
		reset:  true,
		layout: capturedTabLayout{area: domain.Rect{Width: 12, Height: 2}, focus: win.focusedPane().id, valid: true},
		panes: []capturedPaneRenderState{{
			id: win.focusedPane().id, frame: captureTestFrame(win.focusedPane().screen), placement: layout.Placement{ID: win.focusedPane().id, Content: domain.Rect{Width: 12, Height: 2}}, focused: true, damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		bars: barState{status: sess.statusSegmentsFor(ac, true)},
	}
	composed := composeFrame(frameState, composeCacheInput{})
	frame, damage := composed.frame, composed.damage

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

func TestStatusSegmentsResolvesActiveLifecyclePresentation(t *testing.T) {
	tests := []struct {
		name             string
		routeLifecycle   domain.SessionLifecycleID
		wantPresentation string
	}{
		{name: "matching lifecycle", routeLifecycle: domain.SessionLifecycleID{1}, wantPresentation: "vive@arch"},
		{name: "stale lifecycle", routeLifecycle: domain.SessionLifecycleID{99}, wantPresentation: "vive"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p, releasePTY := newBlockingPTY(t)
			_, sess, ac, _ := newManualSessionWithPTYs(t, p)
			defer releasePTY()

			sess.name = "vive"
			sess.incarnation = domain.SessionLifecycleID{1}
			ref := protocol.RouteRef{Key: 1, Generation: 1}
			ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
				Generation: 1,
				Active:     ref,
				ActiveEntry: protocol.RecentRouteEntry{
					Key: ref.Key, Generation: ref.Generation,
					Target: protocol.ExactSessionTarget{LifecycleID: tt.routeLifecycle, SessionName: sess.name},
					Name:   "vive", HostLabel: "user@arch", Kind: protocol.RouteKindRemote,
				},
			})

			require.Equal(t, tt.wantPresentation, sess.statusSegmentsFor(ac, true).session)
			require.Equal(t, "vive", sess.statusSegments(true).session, "route presentation belongs only to the selected attachment")
		})
	}
}

func TestStatusSegmentsIncludesFocusedPaneTitle(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()

	sess.tabs[1].name = "logs"

	pane0 := sess.tabs[0].focusedPane()
	pane0.mu.Lock()
	pane0.title.processName = "vim"
	pane0.title.processNameValid = true
	pane0.mu.Unlock()

	pane1 := sess.tabs[1].focusedPane()
	pane1.mu.Lock()
	pane1.title.terminalTitle = "build output"
	pane1.mu.Unlock()

	snap := sess.statusSegments(true)

	require.Len(t, snap.tabs, 2)
	require.Equal(t, "1", snap.tabs[0].name)
	require.Equal(t, "vim", snap.tabs[0].paneTitle)
	require.Equal(t, "logs", snap.tabs[1].name)
	require.Equal(t, "build output", snap.tabs[1].paneTitle)
}

func TestStatusSegmentsOmitsTerminalTitleWhenDisabled(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()

	sess.tabs[1].name = "logs"

	pane0 := sess.tabs[0].focusedPane()
	pane0.mu.Lock()
	pane0.title.processName = "vim"
	pane0.title.processNameValid = true
	pane0.title.terminalTitle = "server.go — vev"
	pane0.mu.Unlock()

	pane1 := sess.tabs[1].focusedPane()
	pane1.mu.Lock()
	pane1.title.terminalTitle = "build output"
	pane1.mu.Unlock()

	snap := sess.statusSegments(false)

	require.Len(t, snap.tabs, 2)
	require.Equal(t, "1", snap.tabs[0].name)
	require.Equal(t, "vim", snap.tabs[0].paneTitle, "OSC title must be omitted while process name still shows")
	require.Equal(t, "logs", snap.tabs[1].name)
	require.Equal(t, "sh", snap.tabs[1].paneTitle, "no process name and terminal title disabled falls back to the shell fallback")
}

func TestFitTabLabels(t *testing.T) {
	tests := []struct {
		name      string
		tabs      []statusTab
		rowLen    int
		rightText string
		want      []fittedTabLabel
	}{
		{
			name:   "no tabs",
			tabs:   nil,
			rowLen: 10,
			want:   []fittedTabLabel{},
		},
		{
			name:   "everything fits stays untruncated",
			tabs:   []statusTab{{name: "one"}, {name: "two", paneTitle: "shell"}},
			rowLen: 40,
			want:   []fittedTabLabel{{text: "one", nameLen: len("one")}, {text: "two (shell)", nameLen: len("two")}},
		},
		{
			name: "overflow redistributes budget: short tab stays full, long tab gets ellipsis",
			tabs: []statusTab{
				{name: "a"},
				{name: "ed", paneTitle: "verylongtitle"},
			},
			rowLen: 20,
			want:   []fittedTabLabel{{text: "a", nameLen: len("a")}, {text: "ed (verylongt…", nameLen: len("ed")}},
		},
		{
			name:   "tight budget degrades to a truncated tab name only",
			tabs:   []statusTab{{name: "editor", paneTitle: "some pane title text"}},
			rowLen: 7,
			want:   []fittedTabLabel{{text: "edi…", nameLen: len("edi…")}},
		},
		{
			name:      "non-empty rightText shrinks the budget",
			tabs:      []statusTab{{name: "aaaaaaaaaa"}},
			rowLen:    20,
			rightText: "0123456789",
			want:      []fittedTabLabel{{text: "aaaaa…", nameLen: len("aaaaa…")}},
		},
		{
			name:   "single tab wider than the whole row collapses to empty",
			tabs:   []statusTab{{name: "reallyverylongtabname"}},
			rowLen: 3,
			want:   []fittedTabLabel{{}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := fitTabLabels(tt.tabs, tt.rowLen, tt.rightText)

			require.Equal(t, tt.want, got)

			reserve := 1
			if tt.rightText != "" {
				reserve = statusTextWidth(tt.rightText) + 2
			}
			budget := tt.rowLen - reserve
			drawn := 0
			for i, tb := range tt.tabs {
				overhead := 2
				if tb.attention {
					overhead += 1 + renderer.RuneWidth(ui.AttentionGlyph)
				}
				drawn += overhead + statusTextWidth(got[i].text)
				require.LessOrEqual(t, got[i].nameLen, len(got[i].text), "nameLen must not exceed the label's byte length")
				if got[i].nameLen < len(got[i].text) {
					require.True(t, utf8.RuneStart(got[i].text[got[i].nameLen]), "nameLen must land on a rune boundary")
				}
			}
			if budget > 0 {
				require.LessOrEqual(t, drawn, budget, "drawn width must not exceed the derived budget")
			}
		})
	}
}

func TestDrawTopBarSnapshotStylesTabNameAndTitle(t *testing.T) {
	tabs := []statusTab{
		{name: "api", paneTitle: "shell"},
		{name: "logs", active: true, attention: true},
		{name: "err", paneTitle: "build failed", attention: true},
	}
	status := statusSnapshot{tabs: tabs}
	const rowLen = 60

	t.Run("truecolor theme bolds names, mutes titles, and the bell keeps the base background", func(t *testing.T) {
		theme := themeui.Theme{
			Foreground: renderer.RGB{R: 200, G: 200, B: 200},
			Background: renderer.RGB{R: 10, G: 20, B: 30},
			HasFG:      true,
			HasBG:      true,
			Known:      true,
			TrueColor:  true,
		}
		styles := themeui.NewStyles(theme)
		require.False(t, styles.TabTitle.Equal(styles.StatusBar), "sanity: muted title style must differ from the base style on a truecolor theme")
		require.False(t, styles.TabTitleActive.Equal(styles.Accent), "sanity: muted active-title style must differ from the base accent style")

		labels := fitTabLabels(tabs, rowLen, "")

		// Frame 0 is the bell's blank beat: the attention space and the blank
		// bell cell must both be exactly the tab's base style (not
		// renderer.DefaultStyle(), which would punch a hole in a themed bar).
		// This also pins down segment order: name, then (if attention)
		// space+bell, then title, then the trailing space.
		row := make([]renderer.Cell, rowLen)
		drawTopBarSnapshot(row, status, 0, "", styles)
		x := 0

		// tab 0: inactive, no attention: name, title, trailing space.
		nameEnd := x + 1 + labels[0].nameLen
		for i := x; i < nameEnd; i++ {
			require.True(t, row[i].Style.Bold, "cell %d: name segment must be bold", i)
			require.True(t, row[i].Style.Equal(styles.TabName), "cell %d: name segment style mismatch", i)
		}
		x = nameEnd
		titleLen := len(labels[0].text) - labels[0].nameLen
		titleEnd := x + titleLen
		for i := x; i < titleEnd; i++ {
			require.False(t, row[i].Style.Bold, "cell %d: title segment must not be bold", i)
			require.True(t, row[i].Style.Equal(styles.TabTitle), "cell %d: title segment style mismatch", i)
		}
		x = titleEnd
		require.True(t, row[x].Style.Equal(styles.StatusBar), "cell %d: tab 0 trailing space must keep the base style", x)
		x++

		// tab 1: active, attention, no title: name, attention space+bell (both
		// base style at frame 0), trailing space.
		nameEnd = x + 1 + labels[1].nameLen
		for i := x; i < nameEnd; i++ {
			require.True(t, row[i].Style.Bold, "cell %d: active name segment must be bold", i)
			require.True(t, row[i].Style.Equal(styles.TabNameActive), "cell %d: active name segment style mismatch", i)
		}
		require.Equal(t, len(labels[1].text), labels[1].nameLen, "tab 1 has no pane title, so its whole label is the name segment")
		x = nameEnd
		require.True(t, row[x].Style.Equal(styles.Accent), "cell %d: tab 1 attention leading space must keep the active base style", x)
		x++
		tab1Bell := x
		require.True(t, row[x].Style.Equal(styles.Accent), "cell %d: tab 1 blank bell must keep the active base style, not DefaultStyle", x)
		x++
		require.True(t, row[x].Style.Equal(styles.Accent), "cell %d: tab 1 trailing space must keep the active base style", x)
		x++

		// tab 2: inactive, attention, with title: name, attention space+bell
		// (both base style at frame 0), title, trailing space.
		nameEnd = x + 1 + labels[2].nameLen
		for i := x; i < nameEnd; i++ {
			require.True(t, row[i].Style.Bold, "cell %d: tab 2 name segment must be bold", i)
			require.True(t, row[i].Style.Equal(styles.TabName), "cell %d: tab 2 name segment style mismatch", i)
		}
		x = nameEnd
		require.True(t, row[x].Style.Equal(styles.StatusBar), "cell %d: tab 2 attention leading space must keep the base style", x)
		x++
		tab2Bell := x
		require.True(t, row[x].Style.Equal(styles.StatusBar), "cell %d: tab 2 blank bell must keep the base style, not DefaultStyle", x)
		x++
		titleLen = len(labels[2].text) - labels[2].nameLen
		titleEnd = x + titleLen
		for i := x; i < titleEnd; i++ {
			require.False(t, row[i].Style.Bold, "cell %d: tab 2 title segment must not be bold", i)
			require.True(t, row[i].Style.Equal(styles.TabTitle), "cell %d: tab 2 title segment style mismatch", i)
		}
		x = titleEnd
		require.True(t, row[x].Style.Equal(styles.StatusBar), "cell %d: tab 2 trailing space must keep the base style", x)

		// On a visible pulse frame, the bell glyph itself must keep the same
		// background as its tab's base style.
		visibleRow := make([]renderer.Cell, rowLen)
		drawTopBarSnapshot(visibleRow, status, 1, "", styles)
		require.Equal(t, rune(ui.AttentionGlyph), visibleRow[tab1Bell].Rune)
		require.True(t, visibleRow[tab1Bell].Style.HasBackgroundRGB)
		require.Equal(t, styles.Accent.BackgroundRGB, visibleRow[tab1Bell].Style.BackgroundRGB, "tab 1's visible bell must keep the active base background")
		require.Equal(t, rune(ui.AttentionGlyph), visibleRow[tab2Bell].Rune)
		require.True(t, visibleRow[tab2Bell].Style.HasBackgroundRGB)
		require.Equal(t, styles.StatusBar.BackgroundRGB, visibleRow[tab2Bell].Style.BackgroundRGB, "tab 2's visible bell must keep the plain base background")
	})

	t.Run("non-usable theme matches the pre-change fallback styles", func(t *testing.T) {
		styles := resolveStyles(nil)
		require.True(t, styles.TabName.Equal(styles.StatusBar))
		require.True(t, styles.TabTitle.Equal(styles.StatusBar))
		require.True(t, styles.TabNameActive.Equal(styles.Accent))
		require.True(t, styles.TabTitleActive.Equal(styles.Accent))

		row := make([]renderer.Cell, rowLen)
		drawTopBarSnapshot(row, status, 0, "", styles)

		// " api (shell) " (tab 0) + " logs  " (tab 1: name, attention
		// space+blank bell, trailing space) + " err  (build failed) " (tab 2:
		// name, attention space+blank bell, title, trailing space), then blank
		// padding to rowLen: byte-for-byte what the pre-change renderer
		// produced, since every new style field collapses to statusBar/accent.
		const tab0 = " api (shell) "
		const tab1 = " logs   "
		const tab2 = " err   (build failed) "
		require.Equal(t, tab0+tab1+tab2+strings.Repeat(" ", rowLen-len(tab0)-len(tab1)-len(tab2)), rowText(row))

		for i, c := range row[:len(tab0)] {
			require.True(t, c.Style.Equal(styles.StatusBar), "cell %d should use the plain base style", i)
		}
		tab1Start := len(tab0)
		for i := tab1Start; i < tab1Start+len(tab1); i++ {
			require.True(t, row[i].Style.Equal(styles.Accent), "cell %d should use the plain accent style, including the blank bell cell", i)
		}
		tab2Start := tab1Start + len(tab1)
		for i := tab2Start; i < tab2Start+len(tab2); i++ {
			require.True(t, row[i].Style.Equal(styles.StatusBar), "cell %d should use the plain base style, including the blank bell cell", i)
		}
	})
}

func TestTabDisplayName(t *testing.T) {
	tests := []struct {
		name  string
		tab   *tab
		index int
		want  string
	}{
		{name: "custom name", tab: &tab{name: "logs"}, index: 3, want: "logs"},
		{name: "numeric fallback", tab: &tab{}, index: 3, want: "4"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tabDisplayName(tt.tab, tt.index))
		})
	}
}

func TestStatusCompositionUsesTruecolorTheme(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	win := testAttachmentTab(sess)
	win.focusedPane().screen.Resize(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}
	ac.setThemeForTest(themeui.Theme{
		Foreground: renderer.RGB{R: 220, G: 220, B: 220},
		Background: renderer.RGB{R: 10, G: 20, B: 30},
		HasFG:      true,
		HasBG:      true,
		TrueColor:  true,
		Known:      true,
	})

	applied := ac.getAppliedTheme()
	bars := barState{status: sess.statusSegments(true), theme: applied.Raw}
	composed := composeFrame(capturedRenderState{
		reset: true, layout: capturedTabLayout{area: domain.Rect{Width: 12, Height: 2}, valid: true}, bars: bars, theme: bars.theme, styles: applied.Resolved.Styles, styleGeneration: applied.Generation,
	}, composeCacheInput{})
	out, err := renderer.New(renderer.Capabilities{}).Draw(composed.frame, composed.damage)

	require.NoError(t, err)
	require.Contains(t, string(out), ";48;2;")
}

func TestStatusApplyThemeStoresClientAndPropagatesScreens(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	msg := protocol.Theme{HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3}, HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6}, SchemeKnown: true, Light: true}

	d.applyTheme(sess, ac, msg)

	require.Equal(t, renderer.RGB{R: 1, G: 2, B: 3}, ac.getAppliedTheme().Raw.Foreground)
	require.True(t, ac.getAppliedTheme().Raw.SchemeKnown)
	require.True(t, ac.getAppliedTheme().Raw.Light)
	assertSessionDefaultColors(t, sess, renderer.RGB{R: 1, G: 2, B: 3}, renderer.RGB{R: 4, G: 5, B: 6})
	assertSessionColorScheme(t, sess, true)
}

func TestApplyThemePropagatesToFloatingPane(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	defer releasePTY()
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	tb := testAttachmentTab(sess)
	floating := newPane("floating", nil, domain.Size{Cols: 20, Rows: 5})
	installTestFloating(tb, floating, true)
	clientTheme := protocol.Theme{
		HasForeground: true,
		Foreground:    renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true,
		Background:    renderer.RGB{R: 4, G: 5, B: 6},
		SchemeKnown:   true,
		Light:         true,
	}

	d.applyTheme(sess, ac, clientTheme)
	assertPaneDefaultColors(t, floating, clientTheme.Foreground, clientTheme.Background)
	assertPaneColorScheme(t, floating, true)

	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark})
	assertPaneDefaultColors(t, floating, themeui.BuiltinDark.Foreground, themeui.BuiltinDark.Background)
	assertPaneColorScheme(t, floating, false)
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

			d.applyTheme(sess, ac, protocol.Theme{
				HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
				HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
				TrueColor: true, SchemeKnown: true, Light: !tc.want.Light,
			})

			require.Equal(t, tc.want, ac.getAppliedTheme().Raw)
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

			ac, err := d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})
			require.NoError(t, err)

			require.Equal(t, tc.want, ac.getAppliedTheme().Raw)
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
	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true, Light: true,
	})
	assertSessionColorScheme(t, sess, true)

	d.clientGone(sess, ac, ac.transport(), true)

	assertSessionColorSchemeUnknown(t, sess)
}

func TestAttachClientClearsStaleColorSchemeOnReplacement(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeAuto})
	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true, Light: true,
	})
	assertSessionColorScheme(t, sess, true)

	tr, _ := newCapturingTransport(t)
	_, err := d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})
	require.NoError(t, err)

	assertSessionColorSchemeUnknown(t, sess)
}

func TestForcedThemeDetachPreservesBuiltinOnPanes(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	d.ApplyConfig(domain.Config{Theme: domain.ThemeDark})
	d.applyTheme(sess, ac, protocol.Theme{
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

	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true, Light: true,
	})
	d.applyTheme(sess, ac, protocol.Theme{
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
		assertPaneDefaultColors(t, tb.focusedPane(), fg, bg)
	}
}

func assertPaneDefaultColors(t *testing.T, p *pane, fg, bg renderer.RGB) {
	t.Helper()
	var got []byte
	p.mu.Lock()
	p.screen.OnResponse = func(b []byte) { got = append(got, b...) }
	p.screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	p.mu.Unlock()
	require.True(t, strings.Contains(string(got), formatOSCColor(fg)), string(got))
	require.True(t, strings.Contains(string(got), formatOSCColor(bg)), string(got))
}

func assertSessionColorScheme(t *testing.T, sess *session, light bool) {
	t.Helper()
	for _, tb := range sess.tabs {
		assertPaneColorScheme(t, tb.focusedPane(), light)
	}
}

func assertPaneColorScheme(t *testing.T, p *pane, light bool) {
	t.Helper()
	want := "\x1b[?997;1n"
	if light {
		want = "\x1b[?997;2n"
	}
	var got []byte
	p.mu.Lock()
	p.screen.OnResponse = func(b []byte) { got = append(got, b...) }
	p.screen.Write([]byte("\x1b[?996n"))
	p.mu.Unlock()
	require.Equal(t, want, string(got))
}

func assertSessionColorSchemeUnknown(t *testing.T, sess *session) {
	t.Helper()
	for _, tb := range sess.tabs {
		p := tb.focusedPane()
		var got []byte
		p.mu.Lock()
		p.screen.OnResponse = func(b []byte) { got = append(got, b...) }
		p.screen.Write([]byte("\x1b[?996n"))
		p.mu.Unlock()
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
	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})
	tr, _ := newCapturingTransport(t)

	_, err := d.attachClient(sess, tr, domain.Size{Cols: 80, Rows: 24}, attachClientOptions{})
	require.NoError(t, err)

	var got []byte
	tb := testAttachmentTab(sess)
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got)
}

func TestClientGoneResetsScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
	})

	// Sanity: the tab answers OSC 10/11 while ac is attached.
	tb := testAttachmentTab(sess)
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

func TestDetachOnSendErrorResetsScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 7, G: 8, B: 9},
		HasBackground: true, Background: renderer.RGB{R: 10, G: 11, B: 12},
	})

	d.detachOnSendError(sess, ac, ac.transport())

	tb := testAttachmentTab(sess)
	var got []byte
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.Empty(t, got, "OSC 10/11 queries must be swallowed once the client that reported these colors is gone")
}

func TestDetachOnSendErrorParkPreservesScreenDefaultColors(t *testing.T) {
	p, release := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer release()
	ac.resumeCapable = true
	d.applyTheme(sess, ac, protocol.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 7, G: 8, B: 9},
		HasBackground: true, Background: renderer.RGB{R: 10, G: 11, B: 12},
	})

	d.detachOnSendError(sess, ac, ac.transport())

	tb := testAttachmentTab(sess)
	var got []byte
	tb.focusedPane().screen.OnResponse = func(b []byte) { got = append(got, b...) }
	tb.focusedPane().screen.Write([]byte("\x1b]10;?\a\x1b]11;?\a"))
	require.NotEmpty(t, got, "parked clients resume the same attachment, so screen default colors must be preserved")
}

func TestStatusMarksEphemeralSession(t *testing.T) {
	p, release := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer release()
	sess.name = "0"
	sess.ephemeral = true
	win := testAttachmentTab(sess)
	win.focusedPane().screen.Resize(12, 2)
	win.size = domain.Size{Cols: 12, Rows: 2}

	frame := composeFrame(capturedRenderState{
		reset: true, layout: capturedTabLayout{area: domain.Rect{Width: 12, Height: 2}, valid: true}, bars: barState{status: sess.statusSegments(true)},
	}, composeCacheInput{}).frame

	require.Equal(t, " 1 (sh)     ", rowText(frame.Row(0)))
	require.Equal(t, " 0*         ", rowText(frame.Row(3)))
}

func TestTopBarRightAnchor(t *testing.T) {
	tests := []struct {
		name             string
		width            int
		status           statusSnapshot
		topRight         string
		want             string
		continuationCell int
	}{
		{
			name:     "renders flush right when fully fits",
			width:    20,
			status:   statusSnapshot{tabs: []statusTab{{name: "1", active: true}}},
			topRight: "14:32",
			want:     " 1             14:32",
		},
		{
			// The tab budget reserves room for topRight up front (fitTabLabels),
			// so tab labels shrink to nothing before the right-anchored text is
			// dropped; unlike the bottom bar's MRU, top-bar tabs never hide
			// entirely, so topRight still renders once tabs are squeezed out.
			name:     "squeezes tabs empty to keep topRight visible",
			width:    10,
			status:   statusSnapshot{tabs: []statusTab{{name: "1"}, {name: "2"}}},
			topRight: "12345",
			want:     "     12345",
		},
		{
			name:             "uses display width",
			width:            12,
			status:           statusSnapshot{tabs: []statusTab{{name: "1", active: true}}},
			topRight:         "界a",
			want:             " 1       界 a",
			continuationCell: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := make([]renderer.Cell, tt.width)

			drawTopBarSnapshot(row, tt.status, 0, tt.topRight, resolveStyles(nil))

			require.Equal(t, tt.want, rowText(row))
			if tt.continuationCell > 0 {
				require.True(t, row[tt.continuationCell].Continuation)
			}
		})
	}
}

func TestStatusCopyFeedbackRendersOnlyWhenFullyFits(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()
	sess.name = "work"
	win := testAttachmentTab(sess)
	win.focusedPane().screen.Resize(30, 2)
	win.size = domain.Size{Cols: 30, Rows: 2}

	frame := composeFrame(capturedRenderState{
		reset: true, layout: capturedTabLayout{area: domain.Rect{Width: 30, Height: 2}, valid: true}, bars: barState{status: sess.statusSegments(true), statusFeedback: "ok"},
	}, composeCacheInput{}).frame
	require.Equal(t, " work                       ok", rowText(frame.Row(3)))

	frame = composeFrame(capturedRenderState{
		reset: true, layout: capturedTabLayout{area: domain.Rect{Width: 30, Height: 2}, valid: true}, bars: barState{status: sess.statusSegments(true), statusFeedback: "1234567890123456789"},
	}, composeCacheInput{}).frame
	require.Equal(t, " work      1234567890123456789", rowText(frame.Row(3)))

	frame = composeFrame(capturedRenderState{
		reset: true, layout: capturedTabLayout{area: domain.Rect{Width: 30, Height: 2}, valid: true}, bars: barState{status: sess.statusSegments(true), statusFeedback: "selection too large to copy"},
	}, composeCacheInput{}).frame
	require.Equal(t, " work                         ", rowText(frame.Row(3)))
}

func TestAttentionConstants(t *testing.T) {
	require.Equal(t, '', ui.AttentionGlyph)
	require.Equal(t, 30, pulseFrameCount)
	require.Equal(t, 120*time.Millisecond, pulseFrameInterval)
}

func TestPulseStyleFadesFromBaseAndHidesAtFrameZero(t *testing.T) {
	rgbBase := renderer.Style{
		HasForegroundRGB: true, ForegroundRGB: renderer.RGB{R: 200, G: 200, B: 200},
		HasBackgroundRGB: true, BackgroundRGB: renderer.RGB{R: 10, G: 20, B: 30},
	}
	indexedBase := renderer.Style{Foreground: -1, Background: -1, Inverse: true}

	t.Run("invisible frame returns base unchanged", func(t *testing.T) {
		for _, base := range []renderer.Style{rgbBase, indexedBase} {
			style, visible := pulseStyle(0, base)
			require.False(t, visible)
			require.True(t, style.Equal(base))
		}
	})

	t.Run("rgb base blends the glyph foreground from the base background to its foreground, keeping the base background", func(t *testing.T) {
		low, visible := pulseStyle(1, rgbBase)
		require.True(t, visible)
		require.True(t, low.Bold)
		require.True(t, low.HasBackgroundRGB)
		require.Equal(t, rgbBase.BackgroundRGB, low.BackgroundRGB, "bell keeps the caller's background so it never punches a hole in a themed bar")

		peak, visible := pulseStyle(pulseFrameCount/2, rgbBase)
		require.True(t, visible)
		require.Equal(t, rgbBase.ForegroundRGB, peak.ForegroundRGB, "peak intensity reaches the base foreground exactly")
		require.NotEqual(t, low.ForegroundRGB, peak.ForegroundRGB, "the glyph should ramp, not jump straight to full intensity")
	})

	t.Run("non-RGB base falls back to the indexed grey ramp and preserves other base attributes", func(t *testing.T) {
		low, visible := pulseStyle(1, indexedBase)
		require.True(t, visible)
		require.True(t, low.Bold)
		require.True(t, low.Inverse, "non-color base attributes like inverse must survive")

		peak, visible := pulseStyle(pulseFrameCount/2, indexedBase)
		require.True(t, visible)
		require.Greater(t, peak.Foreground, low.Foreground)
	})
}

func TestTopBarRendersAttentionBell(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	_, sess, _, _ := newManualSessionWithPTYs(t, p1, p2)
	defer releasePTY1()
	defer releasePTY2()
	win := testAttachmentTab(sess)
	win.focusedPane().screen.Resize(18, 2)
	win.size = domain.Size{Cols: 18, Rows: 2}
	sess.mu.Lock()
	sess.tabs[1].attention = true
	sess.tabs[1].attentionAt = time.Unix(10, 0)
	sess.mu.Unlock()

	frame := composeFrame(capturedRenderState{
		reset: true, layout: capturedTabLayout{area: domain.Rect{Width: 18, Height: 2}, valid: true}, bars: barState{status: sess.statusSegments(true), attentionFrame: 1},
	}, composeCacheInput{}).frame

	// Tab 2's label is enriched with its focused pane's title ("sh", the
	// default shell fallback) and truncated to fit alongside the bell. The
	// bell sits right after the name, before the (title) segment.
	require.Contains(t, rowText(frame.Row(0)), "2 "+string(ui.AttentionGlyph)+" (s")
	for _, c := range frame.Row(0) {
		if c.Rune == ui.AttentionGlyph {
			require.True(t, c.Style.Bold)
			return
		}
	}
	t.Fatalf("attention glyph not rendered in top bar: %q", rowText(frame.Row(0)))
}

func TestLockAttachmentSessionsRejectsNilEntriesWithoutPanic(t *testing.T) {
	var nilEntry *session
	var typedNil *session
	valid := &session{sessionCore: sessionCore{id: "valid"}}

	for _, tc := range []struct {
		name string
		a    *session
		b    *session
	}{
		{name: "nil interfaces", a: nilEntry, b: nilEntry},
		{name: "typed nil", a: typedNil, b: valid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.NotPanics(t, func() {
				unlock := lockAttachmentSessions(tc.a, tc.b)
				unlock()
			})
		})
	}
}

func TestBarStateForPaletteHintsNormalizesTypedNilSession(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	var sess *session

	require.NotPanics(t, func() {
		state := d.barStateForPaletteHints(sess, "feedback", nil)
		require.Equal(t, "feedback", state.statusFeedback)
		require.Empty(t, state.status.session)
	})
}

func TestBarStateForContextualRecentUsesClientSnapshot(t *testing.T) {
	p, releasePTY := newBlockingPTY(t)
	d, sess, ac, _ := newManualSessionWithPTYs(t, p)
	defer releasePTY()

	snapshot := protocol.RecentRouteSnapshot{
		Generation: 1,
		Entries:    []protocol.RecentRouteEntry{{Key: 2, Generation: 1, Target: testRouteTarget("captured", 2), Name: "captured", Kind: protocol.RouteKindLocal, Attention: true}},
	}
	hints := palette.ContextualHints{
		Kind:         command.ContextHintRecentSessions,
		SelectedRank: 1,
		Recent:       []palette.RecentSessionHint{{Rank: 1, Name: "captured"}},
	}
	// Hold the registry lock: contextual composition must use the immutable
	// attachment snapshot and never consult daemon session history.
	var contextual barState
	done := make(chan struct{})
	d.mu.Lock()
	go func() {
		contextual = d.barStateForAttachmentPaletteHintsFor(sess, ac, "", &hints, snapshot)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		d.mu.Unlock()
		t.Fatal("contextual composition consulted the live session registry")
	}
	d.mu.Unlock()

	require.Empty(t, contextual.mru, "contextual composition must not read the live MRU")
	require.Equal(t, []rankedRecent{{rank: 1, name: "captured", attention: true, selected: true}}, contextual.rankedRecent)
}

func TestCapturePrimaryRenderStatePreservesContextualMRUModeThroughScratchReuse(t *testing.T) {
	_, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	capture := func(bars barState) capturedRenderState {
		t.Helper()
		state, ok := captureRenderState(sess, ac, renderCaptureRequest{
			bars:        bars,
			overlays:    capturedOverlayRenderState{},
			preview:     picker.Preview{},
			floatingCfg: domain.FloatingConfig{},
			reset:       false,
			lease:       nil,
		})
		require.True(t, ok)
		return *state
	}
	draw := func(state capturedRenderState) string {
		t.Helper()
		row := make([]renderer.Cell, 32)
		drawStatusBarState(row, state.bars, resolveStyles(nil))
		return rowText(row)
	}
	normal := barState{status: statusSnapshot{session: "cur"}, mru: []recentRouteDisplay{{name: "vty"}, {name: "misc"}}}
	ac.sendMu.Lock()
	defer ac.sendMu.Unlock()

	// An empty but non-nil list is JRS contextual mode, not normal mode.
	emptyContextual := capture(barState{status: normal.status, mru: normal.mru, rankedRecent: []rankedRecent{}})
	require.NotNil(t, emptyContextual.bars.rankedRecent)
	require.Empty(t, emptyContextual.bars.rankedRecent)
	emptyRow := draw(emptyContextual)
	require.NotContains(t, emptyRow, "vty")
	require.NotContains(t, emptyRow, "misc")

	// A later normal frame must clear contextual mode even after scratch reuse.
	normalAfterEmpty := capture(normal)
	require.Nil(t, normalAfterEmpty.bars.rankedRecent)
	normalRow := draw(normalAfterEmpty)
	require.Contains(t, normalRow, "vty")
	require.Contains(t, normalRow, "misc")

	// A populated contextual list reuses the same scratch and remains distinct.
	populatedContextual := capture(barState{status: normal.status, mru: normal.mru, rankedRecent: []rankedRecent{{rank: 1, name: "jrs"}}})
	require.NotNil(t, populatedContextual.bars.rankedRecent)
	require.Equal(t, []rankedRecent{{rank: 1, name: "jrs"}}, populatedContextual.bars.rankedRecent)
	populatedRow := draw(populatedContextual)
	require.Contains(t, populatedRow, "1:jrs")
	require.NotContains(t, populatedRow, "vty")

	normalAfterPopulated := capture(normal)
	require.Nil(t, normalAfterPopulated.bars.rankedRecent)
	require.Contains(t, draw(normalAfterPopulated), "vty")
}

func TestStatusBarRendersMRUNamesAndInlineBell(t *testing.T) {
	state := barState{
		status:         statusSnapshot{session: "cur"},
		attentionFrame: 1,
		mru: []recentRouteDisplay{
			{name: "fresh"},
			{name: "tmp", attention: true},
		},
	}
	row := make([]renderer.Cell, 24)

	drawStatusBarState(row, state, resolveStyles(nil))

	require.Equal(t, " cur  fresh  tmp       ", rowText(row))
	for _, c := range row {
		if c.Rune == ui.AttentionGlyph {
			require.True(t, c.Style.Bold)
			return
		}
	}
	t.Fatalf("inline bell not rendered: %q", rowText(row))
}

func TestStatusBarCurrentSessionUsesAccentStyle(t *testing.T) {
	theme := themeui.Theme{Foreground: renderer.RGB{R: 220, G: 220, B: 220}, Background: renderer.RGB{R: 10, G: 10, B: 10}, HasFG: true, HasBG: true, TrueColor: true, Known: true}
	styles := themeui.NewStyles(theme)
	row := make([]renderer.Cell, 16)

	drawStatusBarState(row, barState{status: statusSnapshot{session: "cur"}, theme: theme}, styles)

	for _, idx := range []int{0, 1, 2, 3, 4} {
		require.True(t, row[idx].Style.Equal(styles.Accent), "cell %d should use accent style", idx)
	}
	require.NotEqual(t, styles.StatusBar.BackgroundRGB, styles.Accent.BackgroundRGB)
}

func TestStatusBarContextualRanksPreserveOriginalRanksAndSelectedAccent(t *testing.T) {
	styles := resolveStyles(nil)
	row := make([]renderer.Cell, 18)
	state := barState{
		status: statusSnapshot{session: "cur"},
		rankedRecent: []rankedRecent{
			{rank: 1, name: "wide界"},
			{rank: 2, name: "two", selected: true},
			{rank: 3, name: "three"},
		},
	}

	drawStatusBarState(row, state, styles)
	text := rowText(row)
	require.Contains(t, text, "1:wide界")
	require.NotContains(t, text, "2:two") // whole entries fit; no partial rank/name.
	require.NotContains(t, text, "3:three")
	// A rank that does fit is accent-colored and its prefix is bold.
	row = make([]renderer.Cell, 28)
	drawStatusBarState(row, state, styles)
	require.Contains(t, rowText(row), "2:two")
	found := false
	for _, cell := range row {
		if cell.Rune == '2' {
			found = true
			require.True(t, cell.Style.Bold)
			require.Equal(t, styles.Accent.Foreground, cell.Style.Foreground)
			require.Equal(t, styles.Accent.Background, cell.Style.Background)
			break
		}
	}
	require.True(t, found)
}

func TestStatusBarMRUGradientTruecolorAndPlainFallback(t *testing.T) {
	theme := themeui.Theme{Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8}, Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a}, HasFG: true, HasBG: true, TrueColor: true, Known: true, UsePalette: true}
	theme.Palette[2] = renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	theme.Palette[10] = theme.Palette[2]
	theme.PaletteKnown = 1<<2 | 1<<10
	resolved := themeui.Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})
	state := barState{status: statusSnapshot{session: "c"}, mru: []recentRouteDisplay{{name: "a"}, {name: "b"}, {name: "c"}}}
	row := make([]renderer.Cell, 16)

	drawStatusBarState(row, state, resolved.Styles)

	for index, cell := range []int{4, 7, 10} {
		require.True(t, row[cell].Style.Equal(themeui.MRUStyle(resolved.Ramp, index, 3)))
	}
	require.NotEqual(t, resolved.Styles.SurfaceBar.BackgroundRGB, row[10].Style.BackgroundRGB, "oldest MRU remains distinct from the bar")

	plain := resolveStyles(nil).MRUStyle(1, 3)
	require.False(t, plain.HasForegroundRGB)
	require.False(t, plain.HasBackgroundRGB)
}

func TestStatusBarNarrowRowsDropWholeOldestMRUEntries(t *testing.T) {
	state := barState{status: statusSnapshot{session: "cur"}, mru: []recentRouteDisplay{{name: "fresh"}, {name: "middle"}, {name: "old"}}}
	row := make([]renderer.Cell, 21)

	drawStatusBarState(row, state, resolveStyles(nil))

	text := rowText(row)
	require.Contains(t, text, "fresh")
	require.Contains(t, text, "middle")
	require.NotContains(t, text, "old")
}

func TestStatusBarMRUWidthAwareBudget(t *testing.T) {
	mru := make([]recentRouteDisplay, 0, maxMRUSessions)
	for i := 1; i <= maxMRUSessions; i++ {
		mru = append(mru, recentRouteDisplay{name: "recent" + strconv.Itoa(i)})
	}
	state := barState{status: statusSnapshot{session: "cur"}, mru: mru}

	tests := []struct {
		name       string
		cols       int
		wantShown  []string
		wantHidden []string
	}{
		{
			name:       "narrow keeps first recent when it physically fits",
			cols:       15,
			wantShown:  []string{"cur", "recent1"},
			wantHidden: []string{"recent2"},
		},
		{
			name:       "medium reserves right-side room instead of filling all recents",
			cols:       80,
			wantShown:  []string{"recent1", "recent6"},
			wantHidden: []string{"recent7", "recent9"},
		},
		{
			name:       "wide shows more recents while still keeping a right-side reserve",
			cols:       120,
			wantShown:  []string{"recent1", "recent9"},
			wantHidden: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := make([]renderer.Cell, tt.cols)

			drawStatusBarState(row, state, resolveStyles(nil))

			text := rowText(row)
			for _, want := range tt.wantShown {
				require.Contains(t, text, want)
			}
			for _, hidden := range tt.wantHidden {
				require.NotContains(t, text, hidden)
			}
		})
	}
}

func TestStatusBarBottomRightAndMRUFitting(t *testing.T) {
	tests := []struct {
		name              string
		width             int
		state             barState
		want              string
		wantContains      []string
		wantNotContains   []string
		wantSuffix        string
		continuationCells []int
	}{
		{
			name:         "script text and copy feedback",
			width:        32,
			state:        barState{status: statusSnapshot{session: "cur"}, bottomRight: "main ↑3 *", statusFeedback: "copied", mru: []recentRouteDisplay{{name: "a"}}},
			wantContains: []string{" a"},
			wantSuffix:   " main ↑3 * copied",
		},
		{
			name:            "hide on overlap",
			width:           16,
			state:           barState{status: statusSnapshot{session: "cur"}, bottomRight: "main ↑3 *", statusFeedback: "copied", mru: []recentRouteDisplay{{name: "fresh"}}},
			want:            " cur            ",
			wantNotContains: []string{"main", "copied"},
		},
		{
			name:  "empty script keeps copy feedback behavior",
			width: 12,
			state: barState{status: statusSnapshot{session: "cur"}, bottomRight: "", statusFeedback: "copied", mru: []recentRouteDisplay{{name: "fresh"}}},
			want:  " cur  copied",
		},
		{
			name:            "mru whole entry fitting with script text",
			width:           24,
			state:           barState{status: statusSnapshot{session: "cur"}, bottomRight: "git", mru: []recentRouteDisplay{{name: "fresh"}, {name: "middle"}, {name: "old"}}},
			wantContains:    []string{" fresh"},
			wantNotContains: []string{"middle"},
			wantSuffix:      " git",
		},
		{
			name:              "mru fitting reserves wide right anchor width",
			width:             12,
			state:             barState{status: statusSnapshot{session: "cur"}, bottomRight: "界界", mru: []recentRouteDisplay{{name: "a"}}},
			wantNotContains:   []string{" a "},
			wantSuffix:        " 界 界 ",
			continuationCells: []int{9, 11},
		},
		{
			name:            "mru fitting counts wide session names",
			width:           20,
			state:           barState{status: statusSnapshot{session: "cur"}, bottomRight: "git", mru: []recentRouteDisplay{{name: "界"}, {name: "界"}, {name: "b"}}},
			wantContains:    []string{" 界 "},
			wantNotContains: []string{" b "},
			wantSuffix:      " git",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := make([]renderer.Cell, tt.width)

			drawStatusBarState(row, tt.state, resolveStyles(nil))

			text := rowText(row)
			if tt.want != "" {
				require.Equal(t, tt.want, text)
			}
			for _, want := range tt.wantContains {
				require.Contains(t, text, want)
			}
			for _, hidden := range tt.wantNotContains {
				require.NotContains(t, text, hidden)
			}
			if tt.wantSuffix != "" {
				require.True(t, strings.HasSuffix(text, tt.wantSuffix), text)
			}
			for _, cell := range tt.continuationCells {
				require.True(t, row[cell].Continuation)
			}
		})
	}
}

func TestStatusBarCopyFeedbackFullyRenderedAlongsideMRU(t *testing.T) {
	state := barState{status: statusSnapshot{session: "cur"}, statusFeedback: "copied", mru: []recentRouteDisplay{{name: "a"}, {name: "b"}, {name: "c"}}}
	row := make([]renderer.Cell, 20)

	drawStatusBarState(row, state, resolveStyles(nil))

	text := rowText(row)
	require.Contains(t, text, " a")
	require.Contains(t, text, " b")
	require.Contains(t, text, " c")
	require.True(t, strings.HasSuffix(text, " copied"), text)
}

func TestStatusBarCopyFeedbackBoundaryWidths(t *testing.T) {
	state := barState{status: statusSnapshot{session: "cur"}, statusFeedback: "copied", mru: []recentRouteDisplay{{name: "fresh"}}}
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

			drawStatusBarState(row, state, resolveStyles(nil))

			require.Equal(t, tt.want, rowText(row))
		})
	}
}

func TestStatusBarsUseCompleteSemanticSurfaces(t *testing.T) {
	theme := themeui.Theme{
		Foreground: renderer.RGB{R: 0xd8, G: 0xdc, B: 0xe8},
		Background: renderer.RGB{R: 0x08, G: 0x09, B: 0x0a},
		HasFG:      true,
		HasBG:      true,
		TrueColor:  true,
		Known:      true,
		UsePalette: true,
	}
	theme.Palette[2] = renderer.RGB{R: 0x7d, G: 0xb5, B: 0xb5}
	theme.Palette[10] = theme.Palette[2]
	theme.PaletteKnown = 1<<2 | 1<<10
	resolved := themeui.Resolve(theme, domain.ThemeAccent{Mode: domain.ThemeAccentAuto})
	styles := resolved.Styles

	t.Run("top bar maps inactive active and secondary roles while filling the row", func(t *testing.T) {
		row := make([]renderer.Cell, 48)
		status := statusSnapshot{tabs: []statusTab{{name: "idle", paneTitle: "shell"}, {name: "selected", active: true}}}
		drawTopBarSnapshot(row, status, 0, "clock", styles)

		for _, cell := range row[28:40] {
			require.True(t, cell.Style.Equal(styles.SurfaceBar), "unowned top-bar filler must retain SurfaceBar")
		}
		require.True(t, row[1].Style.Equal(styles.TabInactive), "inactive name must use TabInactive")
		require.True(t, row[5].Style.Equal(styles.TabInactiveTitle), "pane title must use contrast-derived secondary text")
		selected := strings.Index(rowText(row), "selected")
		require.GreaterOrEqual(t, selected, 0)
		require.True(t, row[selected].Style.Equal(styles.TabActive), "active tab must outrank inactive styling")
		require.True(t, row[selected].Style.Bold, "active tab stays bold")
	})

	t.Run("bottom bar maps current ranked and MRU roles without sacrificing right-side readability", func(t *testing.T) {
		row := make([]renderer.Cell, 48)
		state := barState{
			status:      statusSnapshot{session: "current"},
			bottomRight: "script ok",
			mru:         []recentRouteDisplay{{name: "new"}, {name: "middle"}, {name: "old"}},
		}
		drawStatusBarState(row, state, styles)
		for i := range len(" current ") {
			require.True(t, row[i].Style.Equal(styles.SurfaceActive), "current session must use SurfaceActive")
		}
		for index, name := range []string{"new", "middle", "old"} {
			at := strings.Index(rowText(row), name)
			require.GreaterOrEqual(t, at, 0)
			require.True(t, row[at].Style.Equal(themeui.MRUStyle(resolved.Ramp, index, 3)), "%s has the wrong MRU position surface", name)
		}
		for _, cell := range row[30:38] {
			require.True(t, cell.Style.Equal(styles.SurfaceBar), "bottom filler must retain SurfaceBar")
		}
		at := strings.Index(rowText(row), "script ok")
		require.GreaterOrEqual(t, at, 0)
		require.True(t, row[at].Style.Equal(styles.SurfaceBar), "right-side script text must remain readable on SurfaceBar")

		ranked := make([]renderer.Cell, 48)
		drawStatusBarState(ranked, barState{
			status:       statusSnapshot{session: "current"},
			rankedRecent: []rankedRecent{{rank: 1, name: "inactive"}, {rank: 2, name: "chosen", selected: true}},
		}, styles)
		inactive := strings.Index(rowText(ranked), "inactive")
		chosen := strings.Index(rowText(ranked), "chosen")
		require.True(t, ranked[inactive].Style.Equal(styles.SurfaceInactive), "unselected ranked recent must use SurfaceInactive")
		require.True(t, ranked[chosen].Style.Equal(styles.SurfaceActive), "selected ranked recent must outrank inactive styling")
	})

	t.Run("MRU zero and singleton cases retain semantic edge surfaces", func(t *testing.T) {
		empty := make([]renderer.Cell, 20)
		drawStatusBarState(empty, barState{status: statusSnapshot{session: "current"}}, styles)
		for _, cell := range empty[len(" current "):] {
			require.True(t, cell.Style.Equal(styles.SurfaceBar), "empty MRU leaves a complete SurfaceBar")
		}

		single := make([]renderer.Cell, 20)
		drawStatusBarState(single, barState{status: statusSnapshot{session: "current"}, mru: []recentRouteDisplay{{name: "only"}}}, styles)
		only := strings.Index(rowText(single), "only")
		require.GreaterOrEqual(t, only, 0)
		require.True(t, single[only].Style.Equal(styles.SurfaceRecent), "a singleton MRU must retain the 22 percent recent surface")
	})

	t.Run("attention pulse preserves its active or inactive base surface", func(t *testing.T) {
		row := make([]renderer.Cell, 32)
		drawTopBarSnapshot(row, statusSnapshot{tabs: []statusTab{{name: "active", active: true, attention: true}, {name: "idle", attention: true}}}, 1, "", styles)
		bells := make([]int, 0, 2)
		for i, cell := range row {
			if cell.Rune == ui.AttentionGlyph {
				bells = append(bells, i)
			}
		}
		require.Len(t, bells, 2)
		require.Equal(t, styles.SurfaceActive.BackgroundRGB, row[bells[0]].Style.BackgroundRGB, "active must outrank attention")
		require.Equal(t, styles.SurfaceInactive.BackgroundRGB, row[bells[1]].Style.BackgroundRGB, "attention must preserve inactive base surface")
	})
}

func TestStatusCoalescesCreateSwitchAndResize(t *testing.T) {
	p1, releasePTY1 := newBlockingPTY(t)
	p2, releasePTY2 := newBlockingPTY(t)
	clock := newCoordinatorMockClock(t, 64)
	d := newTestDaemon(t, newFactorySeq(t, p1, p2), clock.clock)
	tr, sends, releaseConn := newConn(t,
		mustHello(protocol.IntentNew, "work", domain.Size{Cols: 20, Rows: 5}),
		frameInput([]byte("\x1b ")),
		frameInput([]byte("CNT\r")),
		frameInput([]byte("\x1b1")),
		wire.Frame{Type: wire.MsgResize, Payload: mustMarshalResize(protocol.Resize{Size: domain.Size{Cols: 22, Rows: 6}})},
	)

	var hg sync.WaitGroup
	hg.Go(func() { d.handleConn(tr) })
	awaitFrame(t, sends, wire.MsgWelcome)
	first := awaitFrame(t, sends, wire.MsgOutput)
	require.Eventually(t, func() bool {
		sessions := listSessions(t, d)
		return len(sessions.Sessions) == 1 && sessions.Sessions[0].Tabs == 2
	}, 2*time.Second, time.Millisecond)

	// The queued create, switch, and resize transitions may collapse into one
	// latest-state wake. Advance every retained coordinator deadline
	// until that coalesced output is observable.
	resized := awaitCoordinatorOutput(
		t, sends, clock.timers,
		"while awaiting resized output",
		"timed out awaiting resized output frame",
	)

	for _, f := range []wire.Frame{first, resized} {
		out, err := wire.UnmarshalOutput(f.Payload)
		require.NoError(t, err)
		require.Contains(t, string(out.Data), "work")
		require.Contains(t, string(out.Data), ";7m", "active status tab should be inverse-highlighted")
	}

	releaseConn()
	releasePTY1()
	releasePTY2()
	hg.Wait()
	d.sessWg.Wait()
	d.waitNotifies()
}
