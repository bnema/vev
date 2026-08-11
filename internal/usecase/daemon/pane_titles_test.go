package daemon

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestFormatPaneTitle(t *testing.T) {
	tests := []struct {
		name, process, terminal, fallback, want string
	}{
		{name: "process prepends non-prefixed terminal title", process: "nvim", terminal: "project/main.go", fallback: "sh", want: "nvim: project/main.go"},
		{name: "process does not duplicate its terminal-title prefix", process: "zot", terminal: "zot: session title", fallback: "sh", want: "zot: session title"},
		{name: "process only", process: "nvim", fallback: "sh", want: "nvim"},
		{name: "terminal only", terminal: "project/main.go", fallback: "sh", want: "project/main.go"},
		{name: "fallback", fallback: "fish", want: "fish"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, formatPaneTitle(tt.process, tt.terminal, tt.fallback))
		})
	}
}

func TestComposeTabTitle(t *testing.T) {
	tests := []struct {
		name, tabName, paneTitle, want string
	}{
		{name: "name and title", tabName: "api", paneTitle: "nvim: server.go — vev", want: "api (nvim: server.go — vev)"},
		{name: "empty pane title", tabName: "api", paneTitle: "", want: "api"},
		{name: "pane title equals name", tabName: "api", paneTitle: "api", want: "api"},
		{name: "unnamed tab style", tabName: "1", paneTitle: "zsh", want: "1 (zsh)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, composeTabTitle(tt.tabName, tt.paneTitle))
		})
	}
}

func TestPaneTerminalTitleGenerationChangesOnlyWhenTitleChanges(t *testing.T) {
	p := newPane("pane", nil, domain.Size{Cols: 20, Rows: 5})
	p.mu.Lock()
	p.screen.Write([]byte("\x1b]2;project/main.go\a"))
	p.refreshTerminalTitleLocked()
	first := p.title.generation
	p.refreshTerminalTitleLocked()
	second := p.title.generation
	p.screen.Write([]byte("\x1b]0;other\a"))
	p.refreshTerminalTitleLocked()
	third := p.title.generation
	p.mu.Unlock()

	require.Equal(t, uint64(1), first)
	require.Equal(t, first, second)
	require.Equal(t, first+1, third)
}

func TestRefreshPaneTitleUsesForegroundProcessComm(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().ForegroundPgid().Return(1234, nil).Twice()
	_, sess, _, _ := newManualSessionWithPTYs(t, pty)
	clk := portsmocks.NewMockClock(t)
	clk.EXPECT().Now().Return(time.Time{}).Maybe()
	d := newTestDaemon(t, nil, clk)
	d.shell = "/bin/zsh"
	d.procComm = func(pid int) (string, error) {
		require.Equal(t, 1234, pid)
		return "vim\n", nil
	}

	title := d.refreshPaneTitle(sess, "pane-1")
	require.Equal(t, "vim", title)

	p := testAttachmentTab(sess).focusedPane()
	p.mu.Lock()
	p.screen.Write([]byte("\x1b]2;project/main.go\a"))
	p.refreshTerminalTitleLocked()
	p.mu.Unlock()
	title = d.refreshPaneDisplayTitle(sess, p, true)

	require.Equal(t, "vim: project/main.go", title)
	require.Equal(t, "vim", p.title.processName)
}

func TestRefreshPaneTitleUsesProvidedOwningTab(t *testing.T) {
	secondPTY := portsmocks.NewMockPTY(t)
	secondPTY.EXPECT().ForegroundPgid().Return(222, nil).Once()
	_, sess, _, _ := newManualSessionWithPTYs(t, nil)
	second := newTabWithStableID("second-tab", "second-pane", secondPTY, domain.Size{Cols: 80, Rows: 23})
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.mu.Unlock()
	d := newTestDaemon(t, nil, stubClock{})
	d.procComm = func(pid int) (string, error) {
		require.Equal(t, 222, pid)
		return "vim", nil
	}

	require.Equal(t, "vim", d.refreshPaneTitle(sess, "pane-1", second))
	secondPane := second.panes["pane-1"]
	secondPane.mu.Lock()
	require.Equal(t, "vim", secondPane.title.processName)
	secondPane.mu.Unlock()
}

func TestRefreshPaneTitleCachesByTTLAndRefreshesOnFocus(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().ForegroundPgid().Return(1234, nil).Twice()
	_, sess, _, _ := newManualSessionWithPTYs(t, pty)
	clk := portsmocks.NewMockClock(t)
	now := time.Unix(10, 0)
	clk.EXPECT().Now().RunAndReturn(func() time.Time { return now }).Maybe()
	d := newTestDaemon(t, nil, clk)
	var calls atomic.Int32
	d.procComm = func(pid int) (string, error) {
		require.Equal(t, 1234, pid)
		calls.Add(1)
		return "vim\n", nil
	}

	require.Equal(t, "vim", d.refreshPaneTitle(sess, "pane-1"))
	now = now.Add(paneTitleCacheTTL / 2)
	require.Equal(t, "vim", d.refreshPaneTitle(sess, "pane-1"))
	require.Equal(t, int32(1), calls.Load(), "paint refresh inside TTL should use cached title")
	require.Equal(t, "vim", d.refreshPaneTitleOnFocus(sess, "pane-1"))
	require.Equal(t, int32(2), calls.Load(), "focus refresh should bypass TTL")
}

func TestRefreshFloatingPaneTitlePreservesConfiguredCommandFallback(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().ForegroundPgid().Return(0, errors.New("no foreground process")).Twice()
	clk := portsmocks.NewMockClock(t)
	clk.EXPECT().Now().Return(time.Time{}).Maybe()
	d := newTestDaemon(t, nil, clk)
	d.shell = "/usr/bin/fish"
	d.procComm = func(int) (string, error) { return "", errors.New("unused") }
	p := newPane("floating", pty, domain.Size{Cols: 20, Rows: 5})
	p.title.displayFallback = floatingCommandFallback("btop --utf", d.shell)

	require.Equal(t, "btop", d.refreshPaneDisplayTitle(nil, p, false))
	require.Equal(t, "btop", d.refreshPaneDisplayTitle(nil, p, false), "cached refresh must retain the pane-owned fallback")
	require.Equal(t, "btop", d.refreshPaneDisplayTitle(nil, p, true), "process refresh must retain the pane-owned fallback")
	p.mu.Lock()
	require.Equal(t, "btop", p.title.displayFallback)
	p.mu.Unlock()
}

func TestRefreshFloatingPaneTitleUsesShellFallbackForEmptyCommand(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	d.shell = "/usr/local/bin/zsh"
	p := newPane("floating", nil, domain.Size{Cols: 20, Rows: 5})
	p.title.displayFallback = floatingCommandFallback("", d.shell)

	require.Equal(t, "zsh", d.refreshPaneDisplayTitle(nil, p, true))
}

func TestRefreshPaneTitleRetainsNormalPaneShellFallback(t *testing.T) {
	d, sess, _, _ := newManualSessionWithPTYs(t, nil)
	p := testAttachmentTab(sess).focusedPane()
	p.setDisplayFallback("fish")

	d.shell = "/bin/zsh"
	require.Equal(t, "fish", d.refreshPaneTitle(sess, "pane-1"))
	p.mu.Lock()
	firstGeneration := p.title.generation
	p.mu.Unlock()
	require.Equal(t, "fish", d.refreshPaneTitle(sess, "pane-1"))
	p.mu.Lock()
	require.Equal(t, firstGeneration, p.title.generation, "a title refresh must preserve the pane-owned shell fallback")
	p.mu.Unlock()
}

func TestRefreshPaneTitleLookupFailureKeepsProcessNameEmpty(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().ForegroundPgid().Return(0, errors.New("no foreground process")).Twice()
	_, sess, _, _ := newManualSessionWithPTYs(t, pty)
	clk := portsmocks.NewMockClock(t)
	clk.EXPECT().Now().Return(time.Time{}).Maybe()
	d := newTestDaemon(t, nil, clk)
	d.shell = "/usr/bin/fish"
	d.procComm = func(int) (string, error) { return "", errors.New("unused") }
	p := testAttachmentTab(sess).focusedPane()

	p.mu.Lock()
	p.screen.Write([]byte("\x1b]2;terminal\a"))
	p.refreshTerminalTitleLocked()
	p.mu.Unlock()
	require.Equal(t, "terminal", d.refreshPaneDisplayTitle(sess, p, true))
	p.mu.Lock()
	require.Empty(t, p.title.processName, "a lookup failure must not become process state")
	p.mu.Unlock()

	p.mu.Lock()
	p.screen.Write([]byte("\x1b]2;\a"))
	p.refreshTerminalTitleLocked()
	p.mu.Unlock()
	require.Equal(t, "sh", d.refreshPaneDisplayTitle(sess, p, true), "pointer refresh must retain its pane-owned fallback")
}
