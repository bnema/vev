package daemon

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

func TestRefreshPaneTitleUsesForegroundProcessComm(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().ForegroundPgid().Return(1234, nil).Once()
	_, sess, _, _ := newManualSessionWithPTYs(t, pty)
	d := newTestDaemon(t, nil, stubClock{})
	d.shell = "/bin/zsh"
	d.procComm = func(pid int) (string, error) {
		require.Equal(t, 1234, pid)
		return "vim\n", nil
	}

	title := d.refreshPaneTitle(sess, "pane-1")

	require.Equal(t, "vim", title)
	require.Equal(t, "vim", sess.activeTab().focusedPane().title)
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

func TestRefreshPaneTitleFallsBackToShellBase(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().ForegroundPgid().Return(0, errors.New("no foreground process")).Once()
	_, sess, _, _ := newManualSessionWithPTYs(t, pty)
	d := newTestDaemon(t, nil, stubClock{})
	d.shell = "/usr/bin/fish"
	d.procComm = func(int) (string, error) { return "", errors.New("unused") }

	title := d.refreshPaneTitle(sess, "pane-1")

	require.Equal(t, "fish", title)
	require.Equal(t, "fish", sess.activeTab().focusedPane().title)
}
