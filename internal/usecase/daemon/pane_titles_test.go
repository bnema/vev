package daemon

import (
	"errors"
	"testing"

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
