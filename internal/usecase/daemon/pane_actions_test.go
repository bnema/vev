package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestSplitPaneCreatesFocusedShellInRequestedPosition(t *testing.T) {
	tests := []struct {
		name      string
		dir       layout.Direction
		size      domain.Size
		wantNew   domain.Rect
		wantOld   domain.Rect
		isNewSide func(oldRect, newRect domain.Rect) bool
	}{
		{name: "right", dir: layout.Right, size: domain.Size{Cols: 41, Rows: 10}, wantOld: domain.Rect{Width: 20, Height: 10}, wantNew: domain.Rect{X: 21, Width: 20, Height: 10}, isNewSide: func(oldRect, newRect domain.Rect) bool { return newRect.X > oldRect.X }},
		{name: "left", dir: layout.Left, size: domain.Size{Cols: 41, Rows: 10}, wantOld: domain.Rect{X: 21, Width: 20, Height: 10}, wantNew: domain.Rect{Width: 20, Height: 10}, isNewSide: func(oldRect, newRect domain.Rect) bool { return newRect.X < oldRect.X }},
		{name: "down", dir: layout.Down, size: domain.Size{Cols: 80, Rows: 5}, wantOld: domain.Rect{Width: 80, Height: 2}, wantNew: domain.Rect{Y: 3, Width: 80, Height: 2}, isNewSide: func(oldRect, newRect domain.Rect) bool { return newRect.Y > oldRect.Y }},
		{name: "up", dir: layout.Up, size: domain.Size{Cols: 80, Rows: 5}, wantOld: domain.Rect{Y: 3, Width: 80, Height: 2}, wantNew: domain.Rect{Width: 80, Height: 2}, isNewSide: func(oldRect, newRect domain.Rect) bool { return newRect.Y < oldRect.Y }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, oldPTY, factory := newSplitTestDaemon(t, tt.size)
			newPTY := portsmocks.NewMockPTY(t)
			oldPTY.EXPECT().Resize(rectSize(tt.wantOld)).Return(nil).Once()
			newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
			factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", rectSize(tt.wantNew)).Return(newPTY, nil).Once()

			require.NoError(t, d.splitPane(sess, nil, tt.dir))

			tb := sess.activeTab()
			tb.mu.Lock()
			defer tb.mu.Unlock()
			require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
			require.Len(t, tb.panes, 2)
			oldRect := tb.panes["pane-1"].rect
			newRect := tb.panes["pane-2"].rect
			require.Equal(t, tt.wantOld, oldRect)
			require.Equal(t, tt.wantNew, newRect)
			require.True(t, tt.isNewSide(oldRect, newRect))
		})
	}
}

func TestSplitPaneRightFromStackSplitsWholeStack(t *testing.T) {
	d, sess, _, factory := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 4})
	tb := sess.activeTab()
	stackPTY := portsmocks.NewMockPTY(t)
	stackPane := newPane("pane-2", stackPTY, domain.Size{Cols: 41, Rows: 2})
	tb.panes[stackPane.id] = stackPane
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}, Expanded: "pane-2"}, Focus: "pane-2"}
	tb.nextPaneID = 3
	newPTY := portsmocks.NewMockPTY(t)
	stackPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 2}).Return(nil).Once()
	newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 4}).Return(newPTY, nil).Once()

	require.NoError(t, d.splitPane(sess, nil, layout.Right))

	tb.mu.Lock()
	defer tb.mu.Unlock()
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	require.True(t, ok)
	require.Equal(t, layout.PaneID("pane-3"), tb.tree.Focus)
	require.Len(t, tb.panes, 3)
	require.Equal(t, domain.Rect{Y: 2, Width: 20, Height: 2}, placementContent(placements, "pane-2"))
	require.Equal(t, domain.Rect{X: 21, Width: 20, Height: 4}, placementContent(placements, "pane-3"))
}

func TestSplitPaneOpenErrorRollsBackTreeAndPaneMap(t *testing.T) {
	d, sess, _, factory := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).Return(nil, errors.New("open failed")).Once()

	tb := sess.activeTab()
	tb.mu.Lock()
	beforeRoot, beforeFocus, beforeNext, beforePaneCount := tb.tree.Root.Leaf, tb.tree.Focus, tb.nextPaneID, len(tb.panes)
	tb.mu.Unlock()

	require.Error(t, d.splitPane(sess, nil, layout.Right))

	tb.mu.Lock()
	defer tb.mu.Unlock()
	require.Equal(t, beforeRoot, tb.tree.Root.Leaf)
	require.Equal(t, beforeFocus, tb.tree.Focus)
	require.Equal(t, beforeNext+1, tb.nextPaneID)
	require.Len(t, tb.panes, beforePaneCount)
	require.Nil(t, tb.panes["pane-2"])
}

func TestApplyLayoutResizesPTYsAndScreens(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	newPTY := portsmocks.NewMockPTY(t)
	newPane := newPane("pane-2", newPTY, domain.Size{Cols: 41, Rows: 10})
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes["pane-2"] = newPane
	oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Once()
	newPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Once()

	tb.mu.Lock()
	d.applyLayoutLocked(tb)
	tb.mu.Unlock()

	require.Equal(t, 20, tb.panes["pane-1"].screen.Frame.Width)
	require.Equal(t, 20, tb.panes["pane-2"].screen.Frame.Width)
	require.Equal(t, domain.Rect{X: 21, Width: 20, Height: 10}, tb.panes["pane-2"].rect)
}

func TestResizeReflowsAllPanes(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	newPTY := portsmocks.NewMockPTY(t)
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes["pane-2"] = newPane("pane-2", newPTY, domain.Size{Cols: 20, Rows: 10})
	tb.panes["pane-1"].rect = domain.Rect{Width: 20, Height: 10}
	tb.panes["pane-2"].rect = domain.Rect{X: 21, Width: 20, Height: 10}
	oldPTY.EXPECT().Resize(domain.Size{Cols: 30, Rows: 18}).Return(nil).Once()
	newPTY.EXPECT().Resize(domain.Size{Cols: 29, Rows: 18}).Return(nil).Once()

	d.resize(sess, nil, domain.Size{Cols: 60, Rows: 20})

	require.Equal(t, domain.Size{Cols: 60, Rows: 18}, tb.size)
	require.Equal(t, domain.Rect{Width: 30, Height: 18}, tb.panes["pane-1"].rect)
	require.Equal(t, domain.Rect{X: 31, Width: 29, Height: 18}, tb.panes["pane-2"].rect)
	require.Equal(t, 30, tb.panes["pane-1"].screen.Frame.Width)
	require.Equal(t, 29, tb.panes["pane-2"].screen.Frame.Width)
}

func TestFocusDirMovesFocusAndExitsCopyMode(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	newPTY := portsmocks.NewMockPTY(t)
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-1"}
	tb.panes["pane-2"] = newPane("pane-2", newPTY, domain.Size{Cols: 20, Rows: 10})
	tb.panes["pane-1"].rect = domain.Rect{Width: 20, Height: 10}
	tb.panes["pane-2"].rect = domain.Rect{X: 21, Width: 20, Height: 10}
	tr, _ := newCapturingTransport(t)
	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 41, Rows: 12}}
	ac.initOverlays()
	ac.overlays.copyMode = &scopy.Mode{}

	require.NoError(t, d.focusDir(sess, ac, layout.Right))

	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	require.Nil(t, ac.overlays.copyMode)
	oldPTY.AssertNotCalled(t, "Resize", mock.Anything)
	newPTY.AssertNotCalled(t, "Resize", mock.Anything)
}

func TestCloseFocusedPaneRemovesPaneReflowsAndIsIdempotent(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	closingPTY := portsmocks.NewMockPTY(t)
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes["pane-2"] = newPane("pane-2", closingPTY, domain.Size{Cols: 20, Rows: 10})
	tb.panes["pane-1"].rect = domain.Rect{Width: 20, Height: 10}
	tb.panes["pane-2"].rect = domain.Rect{X: 21, Width: 20, Height: 10}
	oldPTY.EXPECT().Resize(domain.Size{Cols: 41, Rows: 10}).Return(nil).Once()
	closingPTY.EXPECT().Close().Return(nil).Once()

	require.NoError(t, d.closeFocusedPane(sess, nil))
	require.NoError(t, d.closePane(sess, tb, "pane-2", nil, true))

	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Focus)
	require.Len(t, tb.panes, 1)
	require.Equal(t, domain.Rect{Width: 41, Height: 10}, tb.panes["pane-1"].rect)
}

func TestReapPaneSharesClosePathAndIsIdempotent(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	reapedPTY := portsmocks.NewMockPTY(t)
	reaped := newPane("pane-2", reapedPTY, domain.Size{Cols: 20, Rows: 10})
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes["pane-2"] = reaped
	tb.panes["pane-1"].rect = domain.Rect{Width: 20, Height: 10}
	tb.panes["pane-2"].rect = domain.Rect{X: 21, Width: 20, Height: 10}
	oldPTY.EXPECT().Resize(domain.Size{Cols: 41, Rows: 10}).Return(nil).Once()
	reapedPTY.EXPECT().Close().Return(nil).Once()

	d.reapPane(sess, tb, reaped)
	d.reapPane(sess, tb, reaped)

	require.Len(t, tb.panes, 1)
	require.NotContains(t, tb.panes, layout.PaneID("pane-2"))
}

func TestClosePaneKeepsTabOpenWhenPendingSpawnLeafExists(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	oldPTY.EXPECT().Close().Return(nil).Once()

	require.NoError(t, d.closePane(sess, tb, "pane-1", nil, false))

	require.Len(t, sess.tabs, 1)
	require.Empty(t, tb.panes)
	require.True(t, layout.ContainsLeaf(tb.tree.Root, "pane-2"))
}

func TestCloseFocusedLastPaneDelegatesToCloseTab(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	otherPTY := portsmocks.NewMockPTY(t)
	other := newTab(otherPTY, domain.Size{Cols: 41, Rows: 10})
	sess.tabs = append(sess.tabs, other)
	d.sessions[sess.id] = sess
	oldPTY.EXPECT().Close().Return(nil).Once()

	require.NoError(t, d.closeFocusedPane(sess, nil))

	require.Len(t, sess.tabs, 1)
	require.Same(t, other, sess.tabs[0])
}

func TestStackPaneCreatesStackAndToggleRestoresSplit(t *testing.T) {
	d, sess, oldPTY, factory := newSplitTestDaemon(t, domain.Size{Cols: 20, Rows: 5})
	newPTY := portsmocks.NewMockPTY(t)
	newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 3}).Return(newPTY, nil).Once()

	require.NoError(t, d.stackPane(sess, nil))
	tb := sess.activeTab()
	require.Equal(t, layout.Stack, tb.tree.Root.Kind)
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Root.Expanded)
	require.Len(t, tb.panes, 2)

	oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 2}).Return(nil).Once()
	newPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 2}).Return(nil).Once()
	require.NoError(t, d.toggleStack(sess, nil))
	require.Equal(t, layout.Split, tb.tree.Root.Kind)
	require.Equal(t, layout.Vertical, tb.tree.Root.Dir)
}

func TestStackFocusWalkExpandsAndOverflowRefuses(t *testing.T) {
	d, sess, oldPTY, factory := newSplitTestDaemon(t, domain.Size{Cols: 20, Rows: 3})
	newPTY := portsmocks.NewMockPTY(t)
	newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 1}).Return(newPTY, nil).Once()
	require.NoError(t, d.stackPane(sess, nil))
	oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 1}).Return(nil).Once()

	require.NoError(t, d.focusDir(sess, nil, layout.Up))

	tb := sess.activeTab()
	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Focus)
	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Root.Expanded)
	thirdPTY := portsmocks.NewMockPTY(t)
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", mock.Anything).Return(thirdPTY, nil).Maybe()
	require.ErrorIs(t, d.stackPane(sess, nil), layout.ErrTooSmall)
	require.Len(t, tb.panes, 2)
}

func newSplitTestDaemon(t *testing.T, sz domain.Size) (*Daemon, *session, *portsmocks.MockPTY, *portsmocks.MockPTYFactory) {
	t.Helper()
	factory := portsmocks.NewMockPTYFactory(t)
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(time.Unix(0, 0)).Maybe()
	d := New(factory, clock, slog.New(slog.NewTextHandler(io.Discard, nil)), WithShell("/bin/sh", nil))
	oldPTY := portsmocks.NewMockPTY(t)
	tb := newTab(oldPTY, sz)
	tctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	tb.ctx, tb.cancel = tctx, cancel
	for _, p := range tb.panes {
		p.ctx, p.cancel = tctx, cancel
	}
	sess := &session{id: "s", name: "s", cwd: "/work", tabs: []*tab{tb}, ctx: tctx, cancel: cancel}
	return d, sess, oldPTY, factory
}

func TestCloseOriginalPaneLeavesSurvivorFunctional(t *testing.T) {
	writes := make(chan []byte, 1)
	d, sess, oldPTY, factory := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	newPTY, releaseNew := newBlockingPTYWithWrites(t, writes)
	defer releaseNew()
	oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Once()
	newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).Return(newPTY, nil).Once()

	require.NoError(t, d.splitPane(sess, nil, layout.Right))
	tb := sess.activeTab()
	tb.mu.Lock()
	tb.tree.Focus = "pane-1"
	tb.mu.Unlock()
	oldPTY.EXPECT().Close().Return(nil).Once()

	require.NoError(t, d.closePane(sess, tb, "pane-1", nil, false))
	require.Equal(t, layout.PaneID("pane-2"), tb.focusedPane().id)

	ac := &attachedClient{}
	ac.initOverlays()
	ac.setSession(sess)
	daemonKeyHandler{d: d, ac: ac}.Forward([]byte("Z"))
	require.Equal(t, []byte("Z"), <-writes)
	tb.focusedPane().screen.Write([]byte("survivor"))
	frame, _ := composeClientFrame(sess, tb, false, "")
	require.Contains(t, frameRowString(frame, 1), "survivor")
}

func blockingRead(t *testing.T) func([]byte) (int, error) {
	t.Helper()
	ch := make(chan struct{})
	return func([]byte) (int, error) {
		<-ch
		return 0, io.EOF
	}
}

func TestSplitPaneInheritsTerminalEnv(t *testing.T) {
	tests := []struct {
		name           string
		term           terminalEnv
		wantContain    []string
		wantNotContain []string
	}{
		{
			name:        "truecolor",
			term:        terminalEnv{TrueColor: true},
			wantContain: []string{"TERM=xterm-direct", "COLORTERM=truecolor", "TERM_PROGRAM=vev"},
		},
		{
			name:           "no truecolor",
			term:           terminalEnv{},
			wantContain:    []string{"TERM=xterm-256color", "TERM_PROGRAM=vev"},
			wantNotContain: []string{"TERM=xterm-direct", "COLORTERM=truecolor"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, oldPTY, factory := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
			sess.terminal = tt.term
			newPTY := portsmocks.NewMockPTY(t)
			oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Once()
			newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
			var gotEnv []string
			factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).RunAndReturn(
				func(_ context.Context, _ string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
					gotEnv = append([]string(nil), env...)
					return newPTY, nil
				},
			).Once()

			require.NoError(t, d.splitPane(sess, nil, layout.Right))
			for _, want := range tt.wantContain {
				require.Contains(t, gotEnv, want)
			}
			for _, notWant := range tt.wantNotContain {
				require.NotContains(t, gotEnv, notWant)
			}
		})
	}
}
