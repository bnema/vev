package daemon

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
)

func TestLayoutGenerationMutationBookkeeping(t *testing.T) {
	t.Run("focus and expanded member share one logical bump", func(t *testing.T) {
		tb := newTab(nil, domain.Size{Cols: 41, Rows: 10})
		second := newPane("pane-2", nil, domain.Size{Cols: 41, Rows: 10})
		tb.mu.Lock()
		tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}, Expanded: "pane-1"}, Focus: "pane-1"}
		tb.panes[second.id] = second
		require.Zero(t, tb.layoutGeneration)
		require.True(t, focusPlacementLocked(tb, second.id))
		require.Equal(t, uint64(1), tb.layoutGeneration)
		require.False(t, focusPlacementLocked(tb, second.id))
		require.Equal(t, uint64(1), tb.layoutGeneration)
		tb.mu.Unlock()
	})

	t.Run("size changes bump only when the value changes", func(t *testing.T) {
		tb := newTab(nil, domain.Size{Cols: 41, Rows: 10})
		tb.mu.Lock()
		if tb.size != (domain.Size{Cols: 80, Rows: 23}) {
			tb.size = domain.Size{Cols: 80, Rows: 23}
			tb.bumpLayoutGenerationLocked()
		}
		require.Equal(t, uint64(1), tb.layoutGeneration)
		if tb.size != (domain.Size{Cols: 80, Rows: 23}) {
			tb.size = domain.Size{Cols: 80, Rows: 23}
			tb.bumpLayoutGenerationLocked()
		}
		require.Equal(t, uint64(1), tb.layoutGeneration)
		tb.mu.Unlock()
	})
}

type layoutRetryClock struct{ timers chan *layoutRetryTimer }

type layoutRetryTimer struct {
	ch    chan time.Time
	delay time.Duration
}

func (c *layoutRetryClock) Now() time.Time { return time.Unix(0, 0) }

func (c *layoutRetryClock) NewTimer(delay time.Duration) ports.Timer {
	timer := &layoutRetryTimer{ch: make(chan time.Time, 1), delay: delay}
	c.timers <- timer
	return timer
}

func (t *layoutRetryTimer) C() <-chan time.Time    { return t.ch }
func (*layoutRetryTimer) Reset(time.Duration) bool { return false }
func (*layoutRetryTimer) Stop() bool               { return true }
func (t *layoutRetryTimer) fire()                  { t.ch <- time.Unix(0, 0) }

func TestLayoutRetryIsBoundedDeduplicatedAndCanceled(t *testing.T) {
	t.Run("persistent failure is bounded and reports once", func(t *testing.T) {
		pty := &transactionalResizePTY{errs: []error{errors.New("initial"), errors.New("retry 1"), errors.New("retry 2"), errors.New("retry 3")}}
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		clock := &layoutRetryClock{timers: make(chan *layoutRetryTimer, 16)}
		d.clock = clock
		tb := sess.activeTab()
		tb.mu.Lock()
		tb.size = domain.Size{Cols: 100, Rows: 20}
		tb.bumpLayoutGenerationLocked()
		tb.mu.Unlock()

		d.applyTabLayout(sess, tb)
		// A second accepted failure joins the existing worker rather than adding
		// another timer/goroutine.
		d.scheduleAcceptedTabLayoutRetry(sess, tb)
		for range maxAcceptedTabLayoutRetries {
			var timer *layoutRetryTimer
			for timer == nil {
				candidate := <-clock.timers
				if candidate.delay == minOutputRenderDeadline {
					timer = candidate
				}
			}
			timer.fire()
		}
		require.Eventually(t, func() bool {
			tb.layoutRetryMu.Lock()
			defer tb.layoutRetryMu.Unlock()
			return !tb.layoutRetryRunning
		}, time.Second, time.Millisecond)
		require.Len(t, pty.requested(), 1+maxAcceptedTabLayoutRetries)
		count := 0
		for _, notice := range d.notices.history() {
			if notice.Code == domain.NoticeResizeFailed {
				count++
			}
		}
		require.Equal(t, 1, count, "background retries must not create a notification storm")
	})

	t.Run("tab cancellation stops a waiting retry", func(t *testing.T) {
		pty := &transactionalResizePTY{errs: []error{errors.New("initial")}}
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		clock := &layoutRetryClock{timers: make(chan *layoutRetryTimer, 16)}
		d.clock = clock
		tb := sess.activeTab()
		tb.mu.Lock()
		tb.size = domain.Size{Cols: 100, Rows: 20}
		tb.bumpLayoutGenerationLocked()
		tb.mu.Unlock()
		d.applyTabLayout(sess, tb)
		var timer *layoutRetryTimer
		for timer == nil {
			candidate := <-clock.timers
			if candidate.delay == minOutputRenderDeadline {
				timer = candidate
			}
		}
		tb.cancel()
		timer.fire()
		require.Eventually(t, func() bool {
			tb.layoutRetryMu.Lock()
			defer tb.layoutRetryMu.Unlock()
			return !tb.layoutRetryRunning
		}, time.Second, time.Millisecond)
		require.Len(t, pty.requested(), 1)
	})
}

func TestFocusDirAtDoesNotApplyUncommittedCandidate(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, sess, _, _ := newManualSessionWithPTYs(t, pty)
	tb := sess.activeTab()
	second := newPane("pane-2", nil, domain.Size{Cols: 80, Rows: 23})
	tb.mu.Lock()
	tb.panes[second.id] = second
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-2"}
	tb.panes["pane-1"].resizeRetry = true
	target := tb.panes["pane-1"]
	tb.mu.Unlock()

	_, err := d.focusDirAt(sess, tb, target, layout.Right)
	require.NoError(t, err)
	require.Empty(t, pty.requested(), "an equality-mismatched candidate was not committed and must not apply")
}

func TestLayoutApplicationRetriesOnlyAcceptedFailedGeometry(t *testing.T) {
	pty := &transactionalResizePTY{errs: []error{errors.New("first resize fails")}}
	d, sess, _, _ := newManualSessionWithPTYs(t, pty)
	clock := &layoutRetryClock{timers: make(chan *layoutRetryTimer, 16)}
	d.clock = clock
	tb, p := sess.activeTab(), sess.activeTab().focusedPane()
	tb.mu.Lock()
	tb.size = domain.Size{Cols: 100, Rows: 20}
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()

	secondResize := make(chan struct{})
	var resized sync.Once
	pty.onResize = func() {
		if len(pty.requested()) == 2 {
			resized.Do(func() { close(secondResize) })
		}
	}
	d.applyTabLayout(sess, tb)

	var timer *layoutRetryTimer
	for timer == nil {
		candidate := <-clock.timers
		if candidate.delay == minOutputRenderDeadline {
			timer = candidate
		}
	}
	require.Equal(t, []domain.Size{{Cols: 100, Rows: 20}}, pty.requested())
	require.Equal(t, domain.Size{Cols: 80, Rows: 23}, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height}, "failed apply must not publish a VT resize")
	timer.fire()
	awaitSignal(t, secondResize, "accepted failure was not retried")
	require.Equal(t, []domain.Size{{Cols: 100, Rows: 20}, {Cols: 100, Rows: 20}}, pty.requested())
	require.Eventually(t, func() bool {
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.screen.Frame.Width == 100 && p.screen.Frame.Height == 20
	}, time.Second, time.Millisecond, "successful retry did not publish its VT size")
}

func TestDelayedRetryUsesFreshLayoutAfterPaneResize(t *testing.T) {
	clock := newCoordinatorMockClock(t, 8)
	// The failed apply also posts its preserved degradation warning; its toast
	// lifetime is unrelated to the coordinator deadlines driven below.
	clock.clock.EXPECT().NewTimer(6 * time.Second).Return(stubTimer{}).Once()
	failed := &transactionalResizePTY{errs: []error{errors.New("first resize fails")}}
	d, sess, ac, _ := newManualSessionWithPTYs(t, failed)
	d.clock = clock.clock
	tb := sess.activeTab()
	first := tb.focusedPane()
	second := newPane("pane-2", nil, domain.Size{Cols: 80, Rows: 23})
	tb.mu.Lock()
	tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(first.id), layout.NewLeaf(second.id)}}, Focus: first.id}
	tb.panes[second.id] = second
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()

	// A client resize commits the new layout but leaves the failed PTY on the
	// coordinator retry lane. Drive both deadlines explicitly so this ordering
	// cannot depend on wall-clock scheduling.
	d.resize(sess, ac, domain.Size{Cols: 80, Rows: 24})
	rc := sess.renderCoordinator()
	resizeTimer := awaitCoordinatorScheduledTimer(t, clock)
	resizeDone := captureResizeCallbackDone(t, rc)
	resizeTimer.ch <- time.Time{}
	awaitTestCompletion(t, resizeDone, "failed client resize did not complete")
	var retryTimer *coordinatorMockTimer
	for retryTimer == nil {
		candidate := awaitCoordinatorScheduledTimer(t, clock)
		if candidate.duration == minOutputRenderDeadline {
			retryTimer = candidate
		}
	}

	// A later pane-resize changes the solved rectangle and repairs the failed
	// PTY before the delayed callback fires. Retrying its old captured 40-column
	// rectangle would regress the PTY, VT, and published pane geometry.
	require.NoError(t, d.resizePane(daemonActionTarget{session: sess, tab: tb, pane: first}, layout.Width, resizeStepCols))
	tb.mu.Lock()
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	currentFirst := placementContent(placements, first.id)
	currentSecond := placementContent(placements, second.id)
	tb.mu.Unlock()
	require.True(t, ok)
	require.Equal(t, currentFirst, first.rect)
	require.Equal(t, currentSecond, second.rect)
	require.Equal(t, rectSize(currentFirst), domain.Size{Cols: first.screen.Frame.Width, Rows: first.screen.Frame.Height})
	require.Equal(t, []domain.Size{{Cols: 40, Rows: 22}, rectSize(currentFirst)}, failed.requested())

	rc.mu.Lock()
	retryDone := rc.retryLane.token.done
	rc.mu.Unlock()
	retryTimer.ch <- time.Time{}
	awaitTestCompletion(t, retryDone, "delayed resize retry did not complete")

	require.Equal(t, []domain.Size{{Cols: 40, Rows: 22}, rectSize(currentFirst)}, failed.requested(), "delayed retry must not apply stale PTY geometry")
	require.Equal(t, currentFirst, first.rect, "delayed retry must not publish a stale rectangle")
	require.Equal(t, rectSize(currentFirst), domain.Size{Cols: first.screen.Frame.Width, Rows: first.screen.Frame.Height}, "delayed retry must not publish a stale VT size")
}

func TestLayoutApplicationDoesNotHoldTabLock(t *testing.T) {
	d, sess, pty, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	tb.focusedPane().rect = domain.Rect{Width: 20, Height: 10}
	entered := make(chan struct{})
	release := make(chan struct{})
	pty.EXPECT().Resize(domain.Size{Cols: 41, Rows: 10}).Run(func(domain.Size) {
		close(entered)
		<-release
	}).Return(nil).Once()

	done := make(chan struct{})
	go func() {
		d.applyTabLayout(sess, tb)
		close(done)
	}()
	awaitSignal(t, entered, "PTY resize did not start")

	locked := make(chan struct{})
	go func() {
		tb.mu.Lock()
		_ = tb.layoutGeneration
		tb.mu.Unlock()
		close(locked)
	}()
	awaitSignal(t, locked, "tab lock remained held across PTY resize")
	close(release)
	awaitSignal(t, done, "layout application did not finish")
}

func TestLayoutApplicationRejectsStalePaneIdentity(t *testing.T) {
	d, sess, oldPTY, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	tb := sess.activeTab()
	oldPane := tb.focusedPane()
	oldPane.rect = domain.Rect{Width: 20, Height: 10}
	oldPane.screen.Resize(20, 10)
	entered := make(chan struct{})
	release := make(chan struct{})
	oldPTY.EXPECT().Resize(domain.Size{Cols: 41, Rows: 10}).Run(func(domain.Size) {
		close(entered)
		<-release
	}).Return(nil).Once()

	done := make(chan struct{})
	go func() {
		d.applyTabLayout(sess, tb)
		close(done)
	}()
	awaitSignal(t, entered, "stale PTY resize did not start")

	newPTY := portsmocks.NewMockPTY(t)
	replacement := newPane("pane-1", newPTY, domain.Size{Cols: 20, Rows: 10})
	newPTY.EXPECT().Resize(domain.Size{Cols: 41, Rows: 10}).Return(nil).Once()
	tb.mu.Lock()
	tb.panes["pane-1"] = replacement
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()
	close(release)
	awaitSignal(t, done, "stale layout did not retry")

	require.Equal(t, domain.Rect{Width: 20, Height: 10}, oldPane.rect, "stale pane rectangle was published")
	require.Equal(t, 20, oldPane.screen.Frame.Width, "stale pane screen was published")
	require.Equal(t, domain.Rect{Width: 41, Height: 10}, replacement.rect)
	require.Equal(t, 41, replacement.screen.Frame.Width)
}

func TestLayoutApplicationRejectsStaleCloseFocusAndSize(t *testing.T) {
	newBlockedPTY := func() (*transactionalResizePTY, <-chan struct{}, chan<- struct{}) {
		entered := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once
		return &transactionalResizePTY{onResize: func() {
			once.Do(func() {
				close(entered)
				<-release
			})
		}}, entered, release
	}

	t.Run("close removes a stale plan member", func(t *testing.T) {
		first, entered, release := newBlockedPTY()
		second := &transactionalResizePTY{}
		d, sess, _, _ := newManualSessionWithPTYs(t, first)
		tb := sess.activeTab()
		p1 := tb.focusedPane()
		p2 := newPane("pane-2", second, domain.Size{Cols: 80, Rows: 23})
		tb.mu.Lock()
		tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf(p1.id), layout.NewLeaf(p2.id)}}, Focus: p1.id}
		tb.panes[p2.id] = p2
		tb.bumpLayoutGenerationLocked()
		tb.mu.Unlock()

		done := make(chan struct{})
		go func() { d.applyTabLayout(sess, tb); close(done) }()
		awaitSignal(t, entered, "old layout apply did not block")
		closed := make(chan error, 1)
		go func() { closed <- d.closePane(sess, tb, p2.id, nil, false) }()
		require.Eventually(t, func() bool {
			tb.mu.Lock()
			defer tb.mu.Unlock()
			return tb.panes[p2.id] == nil && tb.layoutGeneration > 1
		}, time.Second, time.Millisecond, "close must mutate before its serialized apply waits")
		require.Equal(t, domain.Rect{Width: 80, Height: 23}, p1.rect, "stale plan published before release")
		close(release)
		awaitSignal(t, done, "stale layout apply did not finish")
		require.NoError(t, <-closed)

		tb.mu.Lock()
		_, exists := tb.panes[p2.id]
		tb.mu.Unlock()
		require.False(t, exists)
		require.Equal(t, domain.Rect{Width: 80, Height: 23}, p2.rect, "removed pane received a stale rectangle publication")
		require.Equal(t, domain.Size{Cols: 80, Rows: 23}, domain.Size{Cols: p2.screen.Frame.Width, Rows: p2.screen.Frame.Height}, "removed pane received a stale screen publication")
		p2.mu.Lock()
		require.False(t, p2.resizeApplying, "removed pane gate was not cancelled")
		p2.mu.Unlock()
		require.Equal(t, domain.Rect{Width: 80, Height: 23}, p1.rect)
		require.Equal(t, []domain.Size{{Cols: 40, Rows: 23}, {Cols: 80, Rows: 23}}, first.requested(), "the rejected plan must be followed by a fresh surviving-pane apply")
		require.Equal(t, []domain.Size{{Cols: 39, Rows: 23}}, second.requested(), "the removed pane may receive an obsolete external resize but must never be published")
	})

	t.Run("focus expansion replaces a stale stack placement", func(t *testing.T) {
		first, entered, release := newBlockedPTY()
		second := &transactionalResizePTY{}
		d, sess, _, _ := newManualSessionWithPTYs(t, first)
		tb := sess.activeTab()
		p1 := tb.focusedPane()
		p2 := newPane("pane-2", second, domain.Size{Cols: 80, Rows: 23})
		tb.mu.Lock()
		tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Stack, Children: []*layout.Node{layout.NewLeaf(p1.id), layout.NewLeaf(p2.id)}, Expanded: p1.id}, Focus: p1.id}
		tb.panes[p2.id] = p2
		tb.bumpLayoutGenerationLocked()
		tb.mu.Unlock()

		done := make(chan struct{})
		go func() { d.applyTabLayout(sess, tb); close(done) }()
		awaitSignal(t, entered, "old stack apply did not block")
		tb.mu.Lock()
		require.True(t, focusPlacementLocked(tb, p2.id))
		tb.mu.Unlock()
		close(release)
		awaitSignal(t, done, "stale stack apply did not retry")

		require.Equal(t, domain.Rect{Width: 80, Height: 23}, p1.rect, "collapsed pane received stale geometry")
		p1.mu.Lock()
		require.False(t, p1.resizeApplying, "collapsed pane gate was not cancelled")
		p1.mu.Unlock()
		tb.mu.Lock()
		placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
		tb.mu.Unlock()
		require.True(t, ok)
		require.Equal(t, placementContent(placements, p2.id), p2.rect)
		require.Equal(t, []domain.Size{{Cols: 80, Rows: 22}}, first.requested(), "the old expanded pane is applied only by the rejected attempt")
		require.Equal(t, []domain.Size{rectSize(p2.rect)}, second.requested())
	})

	t.Run("client size rejects and retries a blocked plan", func(t *testing.T) {
		pty, entered, release := newBlockedPTY()
		d, sess, _, _ := newManualSessionWithPTYs(t, pty)
		tb := sess.activeTab()
		p := tb.focusedPane()
		p.rect = domain.Rect{Width: 20, Height: 10}

		oldDone := make(chan struct{})
		go func() { d.applyTabLayout(sess, tb); close(oldDone) }()
		awaitSignal(t, entered, "old size apply did not block")
		resizeDone := make(chan bool, 1)
		go func() { resizeDone <- d.requestTransactionalResize(sess, nil, domain.Size{Cols: 100, Rows: 30}, true) }()
		// Session resize now stages the target privately until every external
		// apply validates and the transaction is admitted, so neither the size
		// nor pane geometry is visible while the old PTY call is blocked.
		tb.mu.Lock()
		require.Equal(t, domain.Size{Cols: 80, Rows: 23}, tb.size)
		tb.mu.Unlock()
		require.Equal(t, domain.Rect{Width: 20, Height: 10}, p.rect, "stale size published before release")
		close(release)
		awaitSignal(t, oldDone, "old size apply did not finish")
		require.True(t, <-resizeDone)
		require.Equal(t, domain.Rect{Width: 100, Height: 28}, p.rect)
		require.Equal(t, domain.Size{Cols: 100, Rows: 28}, domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height})
		require.Equal(t, []domain.Size{{Cols: 80, Rows: 23}, {Cols: 100, Rows: 28}}, pty.requested())
	})
}

func awaitSignal(t *testing.T, ch <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal(message)
	}
}

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
	stackPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 3}).Return(nil).Once()
	newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 4}).Return(newPTY, nil).Once()

	require.NoError(t, d.splitPane(sess, nil, layout.Right))

	tb.mu.Lock()
	defer tb.mu.Unlock()
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows})
	require.True(t, ok)
	require.Equal(t, layout.PaneID("pane-3"), tb.tree.Focus)
	require.Len(t, tb.panes, 3)
	require.Equal(t, domain.Rect{Y: 1, Width: 20, Height: 3}, placementContent(placements, "pane-2"))
	require.Equal(t, domain.Rect{X: 21, Width: 20, Height: 4}, placementContent(placements, "pane-3"))
}

func TestSplitPaneOpenErrorRollsBackTreeAndPaneMap(t *testing.T) {
	d, sess, _, factory := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
	cause := errors.New("open failed")
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).Return(nil, cause).Once()

	tb := sess.activeTab()
	tb.mu.Lock()
	beforeRoot, beforeFocus, beforeNext, beforePaneCount := tb.tree.Root.Leaf, tb.tree.Focus, tb.nextPaneID, len(tb.panes)
	tb.mu.Unlock()

	err := d.splitPane(sess, nil, layout.Right)
	require.Error(t, err)

	var ue *domain.UserError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, domain.NoticePaneSpawn, ue.Code)
	require.Equal(t, domain.NoticeError, ue.Severity)
	require.Equal(t, "couldn't open pane: shell failed to start", ue.Msg)
	require.ErrorIs(t, err, cause)
	require.NotContains(t, ue.Msg, cause.Error())

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

	d.applyTabLayout(sess, tb)

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

func TestFocusDirAtReturnsDepartingSpanAndMapsNoNeighbor(t *testing.T) {
	tests := []struct {
		name      string
		direction layout.Direction
		wantErr   error
		wantFocus layout.PaneID
	}{
		{name: "moves right", direction: layout.Right, wantFocus: "pane-2"},
		{name: "maps missing left neighbor", direction: layout.Left, wantErr: errNoNeighbor, wantFocus: "pane-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, _, _ := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
			tb := sess.activeTab()
			target := tb.panes["pane-1"]
			tb.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-1"}
			tb.panes["pane-1"].rect = domain.Rect{Width: 20, Height: 10}
			tb.panes["pane-2"] = newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
			tb.panes["pane-2"].rect = domain.Rect{X: 21, Width: 20, Height: 10}

			span, err := d.focusDirAt(sess, tb, target, tt.direction)

			require.ErrorIs(t, err, tt.wantErr)
			require.Equal(t, domain.Rect{Width: 20, Height: 10}, span)
			require.Equal(t, tt.wantFocus, tb.tree.Focus)
		})
	}
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

	require.NoError(t, d.focusDir(sess, ac, layout.Right, nil))

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
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 4}).Return(newPTY, nil).Once()

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
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 2}).Return(newPTY, nil).Once()
	require.NoError(t, d.stackPane(sess, nil))
	oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 2}).Return(nil).Once()

	require.NoError(t, d.focusDir(sess, nil, layout.Up, nil))

	tb := sess.activeTab()
	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Focus)
	require.Equal(t, layout.PaneID("pane-1"), tb.tree.Root.Expanded)
	thirdPTY := portsmocks.NewMockPTY(t)
	factory.EXPECT().Open(mock.Anything, "/bin/sh", []string(nil), mock.Anything, "/work", mock.Anything).Return(thirdPTY, nil).Maybe()
	err := d.stackPane(sess, nil)
	require.ErrorIs(t, err, layout.ErrTooSmall)
	require.Len(t, tb.panes, 2)

	var ue *domain.UserError
	require.ErrorAs(t, err, &ue)
	require.Equal(t, domain.NoticeLayoutTooSmall, ue.Code)
	require.Equal(t, domain.NoticeWarn, ue.Severity)
	require.Equal(t, "not enough space to split", ue.Msg)
	require.NotContains(t, ue.Msg, layout.ErrTooSmall.Error())
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
	content := domain.Rect{Width: tb.size.Cols, Height: tb.size.Rows}
	frame := composeFrame(capturedRenderState{
		reset:  true,
		layout: capturedTabLayout{area: content, focus: tb.focusedPane().id, valid: true},
		panes: []capturedPaneRenderState{{
			id: tb.focusedPane().id, frame: tb.focusedPane().screen.Frame.Clone(), placement: layout.Placement{ID: tb.focusedPane().id, Content: content}, focused: true, damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		bars: barState{status: sess.statusSegments(true)},
	}, composeCacheInput{}).frame
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

func TestSplitPaneUsesExactAuthoritativeEnvironment(t *testing.T) {
	for _, tt := range []struct {
		name string
		term terminalEnv
		want []string
	}{
		{
			name: "truecolor",
			term: terminalEnv{TrueColor: true},
			want: []string{"ORDINARY=preserved", "DUP=first", "DUP=second", "PAIR=a=b", "SHELL=/bin/first", "SHELL=/usr/bin/fish", "TERM=xterm-direct", "COLORTERM=truecolor", "TERM_PROGRAM=vev"},
		},
		{
			name: "no truecolor",
			want: []string{"ORDINARY=preserved", "DUP=first", "DUP=second", "PAIR=a=b", "SHELL=/bin/first", "SHELL=/usr/bin/fish", "TERM=xterm-256color", "TERM_PROGRAM=vev"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, oldPTY, factory := newSplitTestDaemon(t, domain.Size{Cols: 41, Rows: 10})
			d.shellOverride = false
			sess.terminal = tt.term
			sess.env = []string{"ORDINARY=preserved", "DUP=first", "DUP=second", "PAIR=a=b", "SHELL=/bin/first", "SHELL=/usr/bin/fish", "TERM=old", "COLORTERM=old", "TERM_PROGRAM=old", "VEV=old"}
			newPTY := portsmocks.NewMockPTY(t)
			oldPTY.EXPECT().Resize(domain.Size{Cols: 20, Rows: 10}).Return(nil).Once()
			newPTY.EXPECT().Read(mock.Anything).RunAndReturn(blockingRead(t)).Maybe()
			var command string
			var gotEnv []string
			factory.EXPECT().Open(mock.Anything, mock.Anything, []string(nil), mock.Anything, "/work", domain.Size{Cols: 20, Rows: 10}).RunAndReturn(
				func(_ context.Context, gotCommand string, _ []string, env []string, _ string, _ domain.Size) (ports.PTY, error) {
					command = gotCommand
					gotEnv = append([]string(nil), env...)
					return newPTY, nil
				},
			).Once()

			require.NoError(t, d.splitPane(sess, nil, layout.Right))
			tb := sess.activeTab()
			tb.mu.Lock()
			paneID := tb.panes["pane-2"].stableID
			tabID := tb.stableID
			tb.mu.Unlock()
			require.Equal(t, "/usr/bin/fish", command)
			require.Equal(t, append(tt.want, "VEV=session=s,tab="+tabID+",pane="+paneID), gotEnv)
		})
	}
}
