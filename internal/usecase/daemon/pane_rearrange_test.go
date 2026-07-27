package daemon

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestMutateTargetLayoutPublishesOnlyChangedCandidates(t *testing.T) {
	tests := []struct {
		name            string
		mutate          func(*layout.Tree, domain.Rect) (bool, error)
		wantChanged     bool
		wantGeneration  uint64
		wantSnapshotGen uint64
		wantResizes     bool
	}{
		{
			name: "changed candidate publishes once",
			mutate: func(candidate *layout.Tree, area domain.Rect) (bool, error) {
				return candidate.ConsumeOrExpelPane("pane-1", layout.Right, area)
			},
			wantChanged:     true,
			wantGeneration:  1,
			wantSnapshotGen: 1,
			wantResizes:     true,
		},
		{
			name: "unchanged candidate is discarded",
			mutate: func(candidate *layout.Tree, _ domain.Rect) (bool, error) {
				candidate.Focus = "pane-2"
				return false, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
			before := h.tab.tree.Clone()

			changed, err := h.daemon.mutateTargetLayoutChanged(h.target("pane-1"), true, tt.mutate)

			require.NoError(t, err)
			require.Equal(t, tt.wantChanged, changed)
			h.tab.mu.Lock()
			require.Equal(t, tt.wantGeneration, h.tab.layoutGeneration)
			if tt.wantChanged {
				require.NotEqual(t, before, h.tab.tree)
			} else {
				require.Equal(t, before, h.tab.tree, "changed=false must not publish a mutated candidate")
			}
			h.tab.mu.Unlock()
			h.session.snapshotMu.Lock()
			require.Equal(t, tt.wantSnapshotGen, h.session.snapshotGeneration)
			h.session.snapshotMu.Unlock()
			if tt.wantResizes {
				require.NotZero(t, h.totalResizes())
			} else {
				require.Zero(t, h.totalResizes())
			}
		})
	}
}

func TestMutateTargetLayoutRejectsStaleTargetsBeforeMutation(t *testing.T) {
	tests := []struct {
		name  string
		stale func(*paneRearrangeHarness) daemonActionTarget
	}{
		{
			name: "session",
			stale: func(h *paneRearrangeHarness) daemonActionTarget {
				target := h.target("pane-1")
				h.daemon.sessions[h.session.id] = &session{id: h.session.id}
				return target
			},
		},
		{
			name: "tab",
			stale: func(h *paneRearrangeHarness) daemonActionTarget {
				target := h.target("pane-1")
				h.session.tabs = []*tab{newTab(nil, h.tab.size)}
				return target
			},
		},
		{
			name: "pane identity",
			stale: func(h *paneRearrangeHarness) daemonActionTarget {
				target := h.target("pane-1")
				h.tab.panes[target.pane.id] = newPane(target.pane.id, nil, h.tab.size)
				return target
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
			before := h.tab.tree.Clone()
			called := false

			changed, err := h.daemon.mutateTargetLayoutChanged(tt.stale(h), true, func(*layout.Tree, domain.Rect) (bool, error) {
				called = true
				return true, nil
			})

			require.False(t, changed)
			require.ErrorIs(t, err, layout.ErrNotFound)
			require.False(t, called, "stale identity must be rejected before invoking the mutation")
			require.Equal(t, before, h.tab.tree)
			require.Zero(t, h.tab.layoutGeneration)
			require.Zero(t, h.totalResizes())
		})
	}
}

func TestPaneRearrangeResizeRunsWithoutDaemonStateLocks(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	locksAvailable := make(chan bool, 8)
	for _, tracked := range h.ptys {
		tracked.onResize = func() { locksAvailable <- h.stateLocksAvailable() }
	}

	done := make(chan error, 1)
	go func() {
		done <- (daemonActions{d: h.daemon}).Run(daemonActionRequest{
			kind: daemonActionConsumeOrExpelPane, target: h.target("pane-1"), direction: layout.Right,
		})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("pane rearrange blocked while PTY.Resize attempted to re-enter daemon state locks")
	}
	require.NotZero(t, h.totalResizes())
	close(locksAvailable)
	for available := range locksAvailable {
		require.True(t, available, "PTY.Resize ran while a daemon, session, tab, or pane state lock was held")
	}
}

func TestPaneRearrangeUsesExplicitTargetAndPreservesPaneIdentity(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	h.tab.mu.Lock()
	h.tab.tree.Focus = "pane-3"
	before := make(map[layout.PaneID]*pane, len(h.tab.panes))
	for id, p := range h.tab.panes {
		before[id] = p
	}
	h.tab.mu.Unlock()

	err := (daemonActions{d: h.daemon}).Run(daemonActionRequest{
		kind: daemonActionConsumeOrExpelPane, target: h.target("pane-1"), direction: layout.Right,
	})

	require.NoError(t, err)
	h.tab.mu.Lock()
	require.Equal(t, layout.PaneID("pane-1"), h.tab.tree.Focus, "the explicit non-focused target becomes focused")
	require.Len(t, h.tab.panes, len(before))
	for id, p := range before {
		require.Same(t, p, h.tab.panes[id], "pane %s identity changed", id)
	}
	h.tab.mu.Unlock()
	h.factory.AssertNotCalled(t, "Open")
	for _, tracked := range h.ptys {
		require.Zero(t, tracked.closeCount())
	}
}

func TestPaneRearrangeEdgeNoopHasExactSilentInvariants(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	ac := &attachedClient{}
	ac.setSession(h.session)
	h.session.mu.Lock()
	h.session.client = ac
	h.session.mu.Unlock()
	invalidations := make(chan renderInvalidation, 2)
	rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(inv renderInvalidation) { invalidations <- inv }})
	rc.attach(ac)
	h.session.installRenderCoordinator(rc)
	before := h.snapshot()
	request := daemonActionRequest{
		kind: daemonActionConsumeOrExpelPane, target: h.target("pane-1"), direction: layout.Left,
	}

	err := (daemonActions{d: h.daemon}).Run(request)
	require.ErrorIs(t, err, errPaneRearrangeNoop, "the daemon action must retain an exact no-change sentinel")
	require.NoError(t, (paletteExec{d: h.daemon, sess: h.session, ac: ac}).runAction(request), "palette normalizes the silent sentinel")

	require.Equal(t, before, h.snapshot(), "edge no-op changed tree, generations, snapshot state, pane identity, geometry, or VT size")
	require.Zero(t, h.totalResizes())
	h.factory.AssertNotCalled(t, "Open")
	for _, tracked := range h.ptys {
		require.Zero(t, tracked.closeCount())
	}
	require.Empty(t, h.daemon.notices.history())
	requireNoInvalidation(t, invalidations)
}

func TestPaneRearrangeMapsUnsupportedAndTooSmallToUserNoticesAtomically(t *testing.T) {
	tests := []struct {
		name  string
		size  domain.Size
		tree  *layout.Tree
		pane  layout.PaneID
		dir   layout.Direction
		cause error
	}{
		{
			name: "unsupported layout",
			size: domain.Size{Cols: 80, Rows: 22},
			tree: &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Vertical, Children: []*layout.Node{
				layout.NewLeaf("pane-1"),
				{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-2"), layout.NewLeaf("pane-3")}},
			}}, Focus: "pane-1"},
			pane: "pane-1", dir: layout.Right, cause: layout.ErrUnsupportedColumnLayout,
		},
		{
			name: "too small",
			size: domain.Size{Cols: 40, Rows: 5},
			tree: &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Vertical, Children: []*layout.Node{
				layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2"),
			}}, Focus: "pane-1"},
			pane: "pane-1", dir: layout.Right, cause: layout.ErrTooSmall,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newPaneRearrangeHarness(t, tt.size, tt.tree)
			before := h.snapshot()

			err := (daemonActions{d: h.daemon}).Run(daemonActionRequest{
				kind: daemonActionConsumeOrExpelPane, target: h.target(tt.pane), direction: tt.dir,
			})

			require.ErrorIs(t, err, tt.cause)
			var userErr *domain.UserError
			require.ErrorAs(t, err, &userErr)
			require.Equal(t, domain.NoticeLayoutTooSmall, userErr.Code)
			require.Equal(t, domain.NoticeWarn, userErr.Severity)
			require.Equal(t, before, h.snapshot(), "failed rearrange must be atomic")
			require.Zero(t, h.totalResizes())
			h.factory.AssertNotCalled(t, "Open")
			for _, tracked := range h.ptys {
				require.Zero(t, tracked.closeCount())
			}
		})
	}
}

func TestPaneRearrangeChangedActionPublishesOnce(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	ac := &attachedClient{}
	ac.setSession(h.session)
	h.session.mu.Lock()
	h.session.client = ac
	h.session.mu.Unlock()
	invalidations := make(chan renderInvalidation, 2)
	rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(inv renderInvalidation) { invalidations <- inv }})
	rc.attach(ac)
	h.session.installRenderCoordinator(rc)

	err := (controlExec{d: h.daemon, sess: h.session, target: h.target("pane-1")}).runAction(daemonActionRequest{
		kind: daemonActionConsumeOrExpelPane, direction: layout.Right,
	})

	require.NoError(t, err)
	h.tab.mu.Lock()
	require.Equal(t, uint64(1), h.tab.layoutGeneration)
	h.tab.mu.Unlock()
	h.session.snapshotMu.Lock()
	require.Equal(t, uint64(1), h.session.snapshotGeneration)
	h.session.snapshotMu.Unlock()
	require.NotZero(t, h.totalResizes())
	awaitInvalidation(t, invalidations)
	requireNoInvalidation(t, invalidations)
}

func TestDaemonActionAdaptersNormalizeOnlyPaneRearrangeNoop(t *testing.T) {
	genuine := errors.New("genuine rearrange failure")
	tests := []struct {
		name    string
		err     error
		wantErr error
		wantLog bool
	}{
		{name: "silent no-op", err: errPaneRearrangeNoop},
		{name: "genuine error", err: genuine, wantErr: genuine, wantLog: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := addControlSession(d, "work", "t_work", "p_work")
			ac := &attachedClient{}
			ac.setSession(sess)
			target := resolveDaemonActionTarget(sess)
			request := daemonActionRequest{kind: daemonActionConsumeOrExpelPane, target: target, direction: layout.Left}

			paletteErr := (paletteExec{d: d, sess: sess, ac: ac, actions: &actionRunnerSpy{err: tt.err}}).runAction(request)
			controlErr := (controlExec{d: d, sess: sess, target: target, actions: &actionRunnerSpy{err: tt.err}}).runAction(request)
			require.ErrorIs(t, paletteErr, tt.wantErr)
			require.ErrorIs(t, controlErr, tt.wantErr)

			beforeNotices := len(d.notices.history())
			daemonKeyHandler{d: d, ac: ac, actions: &actionRunnerSpy{err: tt.err}}.Action(keys.ActionEqualizePanes)
			afterNotices := len(d.notices.history())
			if tt.wantLog {
				require.Equal(t, beforeNotices+1, afterNotices, "key action must report a genuine error")
			} else {
				require.Equal(t, beforeNotices, afterNotices, "key action must silently normalize rearrange no-op")
			}
		})
	}
}

type paneRearrangeHarness struct {
	daemon  *Daemon
	session *session
	tab     *tab
	panes   map[layout.PaneID]*pane
	ptys    map[layout.PaneID]*paneRearrangePTY
	factory *portsmocks.MockPTYFactory
}

type paneRearrangeSnapshot struct {
	tree               *layout.Tree
	layoutGeneration   uint64
	snapshotGeneration uint64
	snapshotDirty      bool
	panes              map[layout.PaneID]*pane
	rects              map[layout.PaneID]domain.Rect
	screens            map[layout.PaneID]domain.Size
}

func newPaneRearrangeHarness(t *testing.T, size domain.Size, tree *layout.Tree) *paneRearrangeHarness {
	t.Helper()
	factory := portsmocks.NewMockPTYFactory(t)
	d := newTestDaemon(t, factory, stubClock{})
	ids := layout.LeafIDs(tree.Root)
	require.NotEmpty(t, ids)
	ptys := make(map[layout.PaneID]*paneRearrangePTY, len(ids))
	panes := make(map[layout.PaneID]*pane, len(ids))
	for _, id := range ids {
		ptys[id] = &paneRearrangePTY{}
	}
	tb := newTab(ptys[ids[0]], size)
	first := tb.panes["pane-1"]
	delete(tb.panes, "pane-1")
	first.id = ids[0]
	panes[ids[0]] = first
	tb.panes[ids[0]] = first
	for _, id := range ids[1:] {
		p := newPane(id, ptys[id], size)
		panes[id] = p
		tb.panes[id] = p
	}
	tb.tree = tree.Clone()
	placements, ok := layout.Solve(tb.tree.Root, domain.Rect{Width: size.Cols, Height: size.Rows})
	require.True(t, ok, "test fixture layout must solve")
	for _, placement := range placements {
		p := tb.panes[placement.ID]
		p.rect = placement.Content
		p.screen.Resize(placement.Content.Width, placement.Content.Height)
	}
	tb.ctx, tb.cancel = contextWithTestCleanup(t, d.serveCtx)
	for _, p := range tb.panes {
		p.ctx, p.cancel = contextWithTestCleanup(t, tb.ctx)
	}
	sess := &session{id: "s", name: "work", cwd: "/work", tabs: []*tab{tb}, ctx: d.serveCtx, cancel: func() {}}
	sess.snapEligible.Store(true)
	d.sessions[sess.id] = sess
	return &paneRearrangeHarness{daemon: d, session: sess, tab: tb, panes: panes, ptys: ptys, factory: factory}
}

func contextWithTestCleanup(t *testing.T, parent context.Context) (context.Context, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(parent)
	t.Cleanup(cancel)
	return ctx, cancel
}

func (h *paneRearrangeHarness) target(id layout.PaneID) daemonActionTarget {
	return daemonActionTarget{session: h.session, tab: h.tab, pane: h.panes[id]}
}

func (h *paneRearrangeHarness) totalResizes() int {
	total := 0
	for _, tracked := range h.ptys {
		total += tracked.resizeCount()
	}
	return total
}

func (h *paneRearrangeHarness) snapshot() paneRearrangeSnapshot {
	h.tab.mu.Lock()
	snapshot := paneRearrangeSnapshot{
		tree:             h.tab.tree.Clone(),
		layoutGeneration: h.tab.layoutGeneration,
		panes:            make(map[layout.PaneID]*pane, len(h.tab.panes)),
		rects:            make(map[layout.PaneID]domain.Rect, len(h.tab.panes)),
		screens:          make(map[layout.PaneID]domain.Size, len(h.tab.panes)),
	}
	for id, p := range h.tab.panes {
		snapshot.panes[id] = p
		p.mu.Lock()
		snapshot.rects[id] = p.rect
		snapshot.screens[id] = domain.Size{Cols: p.screen.Frame.Width, Rows: p.screen.Frame.Height}
		p.mu.Unlock()
	}
	h.tab.mu.Unlock()
	h.session.snapshotMu.Lock()
	snapshot.snapshotGeneration = h.session.snapshotGeneration
	snapshot.snapshotDirty = h.session.snapDirty.Load()
	h.session.snapshotMu.Unlock()
	return snapshot
}

func (h *paneRearrangeHarness) stateLocksAvailable() bool {
	done := make(chan struct{})
	go func() {
		h.daemon.mu.Lock()
		h.session.mu.Lock()
		h.tab.mu.Lock()
		for _, p := range h.panes {
			p.mu.Lock()
		}
		for _, p := range h.panes {
			p.mu.Unlock()
		}
		h.tab.mu.Unlock()
		h.session.mu.Unlock()
		h.daemon.mu.Unlock()
		close(done)
	}()
	select {
	case <-done:
		return true
	case <-time.After(250 * time.Millisecond):
		return false
	}
}

func threeColumnTree() *layout.Tree {
	return &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2"), layout.NewLeaf("pane-3"),
	}}, Focus: "pane-1"}
}

type paneRearrangePTY struct {
	mu       sync.Mutex
	resizes  []domain.Size
	closes   int
	onResize func()
}

func (*paneRearrangePTY) Read([]byte) (int, error)    { return 0, io.EOF }
func (*paneRearrangePTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *paneRearrangePTY) Resize(size domain.Size) error {
	p.mu.Lock()
	p.resizes = append(p.resizes, size)
	hook := p.onResize
	p.mu.Unlock()
	if hook != nil {
		hook()
	}
	return nil
}
func (p *paneRearrangePTY) Close() error {
	p.mu.Lock()
	p.closes++
	p.mu.Unlock()
	return nil
}
func (*paneRearrangePTY) Pid() int                     { return 0 }
func (*paneRearrangePTY) ForegroundPgid() (int, error) { return 0, nil }

func (p *paneRearrangePTY) resizeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.resizes)
}

func (p *paneRearrangePTY) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}

var _ ports.PTY = (*paneRearrangePTY)(nil)
