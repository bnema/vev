package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

type fakeBarRunner struct {
	mu    sync.Mutex
	calls []barScriptContext
	outs  []string
	errs  []error
}

func (r *fakeBarRunner) run(_ context.Context, _ string, ctx barScriptContext) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, ctx)
	i := len(r.calls) - 1
	if i < len(r.errs) && r.errs[i] != nil {
		return "", r.errs[i]
	}
	if i < len(r.outs) {
		return r.outs[i], nil
	}
	return "", nil
}

func TestBarScriptRefreshIntervalClampingTickAndLastGoodOnFailure(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top1", "bot1", "top2", "bot2"}}
	d := newBarRefreshTestDaemon(r, 100*time.Millisecond)
	sess := newBarRefreshTestSession()
	sess.client = &attachedClient{size: domain.Size{Cols: 120, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)
	require.Equal(t, 2, len(r.calls))
	require.Equal(t, "top1", d.barStateFor(sess, "").topRight)
	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, int64(500*time.Millisecond)), false), "clamped interval should skip sub-second tick")

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(1, 0), false))
	waitBarRefreshIdle(t, d)
	require.Equal(t, 4, len(r.calls))
	require.Equal(t, "top2", d.barStateFor(sess, "").topRight)

	r.errs = []error{nil, nil, nil, nil, errors.New("boom"), errors.New("boom")}
	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(2, 0), false))
	waitBarRefreshIdle(t, d)
	state := d.barStateFor(sess, "")
	require.Equal(t, "top2", state.topRight)
	require.Equal(t, "bot2", state.bottomRight)
}

func TestBarScriptContextUsesActivePaneSessionAndClientCols(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top", "bottom"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()
	sess.name = "work"
	sess.cwd = "/repo"
	sess.client = &attachedClient{size: domain.Size{Cols: 132, Rows: 40}}
	sess.active = 1
	sess.tabs[1].size = domain.Size{Cols: 132, Rows: 38}
	active := sess.tabs[1].focusedPane()
	active.stableID = "pane-active"

	require.True(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	waitBarRefreshIdle(t, d)
	require.Len(t, r.calls, 2)
	for _, call := range r.calls {
		require.Equal(t, "work", call.Session)
		require.Equal(t, "tab-active", call.Tab)
		require.Equal(t, "pane-active", call.Pane)
		require.Equal(t, "/repo", call.PaneCWD)
		require.Equal(t, 132, call.Cols)
	}
	require.Equal(t, "top-right", r.calls[0].Anchor)
	require.Equal(t, "bottom-right", r.calls[1].Anchor)
}

func TestBarScriptRefreshSkipsWithoutAttachedClient(t *testing.T) {
	r := &fakeBarRunner{}
	d := newBarRefreshTestDaemon(r, time.Second)
	sess := newBarRefreshTestSession()

	require.False(t, d.refreshBarScriptsIfDue(sess, time.Unix(0, 0), true))
	require.Empty(t, r.calls)
}

func TestBarScriptRefreshIsPerSession(t *testing.T) {
	r := &fakeBarRunner{outs: []string{"top-a", "bottom-a", "top-b", "bottom-b"}}
	d := newBarRefreshTestDaemon(r, time.Second)
	a := newBarRefreshTestSession()
	a.id = "a"
	a.name = "a"
	a.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}
	b := newBarRefreshTestSession()
	b.id = "b"
	b.name = "b"
	b.client = &attachedClient{size: domain.Size{Cols: 80, Rows: 24}}

	require.True(t, d.refreshBarScriptsIfDue(a, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)
	require.True(t, d.refreshBarScriptsIfDue(b, time.Unix(0, 0), false))
	waitBarRefreshIdle(t, d)

	stateA := d.barStateFor(a, "")
	stateB := d.barStateFor(b, "")
	require.Equal(t, "top-a", stateA.topRight)
	require.Equal(t, "bottom-a", stateA.bottomRight)
	require.Equal(t, "top-b", stateB.topRight)
	require.Equal(t, "bottom-b", stateB.bottomRight)
}

func newBarRefreshTestDaemon(r *fakeBarRunner, interval time.Duration) *Daemon {
	d := &Daemon{barScripts: &barScriptState{
		cfg:         barScriptConfig{topRight: "top", bottomRight: "bottom", interval: effectiveBarInterval(interval)},
		runner:      r,
		outputs:     make(map[domain.SessionID]barScriptOutputs),
		lastRefresh: make(map[domain.SessionID]time.Time),
		running:     make(map[domain.SessionID]bool),
	}}
	return d
}

func newBarRefreshTestSession() *session {
	inactive := newTabWithStableID("tab-inactive", "pane-inactive", nil, domain.Size{Cols: 80, Rows: 22})
	active := newTabWithStableID("tab-active", "pane-active", nil, domain.Size{Cols: 80, Rows: 22})
	active.tree.Focus = layout.PaneID("pane-1")
	return &session{id: "s", name: "s", cwd: "/tmp", tabs: []*tab{inactive, active}, active: 0, ctx: context.Background()}
}

func waitBarRefreshIdle(t *testing.T, d *Daemon) {
	t.Helper()
	require.Eventually(t, func() bool {
		d.barScripts.mu.Lock()
		defer d.barScripts.mu.Unlock()
		return len(d.barScripts.running) == 0
	}, time.Second, time.Millisecond)
}
