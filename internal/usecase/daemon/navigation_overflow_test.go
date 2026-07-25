package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
)

func TestResolveOverflowObeysAxisConfigurationAndWalls(t *testing.T) {
	tests := []struct {
		name     string
		dir      layout.Direction
		cfg      domain.NavConfig
		position int
		count    int
		want     overflowStep
	}{
		{name: "tabs disabled", dir: layout.Right, cfg: domain.NavConfig{}, position: 0, count: 2},
		{name: "sessions alone do not enable tabs", dir: layout.Right, cfg: domain.NavConfig{OverflowSessions: true}, position: 0, count: 2},
		{name: "right tab", dir: layout.Right, cfg: domain.NavConfig{OverflowTabs: true}, position: 0, count: 2, want: overflowStep{kind: overflowTabs, delta: 1}},
		{name: "left tab", dir: layout.Left, cfg: domain.NavConfig{OverflowTabs: true}, position: 1, count: 2, want: overflowStep{kind: overflowTabs, delta: -1}},
		{name: "left wall", dir: layout.Left, cfg: domain.NavConfig{OverflowTabs: true}, position: 0, count: 2},
		{name: "right wall", dir: layout.Right, cfg: domain.NavConfig{OverflowTabs: true}, position: 1, count: 2},
		{name: "up session", dir: layout.Up, cfg: domain.NavConfig{OverflowSessions: true}, position: 1, count: 2, want: overflowStep{kind: overflowSessions, delta: -1}},
		{name: "down session", dir: layout.Down, cfg: domain.NavConfig{OverflowSessions: true}, position: 0, count: 2, want: overflowStep{kind: overflowSessions, delta: 1}},
		{name: "tabs alone do not enable sessions", dir: layout.Down, cfg: domain.NavConfig{OverflowTabs: true}, position: 0, count: 2},
		{name: "single destination is a wall", dir: layout.Right, cfg: domain.NavConfig{OverflowTabs: true}, position: 0, count: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, resolveOverflow(tt.dir, tt.cfg, tt.position, tt.count))
		})
	}
}

func TestKeyboardHorizontalOverflowLandsOnFacingEdge(t *testing.T) {
	tests := []struct {
		name       string
		start      int
		action     keys.Action
		wantActive int
		wantFocus  layout.PaneID
	}{
		{name: "right enters target left edge", start: 0, action: keys.ActionFocusPaneRight, wantActive: 1, wantFocus: "pane-1"},
		{name: "left enters target right edge", start: 1, action: keys.ActionFocusPaneLeft, wantActive: 0, wantFocus: "pane-2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, nil, nil)
			sess.mu.Lock()
			sess.active = tt.start
			target := sess.tabs[tt.wantActive]
			source := sess.tabs[tt.start]
			sess.mu.Unlock()
			target.mu.Lock()
			target.size = domain.Size{Cols: 41, Rows: 10}
			target.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-1"}
			target.panes["pane-2"] = newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
			target.mu.Unlock()
			d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowTabs: true}})

			daemonKeyHandler{d: d, ac: ac}.Action(tt.action)

			require.Equal(t, tt.wantActive, activeTabIndex(sess))
			target.mu.Lock()
			require.Equal(t, tt.wantFocus, target.tree.Focus)
			target.mu.Unlock()
			require.NotSame(t, source, sess.activeTab())
		})
	}
}

func TestKeyboardHorizontalOverflowRespectsDefaultsWallsFailedEntryAndFloating(t *testing.T) {
	tests := []struct {
		name  string
		setup func(d *Daemon, sess *session)
		dir   keys.Action
	}{
		{name: "defaults off", setup: func(_ *Daemon, _ *session) {}, dir: keys.ActionFocusPaneRight},
		{name: "left wall", setup: func(d *Daemon, _ *session) { d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowTabs: true}}) }, dir: keys.ActionFocusPaneLeft},
		{name: "failed target entry", setup: func(d *Daemon, sess *session) {
			d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowTabs: true}})
			sess.tabs[1].mu.Lock()
			sess.tabs[1].tree = nil
			sess.tabs[1].mu.Unlock()
		}, dir: keys.ActionFocusPaneRight},
		{name: "visible floating pane", setup: func(d *Daemon, sess *session) {
			d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowTabs: true}})
			installTestFloating(sess.tabs[0], newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), true)
		}, dir: keys.ActionFocusPaneRight},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, nil, nil)
			tt.setup(d, sess)

			daemonKeyHandler{d: d, ac: ac}.Action(tt.dir)

			require.Equal(t, 0, activeTabIndex(sess))
		})
	}
}

func TestCommitTabOverflowRevalidatesTabPointerIdentities(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(sess *session, candidate tabOverflowCandidate)
	}{
		{name: "source no longer active", mutate: func(sess *session, _ tabOverflowCandidate) { sess.active = 1 }},
		{name: "target entry replaced", mutate: func(sess *session, candidate tabOverflowCandidate) { sess.tabs[1] = newTab(nil, candidate.target.size) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, _, _ := newManualSessionWithPTYs(t, nil, nil)
			candidate, ok := d.prepareTabOverflow(sess, sess.tabs[0], layout.Right, domain.Rect{Width: 80, Height: 23}, 1)
			require.True(t, ok)
			sess.mu.Lock()
			tt.mutate(sess, candidate)
			sess.mu.Unlock()

			require.False(t, d.commitTabOverflow(sess, candidate))
		})
	}
}
