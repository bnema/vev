package daemon

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/internal/usecase/picker"
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
		{name: "negative position is out of range", dir: layout.Right, cfg: domain.NavConfig{OverflowTabs: true}, position: -1, count: 2},
		{name: "position at count is out of range", dir: layout.Down, cfg: domain.NavConfig{OverflowSessions: true}, position: 2, count: 2},
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
			selectTestAttachmentTabLocked(sess, tt.start)
			target := sess.tabs[tt.wantActive]
			source := sess.tabs[tt.start]
			sess.mu.Unlock()
			target.mu.Lock()
			target.size = domain.Size{Cols: 41, Rows: 10}
			target.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: "pane-1"}
			target.panes["pane-2"] = newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
			target.mu.Unlock()
			d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowTabs: true}})

			daemonKeyHandler{d: d, ac: ac}.Action(tt.action, nil)

			require.Equal(t, tt.wantActive, testAttachmentTabIndex(sess))
			target.mu.Lock()
			require.Equal(t, tt.wantFocus, target.tree.Focus)
			target.mu.Unlock()
			require.NotSame(t, source, testAttachmentTab(sess))
		})
	}
}

func TestKeyboardVerticalOverflowSwitchesOnlyAcrossAlphabeticalLiveSessions(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil, nil)
	alpha.mu.Lock()
	alpha.name = "alpha"
	alpha.mu.Unlock()

	newSession := func(id, name string, active int, focus layout.PaneID) *session {
		tabs := []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10}), newTab(nil, domain.Size{Cols: 41, Rows: 10})}
		target := tabs[active]
		target.mu.Lock()
		target.tree = &layout.Tree{Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2")}}, Focus: focus}
		target.panes["pane-2"] = newPane("pane-2", nil, domain.Size{Cols: 20, Rows: 10})
		target.mu.Unlock()
		return &session{sessionCore: sessionCore{id: domain.SessionID(id), name: name}, ctx: t.Context(), cancel: func() {}, tabs: tabs}
	}
	charlie := newSession("live-charlie", "charlie", 1, "pane-2")
	echo := newSession("live-echo", "echo", 1, "pane-2")

	// Deliberately register the live sessions out of alphabetical order. The
	// stopped name sorts between alpha and charlie but must never be resumed.
	d.mu.Lock()
	delete(d.sessions, alpha.id)
	d.sessions[echo.id] = echo
	d.sessions[alpha.id] = alpha
	d.sessions[charlie.id] = charlie
	d.stopped["bravo"] = stoppedSession{name: "bravo", cwd: "/tmp/bravo"}
	d.mu.Unlock()
	d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowSessions: true}})
	handler := daemonKeyHandler{d: d, ac: ac}

	moves := []struct {
		name   string
		action keys.Action
		want   *session
	}{
		{name: "down to charlie", action: keys.ActionFocusPaneDown, want: charlie},
		{name: "down to echo", action: keys.ActionFocusPaneDown, want: echo},
		{name: "down wall", action: keys.ActionFocusPaneDown, want: echo},
		{name: "up to charlie", action: keys.ActionFocusPaneUp, want: charlie},
		{name: "up to alpha", action: keys.ActionFocusPaneUp, want: alpha},
		{name: "up wall", action: keys.ActionFocusPaneUp, want: alpha},
	}
	for _, move := range moves {
		t.Run(move.name, func(t *testing.T) {
			handler.Action(move.action, nil)
			require.Same(t, move.want, ac.currentSession())
		})
	}

	for _, target := range []*session{charlie, echo} {
		require.Equal(t, 0, testAttachmentTabIndex(target), "new attachment starts at the deterministic first tab")
		target.tabs[1].mu.Lock()
		require.Equal(t, layout.PaneID("pane-2"), target.tabs[1].tree.Focus, "switch preserves the target pane focus")
		target.tabs[1].mu.Unlock()
	}
	d.mu.Lock()
	_, stopped := d.stopped["bravo"]
	var bravoLive bool
	for _, entry := range d.sessions {
		candidate, ok := localSession(entry)
		if !ok {
			continue
		}
		candidate.mu.Lock()
		bravoLive = bravoLive || candidate.name == "bravo"
		candidate.mu.Unlock()
	}
	d.mu.Unlock()
	alpha.mu.Lock()
	attached := alpha.snapshotAttachmentsLocked()
	alpha.mu.Unlock()
	require.True(t, stopped, "vertical overflow leaves the stopped session stopped")
	require.False(t, bravoLive, "the stopped session is absent from the live registry")
	require.Contains(t, attached, ac, "the source session owns the genuinely attached client after returning")
}

func TestKeyboardVerticalOverflowRefusesVisibleFloatingSource(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil)
	alpha.mu.Lock()
	alpha.name = "alpha"
	alpha.mu.Unlock()
	installTestFloating(alpha.tabs[0], newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), true)
	charlie := &session{sessionCore: sessionCore{id: "live-charlie", name: "charlie"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	d.mu.Lock()
	d.sessions[charlie.id] = charlie
	d.mu.Unlock()
	d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowSessions: true}})

	daemonKeyHandler{d: d, ac: ac}.Action(keys.ActionFocusPaneDown, nil)

	require.Same(t, alpha, ac.currentSession())
	alpha.mu.Lock()
	require.Contains(t, alpha.snapshotAttachmentsLocked(), ac)
	alpha.mu.Unlock()
	charlie.mu.Lock()
	require.Empty(t, charlie.snapshotAttachmentsLocked())
	charlie.mu.Unlock()
}

func TestVerticalOverflowRevalidatesFloatingSourceBeforeHandoff(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil)
	alpha.mu.Lock()
	alpha.name = "alpha"
	alpha.mu.Unlock()
	charlie := &session{sessionCore: sessionCore{id: "live-charlie", name: "charlie"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	d.mu.Lock()
	d.sessions[charlie.id] = charlie
	d.mu.Unlock()

	target, ok := d.prepareSessionOverflow(alpha, layout.Down, domain.NavConfig{OverflowSessions: true})
	require.True(t, ok)
	installTestFloating(alpha.tabs[0], newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), true)

	require.ErrorIs(t, d.commitSessionOverflow(alpha, ac, alpha.tabs[0], target), errNoNeighbor)
	require.Same(t, alpha, ac.currentSession())
	alpha.mu.Lock()
	require.Contains(t, alpha.snapshotAttachmentsLocked(), ac)
	alpha.mu.Unlock()
	charlie.mu.Lock()
	require.Empty(t, charlie.snapshotAttachmentsLocked())
	charlie.mu.Unlock()
}

func TestVerticalOverflowRejectsStaleSourceTab(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil, nil)
	alpha.mu.Lock()
	alpha.name = "alpha"
	expectedSource := alpha.tabs[0]
	selectTestAttachmentTabLocked(alpha, 1)
	alpha.mu.Unlock()
	charlie := &session{sessionCore: sessionCore{id: "live-charlie", name: "charlie"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	d.mu.Lock()
	d.sessions[charlie.id] = charlie
	d.mu.Unlock()

	require.ErrorIs(t, d.commitSessionOverflow(alpha, ac, expectedSource, picker.Target{Session: charlie.id, TabIndex: -1}), errNoNeighbor)
	require.Same(t, alpha, ac.currentSession())
	alpha.mu.Lock()
	require.Contains(t, alpha.snapshotAttachmentsLocked(), ac)
	alpha.mu.Unlock()
	charlie.mu.Lock()
	require.Empty(t, charlie.snapshotAttachmentsLocked())
	charlie.mu.Unlock()
}

func TestOrdinarySessionSwitchDoesNotApplyOverflowGuard(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil)
	installTestFloating(alpha.tabs[0], newPane("floating", nil, domain.Size{Cols: 20, Rows: 5}), true)
	charlie := &session{sessionCore: sessionCore{id: "live-charlie", name: "charlie"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	d.mu.Lock()
	d.sessions[charlie.id] = charlie
	d.mu.Unlock()

	require.NoError(t, d.switchToTarget(alpha, ac, picker.Target{Session: charlie.id, TabIndex: -1}))
	require.Same(t, charlie, ac.currentSession())
}

func TestVerticalOverflowTreatsDisplacedSourceClientAsNoNeighbor(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil)
	alpha.mu.Lock()
	alpha.name = "alpha"
	alpha.mu.Unlock()
	charlie := &session{sessionCore: sessionCore{id: "live-charlie", name: "charlie"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	d.mu.Lock()
	d.sessions[charlie.id] = charlie
	d.mu.Unlock()

	target, ok := d.prepareSessionOverflow(alpha, layout.Down, domain.NavConfig{OverflowSessions: true})
	require.True(t, ok)
	replacement := &attachedClient{}
	alpha.mu.Lock()
	alpha.registerAttachmentLocked(replacement)
	alpha.mu.Unlock()

	require.ErrorIs(t, d.commitSessionOverflow(alpha, ac, alpha.tabs[0], target), errNoNeighbor)
	require.Same(t, alpha, ac.currentSession())
	alpha.mu.Lock()
	require.Contains(t, alpha.snapshotAttachmentsLocked(), replacement)
	alpha.mu.Unlock()
	charlie.mu.Lock()
	require.Empty(t, charlie.snapshotAttachmentsLocked())
	charlie.mu.Unlock()
}

func TestVerticalOverflowIsRaceFreeDuringSessionRename(t *testing.T) {
	d, alpha, ac, _ := newManualSessionWithPTYs(t, nil)
	alpha.mu.Lock()
	alpha.name = "alpha"
	alpha.mu.Unlock()
	charlie := &session{sessionCore: sessionCore{id: "live-charlie", name: "charlie"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	echo := &session{sessionCore: sessionCore{id: "live-echo", name: "echo"}, ctx: t.Context(), cancel: func() {}, tabs: []*tab{newTab(nil, domain.Size{Cols: 41, Rows: 10})}}
	d.mu.Lock()
	d.sessions[echo.id] = echo
	d.sessions[charlie.id] = charlie
	d.mu.Unlock()
	d.ApplyConfig(domain.Config{Nav: domain.NavConfig{OverflowSessions: true}})

	start := make(chan struct{})
	renameErrors := make(chan error, 40)
	var wg sync.WaitGroup
	wg.Go(func() {
		<-start
		for i := range 40 {
			name := "charlie"
			if i%2 == 0 {
				name = "delta"
			}
			if err := d.renameSession(charlie, name); err != nil {
				renameErrors <- err
			}
		}
	})

	close(start)
	handler := daemonKeyHandler{d: d, ac: ac}
	for range 20 {
		handler.Action(keys.ActionFocusPaneDown, nil)
		handler.Action(keys.ActionFocusPaneUp, nil)
	}
	wg.Wait()
	close(renameErrors)
	for err := range renameErrors {
		require.NoError(t, err)
	}

	current := ac.currentSession()
	owners := make([]*session, 0, 1)
	d.mu.Lock()
	for _, entry := range d.sessions {
		candidate, ok := localSession(entry)
		if !ok {
			continue
		}
		candidate.mu.Lock()
		if attachmentRegisteredLocked(candidate, ac) {
			owners = append(owners, candidate)
		}
		candidate.mu.Unlock()
	}
	d.mu.Unlock()
	require.Len(t, owners, 1)
	require.Same(t, current, owners[0])
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

			daemonKeyHandler{d: d, ac: ac}.Action(tt.dir, nil)

			require.Equal(t, 0, testAttachmentTabIndex(sess))
		})
	}
}
