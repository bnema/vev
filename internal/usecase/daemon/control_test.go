package daemon

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

func TestPaletteAndControlShareExplicitDaemonActionTarget(t *testing.T) {
	tests := []struct {
		name      string
		palette   func(paletteExec) error
		control   func(controlExec) error
		kind      daemonActionKind
		direction layout.Direction
	}{
		{name: "split right", palette: func(e paletteExec) error { return e.SplitRight() }, control: func(e controlExec) error { return e.SplitRight() }, kind: daemonActionSplitPane, direction: layout.Right},
		{name: "consume or expel left", palette: func(e paletteExec) error { return e.ConsumeOrExpelPaneLeft() }, control: func(e controlExec) error { return e.ConsumeOrExpelPaneLeft() }, kind: daemonActionConsumeOrExpelPane, direction: layout.Left},
		{name: "consume or expel right", palette: func(e paletteExec) error { return e.ConsumeOrExpelPaneRight() }, control: func(e controlExec) error { return e.ConsumeOrExpelPaneRight() }, kind: daemonActionConsumeOrExpelPane, direction: layout.Right},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			sess := addControlSession(d, "work", "t_work", "p_work")
			tb := sess.activeTab()
			tb.mu.Lock()
			pane := tb.focusedPane()
			tb.mu.Unlock()
			spy := &actionRunnerSpy{}
			target := daemonActionTarget{session: sess, tab: tb, pane: pane}

			require.NoError(t, tt.palette(paletteExec{d: d, sess: sess, actions: spy}))
			require.NoError(t, tt.control(controlExec{d: d, sess: sess, tab: tb, actions: spy, target: target}))

			require.Len(t, spy.requests, 2)
			for _, request := range spy.requests {
				require.Equal(t, tt.kind, request.kind)
				require.Same(t, sess, request.target.session)
				require.Same(t, tb, request.target.tab)
				require.Same(t, pane, request.target.pane)
				require.Equal(t, tt.direction, request.direction)
			}
		})
	}
}

func TestConsumeOrExpelControlSelfTargetsNonFocusedPane(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	h.tab.mu.Lock()
	h.tab.tree.Focus = "pane-1"
	beforeGeneration := h.tab.layoutGeneration
	tabStableID := h.tab.stableID
	targetStableID := h.panes["pane-3"].stableID
	h.tab.mu.Unlock()

	result := sendCommand(t, h.daemon, ports.CommandRequest{
		Slug:          "consume-or-expel-pane-left",
		Self:          true,
		TargetSession: "work",
		TargetTab:     tabStableID,
		TargetPane:    targetStableID,
	})

	require.True(t, result.OK, result.Text)
	require.Empty(t, result.Output)
	h.session.mu.Lock()
	require.Zero(t, h.session.active, "--self must not change the active tab")
	h.session.mu.Unlock()
	h.tab.mu.Lock()
	require.Equal(t, layout.PaneID("pane-3"), h.tab.tree.Focus, "a moved explicit target becomes focused")
	require.Equal(t, beforeGeneration+1, h.tab.layoutGeneration)
	require.Len(t, h.tab.tree.Root.Children, 2)
	h.tab.mu.Unlock()
}

func TestConsumeOrExpelControlEdgeNoopReturnsOKAndPreservesFocus(t *testing.T) {
	h := newPaneRearrangeHarness(t, domain.Size{Cols: 80, Rows: 22}, threeColumnTree())
	ac := &attachedClient{}
	ac.setSession(h.session)
	h.session.mu.Lock()
	h.session.client = ac
	h.session.mu.Unlock()
	invalidations := make(chan renderInvalidation, 1)
	rc := newRenderCoordinator(renderCoordinatorOptions{onInvalidate: func(inv renderInvalidation) { invalidations <- inv }})
	rc.attach(ac)
	h.session.installRenderCoordinator(rc)
	before := h.snapshot()

	result := sendCommand(t, h.daemon, ports.CommandRequest{
		Slug:          "consume-or-expel-pane-left",
		Self:          true,
		TargetSession: "work",
		TargetTab:     h.tab.stableID,
		TargetPane:    h.panes["pane-1"].stableID,
	})

	require.True(t, result.OK, result.Text)
	require.Empty(t, result.Output)
	require.Equal(t, before, h.snapshot())
	require.Empty(t, h.daemon.notices.history())
	requireNoInvalidation(t, invalidations)
}

type actionRunnerSpy struct {
	requests []daemonActionRequest
	err      error
}

func (s *actionRunnerSpy) Run(request daemonActionRequest) error {
	s.requests = append(s.requests, request)
	return s.err
}

func TestHandleCommandRejectsVersionBeforeDecodeOrDispatch(t *testing.T) {
	// The version prefix is valid and mismatched, while the remainder is not a
	// decodable request. The daemon must still return ErrVersionMismatch.
	frame := ports.Frame{Type: ports.MsgCommand, Payload: []byte{0, byte(ports.ProtocolVersion + 1)}}
	tr, sends, _ := newConn(t, frame)

	d := newTestDaemon(t, nil, stubClock{})
	d.handleCommand(tr, frame)

	result := awaitCommandResult(t, sends)
	require.False(t, result.OK)
	require.Equal(t, ports.ErrVersionMismatch, result.Code)
}

func TestHandleConnRoutesCommand(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	request := ports.CommandRequest{Version: ports.ProtocolVersion, Slug: "list-sessions"}
	payload, err := ports.MarshalCommandRequest(request)
	require.NoError(t, err)
	frame := ports.Frame{Type: ports.MsgCommand, Payload: payload}
	tr, sends, _ := newConn(t, frame)

	d.handleConn(tr)

	result := awaitCommandResult(t, sends)
	require.True(t, result.OK, result.Text)
}

func TestHandleCommandDispatchAndTargetErrors(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(*Daemon)
		request ports.CommandRequest
		code    uint16
	}{
		{name: "unknown slug", request: ports.CommandRequest{Slug: "no-such"}, code: ports.ErrUnknownCommand},
		{name: "non-scriptable command", request: ports.CommandRequest{Slug: "session-picker"}, code: ports.ErrNotScriptable},
		{name: "no sessions", request: ports.CommandRequest{Slug: "split-right"}, code: ports.ErrNoSuchTarget},
		{name: "ambiguous sessions", arrange: func(d *Daemon) {
			addControlSession(d, "one", "t_one", "p_one")
			addControlSession(d, "two", "t_two", "p_two")
		}, request: ports.CommandRequest{Slug: "split-right"}, code: ports.ErrAmbiguousTarget},
		{name: "missing explicit session", arrange: func(d *Daemon) {
			addControlSession(d, "work", "t_work", "p_work")
		}, request: ports.CommandRequest{Slug: "new-tab", TargetSession: "missing"}, code: ports.ErrNoSuchTarget},
		{name: "missing stable IDs", arrange: func(d *Daemon) {
			addControlSession(d, "work", "t_work", "p_work")
		}, request: ports.CommandRequest{Slug: "split-right", TargetTab: "t_nope", TargetPane: "p_nope"}, code: ports.ErrNoSuchTarget},
		{name: "duplicate stable IDs are ambiguous", arrange: func(d *Daemon) {
			addControlSession(d, "one", "t_shared", "p_shared")
			addControlSession(d, "two", "t_shared", "p_shared")
		}, request: ports.CommandRequest{Slug: "toast", Args: []string{"hello"}, TargetTab: "t_shared", TargetPane: "p_shared"}, code: ports.ErrAmbiguousTarget},
		{name: "self requires both IDs", arrange: func(d *Daemon) {
			addControlSession(d, "work", "t_work", "p_work")
		}, request: ports.CommandRequest{Slug: "split-right", Self: true, TargetSession: "work", TargetTab: "t_work"}, code: ports.ErrNoSuchTarget},
		{name: "self rejects pane from another tab", arrange: func(d *Daemon) {
			sess := addControlSession(d, "work", "t_work", "p_work")
			other := newTabWithStableID("t_other", "p_other", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
			other.ctx, other.cancel = context.WithCancel(d.serveCtx)
			sess.tabs = append(sess.tabs, other)
		}, request: ports.CommandRequest{Slug: "split-right", Self: true, TargetSession: "work", TargetTab: "t_work", TargetPane: "p_other"}, code: ports.ErrNoSuchTarget},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			if tt.arrange != nil {
				tt.arrange(d)
			}
			result := sendCommand(t, d, tt.request)
			require.False(t, result.OK)
			require.Equal(t, tt.code, result.Code)
		})
	}
}

func TestHandleCommandResolvesStaleSessionNameByStableIDs(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "renamed", "t_work", "p_work")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "toast", Args: []string{"hello"},
		TargetSession: "old-name", TargetTab: "t_work", TargetPane: "p_work",
	})

	require.True(t, result.OK, result.Text)
	require.Equal(t, uint64(1), sess.mruAt.Load())
}

func TestHandleCommandStableIDsResolveSessionWithoutSelectingTab(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "t_first", "p_first")
	first := sess.tabs[0]
	second := newTabWithStableID("t_second", "p_second", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	second.ctx, second.cancel = context.WithCancel(d.serveCtx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.active = 0
	sess.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "rename-tab", Args: []string{"targeted"}, TargetSession: "work",
		TargetTab: "t_second", TargetPane: "p_second",
	})

	require.True(t, result.OK, result.Text)
	first.mu.Lock()
	require.Equal(t, "targeted", first.name)
	first.mu.Unlock()
	second.mu.Lock()
	require.Empty(t, second.name)
	second.mu.Unlock()
	sess.mu.Lock()
	require.Zero(t, sess.active)
	sess.mu.Unlock()
}

func TestHandleCommandStableIDsDoNotRedirectSplitFromCurrentFocus(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_active", "p_active")
	active := sess.tabs[0]
	originFocus := active.tree.Focus
	origin := active.focusedPane()
	second := newTabWithStableID("t_origin", "p_origin", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	second.ctx, second.cancel = context.WithCancel(d.serveCtx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, second)
	sess.active = 0
	sess.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "split-right", TargetSession: "work",
		TargetTab: "t_origin", TargetPane: "p_origin",
	})

	require.True(t, result.OK, result.Text)
	sess.mu.Lock()
	activeIndex := sess.active
	sess.mu.Unlock()
	require.Zero(t, activeIndex)
	active.mu.Lock()
	activePaneCount := len(active.panes)
	activeFocus := active.tree.Focus
	originRetained := active.panes[origin.id] == origin
	active.mu.Unlock()
	require.Equal(t, 2, activePaneCount, "split must mutate the daemon-focused tab")
	require.NotEqual(t, originFocus, activeFocus, "split must focus the new pane beside the daemon-focused pane")
	require.True(t, originRetained)
	second.mu.Lock()
	secondPaneCount := len(second.panes)
	second.mu.Unlock()
	require.Equal(t, 1, secondPaneCount, "stable IDs are only a session locator")
}

func TestHandleCommandSelfTargetsNonActiveTabAndPane(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_active", "p_active")
	active := sess.tabs[0]
	invoking := newTabWithStableID("t_invoking", "p_invoking", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	invoking.ctx, invoking.cancel = context.WithCancel(d.serveCtx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, invoking)
	sess.active = 0
	sess.mu.Unlock()

	rename := sendCommand(t, d, ports.CommandRequest{
		Slug: "rename-tab", Args: []string{"invoking"}, Self: true,
		TargetSession: "work", TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	require.True(t, rename.OK, rename.Text)
	active.mu.Lock()
	require.Empty(t, active.name)
	active.mu.Unlock()
	invoking.mu.Lock()
	require.Equal(t, "invoking", invoking.name)
	invoking.mu.Unlock()

	split := sendCommand(t, d, ports.CommandRequest{
		Slug: "split-right", Self: true,
		TargetSession: "work", TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	require.True(t, split.OK, split.Text)
	active.mu.Lock()
	require.Len(t, active.panes, 1)
	active.mu.Unlock()
	invoking.mu.Lock()
	require.Len(t, invoking.panes, 2)
	beforeFocus := invoking.tree.Focus
	beforeGeneration := invoking.layoutGeneration
	invoking.mu.Unlock()

	grow := sendCommand(t, d, ports.CommandRequest{
		Slug: "grow-pane-width", Self: true,
		TargetSession: "work", TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	require.True(t, grow.OK, grow.Text)
	invoking.mu.Lock()
	require.Equal(t, beforeFocus, invoking.tree.Focus, "targeted resize must not refocus the tab")
	require.Equal(t, beforeGeneration+1, invoking.layoutGeneration)
	require.NotZero(t, invoking.tree.Root.Children[0].Weight, "resize must update the invoking tab's shares")
	invoking.mu.Unlock()
	active.mu.Lock()
	require.Len(t, active.panes, 1, "--self resize must not mutate the active tab")
	active.mu.Unlock()

	equalize := sendCommand(t, d, ports.CommandRequest{
		Slug: "equalize-panes", Self: true,
		TargetSession: "work", TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	require.True(t, equalize.OK, equalize.Text)
	invoking.mu.Lock()
	require.Equal(t, beforeFocus, invoking.tree.Focus, "targeted equalize must not refocus the tab")
	for _, child := range invoking.tree.Root.Children {
		require.Zero(t, child.Weight, "equalize must clear each target-tab share")
	}
	invoking.mu.Unlock()
}

func TestResizeControlOneShotsTargetDetachedSessions(t *testing.T) {
	targets := []struct {
		name    string
		request func() ports.CommandRequest
	}{
		{"named", func() ports.CommandRequest { return ports.CommandRequest{TargetSession: "work"} }},
		{"unique", func() ports.CommandRequest { return ports.CommandRequest{} }},
	}
	for _, target := range targets {
		t.Run(target.name, func(t *testing.T) {
			for _, slug := range []string{"grow-pane-width", "shrink-pane-width", "grow-pane-height", "shrink-pane-height", "equalize-panes"} {
				t.Run(slug, func(t *testing.T) {
					factory := &controlPTYFactory{}
					d := newTestDaemon(t, factory, stubClock{})
					t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
					sess := addControlSession(d, "work", "t_work", "p_work")
					require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
					require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-down", TargetSession: "work"}).OK)
					tb := sess.activeTab()
					tb.mu.Lock()
					generation := tb.layoutGeneration
					tb.mu.Unlock()
					sess.snapEligible.Store(true)
					sess.snapshotMu.Lock()
					snapshotGeneration := sess.snapshotGeneration
					sess.snapshotMu.Unlock()
					request := target.request()
					request.Slug = slug
					result := sendCommand(t, d, request)
					require.True(t, result.OK, result.Text)
					require.Empty(t, result.Output)
					tb.mu.Lock()
					require.Equal(t, generation+1, tb.layoutGeneration, "one accepted action has one layout generation boundary")
					tb.mu.Unlock()
					sess.snapshotMu.Lock()
					require.Equal(t, snapshotGeneration+1, sess.snapshotGeneration, "one accepted action has one snapshot dirty boundary")
					sess.snapshotMu.Unlock()
					sess.mu.Lock()
					require.Nil(t, sess.client, "one-shots must work headless")
					sess.mu.Unlock()
				})
			}
		})
	}
}

func TestResizeControlNonSelfVEVLocatorUsesActiveTarget(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_active", "p_active")
	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
	active := sess.tabs[0]
	locator := newTabWithStableID("t_locator", "p_locator", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	locator.ctx, locator.cancel = context.WithCancel(d.serveCtx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, locator)
	sess.active = 0
	sess.mu.Unlock()
	active.mu.Lock()
	before := active.layoutGeneration
	active.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "grow-pane-width", TargetSession: "work", TargetTab: "t_locator", TargetPane: "p_locator",
	})
	require.True(t, result.OK, result.Text)
	active.mu.Lock()
	require.Equal(t, before+1, active.layoutGeneration, "stable IDs locate the session but must not redirect a non-self action")
	active.mu.Unlock()
	locator.mu.Lock()
	require.Zero(t, locator.layoutGeneration)
	locator.mu.Unlock()
	sess.mu.Lock()
	require.Zero(t, sess.active)
	sess.mu.Unlock()
}

func TestResizeControlTooSmallUsesStableFailure(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")
	require.True(t, sendCommand(t, d, ports.CommandRequest{Slug: "split-right", TargetSession: "work"}).OK)
	tb := sess.activeTab()
	tb.mu.Lock()
	tb.size.Cols = 41
	tb.bumpLayoutGenerationLocked()
	tb.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{Slug: "grow-pane-width", TargetSession: "work"})
	require.False(t, result.OK)
	require.Equal(t, ports.ErrNoSuchTarget, result.Code)
	require.Equal(t, "pane cannot be resized further", result.Text)
}

func TestHandleCommandSelfListPanesUsesInvokingTab(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "t_active", "p_active")
	invoking := newTabWithStableID("t_invoking", "p_invoking", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	invoking.ctx, invoking.cancel = context.WithCancel(d.serveCtx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, invoking)
	sess.active = 0
	sess.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "list-panes", Self: true, JSON: true,
		TargetSession: "work", TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	require.True(t, result.OK, result.Text)
	require.Contains(t, result.Output, "p_invoking")
	require.NotContains(t, result.Output, "p_active")
}

func TestHandleCommandRenameSessionRejectsInvalidNameAsCommandArgs(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	addControlSession(d, "work", "t_work", "p_work")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "rename-session", Args: []string{"invalid name"}, TargetSession: "work",
	})

	require.False(t, result.OK)
	require.Equal(t, ports.ErrInvalidCommandArgs, result.Code)
}

func TestCloseCommandsReportMutationOutcome(t *testing.T) {
	t.Run("stale close tab target fails", func(t *testing.T) {
		d := newTestDaemon(t, nil, stubClock{})
		sess := addControlSession(d, "work", "t_work", "p_work")
		stale := newTabWithStableID("t_stale", "p_stale", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
		cmd, ok := command.BySlug("close-tab")
		require.True(t, ok)

		result := d.runControl(cmd, controlExec{
			d: d, sess: sess, tab: stale,
			target: daemonActionTarget{session: sess, tab: stale},
		}, ports.CommandRequest{Slug: "close-tab"})

		require.False(t, result.OK)
		require.Equal(t, ports.ErrInternal, result.Code)
		require.Len(t, sess.tabs, 1, "a stale close must retain the live tab")
	})

	t.Run("failed final pane close fails and retains session", func(t *testing.T) {
		d := newTestDaemon(t, &controlPTYFactory{}, stubClock{})
		d.snapsEnabled = true
		d.snapshotRepository = portsmocks.NewMockSnapshotRepository(t)
		sess := addControlSession(d, "work", "t_work", "p_work")
		sess.mu.Lock()
		sess.ephemeral = false
		sess.mu.Unlock()
		sess.snapEligible.Store(true)

		result := sendCommand(t, d, ports.CommandRequest{Slug: "close-pane", TargetSession: "work"})

		require.False(t, result.OK)
		require.Equal(t, ports.ErrInternal, result.Code)
		d.mu.Lock()
		require.Same(t, sess, d.sessions[sess.id], "failed final-pane close must retain the session")
		d.mu.Unlock()
		sess.mu.Lock()
		require.Len(t, sess.tabs, 1)
		sess.mu.Unlock()
	})

	t.Run("successful close tab remains successful", func(t *testing.T) {
		d := newTestDaemon(t, nil, stubClock{})
		sess := addControlSession(d, "work", "t_first", "p_first")
		second := newTabWithStableID("t_second", "p_second", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
		second.ctx, second.cancel = context.WithCancel(d.serveCtx)
		sess.mu.Lock()
		sess.tabs = append(sess.tabs, second)
		sess.active = 1
		sess.mu.Unlock()

		result := sendCommand(t, d, ports.CommandRequest{Slug: "close-tab", TargetSession: "work"})

		require.True(t, result.OK, result.Text)
		sess.mu.Lock()
		require.Len(t, sess.tabs, 1)
		require.Equal(t, "t_first", sess.tabs[0].stableID)
		sess.mu.Unlock()
	})
}

func TestHandleCommandHeadlessMutations(t *testing.T) {
	tests := []struct {
		name   string
		slug   string
		args   []string
		verify func(*testing.T, *Daemon, *session)
	}{
		{name: "split focused pane", slug: "split-right", verify: func(t *testing.T, _ *Daemon, sess *session) {
			tb := sess.activeTab()
			tb.mu.Lock()
			defer tb.mu.Unlock()
			require.Len(t, tb.panes, 2)
		}},
		{name: "create tab from retained viewport", slug: "new-tab", verify: func(t *testing.T, _ *Daemon, sess *session) {
			sess.mu.Lock()
			defer sess.mu.Unlock()
			require.Len(t, sess.tabs, 2)
			require.Equal(t, 1, sess.active)
		}},
		{name: "rename tab", slug: "rename-tab", args: []string{"logs"}, verify: func(t *testing.T, _ *Daemon, sess *session) {
			tb := sess.activeTab()
			tb.mu.Lock()
			defer tb.mu.Unlock()
			require.Equal(t, "logs", tb.name)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &controlPTYFactory{}
			d := newTestDaemon(t, factory, stubClock{})
			t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
			sess := addControlSession(d, "work", "t_work", "p_work")

			result := sendCommand(t, d, ports.CommandRequest{Slug: tt.slug, Args: tt.args, TargetSession: "work"})

			require.True(t, result.OK, result.Text)
			tt.verify(t, d, sess)
		})
	}
}

func TestHandleCommandNewSessionInheritsHeadlessIdentityAndViewport(t *testing.T) {
	factory := &controlPTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	source := addControlSession(d, "work", "t_work", "p_work")
	source.mu.Lock()
	source.cwd = "/tmp/work"
	source.env = []string{"INHERITED=yes"}
	source.terminal = terminalEnv{TrueColor: true}
	source.tabs[0].size = domain.Size{Cols: 118, Rows: 38}
	source.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{Slug: "new-session", Args: []string{"scripted"}, TargetSession: "work"})

	require.True(t, result.OK, result.Text)
	d.mu.Lock()
	created := d.findByNameLocked("scripted")
	d.mu.Unlock()
	require.NotNil(t, created)
	created.mu.Lock()
	require.Nil(t, created.client)
	require.Equal(t, "/tmp/work", created.cwd)
	require.Equal(t, []string{"INHERITED=yes"}, created.env)
	require.True(t, created.terminal.TrueColor)
	tb := created.tabs[0]
	created.mu.Unlock()
	tb.mu.Lock()
	require.Equal(t, domain.Size{Cols: 118, Rows: 38}, tb.size)
	tb.mu.Unlock()
	require.Nil(t, source.client)

	taken := sendCommand(t, d, ports.CommandRequest{Slug: "new-session", Args: []string{"scripted"}, TargetSession: "work"})
	require.False(t, taken.OK)
	require.Equal(t, ports.ErrNameTaken, taken.Code)
}

func TestHandleCommandValidatesToastAndQueuesForDetachedSession(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "t_work", "p_work")

	bad := sendCommand(t, d, ports.CommandRequest{Slug: "toast", Args: []string{"-l", "loud", "hello"}, TargetSession: "work"})
	require.False(t, bad.OK)
	require.Equal(t, ports.ErrInvalidCommandArgs, bad.Code)

	good := sendCommand(t, d, ports.CommandRequest{Slug: "toast", Args: []string{"-l", "warn", "hello"}, TargetSession: "work"})
	require.True(t, good.OK, good.Text)
	d.notices.mu.Lock()
	require.Len(t, d.notices.pending, 1)
	require.Equal(t, domain.NoticeUser, d.notices.pending[0].Code)
	require.Equal(t, sess.id, d.notices.pending[0].SessionID)
	d.notices.mu.Unlock()
}

func TestHandleCommandListingsContainStableIDsMarkersAndCWD(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "t_work", "p_work")
	sess.tabs[0].name = "shell"

	sessions := sendCommand(t, d, ports.CommandRequest{Slug: "list-sessions"})
	require.True(t, sessions.OK, sessions.Text)
	require.Contains(t, sessions.Output, "NAME\tSTATE\tTABS\tATTACHED\tACTIVE")
	require.Contains(t, sessions.Output, "work\tephemeral\t1\tfalse\ttrue")

	tabs := sendCommand(t, d, ports.CommandRequest{Slug: "list-tabs", TargetSession: "work"})
	require.True(t, tabs.OK, tabs.Text)
	require.Contains(t, tabs.Output, "0\tt_work\tshell\t1\ttrue")

	panes := sendCommand(t, d, ports.CommandRequest{Slug: "list-panes", TargetSession: "work", JSON: true})
	require.True(t, panes.OK, panes.Text)
	var decoded []map[string]any
	require.NoError(t, json.Unmarshal([]byte(panes.Output), &decoded))
	require.Len(t, decoded, 1)
	require.Equal(t, "p_work", decoded[0]["id"])
	require.Equal(t, "/tmp/work", decoded[0]["cwd"])
	require.Equal(t, true, decoded[0]["focused"])
}

func TestRemoteCatalogJSONOutput(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	work := addControlSession(d, "work", "t_work", "p_work")
	work.ephemeral = false
	work.client = &attachedClient{}
	work.mruAt.Store(2)
	tb := newTabWithStableID("t_work_2", "p_work_2", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	tb.ctx, tb.cancel = context.WithCancel(d.serveCtx)
	work.mu.Lock()
	work.tabs = append(work.tabs, tb)
	work.mu.Unlock()

	build := addControlSession(d, "build", "t_build", "p_build")
	build.ephemeral = true
	build.mruAt.Store(1)

	d.mu.Lock()
	d.stopped["old"] = stoppedSession{name: "old", cwd: "/tmp/old", createdAt: 1, state: ports.SessionStopped}
	d.mu.Unlock()

	listBefore := sendCommand(t, d, ports.CommandRequest{Slug: "list-sessions"})
	require.True(t, listBefore.OK, listBefore.Text)

	missingJSON := sendCommand(t, d, ports.CommandRequest{Slug: "remote-catalog"})
	require.False(t, missingJSON.OK)
	require.Equal(t, ports.ErrInvalidCommandArgs, missingJSON.Code)

	withArgs := sendCommand(t, d, ports.CommandRequest{Slug: "remote-catalog", Args: []string{"extra"}, JSON: true})
	require.False(t, withArgs.OK)
	require.Equal(t, ports.ErrInvalidCommandArgs, withArgs.Code)

	result := sendCommand(t, d, ports.CommandRequest{Slug: "remote-catalog", JSON: true})
	require.True(t, result.OK, result.Text)
	require.True(t, strings.HasSuffix(result.Output, "\n"), "catalog output must be newline-terminated")

	var catalog ports.RemoteCatalog
	require.NoError(t, json.Unmarshal([]byte(result.Output), &catalog))
	require.Equal(t, ports.ProtocolVersion, catalog.ProtocolVersion)

	raw := map[string]json.RawMessage{}
	require.NoError(t, json.Unmarshal([]byte(result.Output), &raw))
	require.Len(t, raw, 2)
	_, hasProtocol := raw["protocol_version"]
	_, hasSessions := raw["sessions"]
	require.True(t, hasProtocol && hasSessions)

	require.Equal(t, []ports.RemoteCatalogSession{
		{Name: "build", State: "running", Ephemeral: true, Tabs: 1, Attached: false},
		{Name: "work", State: "running", Ephemeral: false, Tabs: 2, Attached: true},
	}, catalog.Sessions)

	for _, session := range catalog.Sessions {
		require.Equal(t, "running", session.State)
		require.NotEqual(t, "old", session.Name)
	}

	listAfter := sendCommand(t, d, ports.CommandRequest{Slug: "list-sessions"})
	require.True(t, listAfter.OK, listAfter.Text)
	require.Equal(t, listBefore.Output, listAfter.Output)
	require.Contains(t, listAfter.Output, "work\tnamed\t2\ttrue\t")
	require.NotContains(t, listAfter.Output, "old")
}

func TestRemoteCatalogTabCountSaturates(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	sess := addControlSession(d, "work", "t_work", "p_work")
	sess.mu.Lock()
	base := sess.tabs[0]
	sess.tabs = make([]*tab, int(^uint16(0))+1)
	for i := range sess.tabs {
		sess.tabs[i] = base
	}
	sess.mu.Unlock()

	result := sendCommand(t, d, ports.CommandRequest{Slug: "remote-catalog", JSON: true})
	require.True(t, result.OK, result.Text)
	var catalog ports.RemoteCatalog
	require.NoError(t, json.Unmarshal([]byte(result.Output), &catalog))
	require.Equal(t, []ports.RemoteCatalogSession{{
		Name: "work", State: "running", Ephemeral: true, Tabs: ^uint16(0), Attached: false,
	}}, catalog.Sessions)
}

func TestHandleCommandSerializesSelfTargetOnNonActiveTab(t *testing.T) {
	factory := &controlPTYFactory{entered: make(chan struct{}), release: make(chan struct{})}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_active", "p_active")
	target := newTabWithStableID("t_invoking", "p_invoking", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	target.ctx, target.cancel = context.WithCancel(d.serveCtx)
	sess.mu.Lock()
	sess.tabs = append(sess.tabs, target)
	sess.active = 0
	sess.mu.Unlock()
	original := target.tree.Focus

	firstFrame := commandFrame(t, ports.CommandRequest{
		Slug: "split-right", Self: true, TargetSession: "work",
		TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	firstTransport, firstSends, _ := newConn(t, firstFrame)
	firstDone := make(chan struct{})
	go func() {
		d.handleCommand(firstTransport, firstFrame)
		close(firstDone)
	}()
	select {
	case <-factory.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first command did not enter its blocked action")
	}

	secondFrame := commandFrame(t, ports.CommandRequest{
		Slug: "focus-pane-right", Self: true, TargetSession: "work",
		TargetTab: "t_invoking", TargetPane: "p_invoking",
	})
	secondTransport, secondSends, _ := newConn(t, secondFrame)
	secondDone := make(chan struct{})
	go func() {
		d.handleCommand(secondTransport, secondFrame)
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("second command bypassed serialization")
	case <-time.After(100 * time.Millisecond):
	}
	close(factory.release)
	<-firstDone
	<-secondDone
	require.True(t, awaitCommandResult(t, firstSends).OK)
	require.True(t, awaitCommandResult(t, secondSends).OK)

	sess.mu.Lock()
	require.Zero(t, sess.active, "self commands must not select the invoking tab")
	sess.mu.Unlock()
	target.mu.Lock()
	defer target.mu.Unlock()
	require.Len(t, target.panes, 2)
	require.NotEqual(t, original, target.tree.Focus, "second command must execute after the split creates its right neighbor")
}

func TestHandleCommandMovePaneUsesActiveFocusedSourceWithSessionFlag(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := addControlSession(d, "work", "t_active", "p_active")
	inactive := newTabWithStableID("t_inactive", "p_inactive", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	inactive.ctx, inactive.cancel = context.WithCancel(d.serveCtx)
	publishTiledPaneOwners(source, inactive)
	source.mu.Lock()
	source.tabs = append(source.tabs, inactive)
	source.active = 0
	source.mu.Unlock()
	destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "move-pane", Args: []string{"dest", "t_dest"}, TargetSession: "work",
	})
	require.True(t, result.OK, result.Text)

	source.mu.Lock()
	require.Len(t, source.tabs, 1)
	require.Equal(t, "t_inactive", source.tabs[0].stableID)
	source.mu.Unlock()
	destination.mu.Lock()
	destTab := destination.tabs[0]
	destination.mu.Unlock()
	destTab.mu.Lock()
	var moved *pane
	for _, candidate := range destTab.panes {
		if candidate.stableID == "p_active" {
			moved = candidate
		}
	}
	require.NotNil(t, moved)
	destTab.mu.Unlock()
}

func TestHandleCommandMovePaneSelfUsesStableSourceIDs(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := addControlSession(d, "work", "t_active", "p_active")
	inactive := newTabWithStableID("t_inactive", "p_inactive", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	inactive.ctx, inactive.cancel = context.WithCancel(d.serveCtx)
	publishTiledPaneOwners(source, inactive)
	source.mu.Lock()
	source.tabs = append(source.tabs, inactive)
	source.active = 0
	source.mu.Unlock()
	destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "move-pane", Args: []string{"dest", "t_dest"}, Self: true,
		TargetSession: "work", TargetTab: "t_inactive", TargetPane: "p_inactive",
	})
	require.True(t, result.OK, result.Text)

	active := source.tabs[0]
	active.mu.Lock()
	require.Len(t, active.panes, 1)
	require.Equal(t, "p_active", active.panes[layout.PaneID("pane-1")].stableID)
	active.mu.Unlock()
	destination.mu.Lock()
	destTab := destination.tabs[0]
	destination.mu.Unlock()
	destTab.mu.Lock()
	var moved *pane
	for _, candidate := range destTab.panes {
		if candidate.stableID == "p_inactive" {
			moved = candidate
		}
	}
	require.NotNil(t, moved)
	destTab.mu.Unlock()
}

func TestHandleCommandMoveTabUsesActiveTabWithoutSelf(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := addControlSession(d, "work", "t_first", "p_first")
	second := newTabWithStableID("t_second", "p_second", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	second.ctx, second.cancel = context.WithCancel(d.serveCtx)
	publishTiledPaneOwners(source, second)
	source.mu.Lock()
	source.tabs = append(source.tabs, second)
	source.active = 1
	source.mu.Unlock()
	destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "move-tab", Args: []string{"dest"}, TargetSession: "work",
	})
	require.True(t, result.OK, result.Text)

	source.mu.Lock()
	require.Len(t, source.tabs, 1)
	require.Equal(t, "t_first", source.tabs[0].stableID)
	source.mu.Unlock()
	destination.mu.Lock()
	require.Len(t, destination.tabs, 2)
	require.Equal(t, "t_second", destination.tabs[1].stableID)
	destination.mu.Unlock()
}

func TestHandleCommandMoveTabSelfUsesStableTab(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := addControlSession(d, "work", "t_first", "p_first")
	second := newTabWithStableID("t_second", "p_second", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	second.ctx, second.cancel = context.WithCancel(d.serveCtx)
	publishTiledPaneOwners(source, second)
	source.mu.Lock()
	source.tabs = append(source.tabs, second)
	source.active = 0
	source.mu.Unlock()
	destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "move-tab", Args: []string{"dest"}, Self: true,
		TargetSession: "work", TargetTab: "t_second", TargetPane: "p_second",
	})
	require.True(t, result.OK, result.Text)

	source.mu.Lock()
	require.Len(t, source.tabs, 1)
	require.Equal(t, "t_first", source.tabs[0].stableID)
	source.mu.Unlock()
	destination.mu.Lock()
	require.Len(t, destination.tabs, 2)
	require.Equal(t, "t_second", destination.tabs[1].stableID)
	destination.mu.Unlock()
}

func TestHandleCommandMoveCommandsRejectInvalidArgsAndTargets(t *testing.T) {
	tests := []struct {
		name    string
		arrange func(*Daemon) ports.CommandRequest
		code    uint16
		text    string
	}{
		{
			name: "move-pane missing destination session",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"work", "t_work"}, TargetSession: "work"}
			},
			code: ports.ErrNoSuchTarget,
			text: "move request is invalid",
		},
		{
			name: "move-pane unknown destination session",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"missing", "t_dest"}, TargetSession: "work"}
			},
			code: ports.ErrNoSuchTarget,
			text: "move target is no longer available",
		},
		{
			name: "move-pane destination tab not in session",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				addNamedMoveDestination(d, "dest", "t_dest", "p_dest")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"dest", "t_other"}, TargetSession: "work"}
			},
			code: ports.ErrNoSuchTarget,
			text: "move target is no longer available",
		},
		{
			name: "move-pane same source tab",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"work", "t_work"}, TargetSession: "work"}
			},
			code: ports.ErrNoSuchTarget,
			text: "move request is invalid",
		},
		{
			name: "move-tab same session",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-tab", Args: []string{"work"}, TargetSession: "work"}
			},
			code: ports.ErrNoSuchTarget,
			text: "move request is invalid",
		},
		{
			name: "move-tab stopping destination",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")
				destination.teardownMu.Lock()
				destination.teardownActive = true
				destination.teardownMu.Unlock()
				return ports.CommandRequest{Slug: "move-tab", Args: []string{"dest"}, TargetSession: "work"}
			},
			code: ports.ErrNoSuchTarget,
			text: "move target is no longer available",
		},
		{
			name: "move-pane too few args",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"dest"}, TargetSession: "work"}
			},
			code: ports.ErrInvalidCommandArgs,
		},
		{
			name: "move-pane too many args",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"dest", "t_dest", "extra"}, TargetSession: "work"}
			},
			code: ports.ErrInvalidCommandArgs,
		},
		{
			name: "move-tab missing destination",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "work", "t_work", "p_work")
				return ports.CommandRequest{Slug: "move-tab", TargetSession: "work"}
			},
			code: ports.ErrInvalidCommandArgs,
		},
		{
			name: "explicit session name without IDs remains authoritative",
			arrange: func(d *Daemon) ports.CommandRequest {
				addControlSession(d, "renamed", "t_work", "p_work")
				addNamedMoveDestination(d, "dest", "t_dest", "p_dest")
				return ports.CommandRequest{Slug: "move-pane", Args: []string{"dest", "t_dest"}, TargetSession: "old-name"}
			},
			code: ports.ErrNoSuchTarget,
			text: "no such session: old-name",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, nil, stubClock{})
			request := tt.arrange(d)
			result := sendCommand(t, d, request)
			require.False(t, result.OK)
			require.Equal(t, tt.code, result.Code)
			if tt.text != "" {
				require.Equal(t, tt.text, result.Text)
			}
		})
	}
}

func TestHandleCommandMovePaneRelocatedStableIDsOverrideAdvisorySessionName(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	advisory := addControlSession(d, "old-name", "t_other", "p_other")
	source := addControlSession(d, "renamed", "t_work", "p_work")
	destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "move-pane", Args: []string{"dest", "t_dest"}, Self: true,
		TargetSession: "old-name", TargetTab: "t_work", TargetPane: "p_work",
	})
	require.True(t, result.OK, result.Text)

	source.mu.Lock()
	require.Len(t, source.tabs, 0)
	source.mu.Unlock()
	advisory.mu.Lock()
	require.Len(t, advisory.tabs, 1, "the advisory session-name match must remain untouched")
	advisory.mu.Unlock()
	destination.mu.Lock()
	destTab := destination.tabs[0]
	destination.mu.Unlock()
	destTab.mu.Lock()
	require.Len(t, destTab.panes, 2)
	destTab.mu.Unlock()
}

func TestHandleCommandMovePaneStableIDsLocateSessionWithoutSelfRedirect(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	source := addControlSession(d, "renamed", "t_active", "p_active")
	inactive := newTabWithStableID("t_inactive", "p_inactive", newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	inactive.ctx, inactive.cancel = context.WithCancel(d.serveCtx)
	publishTiledPaneOwners(source, inactive)
	source.mu.Lock()
	source.tabs = append(source.tabs, inactive)
	source.active = 1
	source.mu.Unlock()
	destination := addNamedMoveDestination(d, "dest", "t_dest", "p_dest")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "move-pane", Args: []string{"dest", "t_dest"},
		TargetSession: "old-name", TargetTab: "t_inactive", TargetPane: "p_inactive",
	})
	require.True(t, result.OK, result.Text)

	source.mu.Lock()
	require.Len(t, source.tabs, 1)
	require.Equal(t, "t_active", source.tabs[0].stableID)
	source.mu.Unlock()
	destination.mu.Lock()
	destTab := destination.tabs[0]
	destination.mu.Unlock()
	destTab.mu.Lock()
	var moved *pane
	for _, candidate := range destTab.panes {
		if candidate.stableID == "p_inactive" {
			moved = candidate
		}
	}
	require.NotNil(t, moved)
	destTab.mu.Unlock()
}

func TestHandleCommandOppositeMoveCommandsDoNotDeadlock(t *testing.T) {
	d, left, _, _ := newManualSessionWithPTYs(t, newQuietPTY(), newQuietPTY())
	left.mu.Lock()
	left.name = "left"
	left.tabs[0].stableID = "left-moved"
	left.active = 0
	left.mu.Unlock()

	right := addMoveTabTestSession(d, "right", "right-stays")
	rightMoved := newTabWithStableID("right-moved", "right-pane", newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	rightMoved.ctx, rightMoved.cancel = context.WithCancel(right.ctx)
	publishTiledPaneOwners(right, rightMoved)
	right.mu.Lock()
	right.tabs = append(right.tabs, rightMoved)
	right.active = 1
	right.mu.Unlock()

	leftFrame := commandFrame(t, ports.CommandRequest{
		Slug: "move-tab", Args: []string{"right"}, TargetSession: "left",
	})
	rightFrame := commandFrame(t, ports.CommandRequest{
		Slug: "move-tab", Args: []string{"left"}, TargetSession: "right",
	})
	leftTransport, leftSends, _ := newConn(t, leftFrame)
	rightTransport, rightSends, _ := newConn(t, rightFrame)
	start := make(chan struct{})
	done := make(chan struct{}, 2)
	go func() {
		<-start
		d.handleCommand(leftTransport, leftFrame)
		done <- struct{}{}
	}()
	go func() {
		<-start
		d.handleCommand(rightTransport, rightFrame)
		done <- struct{}{}
	}()
	close(start)
	for _, sends := range []chan ports.Frame{leftSends, rightSends} {
		select {
		case <-done:
			result := awaitCommandResult(t, sends)
			require.True(t, result.OK, result.Text)
		case <-time.After(5 * time.Second):
			t.Fatal("opposite-direction move commands deadlocked")
		}
	}
}

func addNamedMoveDestination(d *Daemon, name, tabID, paneID string) *session {
	ctx, cancel := context.WithCancel(d.serveCtx)
	tb := newTabWithStableID(tabID, paneID, newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	tb.ctx, tb.cancel = context.WithCancel(ctx)
	sess := &session{
		id: domain.SessionID("sess-" + name), name: name, incarnation: domain.IncarnationID{5},
		ephemeral: true, ctx: ctx, cancel: cancel, tabs: []*tab{tb}, active: 0,
	}
	publishTiledPaneOwners(sess, tb)
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	return sess
}

func commandFrame(t *testing.T, request ports.CommandRequest) ports.Frame {
	t.Helper()
	if request.Version == 0 {
		request.Version = ports.ProtocolVersion
	}
	payload, err := ports.MarshalCommandRequest(request)
	require.NoError(t, err)
	return ports.Frame{Type: ports.MsgCommand, Payload: payload}
}

func sendCommand(t *testing.T, d *Daemon, request ports.CommandRequest) ports.CommandResult {
	t.Helper()
	frame := commandFrame(t, request)
	tr, sends, _ := newConn(t, frame)
	d.handleCommand(tr, frame)
	return awaitCommandResult(t, sends)
}

func awaitCommandResult(t *testing.T, sends chan ports.Frame) ports.CommandResult {
	t.Helper()
	reply := awaitFrame(t, sends, ports.MsgCommandResult)
	result, err := ports.UnmarshalCommandResult(reply.Payload)
	require.NoError(t, err)
	return result
}

type controlPTYFactory struct {
	mu      sync.Mutex
	ptys    []*quietPTY
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (f *controlPTYFactory) Open(_ context.Context, _ string, _ []string, _ []string, _ string, _ domain.Size) (ports.PTY, error) {
	f.once.Do(func() {
		if f.entered != nil {
			close(f.entered)
			<-f.release
		}
	})
	pty := newQuietPTY()
	f.mu.Lock()
	f.ptys = append(f.ptys, pty)
	f.mu.Unlock()
	return pty, nil
}

func (f *controlPTYFactory) close() {
	if f.release != nil {
		select {
		case <-f.release:
		default:
			close(f.release)
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, pty := range f.ptys {
		_ = pty.Close()
	}
}

func addControlSession(d *Daemon, name, tabID, paneID string) *session {
	tb := newTabWithStableID(tabID, paneID, newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	tb.ctx, tb.cancel = context.WithCancel(d.serveCtx)
	sess := &session{
		id: domain.SessionID("sess-" + name), name: name, ephemeral: true, cwd: "/tmp/" + name,
		ctx: d.serveCtx, cancel: func() {}, tabs: []*tab{tb}, env: []string{"MARK=" + name},
	}
	sess.mruAt.Store(1)
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	return sess
}
