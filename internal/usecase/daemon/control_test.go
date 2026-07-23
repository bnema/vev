package daemon

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

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
	frame := ports.Frame{Type: ports.MsgCommand, Payload: ports.MarshalCommandRequest(request)}
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
		{name: "cross-session IDs rejected", arrange: func(d *Daemon) {
			addControlSession(d, "one", "t_one", "p_one")
			addControlSession(d, "two", "t_two", "p_two")
		}, request: ports.CommandRequest{Slug: "split-right", TargetSession: "one", TargetTab: "t_two", TargetPane: "p_two"}, code: ports.ErrNoSuchTarget},
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

func TestHandleCommandRenameSessionRejectsInvalidNameAsCommandArgs(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	addControlSession(d, "work", "t_work", "p_work")

	result := sendCommand(t, d, ports.CommandRequest{
		Slug: "rename-session", Args: []string{"invalid name"}, TargetSession: "work",
	})

	require.False(t, result.OK)
	require.Equal(t, ports.ErrInvalidCommandArgs, result.Code)
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

func TestHandleCommandSerializesResolutionAndExecutionPerSession(t *testing.T) {
	factory := &controlPTYFactory{entered: make(chan struct{}), release: make(chan struct{})}
	d := newTestDaemon(t, factory, stubClock{})
	t.Cleanup(func() { factory.close(); d.sessWg.Wait() })
	sess := addControlSession(d, "work", "t_work", "p_work")
	original := sess.tabs[0].tree.Focus

	firstFrame := commandFrame(ports.CommandRequest{Slug: "split-right", TargetSession: "work"})
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

	secondFrame := commandFrame(ports.CommandRequest{Slug: "focus-pane-left", TargetSession: "work"})
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

	tb := sess.activeTab()
	tb.mu.Lock()
	defer tb.mu.Unlock()
	require.Len(t, tb.panes, 2)
	require.Equal(t, original, tb.tree.Focus, "second command must resolve after the split changed focus")
}

func commandFrame(request ports.CommandRequest) ports.Frame {
	if request.Version == 0 {
		request.Version = ports.ProtocolVersion
	}
	return ports.Frame{Type: ports.MsgCommand, Payload: ports.MarshalCommandRequest(request)}
}

func sendCommand(t *testing.T, d *Daemon, request ports.CommandRequest) ports.CommandResult {
	t.Helper()
	frame := commandFrame(request)
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
