package daemon

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func terminalPane(t *testing.T, id, stableID, cwd string, historyRows, visibleRows [][]renderer.Cell) snapcodec.Pane {
	t.Helper()
	h := vt.NewHistory(vt.HistoryConfig{MaxRows: defaultScrollbackRows})
	for _, row := range historyRows {
		require.NoError(t, h.Append(row))
	}
	sealed, tail, err := vt.MarshalSealedHistory(h.SealAndView())
	require.NoError(t, err)
	frame := renderer.NewFrame(40, 24)
	for y, row := range visibleRows {
		copy(frame.Row(y), row)
	}
	visible, err := vt.MarshalVisible(frame)
	require.NoError(t, err)
	return snapcodec.Pane{ID: layout.PaneID(id), StableID: stableID, Cwd: cwd, SealedChunks: sealed, Tail: tail, Visible: visible}
}

func TestRestorePaneTerminalRejectsMissingOrMalformedCanonicalBlobs(t *testing.T) {
	valid := terminalPane(t, "pane", "p", "/work", nil, nil)
	tests := []struct {
		name   string
		mutate func(*snapcodec.Pane)
	}{
		{name: "missing tail", mutate: func(p *snapcodec.Pane) { p.Tail = nil }},
		{name: "missing visible", mutate: func(p *snapcodec.Pane) { p.Visible = nil }},
		{name: "malformed tail", mutate: func(p *snapcodec.Pane) { p.Tail = []byte("bad") }},
		{name: "malformed visible", mutate: func(p *snapcodec.Pane) { p.Visible = []byte("bad") }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snap := valid
			tt.mutate(&snap)
			p := newPane("pane", newRestorePTY(), domain.Size{Cols: 40, Rows: 24})
			require.Error(t, restorePaneTerminal(p, snap))
		})
	}
}

func TestRestoreSnapshotsNilDaemonDoesNotPanic(t *testing.T) {
	var d *Daemon
	require.NotPanics(t, func() { d.restoreSnapshots(t.Context()) })
}

func TestRestoreSessionRejectsInvalidActiveTab(t *testing.T) {
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})

	err := d.restoreSession(context.Background(), snapcodec.Session{Name: "bad-active", Active: 1, Tabs: []snapcodec.Tab{{
		Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/tmp")},
	}}})

	require.Error(t, err)
	require.Empty(t, factory.opens)
}

func TestRestoreSessionClosesOpenedPTYWhenTerminalRestoreFails(t *testing.T) {
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	pane := emptyTerminalPane(t, "pane-1", "/tmp")
	pane.Tail = []byte("malformed")

	err := d.restoreSession(context.Background(), snapcodec.Session{Name: "bad", Tabs: []snapcodec.Tab{{
		Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{pane},
	}}})

	require.Error(t, err)
	require.Len(t, factory.opens, 1)
	require.Equal(t, 1, factory.opens[0].pty.closeCount())
}

func TestRestoreSessionUsesDaemonFallbackEnvironmentAndShellBeforeAttach(t *testing.T) {
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	d.baseEnv = []string{"ORDINARY=preserved", "SHELL=/usr/local/bin/daemon-shell", "TERM=stale", "RAW"}

	require.NoError(t, d.restoreSession(context.Background(), snapcodec.Session{Name: "work", Tabs: []snapcodec.Tab{{
		StableID: "tab", Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{terminalPane(t, "pane-1", "pane", "/tmp", nil, nil)},
	}}}))
	initialOpens := factory.snapshotOpens()
	require.Len(t, initialOpens, 1)
	require.Equal(t, "/usr/local/bin/daemon-shell", initialOpens[0].command)
	require.Equal(t, []string{
		"ORDINARY=preserved", "SHELL=/usr/local/bin/daemon-shell", "RAW",
		"TERM=xterm-256color", "TERM_PROGRAM=vev", "VEV=session=work,tab=tab,pane=pane",
	}, initialOpens[0].env)

	d.baseEnv[0] = "ORDINARY=changed"
	d.mu.Lock()
	restored := d.findByNameLocked("work")
	d.mu.Unlock()
	require.NotNil(t, restored)
	restored.mu.Lock()
	require.Equal(t, []string{"ORDINARY=preserved", "SHELL=/usr/local/bin/daemon-shell", "TERM=stale", "RAW"}, restored.env)
	restored.mu.Unlock()

	tr, _ := newCapturingTransport(t)
	_, ac, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentAttach,
		Name:    "work",
		Size:    domain.Size{Cols: 80, Rows: 24},
		Env:     []string{"ORDINARY=attached", "SHELL=/bin/attached-shell"},
	}, tr)
	require.NoError(t, err)
	require.NoError(t, d.createTab(restored, ac.size))

	restored.mu.Lock()
	attachedTab := restored.tabs[1]
	restored.mu.Unlock()
	attachedTab.mu.Lock()
	attachedPane := attachedTab.panes["pane-1"]
	attachedTabStableID := attachedTab.stableID
	attachedPaneStableID := attachedPane.stableID
	attachedTab.mu.Unlock()
	expectedVEV := "VEV=session=work,tab=" + attachedTabStableID + ",pane=" + attachedPaneStableID
	var attachedOpens []restorePTYOpen
	for _, open := range factory.snapshotOpens() {
		if containsEnv(open.env, expectedVEV) {
			attachedOpens = append(attachedOpens, open)
		}
	}
	require.Len(t, attachedOpens, 1)
	require.Equal(t, "/bin/attached-shell", attachedOpens[0].command)
	require.Equal(t, []string{
		"ORDINARY=attached", "SHELL=/bin/attached-shell",
		"TERM=xterm-256color", "TERM_PROGRAM=vev", expectedVEV,
	}, attachedOpens[0].env)
	require.NoError(t, d.killSession(restored, ports.ReasonSessionKilled, false))
	d.sessWg.Wait()
}

func TestRestoreSnapshotsRestoresLayoutCwdAndRows(t *testing.T) {
	store := &restoreSnapshotStore{}
	store.blobs = []ports.SnapshotBlob{{Name: "work", Data: mustSnapshotBytes(t, snapcodec.Session{
		Name:      "work",
		CreatedAt: 99,
		Active:    0,
		Tabs: []snapcodec.Tab{{
			StableID:   "t_saved",
			Cols:       80,
			Rows:       24,
			NextPaneID: 3,
			Focus:      "pane-2",
			Tree: &layout.Tree{Focus: "pane-2", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
				layout.NewLeaf("pane-1"),
				layout.NewLeaf("pane-2"),
			}}},
			Panes: []snapcodec.Pane{
				terminalPane(t, "pane-1", "p_saved_1", "/one", [][]renderer.Cell{cells("old1")}, [][]renderer.Cell{cells("vis1")}),
				terminalPane(t, "pane-2", "p_saved_2", "/two", [][]renderer.Cell{cells("old2")}, [][]renderer.Cell{cells("vis2")}),
			},
		}},
	})}}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	persistStore, persistState := newMockStore(t)
	WithStore(persistStore)(d)
	WithSnapshotStore(store)(d)
	d.stopped["work"] = stoppedSession{name: "work", createdAt: 99, tabNames: []string{"logs"}}

	d.restoreSnapshots(context.Background())

	require.Len(t, factory.opens, 2)
	require.Equal(t, "/one", factory.opens[0].dir)
	require.Equal(t, "/two", factory.opens[1].dir)
	require.Equal(t, domain.Size{Cols: 40, Rows: 24}, factory.opens[0].size)

	d.mu.Lock()
	restored := d.findByNameLocked("work")
	d.mu.Unlock()
	require.NotNil(t, restored)
	require.False(t, restored.snapDirty.Load())
	require.Equal(t, domain.SessionID("sess-0"), restored.id)
	require.Equal(t, int64(99), restored.createdAt)
	require.Equal(t, "/one", restored.cwd)

	restored.mu.Lock()
	require.Len(t, restored.tabs, 1)
	tb := restored.tabs[0]
	restored.mu.Unlock()
	require.Equal(t, "logs", tb.name)
	require.Equal(t, []string{"logs"}, persistState.record(t, "work").TabNames)
	tb.mu.Lock()
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	require.Equal(t, 3, tb.nextPaneID)
	p := tb.panes["pane-2"]
	tb.mu.Unlock()
	p.mu.Lock()
	require.Same(t, p.screen.History(), p.history)
	require.Equal(t, "old2", rowText(p.history.View().Row(0)))
	require.Equal(t, 1, p.history.Len())
	require.Equal(t, "vis2", rowText(p.screen.PrimaryVisibleRows()[0][:4]))
	copySnap := scopy.NewSnapshot(p.history, p.screen.Frame)
	require.Equal(t, "old2", rowText(copySnap.Row(0)[:4]))
	require.Equal(t, "vis2", rowText(copySnap.Row(1)[:4]))
	p.mu.Unlock()

	tb.mu.Lock()
	tabStableID := tb.stableID
	pane1StableID := tb.panes["pane-1"].stableID
	pane2StableID := tb.panes["pane-2"].stableID
	tb.mu.Unlock()
	require.Equal(t, "t_saved", tabStableID)
	require.Equal(t, "p_saved_1", pane1StableID)
	require.Equal(t, "p_saved_2", pane2StableID)
	require.Contains(t, factory.opens[0].env, "TERM=xterm-256color")
	require.NotContains(t, factory.opens[0].env, "TERM=xterm-direct")
	require.NotContains(t, factory.opens[0].env, "COLORTERM=truecolor")
	require.Contains(t, factory.opens[0].env, "TERM_PROGRAM=vev")
	require.Contains(t, factory.opens[0].env, "VEV=session=work,tab="+tabStableID+",pane="+pane1StableID)
	require.Contains(t, factory.opens[1].env, "TERM=xterm-256color")
	require.NotContains(t, factory.opens[1].env, "TERM=xterm-direct")
	require.NotContains(t, factory.opens[1].env, "COLORTERM=truecolor")
	require.Contains(t, factory.opens[1].env, "TERM_PROGRAM=vev")
	require.Contains(t, factory.opens[1].env, "VEV=session=work,tab="+tabStableID+",pane="+pane2StableID)
}

func TestRestoreSnapshotsLeavesFloatingUninitializedAndInactiveTabsCold(t *testing.T) {
	store := &restoreSnapshotStore{blobs: []ports.SnapshotBlob{{Name: "work", Data: mustSnapshotBytes(t, snapcodec.Session{
		Name: "work", Active: 0, Tabs: []snapcodec.Tab{
			{Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/one")}},
			{Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/two")}},
		},
	})}}}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	d.ApplyConfig(domain.Config{Floating: domain.FloatingConfig{Command: "btop", Width: 80, Height: 80}})
	WithSnapshotStore(store)(d)
	d.restoreSnapshots(context.Background())
	// Only the persisted normal panes may be restored. Floating runtimes are
	// deliberately excluded and must wait for a Phase-5 activation path.
	require.Len(t, factory.opens, 2)
	d.mu.Lock()
	restored := d.findByNameLocked("work")
	d.mu.Unlock()
	require.NotNil(t, restored)
	restored.mu.Lock()
	tabs := append([]*tab(nil), restored.tabs...)
	restored.mu.Unlock()
	require.Len(t, tabs, 2)
	for _, tb := range tabs {
		tb.mu.Lock()
		require.Equal(t, floatingUninitialized, tb.floating.state)
		require.Nil(t, tb.floating.pane)
		tb.mu.Unlock()
	}
}

func processRestoreTestStore(t *testing.T, name string, proc *snapcodec.Process) *restoreSnapshotStore {
	t.Helper()
	pane := emptyTerminalPane(t, "pane-1", "/tmp")
	pane.Process = proc
	return &restoreSnapshotStore{blobs: []ports.SnapshotBlob{{Name: name, Data: mustSnapshotBytes(t, snapcodec.Session{
		Name: name,
		Tabs: []snapcodec.Tab{{
			Cols:  80,
			Rows:  24,
			Tree:  layout.NewTree("pane-1"),
			Panes: []snapcodec.Pane{pane},
		}},
	})}}}
}

func TestRestoreSnapshotsProcessRestoreCommands(t *testing.T) {
	lessProc := &snapcodec.Process{Argv: []string{"less", "README.md"}, Strategy: processStrategyGeneric}
	agentProc := &snapcodec.Process{Argv: []string{"pi"}, Strategy: processStrategyPi, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc123"}}
	controlAgentProc := &snapcodec.Process{Argv: []string{"pi"}, Strategy: processStrategyPi, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc\nmalicious"}}

	tests := []struct {
		name       string
		store      *restoreSnapshotStore
		allow      []string
		wantWrites [][]string
	}{
		{
			name:       "allowlisted generic command",
			store:      processRestoreTestStore(t, "allowlisted", lessProc),
			allow:      []string{"less"},
			wantWrites: [][]string{{"less README.md\n"}},
		},
		{
			name:       "denied generic command",
			store:      processRestoreTestStore(t, "denied", lessProc),
			allow:      []string{"vim"},
			wantWrites: [][]string{{}},
		},
		{
			name: "pane scoped command",
			store: &restoreSnapshotStore{blobs: []ports.SnapshotBlob{{Name: "scoped", Data: mustSnapshotBytes(t, snapcodec.Session{
				Name: "scoped",
				Tabs: []snapcodec.Tab{
					{
						Cols: 80,
						Rows: 24,
						Tree: layout.NewTree("pane-1"),
						Panes: func() []snapcodec.Pane {
							pane := emptyTerminalPane(t, "pane-1", "/one")
							pane.Process = lessProc
							return []snapcodec.Pane{pane}
						}(),
					},
					{
						Cols:  80,
						Rows:  24,
						Tree:  layout.NewTree("pane-1"),
						Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/two")},
					},
				},
			})}}},
			allow:      []string{"less"},
			wantWrites: [][]string{{"less README.md\n"}, {}},
		},
		{
			name:       "agent ID command",
			store:      processRestoreTestStore(t, "agent", agentProc),
			allow:      []string{"pi"},
			wantWrites: [][]string{{"pi --resume abc123\n"}},
		},
		{
			name:       "agent ID with control byte rejected",
			store:      processRestoreTestStore(t, "agent-control", controlAgentProc),
			allow:      []string{"pi"},
			wantWrites: [][]string{{}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &restorePTYFactory{}
			d := newTestDaemon(t, factory, stubClock{})
			WithSnapshotStore(tt.store)(d)
			d.ApplyConfig(domain.Config{Snapshot: domain.SnapshotConfig{RestoreProcessesSet: true, RestoreProcesses: tt.allow}})

			d.restoreSnapshots(context.Background())

			require.Len(t, factory.opens, len(tt.wantWrites))
			for i, want := range tt.wantWrites {
				if len(want) == 0 {
					require.Empty(t, factory.opens[i].pty.writes)
					continue
				}
				require.Equal(t, want, factory.opens[i].pty.writes)
			}
		})
	}
}

func TestRestoreSnapshotsProcessCommandWriteFailureIsNonFatal(t *testing.T) {
	proc := &snapcodec.Process{Argv: []string{"pi"}, Strategy: processStrategyPi, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc123"}}
	factory := &restorePTYFactory{writeErr: errors.New("boom")}
	d := newTestDaemon(t, factory, stubClock{})
	d.ApplyConfig(domain.Config{Snapshot: domain.SnapshotConfig{RestoreProcessesSet: true, RestoreProcesses: []string{"pi"}}})

	pane := emptyTerminalPane(t, "pane-1", "/tmp")
	pane.Process = proc
	require.NoError(t, d.restoreSession(context.Background(), snapcodec.Session{Name: "agent", Tabs: []snapcodec.Tab{{Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{pane}}}}))
	require.Len(t, factory.opens, 1)
	require.Equal(t, []string{"pi --resume abc123\n"}, factory.opens[0].pty.writes)
}

func TestRestoreSessionPassesRestoreContextToPTYOpen(t *testing.T) {
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	require.NoError(t, d.restoreSession(ctx, snapcodec.Session{Name: "restore-context", Tabs: []snapcodec.Tab{{
		Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/tmp")},
	}}}))
	require.Len(t, factory.opens, 1)
	require.Same(t, ctx, factory.opens[0].ctx)
}

func TestRestoreSnapshotsOpensCollapsedStackPanesWithValidPTYSize(t *testing.T) {
	store := &restoreSnapshotStore{blobs: []ports.SnapshotBlob{{Name: "stacked", Data: mustSnapshotBytes(t, snapcodec.Session{
		Name: "stacked",
		Tabs: []snapcodec.Tab{{
			Cols: 20,
			Rows: 4,
			Tree: &layout.Tree{Focus: "b", Root: &layout.Node{Kind: layout.Stack, Expanded: "b", Children: []*layout.Node{
				layout.NewLeaf("a"), layout.NewLeaf("b"), layout.NewLeaf("c"),
			}}},
			Panes: []snapcodec.Pane{
				emptyTerminalPane(t, "a", "/a"),
				emptyTerminalPane(t, "b", "/b"),
				emptyTerminalPane(t, "c", "/c"),
			},
		}},
	})}}}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	WithSnapshotStore(store)(d)

	d.restoreSnapshots(context.Background())

	require.Len(t, factory.opens, 3)
	for _, open := range factory.opens {
		require.True(t, open.size.Valid(), "open size for %s was %#v", open.dir, open.size)
		require.GreaterOrEqual(t, open.size.Cols, layout.MinPaneCols)
		require.GreaterOrEqual(t, open.size.Rows, layout.MinPaneRows)
	}
	d.mu.Lock()
	restored := d.findByNameLocked("stacked")
	d.mu.Unlock()
	require.NotNil(t, restored)
	restored.mu.Lock()
	tb := restored.tabs[0]
	restored.mu.Unlock()
	tb.mu.Lock()
	require.Equal(t, layout.Stack, tb.tree.Root.Kind)
	require.Equal(t, layout.PaneID("b"), tb.tree.Root.Expanded)
	require.Len(t, tb.panes, 3)
	tb.mu.Unlock()
}

func TestRestoreSnapshotsSkipsLiveCorruptAndEmpty(t *testing.T) {
	valid := mustSnapshotBytes(t, snapcodec.Session{
		Name:      "live",
		CreatedAt: 1,
		Tabs: []snapcodec.Tab{{
			Cols:  80,
			Rows:  24,
			Tree:  layout.NewTree("pane-1"),
			Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/ok")},
		}},
	})
	badVersion := append([]byte(nil), valid...)
	binary.BigEndian.PutUint16(badVersion[4:6], 1)
	tests := []struct {
		name     string
		blobs    []ports.SnapshotBlob
		liveName string
		wantOpen int
	}{
		{name: "empty store"},
		{name: "corrupt blob", blobs: []ports.SnapshotBlob{{Name: "bad", Data: []byte("not a snapshot")}}},
		{name: "old v1 blob", blobs: []ports.SnapshotBlob{{Name: "old", Data: badVersion}}},
		{name: "live name", blobs: []ports.SnapshotBlob{{Name: "live", Data: valid}}, liveName: "live"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := &restorePTYFactory{}
			d := newTestDaemon(t, factory, stubClock{})
			WithSnapshotStore(&restoreSnapshotStore{blobs: tt.blobs})(d)
			if tt.liveName != "" {
				d.sessions["existing"] = &session{id: "existing", name: tt.liveName}
			}

			require.NotPanics(t, func() { d.restoreSnapshots(context.Background()) })
			require.Len(t, factory.opens, tt.wantOpen)
		})
	}
}

func TestRestoreSnapshotsSkipsCreatedAtOverflow(t *testing.T) {
	store := &restoreSnapshotStore{blobs: []ports.SnapshotBlob{{Name: "overflow", Data: mustSnapshotBytes(t, snapcodec.Session{
		Name:      "overflow",
		CreatedAt: uint64(math.MaxInt64) + 1,
		Tabs: []snapcodec.Tab{{
			Cols:  80,
			Rows:  24,
			Tree:  layout.NewTree("pane-1"),
			Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/bad")},
		}},
	})}}}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	WithSnapshotStore(store)(d)

	d.restoreSnapshots(context.Background())

	records, err := d.persist.LoadAll()
	require.NoError(t, err)
	require.Empty(t, records)
	require.Len(t, factory.opens, 0)
	d.mu.Lock()
	restored := d.findByNameLocked("overflow")
	d.mu.Unlock()
	require.Nil(t, restored)
}

// TestRestoreSnapshotsNotifiesGlobalOnUnrestorableSnapshots proves each
// unrestorable snapshot is recorded to history individually, while the
// pending-global queue dedups them by code into a single toast with the
// combined Count. Both blobs decode fine (valid snapcodec bytes, Marshal
// itself validates Active/tab range) but fail in restoreSession itself: an
// empty embedded session name is rejected deterministically, before any lock
// is taken or PTY is opened.
func TestRestoreSnapshotsNotifiesGlobalOnUnrestorableSnapshots(t *testing.T) {
	badSnap := mustSnapshotBytes(t, snapcodec.Session{Name: ""})
	store := &restoreSnapshotStore{blobs: []ports.SnapshotBlob{
		{Name: "bad-1", Data: badSnap},
		{Name: "bad-2", Data: badSnap},
	}}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, newNoticeClock())
	WithSnapshotStore(store)(d)

	// Mirrors real startup (daemon.go's Serve): restoreSnapshots runs off the
	// caller goroutine and signals completion by closing d.restoreDone.
	d.sessWg.Go(func() { d.restoreSnapshots(d.serveCtx) })

	select {
	case <-d.restoreDone:
	case <-time.After(2 * time.Second):
		t.Fatal("restoreSnapshots did not close restoreDone in time")
	}

	require.Empty(t, factory.opens, "both snapshots must fail before any PTY is opened")
	require.Len(t, d.notices.history(), 2, "each failure is recorded individually")
	for _, n := range d.notices.history() {
		require.Equal(t, domain.NoticeSnapshotRestore, n.Code)
		require.Equal(t, domain.NoticeError, n.Severity)
	}

	tr, _ := newCapturingTransport(t)
	sess, ac, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentEphemeral,
		Size:    domain.Size{Cols: 80, Rows: 24},
	}, tr)
	require.NoError(t, err)
	d.firstPaint(sess, ac, ac.size)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeSnapshotRestore, toasts[0].Code)
	require.Equal(t, 2, toasts[0].Count, "two same-code failures dedup into one toast counted twice")
	require.Empty(t, d.notices.drainPending(), "firstPaint must consume the queue")
}

func TestRestoreSnapshotsDoesNotNotifyForCancelledRestore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	factory := &restorePTYFactory{onOpen: cancel}
	store := &restoreSnapshotStore{blobs: []ports.SnapshotBlob{{Name: "cancelled", Data: mustSnapshotBytes(t, snapcodec.Session{
		Name: "cancelled",
		Tabs: []snapcodec.Tab{{
			Cols: 80, Rows: 24,
			Tree: &layout.Tree{Focus: "one", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
				layout.NewLeaf("one"), layout.NewLeaf("two"),
			}}},
			Panes: []snapcodec.Pane{
				emptyTerminalPane(t, "one", "/one"),
				emptyTerminalPane(t, "two", "/two"),
			},
		}},
	})}}}
	d := newTestDaemon(t, factory, stubClock{})
	WithSnapshotStore(store)(d)

	d.restoreSnapshots(ctx)

	require.Len(t, factory.snapshotOpens(), 1, "cancellation stops restore before the next pane")
	require.Empty(t, d.notices.history(), "shutdown cancellation is not a user-visible restore failure")
}

func TestRestoreSnapshotsNotifiesGlobalOnResumeCommandWriteFailure(t *testing.T) {
	proc := &snapcodec.Process{Argv: []string{"pi"}, Strategy: processStrategyPi, Opts: snapcodec.ProcessOpts{AgentSessionID: "abc123"}}
	store := processRestoreTestStore(t, "agent-session", proc)
	factory := &restorePTYFactory{writeErr: errors.New("boom")}
	d := newTestDaemon(t, factory, newNoticeClock())
	WithSnapshotStore(store)(d)
	d.ApplyConfig(domain.Config{Snapshot: domain.SnapshotConfig{RestoreProcessesSet: true, RestoreProcesses: []string{"pi"}}})

	// Mirrors real startup (daemon.go's Serve): restoreSnapshots runs off the
	// caller goroutine and signals completion by closing d.restoreDone.
	d.sessWg.Go(func() { d.restoreSnapshots(d.serveCtx) })

	select {
	case <-d.restoreDone:
	case <-time.After(2 * time.Second):
		t.Fatal("restoreSnapshots did not close restoreDone in time")
	}

	require.Len(t, factory.opens, 1, "the pane still opens even though the resume write fails")
	require.Equal(t, []string{"pi --resume abc123\n"}, factory.opens[0].pty.writes)

	history := d.notices.history()
	require.Len(t, history, 1)
	require.Equal(t, domain.NoticeAutoResume, history[0].Code)
	require.Equal(t, domain.NoticeWarn, history[0].Severity)
	require.Equal(t, "couldn't restore the running program in session agent-session", history[0].Message)

	tr, _ := newCapturingTransport(t)
	sess, ac, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion,
		Intent:  ports.IntentEphemeral,
		Size:    domain.Size{Cols: 80, Rows: 24},
	}, tr)
	require.NoError(t, err)
	d.firstPaint(sess, ac, ac.size)

	toasts := awaitToastCount(t, ac, 1)
	require.Equal(t, domain.NoticeAutoResume, toasts[0].Code)
	require.Empty(t, d.notices.drainPending(), "firstPaint must consume the queue")
}

func TestNamedRouteWaitsForRestoreBarrier(t *testing.T) {
	releaseLoad := make(chan struct{})
	loaded := make(chan struct{})
	store := &restoreSnapshotStore{
		loadFn: func() ([]ports.SnapshotBlob, error) {
			close(loaded)
			<-releaseLoad
			return []ports.SnapshotBlob{{Name: "work", Data: mustSnapshotBytes(t, snapcodec.Session{
				Name: "work",
				Tabs: []snapcodec.Tab{{
					Cols:  80,
					Rows:  24,
					Tree:  layout.NewTree("pane-1"),
					Panes: []snapcodec.Pane{emptyTerminalPane(t, "pane-1", "/restored")},
				}},
			})}}, nil
		},
	}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	WithSnapshotStore(store)(d)

	go d.restoreSnapshots(context.Background())
	<-loaded

	routed := make(chan *session, 1)
	go func() {
		tr, _ := newCapturingTransport(t)
		sess, _, err := d.route(ports.Hello{Intent: ports.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 26}}, tr)
		require.NoError(t, err)
		routed <- sess
	}()

	select {
	case <-routed:
		t.Fatal("named attach routed before restore barrier closed")
	case <-time.After(25 * time.Millisecond):
	}
	close(releaseLoad)
	select {
	case sess := <-routed:
		require.Equal(t, "work", sess.name)
		require.Len(t, factory.opens, 1)
		require.Equal(t, "/restored", factory.opens[0].dir)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for named attach after restore")
	}
}

func TestNamedRouteRestoreBarrierReturnsShutdown(t *testing.T) {
	d := newTestDaemon(t, &restorePTYFactory{}, stubClock{})
	WithSnapshotStore(&restoreSnapshotStore{})(d)
	d.serveCancel()

	tr, _ := newCapturingTransport(t)
	sess, ac, err := d.route(ports.Hello{Intent: ports.IntentAttach, Name: "work", Size: domain.Size{Cols: 80, Rows: 24}}, tr)

	require.Nil(t, sess)
	require.Nil(t, ac)
	var perr *protoErr
	require.ErrorAs(t, err, &perr)
	require.Equal(t, ports.ErrServerShutdown, perr.code)
}

func emptyTerminalPane(t *testing.T, id, cwd string) snapcodec.Pane {
	t.Helper()
	return terminalPane(t, id, "", cwd, nil, nil)
}

func mustSnapshotBytes(t *testing.T, s snapcodec.Session) []byte {
	t.Helper()
	b, err := snapcodec.Marshal(s)
	require.NoError(t, err)
	return b
}

func cells(s string) []renderer.Cell {
	out := make([]renderer.Cell, 0, len(s))
	for _, r := range s {
		out = append(out, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
	}
	return out
}

type restoreSnapshotStore struct {
	blobs  []ports.SnapshotBlob
	loadFn func() ([]ports.SnapshotBlob, error)
}

func (s *restoreSnapshotStore) Write(string, []byte) error { return nil }
func (s *restoreSnapshotStore) Delete(string) error        { return nil }
func (s *restoreSnapshotStore) Load() ([]ports.SnapshotBlob, error) {
	if s.loadFn != nil {
		return s.loadFn()
	}
	return s.blobs, nil
}

type restorePTYFactory struct {
	mu       sync.Mutex
	opens    []restorePTYOpen
	writeErr error
	onOpen   func()
}

type restorePTYOpen struct {
	ctx     context.Context
	command string
	dir     string
	size    domain.Size
	env     []string
	pty     *restorePTY
}

func (f *restorePTYFactory) Open(ctx context.Context, command string, _ []string, env []string, dir string, sz domain.Size) (ports.PTY, error) {
	f.mu.Lock()
	pty := newRestorePTY()
	pty.writeErr = f.writeErr
	f.opens = append(f.opens, restorePTYOpen{ctx: ctx, command: command, dir: dir, size: sz, env: append([]string(nil), env...), pty: pty})
	onOpen := f.onOpen
	f.mu.Unlock()
	if onOpen != nil {
		onOpen()
	}
	return pty, nil
}

func (f *restorePTYFactory) snapshotOpens() []restorePTYOpen {
	f.mu.Lock()
	defer f.mu.Unlock()
	opens := make([]restorePTYOpen, len(f.opens))
	copy(opens, f.opens)
	for i := range opens {
		opens[i].env = append([]string(nil), opens[i].env...)
	}
	return opens
}

type restorePTY struct {
	mu       sync.Mutex
	writes   []string
	closes   int
	writeErr error
	done     chan struct{}
	once     sync.Once
}

func newRestorePTY() *restorePTY { return &restorePTY{done: make(chan struct{})} }
func (p *restorePTY) Read([]byte) (int, error) {
	<-p.done
	return 0, io.EOF
}
func (p *restorePTY) Write(b []byte) (int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.writes = append(p.writes, string(b))
	if p.writeErr != nil {
		return 0, p.writeErr
	}
	return len(b), nil
}
func (p *restorePTY) Close() error {
	p.mu.Lock()
	p.closes++
	p.mu.Unlock()
	p.once.Do(func() { close(p.done) })
	return nil
}
func (p *restorePTY) closeCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.closes
}
func (p *restorePTY) Resize(domain.Size) error     { return nil }
func (p *restorePTY) Pid() int                     { return 0 }
func (p *restorePTY) ForegroundPgid() (int, error) { return 0, nil }
