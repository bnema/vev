package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/internal/usecase/layout"
	snapcodec "github.com/bnema/vev/internal/usecase/snapshot"
	"github.com/bnema/vev/internal/usecase/visualsearch"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
)

func TestSnapshotCaptureMarshalUnmarshalRestorePreservesTopologyAndTerminalState(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	sess := newSnapshotTestSession(t, "acceptance", false, "/work")
	first := sess.tabs[0]
	first.size = domain.Size{Cols: 80, Rows: 24}
	first.stableID = "tab-one"
	first.nextPaneID = 3
	first.panes["pane-1"].stableID = "pane-one"
	secondPane := newPaneWithStableID("pane-2", "pane-two", snapshotAcceptancePTY(t), domain.Size{Cols: 40, Rows: 24})
	secondPane.history.Append([]renderer.Cell{{Rune: 't'}, {Rune: 'w'}, {Rune: 'o'}})
	secondPane.screen.Write([]byte("two"))
	first.panes["pane-2"] = secondPane
	first.tree = &layout.Tree{Focus: "pane-2", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
		layout.NewLeaf("pane-1"),
		layout.NewLeaf("pane-2"),
	}}}

	second := newTab(snapshotAcceptancePTY(t), domain.Size{Cols: 90, Rows: 30})
	second.stableID = "tab-two"
	second.nextPaneID = 2
	second.panes["pane-1"].stableID = "pane-three"
	second.panes["pane-1"].history.Append([]renderer.Cell{{Rune: 't'}, {Rune: 'h'}, {Rune: 'r'}, {Rune: 'e'}, {Rune: 'e'}})
	second.panes["pane-1"].screen.Write([]byte("three"))
	sess.tabs = []*tab{first, second}
	sess.active = 1

	require.True(t, d.captureSession(sess))
	encoded := <-store.writes
	captured, err := snapcodec.Unmarshal(encoded)
	require.NoError(t, err)
	assertCapturedSessionTopology(t, captured)

	restoreFactory := &restorePTYFactory{}
	restoredDaemon := newTestDaemon(t, restoreFactory, stubClock{})
	require.NoError(t, restoredDaemon.restoreSession(t.Context(), captured))
	restoredDaemon.mu.Lock()
	restored := restoredDaemon.findByNameLocked("acceptance")
	restoredDaemon.mu.Unlock()
	require.NotNil(t, restored)
	require.Equal(t, 1, restored.active)
	require.Len(t, restored.tabs, 2)

	for _, tt := range []struct {
		name      string
		tab       int
		pane      layout.PaneID
		wantRow   string
		wantFocus layout.PaneID
	}{
		{name: "first tab focused second pane", tab: 0, pane: "pane-2", wantRow: "two", wantFocus: "pane-2"},
		{name: "second tab focused only pane", tab: 1, pane: "pane-1", wantRow: "three", wantFocus: "pane-1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tab := restored.tabs[tt.tab]
			tab.mu.Lock()
			defer tab.mu.Unlock()
			require.Equal(t, tt.wantFocus, tab.tree.Focus)
			pane := tab.panes[tt.pane]
			require.NotNil(t, pane)
			pane.mu.Lock()
			defer pane.mu.Unlock()
			require.Equal(t, tt.wantRow, rowText(pane.history.View().Row(0)))
		})
	}
}

func TestSnapshotCaptureRetainsImmutableValuesAfterTailMutationAndEviction(t *testing.T) {
	store := &channelSnapshotStore{writes: make(chan []byte, 1)}
	d := newTestDaemon(t, portsmocks.NewMockPTYFactory(t), stubClock{})
	WithSnapshotStore(store)(d)
	startSnapshotEncodeWorker(t, d)

	entered := make(chan struct{})
	release := make(chan struct{})
	d.snapshotMarshal = func(s snapcodec.Session) ([]byte, error) {
		close(entered)
		<-release
		return snapcodec.Marshal(s)
	}
	sess := newSnapshotTestSession(t, "frozen", false, "/work")
	pane := sess.tabs[0].panes["pane-1"]
	pane.history = vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	pane.history.Append([]renderer.Cell{{Rune: 'o'}, {Rune: 'l'}, {Rune: 'd'}})
	pane.screen = vt.NewScreen(8, 3)
	pane.screen.Write([]byte("before"))

	require.True(t, d.captureSession(sess))
	<-entered
	pane.mu.Lock()
	pane.history.Append([]renderer.Cell{{Rune: 'n'}, {Rune: 'e'}, {Rune: 'w'}})
	pane.history.Append([]renderer.Cell{{Rune: 'e'}, {Rune: 'v'}, {Rune: 'i'}, {Rune: 'c'}, {Rune: 't'}})
	pane.screen.Write([]byte("\rafter"))
	pane.mu.Unlock()
	close(release)

	captured, err := snapcodec.Unmarshal(<-store.writes)
	require.NoError(t, err)
	restoredHistory, err := vt.HistoryFromBlobs(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1}, captured.Tabs[0].Panes[0].SealedChunks, captured.Tabs[0].Panes[0].Tail)
	require.NoError(t, err)
	require.Equal(t, "old", rowText(restoredHistory.View().Row(0)))
	frame, err := vt.UnmarshalVisible(captured.Tabs[0].Panes[0].Visible)
	require.NoError(t, err)
	require.Contains(t, rowText(frame.Row(0)), "before")
}

func TestDaemonHistoryEvictionLeavesCopyAndSearchSnapshotsUsable(t *testing.T) {
	sess := newSnapshotTestSession(t, "copy-lifetime", false, "/work")
	pane := sess.tabs[0].panes["pane-1"]
	pane.history = vt.NewHistory(vt.HistoryConfig{MaxRows: 2, ChunkRows: 1})
	for _, text := range []string{"first", "second"} {
		pane.history.Append(cells(text))
	}
	pane.screen = vt.NewScreen(8, 1)
	pane.screen.Write([]byte("screen"))

	document := scopy.NewSnapshot(pane.history, pane.screen.Frame)
	for _, text := range []string{"third", "fourth"} {
		pane.history.Append(cells(text))
	}

	for _, tt := range []struct {
		name string
		rows func() [][]renderer.Cell
		want []string
	}{
		{name: "bounded live history is oldest first after eviction", rows: func() [][]renderer.Cell {
			view := pane.history.View()
			rows := make([][]renderer.Cell, view.Len())
			for i := range rows {
				rows[i] = view.Row(i)
			}
			return rows
		}, want: []string{"third", "fourth"}},
		{name: "captured document retains evicted rows", rows: func() [][]renderer.Cell {
			rows := make([][]renderer.Cell, document.Len()-document.Height)
			for i := range rows {
				rows[i] = document.Row(i)
			}
			return rows
		}, want: []string{"first", "second"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rows := tt.rows()
			got := make([]string, len(rows))
			for i, row := range rows {
				got[i] = rowText(row)
			}
			require.Equal(t, tt.want, got)
		})
	}

	search := visualsearch.New(document)
	for _, r := range "first" {
		search.Insert(r)
	}
	match, ok := search.Selected()
	require.True(t, ok)
	require.Equal(t, "first", match.Text)
}

func snapshotAcceptancePTY(t *testing.T) *portsmocks.MockPTY {
	t.Helper()
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().Pid().Return(0).Maybe()
	pty.EXPECT().ForegroundPgid().Return(0, nil).Maybe()
	return pty
}

func assertCapturedSessionTopology(t *testing.T, got snapcodec.Session) {
	t.Helper()
	require.Equal(t, "acceptance", got.Name)
	require.Equal(t, uint16(1), got.Active)
	require.Len(t, got.Tabs, 2)
	for _, tt := range []struct {
		name       string
		tab        int
		stableID   string
		focus      layout.PaneID
		nextPaneID uint64
		paneIDs    []layout.PaneID
		wantSplit  bool
	}{
		{name: "first split tab", tab: 0, stableID: "tab-one", focus: "pane-2", nextPaneID: 3, paneIDs: []layout.PaneID{"pane-1", "pane-2"}, wantSplit: true},
		{name: "second leaf tab", tab: 1, stableID: "tab-two", focus: "pane-1", nextPaneID: 2, paneIDs: []layout.PaneID{"pane-1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			tab := got.Tabs[tt.tab]
			require.Equal(t, tt.stableID, tab.StableID)
			require.Equal(t, tt.focus, tab.Focus)
			require.Equal(t, tt.nextPaneID, tab.NextPaneID)
			require.NotNil(t, tab.Tree)
			require.Equal(t, tt.focus, tab.Tree.Focus)
			if tt.wantSplit {
				require.Equal(t, layout.Split, tab.Tree.Root.Kind)
			}
			ids := make(map[layout.PaneID]struct{}, len(tab.Panes))
			for _, pane := range tab.Panes {
				ids[pane.ID] = struct{}{}
			}
			for _, id := range tt.paneIDs {
				_, ok := ids[id]
				require.True(t, ok, "missing pane %q", id)
			}
			_, focused := ids[tab.Focus]
			require.True(t, focused, "focus %q is dangling", tab.Focus)
			assertTreeReferencesCapturedPanes(t, tab.Tree.Root, ids)
		})
	}
}

func assertTreeReferencesCapturedPanes(t *testing.T, node *layout.Node, panes map[layout.PaneID]struct{}) {
	t.Helper()
	if node == nil {
		return
	}
	if node.Kind == layout.Leaf {
		_, ok := panes[node.Leaf]
		require.True(t, ok, "tree leaf %q is dangling", node.Leaf)
		return
	}
	if node.Kind == layout.Stack && node.Expanded != "" {
		_, ok := panes[node.Expanded]
		require.True(t, ok, "stack expanded pane %q is dangling", node.Expanded)
	}
	for _, child := range node.Children {
		assertTreeReferencesCapturedPanes(t, child, panes)
	}
}
