package daemon

import (
	"context"
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
)

func TestRestoreSnapshotsRestoresLayoutCwdAndRows(t *testing.T) {
	store := &restoreSnapshotStore{}
	store.blobs = []ports.SnapshotBlob{{Name: "work", Data: mustSnapshotBytes(t, snapcodec.Session{
		Name:      "work",
		CreatedAt: 99,
		Active:    0,
		Tabs: []snapcodec.Tab{{
			Cols:       80,
			Rows:       24,
			NextPaneID: 3,
			Focus:      "pane-2",
			Tree: &layout.Tree{Focus: "pane-2", Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
				layout.NewLeaf("pane-1"),
				layout.NewLeaf("pane-2"),
			}}},
			Panes: []snapcodec.Pane{
				{ID: "pane-1", Cwd: "/one", Scrollback: [][]renderer.Cell{cells("old1")}, Visible: [][]renderer.Cell{cells("vis1")}},
				{ID: "pane-2", Cwd: "/two", Scrollback: [][]renderer.Cell{cells("old2")}, Visible: [][]renderer.Cell{cells("vis2")}},
			},
		}},
	})}}
	factory := &restorePTYFactory{}
	d := newTestDaemon(t, factory, stubClock{})
	WithSnapshotStore(store)(d)

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
	tb.mu.Lock()
	require.Equal(t, layout.PaneID("pane-2"), tb.tree.Focus)
	require.Equal(t, 3, tb.nextPaneID)
	p := tb.panes["pane-2"]
	tb.mu.Unlock()
	p.mu.Lock()
	require.Equal(t, "old2", rowText(p.scrollback.Row(0)))
	require.Equal(t, 1, p.scrollback.Len())
	require.Equal(t, "vis2", rowText(p.screen.PrimaryVisibleRows()[0][:4]))
	copySnap := scopy.NewSnapshot(p.scrollback, p.screen.Frame)
	require.Equal(t, "old2", rowText(copySnap.Rows[0][:4]))
	require.Equal(t, "vis2", rowText(copySnap.Rows[1][:4]))
	p.mu.Unlock()
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
			Panes: []snapcodec.Pane{{ID: "a", Cwd: "/a"}, {ID: "b", Cwd: "/b"}, {ID: "c", Cwd: "/c"}},
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
	valid := mustSnapshotBytes(t, snapcodec.Session{Name: "live", CreatedAt: 1, Tabs: []snapcodec.Tab{{Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{{ID: "pane-1", Cwd: "/ok"}}}}})
	tests := []struct {
		name     string
		blobs    []ports.SnapshotBlob
		liveName string
		wantOpen int
	}{
		{name: "empty store"},
		{name: "corrupt blob", blobs: []ports.SnapshotBlob{{Name: "bad", Data: []byte("not a snapshot")}}},
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
			Panes: []snapcodec.Pane{{ID: "pane-1", Cwd: "/bad"}},
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

func TestNamedRouteWaitsForRestoreBarrier(t *testing.T) {
	releaseLoad := make(chan struct{})
	loaded := make(chan struct{})
	store := &restoreSnapshotStore{
		loadFn: func() ([]ports.SnapshotBlob, error) {
			close(loaded)
			<-releaseLoad
			return []ports.SnapshotBlob{{Name: "work", Data: mustSnapshotBytes(t, snapcodec.Session{Name: "work", Tabs: []snapcodec.Tab{{Cols: 80, Rows: 24, Tree: layout.NewTree("pane-1"), Panes: []snapcodec.Pane{{ID: "pane-1", Cwd: "/restored"}}}}})}}, nil
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
	mu    sync.Mutex
	opens []restorePTYOpen
}

type restorePTYOpen struct {
	dir  string
	size domain.Size
}

func (f *restorePTYFactory) Open(_ string, _ []string, _ []string, dir string, sz domain.Size) (ports.PTY, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.opens = append(f.opens, restorePTYOpen{dir: dir, size: sz})
	return newRestorePTY(), nil
}

type restorePTY struct {
	done chan struct{}
	once sync.Once
}

func newRestorePTY() *restorePTY { return &restorePTY{done: make(chan struct{})} }
func (p *restorePTY) Read([]byte) (int, error) {
	<-p.done
	return 0, io.EOF
}
func (p *restorePTY) Write(b []byte) (int, error)  { return len(b), nil }
func (p *restorePTY) Close() error                 { p.once.Do(func() { close(p.done) }); return nil }
func (p *restorePTY) Resize(domain.Size) error     { return nil }
func (p *restorePTY) Pid() int                     { return 0 }
func (p *restorePTY) ForegroundPgid() (int, error) { return 0, nil }
