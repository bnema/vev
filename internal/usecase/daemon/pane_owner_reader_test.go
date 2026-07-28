package daemon

import (
	"io"
	"sync/atomic"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

type ownerRoutingPTY struct {
	steps         chan channelPTYStep
	processed     chan struct{}
	activeReaders atomic.Int32
	maxReaders    atomic.Int32
	reads         atomic.Int32
	closes        atomic.Int32
}

func newOwnerRoutingPTY() *ownerRoutingPTY {
	return &ownerRoutingPTY{
		steps:     make(chan channelPTYStep),
		processed: make(chan struct{}, 4),
	}
}

func (p *ownerRoutingPTY) Read(buf []byte) (int, error) {
	active := p.activeReaders.Add(1)
	defer p.activeReaders.Add(-1)
	for {
		maximum := p.maxReaders.Load()
		if active <= maximum || p.maxReaders.CompareAndSwap(maximum, active) {
			break
		}
	}
	if p.reads.Add(1) > 1 {
		p.processed <- struct{}{}
	}
	step := <-p.steps
	return copy(buf, step.data), step.err
}

func (*ownerRoutingPTY) Write(b []byte) (int, error) { return len(b), nil }
func (p *ownerRoutingPTY) Close() error              { p.closes.Add(1); return nil }
func (*ownerRoutingPTY) Resize(domain.Size) error    { return nil }
func (*ownerRoutingPTY) Pid() int                    { return 0 }
func (*ownerRoutingPTY) ForegroundPgid() (int, error) {
	return 0, nil
}

func installTiledPaneOwnerForTest(sess *session, tb *tab, p *pane) {
	tb.mu.Lock()
	old := tb.panes[layout.PaneID("pane-1")]
	delete(tb.panes, layout.PaneID("pane-1"))
	if old != nil {
		old.clearOwnerForTest()
	}
	p.id = layout.PaneID("pane-1")
	tb.panes[p.id] = p
	tb.tree = &layout.Tree{Root: layout.NewLeaf(p.id), Focus: p.id}
	publishPaneOwner(p, sess, tb, 0)
	tb.mu.Unlock()
}

func (p *pane) clearOwnerForTest() {
	p.mu.Lock()
	p.clearOwnerLocked()
	p.mu.Unlock()
}

func TestPTYReaderRoutesEffectsToOwnerPublishedForEachRead(t *testing.T) {
	pty := newOwnerRoutingPTY()
	d, source, _, _ := newManualSessionWithPTYs(t, pty)
	pane := source.activeTab().focusedPane()
	pane.onExit = func() {}
	source.snapEligible.Store(true)

	destinationTab := newTab(nil, domain.Size{Cols: 80, Rows: 23})
	destination := &session{
		id:           domain.SessionID("destination"),
		name:         "destination",
		tabs:         []*tab{destinationTab},
		client:       &attachedClient{},
		snapEligible: atomic.Bool{},
	}
	destination.snapEligible.Store(true)
	destination.client.initOverlays()
	destination.client.setSession(destination)
	d.sessions[destination.id] = destination

	sourceInvalidations := make(chan renderInvalidation, 2)
	destinationInvalidations := make(chan renderInvalidation, 2)
	sourceCoordinator := d.attachCoordinator(source, nil, source.client, true)
	sourceCoordinator.opts.onInvalidate = func(invalidation renderInvalidation) { sourceInvalidations <- invalidation }
	destinationCoordinator := d.attachCoordinator(destination, nil, destination.client, true)
	destinationCoordinator.opts.onInvalidate = func(invalidation renderInvalidation) { destinationInvalidations <- invalidation }

	d.startPaneGoroutines(source, source.activeTab(), pane)
	pty.steps <- channelPTYStep{data: []byte("source\a")}
	<-pty.processed
	require.Len(t, sourceInvalidations, 1)
	require.Empty(t, destinationInvalidations)
	require.Equal(t, uint64(1), source.snapshotGeneration)
	require.Zero(t, destination.snapshotGeneration)
	require.True(t, source.activeTab().attention)

	installTiledPaneOwnerForTest(destination, destinationTab, pane)
	pty.steps <- channelPTYStep{data: []byte("destination\a")}
	<-pty.processed

	require.Len(t, sourceInvalidations, 1, "output after owner publication must not invalidate the source")
	require.Len(t, destinationInvalidations, 1, "output after owner publication must invalidate the destination")
	require.Equal(t, uint64(1), source.snapshotGeneration, "source snapshot must not be dirtied after transfer")
	require.Equal(t, uint64(1), destination.snapshotGeneration)
	require.True(t, destinationTab.attention)
	require.Equal(t, int32(1), pty.maxReaders.Load(), "owner publication must not start another PTY reader")

	pty.steps <- channelPTYStep{err: io.EOF}
	d.sessWg.Wait()
}

func TestPTYReaderExitReapsExactlyCurrentOwnerAfterPublication(t *testing.T) {
	pty := newOwnerRoutingPTY()
	d, source, _, _ := newManualSessionWithPTYs(t, pty)
	sourceTab := source.activeTab()
	moved := sourceTab.focusedPane()

	destinationTab := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	other := newPane(layout.PaneID("pane-2"), newQuietPTY(), domain.Size{Cols: 40, Rows: 23})
	destinationTab.mu.Lock()
	require.NoError(t, destinationTab.tree.Split(layout.PaneID("pane-1"), layout.Right, true, other.id, domain.Rect{Width: 80, Height: 23}))
	destinationTab.panes[other.id] = other
	destinationTab.mu.Unlock()
	destination := &session{id: domain.SessionID("destination"), name: "destination", tabs: []*tab{destinationTab}}
	d.sessions[destination.id] = destination

	// Model a completed owner commit while the one reader remains blocked in
	// Read. The source immediately reuses the old local ID, which makes stale
	// ID-only exit routing observably close the wrong pane.
	replacement := newPane(layout.PaneID("pane-1"), newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	sourceTab.mu.Lock()
	sourceTab.panes[replacement.id] = replacement
	publishPaneOwner(replacement, source, sourceTab, 0)
	sourceTab.mu.Unlock()

	destinationTab.mu.Lock()
	displaced := destinationTab.panes[layout.PaneID("pane-1")]
	displaced.clearOwnerForTest()
	destinationTab.panes[moved.id] = moved
	publishPaneOwner(moved, destination, destinationTab, 0)
	destinationTab.mu.Unlock()

	d.sessWg.Add(1)
	go d.ptyReader(source, sourceTab, moved)
	pty.steps <- channelPTYStep{err: io.EOF}
	d.sessWg.Wait()

	d.mu.Lock()
	require.Same(t, source, d.sessions[source.id], "stale exit must not close the source replacement")
	require.Same(t, destination, d.sessions[destination.id])
	d.mu.Unlock()
	sourceTab.mu.Lock()
	require.Same(t, replacement, sourceTab.panes[replacement.id])
	sourceTab.mu.Unlock()
	destinationTab.mu.Lock()
	require.NotContains(t, destinationTab.panes, moved.id, "the exited pane must be removed from its current owner")
	require.Same(t, other, destinationTab.panes[other.id])
	destinationTab.mu.Unlock()
	require.Nil(t, moved.ownerSnapshot())
	require.True(t, replacement.effectLease().Current())
	require.Equal(t, int32(1), pty.maxReaders.Load())

	// A stale second reap is a no-op and cannot close either surviving owner.
	d.reapPaneOwner(moved)
	sourceTab.mu.Lock()
	require.Same(t, replacement, sourceTab.panes[replacement.id])
	sourceTab.mu.Unlock()
	destinationTab.mu.Lock()
	require.Same(t, other, destinationTab.panes[other.id])
	destinationTab.mu.Unlock()
	require.Equal(t, int32(1), pty.closes.Load(), "exit must release the process exactly once")
}

var _ ports.PTY = (*ownerRoutingPTY)(nil)
