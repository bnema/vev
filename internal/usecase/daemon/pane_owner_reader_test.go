package daemon

import (
	"io"
	"sync/atomic"
	"testing"
	"testing/synctest"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/domain/layout"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ownerRoutingState is the observable state behind newOwnerRoutingPTY's mock.
type ownerRoutingState struct {
	steps         chan channelPTYStep
	processed     chan struct{}
	activeReaders atomic.Int32
	maxReaders    atomic.Int32
	reads         atomic.Int32
	closes        atomic.Int32
}

func newOwnerRoutingPTY(t testing.TB) (*portsmocks.MockPTY, *ownerRoutingState) {
	t.Helper()
	st := &ownerRoutingState{
		steps:     make(chan channelPTYStep),
		processed: make(chan struct{}, 4),
	}
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		active := st.activeReaders.Add(1)
		defer st.activeReaders.Add(-1)
		for {
			maximum := st.maxReaders.Load()
			if active <= maximum || st.maxReaders.CompareAndSwap(maximum, active) {
				break
			}
		}
		if st.reads.Add(1) > 1 {
			st.processed <- struct{}{}
		}
		step := <-st.steps
		return copy(buf, step.data), step.err
	}).Maybe()
	p.EXPECT().Write(mock.Anything).RunAndReturn(func(b []byte) (int, error) { return len(b), nil }).Maybe()
	p.EXPECT().Close().RunAndReturn(func() error { st.closes.Add(1); return nil }).Maybe()
	p.EXPECT().Resize(mock.Anything).Return(nil).Maybe()
	p.EXPECT().Pid().Return(0).Maybe()
	p.EXPECT().ForegroundPgid().Return(0, nil).Maybe()
	return p, st
}

func (p *pane) clearOwnerForTest() {
	p.mu.Lock()
	p.clearOwnerLocked()
	p.mu.Unlock()
}

func TestPTYReaderExitReapsExactlyCurrentOwnerAfterPublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ptyMock, pty := newOwnerRoutingPTY(t)
		d, source, ac, _ := newManualSessionWithPTYs(t, ptyMock)
		stopOverlayTimersAtCleanup(t, ac)
		sourceTab := testAttachmentTab(source)
		moved := sourceTab.focusedPane()

		destinationTab := newTab(newQuietPTY(t), domain.Size{Cols: 80, Rows: 23})
		other := newPane(layout.PaneID("pane-2"), newQuietPTY(t), domain.Size{Cols: 40, Rows: 23})
		destinationTab.mu.Lock()
		require.NoError(t, destinationTab.tree.Split(layout.PaneID("pane-1"), layout.Right, true, other.id, domain.Rect{Width: 80, Height: 23}))
		destinationTab.panes[other.id] = other
		destinationTab.mu.Unlock()
		destination := &session{sessionCore: sessionCore{id: domain.SessionID("destination"), name: "destination"}, tabs: []*tab{destinationTab}}
		d.sessions[destination.id] = destination

		// Model a completed owner commit while the one reader remains blocked in
		// Read. The source immediately reuses the old local ID, which makes stale
		// ID-only exit routing observably close the wrong pane.
		replacement := newPane(layout.PaneID("pane-1"), newQuietPTY(t), domain.Size{Cols: 80, Rows: 23})
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
	})
}
