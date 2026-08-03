package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

type paneSyncOwnerFixture struct {
	d                    *Daemon
	pane                 *pane
	source, destination  *session
	sourceTab, targetTab *tab
	sourceCoordinator    *renderCoordinator
	targetCoordinator    *renderCoordinator
	sourceClock          *coordinatorMockClock
	targetClock          *coordinatorMockClock
}

func newPaneSyncOwnerFixture(t *testing.T) paneSyncOwnerFixture {
	t.Helper()
	size := domain.Size{Cols: 80, Rows: 23}
	sourceTab := newTab(newQuietPTY(), size)
	targetTab := newTab(newQuietPTY(), size)
	source := &session{sessionCore: sessionCore{id: "sync-source", attachments: map[*attachedClient]struct{}{&attachedClient{}: {}}}, tabs: []*tab{sourceTab}}
	destination := &session{sessionCore: sessionCore{id: "sync-destination", attachments: map[*attachedClient]struct{}{&attachedClient{}: {}}}, tabs: []*tab{targetTab}}
	d := &Daemon{sessions: map[domain.SessionID]attachmentSession{source.id: source, destination.id: destination}}
	p := sourceTab.focusedPane()
	publishPaneOwner(p, source, sourceTab, 0)

	// Model the membership commit that accompanies owner publication. Keeping
	// the source membership is harmless here; callbacks must route by owner.
	targetTab.mu.Lock()
	targetTab.panes[p.id] = p
	targetTab.tree.Root = layout.NewLeaf(p.id)
	targetTab.tree.Focus = p.id
	targetTab.mu.Unlock()

	sourceClock := newCoordinatorMockClock(t, 8)
	targetClock := newCoordinatorMockClock(t, 8)
	sourceCoordinator := newRenderCoordinator(renderCoordinatorOptions{clock: sourceClock.clock})
	targetCoordinator := newRenderCoordinator(renderCoordinatorOptions{clock: targetClock.clock})
	source.installRenderCoordinator(sourceCoordinator)
	destination.installRenderCoordinator(targetCoordinator)
	return paneSyncOwnerFixture{
		d: d, pane: p, source: source, destination: destination,
		sourceTab: sourceTab, targetTab: targetTab,
		sourceCoordinator: sourceCoordinator, targetCoordinator: targetCoordinator,
		sourceClock: sourceClock, targetClock: targetClock,
	}
}

func (f paneSyncOwnerFixture) armSourceBatch(t *testing.T, active bool, force func()) (*coordinatorMockTimer, paneEffectLease) {
	t.Helper()
	f.pane.mu.Lock()
	if active {
		f.pane.screen.Write([]byte(renderer.SyncStartCSI))
	}
	generation := f.source.syncGen.Add(1)
	f.pane.syncGen = generation
	lease := f.pane.effectLeaseLocked()
	f.pane.mu.Unlock()
	f.sourceCoordinator.noteSyncBeginWithRenderability(f.pane, generation, lease.Current, force)
	return awaitCoordinatorScheduledTimer(t, f.sourceClock), lease
}

func TestMigratePaneSynchronizedOutputOwner(t *testing.T) {
	t.Run("active batch moves to destination and only destination consumes sync end", func(t *testing.T) {
		fixture := newPaneSyncOwnerFixture(t)
		forceEntered := make(chan struct{})
		var enterOnce sync.Once
		var oldLease paneEffectLease
		oldTimer, oldLease := fixture.armSourceBatch(t, true, func() {
			enterOnce.Do(func() { close(forceEntered) })
			fixture.pane.mu.Lock()
			if fixture.pane.owner.Load() == oldLease.owner && fixture.pane.screen.SyncUpdateActive() {
				fixture.pane.screen.ForceSyncEnd()
			}
			fixture.pane.mu.Unlock()
		})
		fixture.sourceCoordinator.mu.Lock()
		oldWorkerDone := fixture.sourceCoordinator.syncBatches[fixture.pane].lane.token.done
		fixture.sourceCoordinator.mu.Unlock()

		// Select the old watchdog while the pane parsing fence is held. The
		// migration must detach it without waiting for its blocked callback.
		fixture.pane.mu.Lock()
		oldTimer.ch <- time.Time{}
		<-forceEntered
		oldOwner, newOwner := fixture.pane.publishOwnerLocked(fixture.destination, fixture.targetTab, 0)
		cleanup := fixture.d.migratePaneSyncOwnerLocked(fixture.pane, oldOwner, newOwner)
		oldTimer.mock.AssertNotCalled(t, "Stop")
		fixture.sourceCoordinator.mu.Lock()
		require.NotContains(t, fixture.sourceCoordinator.syncBatches, fixture.pane)
		fixture.sourceCoordinator.mu.Unlock()
		fixture.targetCoordinator.mu.Lock()
		targetBatch := fixture.targetCoordinator.syncBatches[fixture.pane]
		fixture.targetCoordinator.mu.Unlock()
		require.NotNil(t, targetBatch)
		require.Equal(t, fixture.destination.syncGen.Load(), targetBatch.generation)
		fixture.pane.mu.Unlock()
		cleanup.finish()
		oldTimer.mock.AssertNumberOfCalls(t, "Stop", 1)
		<-oldWorkerDone

		fixture.pane.mu.Lock()
		require.True(t, fixture.pane.screen.SyncUpdateActive(), "selected old watchdog must become a stale no-op")
		fixture.pane.mu.Unlock()

		fixture.d.processPanePTYData(fixture.pane, []byte(renderer.SyncEndCSI), false)
		fixture.sourceCoordinator.mu.Lock()
		require.NotContains(t, fixture.sourceCoordinator.syncBatches, fixture.pane)
		fixture.sourceCoordinator.mu.Unlock()
		fixture.targetCoordinator.mu.Lock()
		require.NotContains(t, fixture.targetCoordinator.syncBatches, fixture.pane)
		fixture.targetCoordinator.mu.Unlock()
		fixture.pane.mu.Lock()
		require.False(t, fixture.pane.screen.SyncUpdateActive())
		fixture.pane.mu.Unlock()
	})

	t.Run("destination callbacks reject a newer owner generation", func(t *testing.T) {
		fixture := newPaneSyncOwnerFixture(t)
		_, _ = fixture.armSourceBatch(t, true, nil)
		fixture.pane.mu.Lock()
		oldOwner, newOwner := fixture.pane.publishOwnerLocked(fixture.destination, fixture.targetTab, 0)
		cleanup := fixture.d.migratePaneSyncOwnerLocked(fixture.pane, oldOwner, newOwner)
		fixture.pane.mu.Unlock()
		cleanup.finish()
		_ = awaitCoordinatorScheduledTimer(t, fixture.targetClock)

		fixture.targetCoordinator.mu.Lock()
		targetBatch := fixture.targetCoordinator.syncBatches[fixture.pane]
		fixture.targetCoordinator.mu.Unlock()
		require.NotNil(t, targetBatch)
		require.True(t, targetBatch.renderable())

		fixture.pane.mu.Lock()
		fixture.pane.publishOwnerLocked(fixture.source, fixture.sourceTab, 0)
		fixture.pane.mu.Unlock()
		require.False(t, targetBatch.renderable(), "renderability must validate the destination owner generation")
		targetBatch.force()
		fixture.pane.mu.Lock()
		require.True(t, fixture.pane.screen.SyncUpdateActive(), "force-end must validate the destination owner generation")
		fixture.pane.mu.Unlock()
		fixture.targetCoordinator.noteSyncPaneRemoved(fixture.pane)
	})

	t.Run("inactive screen removes source batch without creating destination state", func(t *testing.T) {
		fixture := newPaneSyncOwnerFixture(t)
		oldTimer, _ := fixture.armSourceBatch(t, false, nil)

		fixture.pane.mu.Lock()
		oldOwner, newOwner := fixture.pane.publishOwnerLocked(fixture.destination, fixture.targetTab, 0)
		cleanup := fixture.d.migratePaneSyncOwnerLocked(fixture.pane, oldOwner, newOwner)
		require.Zero(t, fixture.pane.syncGen)
		fixture.pane.mu.Unlock()
		cleanup.finish()

		fixture.sourceCoordinator.mu.Lock()
		require.NotContains(t, fixture.sourceCoordinator.syncBatches, fixture.pane)
		fixture.sourceCoordinator.mu.Unlock()
		fixture.targetCoordinator.mu.Lock()
		require.NotContains(t, fixture.targetCoordinator.syncBatches, fixture.pane)
		fixture.targetCoordinator.mu.Unlock()
		require.Zero(t, fixture.destination.syncGen.Load())
		require.Empty(t, fixture.targetClock.timers)
		oldTimer.mock.AssertNumberOfCalls(t, "Stop", 1)
	})
}
