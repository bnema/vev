package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
	"github.com/bnema/vev/pkg/renderer"
)

func TestMoveTabPreservesWholeTabAndActivatesLogicalNeighbor(t *testing.T) {
	for _, destinationEphemeral := range []bool{false, true} {
		t.Run(map[bool]string{false: "named", true: "ephemeral"}[destinationEphemeral], func(t *testing.T) {
			movedPTY := newQuietPTY()
			remainingPTY := newQuietPTY()
			destinationPTY := newQuietPTY()
			floatingPTY := newQuietPTY()
			d, source, _, _ := newManualSessionWithPTYs(t, movedPTY, remainingPTY)
			moved := source.tabs[0]
			moved.stableID = "moved-tab"
			moved.name = "preserved-name"
			remaining := source.tabs[1]
			remaining.stableID = "remaining-tab"
			selectTestAttachmentTab(source, 0)

			destinationCtx, destinationCancel := context.WithCancel(d.serveCtx)
			t.Cleanup(destinationCancel)
			destinationTab := newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 100, Rows: 30})
			destinationTab.ctx, destinationTab.cancel = context.WithCancel(destinationCtx)
			destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", incarnation: domain.IncarnationID{9}, ephemeral: destinationEphemeral}, ctx: destinationCtx, cancel: destinationCancel, tabs: []*tab{destinationTab}}
			publishTiledPaneOwners(destination, destinationTab)

			floating := newPaneWithStableID("floating", "floating-stable", floatingPTY, domain.Size{Cols: 20, Rows: 8})
			moved.mu.Lock()
			moved.floating = floatingSlot{state: floatingHidden, pane: floating, generation: 7}
			moved.mu.Unlock()
			publishPaneOwner(floating, source, moved, 7)

			d.mu.Lock()
			d.sessions[destination.id] = destination
			d.mu.Unlock()

			tiled := moved.focusedPane()
			tiledScreen := tiled.screen
			tiledOwnerGeneration := tiled.ownerGeneration
			floatingOwnerGeneration := floating.ownerGeneration
			oldWorkerCtx := moved.ctx
			oldTree := moved.tree
			oldPanes := moved.panes
			beforeDestinationActive := destination.tabs[testAttachmentTabIndex(destination)]

			err := d.moveTab(moveTabRequest{
				Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
				SourceTabID: domain.TabStableID(moved.stableID),
				Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			})
			require.NoError(t, err)

			require.Equal(t, []*tab{remaining}, source.tabs)
			require.Zero(t, testAttachmentTabIndex(source))
			require.Same(t, remaining, source.tabs[testAttachmentTabIndex(source)])
			require.Equal(t, []*tab{destinationTab, moved}, destination.tabs)
			require.Same(t, beforeDestinationActive, destination.tabs[testAttachmentTabIndex(destination)], "moved tab stays in the background")
			require.Equal(t, destinationTab.size, moved.size, "destination geometry becomes authoritative")
			require.Equal(t, "preserved-name", moved.name)
			require.Same(t, oldTree, moved.tree)
			require.Equal(t, oldPanes, moved.panes)
			require.Same(t, movedPTY, tiled.pty)
			require.Same(t, tiledScreen, tiled.screen)
			require.Same(t, floating, moved.floating.pane)
			require.Same(t, floatingPTY, floating.pty)
			require.Equal(t, floatingHidden, moved.floating.state)
			require.Equal(t, uint64(7), moved.floating.generation)
			require.Equal(t, tiledOwnerGeneration+1, tiled.ownerGeneration)
			require.Equal(t, floatingOwnerGeneration+1, floating.ownerGeneration)
			require.Same(t, destination, tiled.ownerSnapshot().session)
			require.Same(t, destination, floating.ownerSnapshot().session)
			require.Error(t, oldWorkerCtx.Err(), "source-derived tab worker context is cancelled")
			require.NoError(t, moved.ctx.Err(), "replacement tab worker context belongs to live destination")
		})
	}
}

func TestMoveTabRejectsSameSessionStaleMembershipAndWarming(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(source, destination *session, moved *tab, req *moveTabRequest)
		wantErr error
	}{
		{name: "same session", wantErr: errMovePaneInvalid, mutate: func(source, _ *session, _ *tab, req *moveTabRequest) {
			req.Destination = req.Source
		}},
		{name: "stale incarnation", wantErr: errMoveStaleTarget, mutate: func(_, _ *session, _ *tab, req *moveTabRequest) {
			req.Destination.Incarnation[0]++
		}},
		{name: "missing membership", wantErr: errMoveStaleTarget, mutate: func(source, _ *session, moved *tab, _ *moveTabRequest) {
			source.tabs = source.tabs[1:]
			_ = moved
		}},
		{name: "warming floating", wantErr: errMoveFloatingWarming, mutate: func(_, _ *session, moved *tab, _ *moveTabRequest) {
			moved.floating.state = floatingWarming
			moved.floating.generation = 3
		}},
		{name: "destination teardown", wantErr: errMoveStaleTarget, mutate: func(_, destination *session, _ *tab, _ *moveTabRequest) {
			destination.teardownMu.Lock()
			destination.teardownActive = true
			destination.teardownMu.Unlock()
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, _, _ := newManualSessionWithPTYs(t, newQuietPTY(), newQuietPTY())
			moved := source.tabs[0]
			moved.stableID = "moved-tab"
			destination := addMoveTabTestSession(d, "destination", "destination-tab")
			req := moveTabRequest{
				Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
				Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			}
			tt.mutate(source, destination, moved, &req)
			beforeDestination := append([]*tab(nil), destination.tabs...)
			beforeOwner := moved.focusedPane().ownerSnapshot()

			err := d.moveTab(req)
			destination.teardownMu.Lock()
			destination.teardownActive = false
			destination.teardownMu.Unlock()
			require.ErrorIs(t, err, tt.wantErr)
			var rejection *moveRejection
			require.ErrorAs(t, err, &rejection)
			require.Equal(t, beforeDestination, destination.tabs)
			require.Same(t, beforeOwner, moved.focusedPane().ownerSnapshot())
		})
	}
}

func TestMoveTabFinalSourceVisibleFloatingRejectsStaleSlotTransitions(t *testing.T) {
	for _, tt := range []struct {
		name   string
		mutate func(*tab)
	}{
		{name: "generation", mutate: func(moved *tab) { moved.floating.generation++ }},
		{name: "warming", mutate: func(moved *tab) { moved.floating.state = floatingWarming }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, source, client, _ := newManualSessionWithPTYs(t, newQuietPTY())
			moved := source.tabs[0]
			moved.stableID = "moved-tab"
			require.NotNil(t, d.attachCoordinator(source, nil, client, true))
			destination := addMoveTabTestSession(d, "destination", "destination-tab")
			floating := newPaneWithStableID("floating", "floating-stable", newQuietPTY(), domain.Size{Cols: 20, Rows: 8})
			moved.mu.Lock()
			moved.floating = floatingSlot{state: floatingVisible, pane: floating, desiredVisible: true, generation: 7}
			moved.mu.Unlock()
			publishPaneOwner(floating, source, moved, 7)

			source.layoutApplyMu.Lock()
			admitted := make(chan struct{})
			d.afterAttachmentEffectGateFrozen = func(action string, ac *attachedClient) {
				if action == "move-tab" && ac == client {
					close(admitted)
				}
			}
			moveDone := make(chan error, 1)
			go func() {
				moveDone <- d.moveTab(moveTabRequest{
					Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
					Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
				})
			}()
			awaitTestCompletion(t, admitted, "move-tab did not finish admission")
			moved.mu.Lock()
			tt.mutate(moved)
			moved.mu.Unlock()
			source.layoutApplyMu.Unlock()

			require.Error(t, awaitTestValue(t, moveDone, "stale floating transfer did not return"))
			require.NotContains(t, destination.tabs, moved)
			require.Same(t, source, floating.ownerSnapshot().session)
			d.afterAttachmentEffectGateFrozen = nil
		})
	}
}

func TestMoveTabWaitsForSourceResizeAndAppliesDestinationGeometryLast(t *testing.T) {
	pty := &transactionalResizePTY{}
	d, source, _, _ := newManualSessionWithPTYs(t, pty, newQuietPTY())
	moved := source.tabs[0]
	moved.stableID = "moved-tab"
	destination := addMoveTabTestSession(d, "destination", "destination-tab")
	destination.tabs[0].mu.Lock()
	destination.tabs[0].size = domain.Size{Cols: 100, Rows: 30}
	destination.tabs[0].mu.Unlock()
	moved.mu.Lock()
	moved.size = domain.Size{Cols: 90, Rows: 20}
	moved.bumpLayoutGenerationLocked()
	moved.mu.Unlock()

	resizeEntered := make(chan struct{})
	releaseResize := make(chan struct{})
	var once sync.Once
	pty.onResize = func() {
		once.Do(func() {
			close(resizeEntered)
			<-releaseResize
		})
	}
	layoutDone := make(chan bool, 1)
	go func() { layoutDone <- d.applyTabLayout(source, moved) }()
	<-resizeEntered
	reserved := make(chan struct{})
	d.afterMoveLifecycleReserved = func() { close(reserved) }
	defer func() { d.afterMoveLifecycleReserved = nil }()
	moveDone := make(chan error, 1)
	go func() {
		moveDone <- d.moveTab(moveTabRequest{
			Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
			Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		})
	}()
	<-reserved
	select {
	case err := <-moveDone:
		t.Fatalf("move crossed blocked source resize: %v", err)
	default:
	}
	close(releaseResize)
	<-layoutDone // the old transaction may lose owner publication after its PTY call
	require.NoError(t, <-moveDone)

	pty.mu.Lock()
	sizes := append([]domain.Size(nil), pty.sizes...)
	pty.mu.Unlock()
	require.GreaterOrEqual(t, len(sizes), 2)
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, sizes[len(sizes)-1], "destination geometry must be the final external resize")
}

func TestMoveTabMigratesSyncAndInvalidatesSourceRetryOwner(t *testing.T) {
	d, source, _, _ := newManualSessionWithPTYs(t, newQuietPTY(), newQuietPTY())
	moved := source.tabs[0]
	moved.stableID = "moved-tab"
	destination := addMoveTabTestSession(d, "destination", "destination-tab")
	p := moved.focusedPane()
	sourceCoordinator := d.ensureRenderCoordinator(source)
	destinationCoordinator := d.ensureRenderCoordinator(destination)

	p.mu.Lock()
	p.screen.Write([]byte(renderer.SyncStartCSI))
	generation := source.syncGen.Add(1)
	p.syncGen = generation
	oldLease := p.effectLeaseLocked()
	p.resizeRetry = true
	p.mu.Unlock()
	sourceCoordinator.noteSyncBeginWithRenderability(p, generation, oldLease.Current, nil)
	oldRetry := newAcceptedTabLayoutRetryToken(source, moved, []resizeMember{{session: source, tab: moved, pane: p, owner: oldLease}})

	require.NoError(t, d.moveTab(moveTabRequest{
		Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
		Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
	}))

	require.False(t, oldLease.Current(), "source effects are revoked at owner publication")
	require.False(t, oldRetry.current(), "source delayed retry cannot run after transfer")
	require.Same(t, destination, p.ownerSnapshot().session)
	sourceCoordinator.mu.Lock()
	require.NotContains(t, sourceCoordinator.syncBatches, p)
	sourceCoordinator.mu.Unlock()
	destinationCoordinator.mu.Lock()
	require.Contains(t, destinationCoordinator.syncBatches, p)
	destinationCoordinator.mu.Unlock()
	require.Equal(t, destination.syncGen.Load(), p.syncGen)
	p.mu.Lock()
	require.False(t, p.resizeRetry, "required resize work is completed under the destination owner")
	p.mu.Unlock()
}

func TestMoveTabExitReapsOnlyDestination(t *testing.T) {
	d, source, _, _ := newManualSessionWithPTYs(t, newQuietPTY(), newQuietPTY())
	moved := source.tabs[0]
	moved.stableID = "moved-tab"
	remaining := source.tabs[1]
	destination := addMoveTabTestSession(d, "destination", "destination-tab")
	p := moved.focusedPane()

	require.NoError(t, d.moveTab(moveTabRequest{
		Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
		Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
	}))
	d.reapPaneOwner(p)

	require.Equal(t, []*tab{remaining}, source.tabs)
	require.NotContains(t, destination.tabs, moved, "exit removes the tab from its destination owner")
	require.Nil(t, p.ownerSnapshot())
}

func TestMoveTabOppositeDirectionsDoNotDeadlock(t *testing.T) {
	d, left, _, _ := newManualSessionWithPTYs(t, newQuietPTY(), newQuietPTY())
	left.tabs[0].stableID = "left-moved"
	right := addMoveTabTestSession(d, "right", "right-stays")
	rightMoved := newTabWithStableID("right-moved", "right-pane", newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	rightMoved.ctx, rightMoved.cancel = context.WithCancel(right.ctx)
	publishTiledPaneOwners(right, rightMoved)
	right.tabs = append(right.tabs, rightMoved)

	start := make(chan struct{})
	done := make(chan error, 2)
	go func() {
		<-start
		done <- d.moveTab(moveTabRequest{Source: moveSessionLocator{ID: left.id, Incarnation: left.incarnation}, SourceTabID: domain.TabStableID("left-moved"), Destination: moveSessionLocator{ID: right.id, Incarnation: right.incarnation}})
	}()
	go func() {
		<-start
		done <- d.moveTab(moveTabRequest{Source: moveSessionLocator{ID: right.id, Incarnation: right.incarnation}, SourceTabID: domain.TabStableID("right-moved"), Destination: moveSessionLocator{ID: left.id, Incarnation: left.incarnation}})
	}()
	close(start)
	for range 2 {
		select {
		case err := <-done:
			require.NoError(t, err)
		case <-time.After(5 * time.Second):
			t.Fatal("opposite-direction move tabs deadlocked")
		}
	}
}

type moveTabPurgeCatalogue struct {
	mu      sync.Mutex
	records map[string]domain.CatalogueRecord
	events  []string
}

func (c *moveTabPurgeCatalogue) Records() ([]domain.CatalogueRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	records := make([]domain.CatalogueRecord, 0, len(c.records))
	for _, record := range c.records {
		records = append(records, record)
	}
	return records, nil
}

func (c *moveTabPurgeCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[name]
	return record, ok, nil
}

func (c *moveTabPurgeCatalogue) Create(domain.CatalogueRecord) error {
	return errors.New("move tab purge catalogue: Create is unsupported")
}

func (c *moveTabPurgeCatalogue) Replace(string, domain.CatalogueRecord) error {
	return errors.New("move tab purge catalogue: Replace is unsupported")
}

func (c *moveTabPurgeCatalogue) Rename(string, domain.CatalogueRecord) error {
	return errors.New("move tab purge catalogue: Rename is unsupported")
}

func (c *moveTabPurgeCatalogue) Delete(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.records, name)
	c.events = append(c.events, "catalogue-delete:"+name)
	return nil
}

func (c *moveTabPurgeCatalogue) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, "metadata:"+update.Name)
	return nil
}

func (c *moveTabPurgeCatalogue) Sync() error  { return nil }
func (c *moveTabPurgeCatalogue) Close() error { return nil }

type moveTabPurgeRepository struct {
	noOpSnapshotRepository
	d           *Daemon
	source      *session
	destination *session
	moved       *tab
	pane        *pane
	client      *attachedClient
	catalogue   *moveTabPurgeCatalogue
	err         error
	deleted     []domain.IncarnationID
	outside     bool
	dirty       bool
}

func (r *moveTabPurgeRepository) DeleteIncarnation(_ context.Context, id domain.IncarnationID) error {
	r.deleted = append(r.deleted, id)
	daemonUnlocked := r.d.mu.TryLock()
	routingUnlocked := r.d.notices.routingMu.TryLock()
	sourceUnlocked := r.source.mu.TryLock()
	destinationUnlocked := r.destination.mu.TryLock()
	tabUnlocked := r.moved.mu.TryLock()
	paneUnlocked := r.pane.mu.TryLock()
	sourceFenceUnlocked := r.source.layoutApplyMu.TryLock()
	destinationFenceUnlocked := r.destination.layoutApplyMu.TryLock()
	tabFenceUnlocked := r.moved.layoutApplyMu.TryLock()
	roleUnlocked := r.client.attachmentEffects.mu.TryLock()
	roleStable := roleUnlocked && r.client.attachmentEffects.phase == attachmentEffectsStable
	r.outside = daemonUnlocked && routingUnlocked && sourceUnlocked && destinationUnlocked && tabUnlocked && paneUnlocked &&
		sourceFenceUnlocked && destinationFenceUnlocked && tabFenceUnlocked && roleStable
	if roleUnlocked {
		r.client.attachmentEffects.mu.Unlock()
	}
	if tabFenceUnlocked {
		r.moved.layoutApplyMu.Unlock()
	}
	if destinationFenceUnlocked {
		r.destination.layoutApplyMu.Unlock()
	}
	if sourceFenceUnlocked {
		r.source.layoutApplyMu.Unlock()
	}
	if paneUnlocked {
		r.pane.mu.Unlock()
	}
	if tabUnlocked {
		r.moved.mu.Unlock()
	}
	if destinationUnlocked {
		r.destination.mu.Unlock()
	}
	if sourceUnlocked {
		r.source.mu.Unlock()
	}
	if routingUnlocked {
		r.d.notices.routingMu.Unlock()
	}
	if daemonUnlocked {
		r.d.mu.Unlock()
	}
	r.destination.snapshotMu.Lock()
	r.dirty = r.destination.snapDirty.Load() && r.destination.snapshotGeneration > 0
	r.destination.snapshotMu.Unlock()
	r.catalogue.mu.Lock()
	r.catalogue.events = append(r.catalogue.events, "repository-delete")
	r.catalogue.mu.Unlock()
	return r.err
}

func TestMoveTabFinalNamedSourcePurgesAfterDestinationAdmissionAndFailureDoesNotRollback(t *testing.T) {
	d, source, client, _ := newManualSessionWithPTYs(t, newQuietPTY())
	require.NotNil(t, d.attachCoordinator(source, nil, client, true))
	source.incarnation = domain.IncarnationID{4}
	moved := source.tabs[0]
	moved.stableID = "moved-tab"
	destination := addMoveTabTestSession(d, "destination", "destination-tab")
	destination.ephemeral = false
	destination.snapEligible.Store(true)

	catalogue := &moveTabPurgeCatalogue{records: map[string]domain.CatalogueRecord{
		source.name:      {Name: source.name, IncarnationID: source.incarnation},
		destination.name: {Name: destination.name, IncarnationID: destination.incarnation},
	}}
	failure := errors.New("repository delete failed")
	repository := &moveTabPurgeRepository{
		d: d, source: source, destination: destination, moved: moved, pane: moved.focusedPane(), client: client,
		catalogue: catalogue, err: failure,
	}
	WithCatalogue(catalogue, nil)(d)
	WithSnapshotRepository(repository)(d)
	WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

	require.NoError(t, d.moveTab(moveTabRequest{
		Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
		Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
	}))

	require.Equal(t, []domain.IncarnationID{source.incarnation}, repository.deleted)
	require.True(t, repository.outside, "repository purge must run outside architecture and owner locks")
	require.True(t, repository.dirty, "destination snapshot dirtiness must be admitted before purge")
	catalogue.mu.Lock()
	require.Equal(t, []string{"metadata:destination", "catalogue-delete:work", "repository-delete"}, catalogue.events)
	catalogue.mu.Unlock()
	require.Same(t, moved, destination.tabs[len(destination.tabs)-1], "best-effort purge failure cannot roll back committed membership")
	require.Same(t, destination, moved.focusedPane().ownerSnapshot().session)
}

func addMoveTabTestSession(d *Daemon, id domain.SessionID, tabID string) *session {
	ctx, cancel := context.WithCancel(d.serveCtx)
	tb := newTabWithStableID(tabID, tabID+"-pane", newQuietPTY(), domain.Size{Cols: 80, Rows: 23})
	tb.ctx, tb.cancel = context.WithCancel(ctx)
	sess := &session{sessionCore: sessionCore{id: id, name: string(id), incarnation: domain.IncarnationID{5}, ephemeral: true}, ctx: ctx, cancel: cancel, tabs: []*tab{tb}}
	publishTiledPaneOwners(sess, tb)
	d.mu.Lock()
	d.sessions[sess.id] = sess
	d.mu.Unlock()
	return sess
}
