package daemon

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	recoveryusecase "github.com/bnema/vev/internal/usecase/recovery"
)

type movePersistenceCatalogue struct {
	mu                 sync.Mutex
	records            map[string]domain.CatalogueRecord
	events             []string
	source             *session
	destination        *session
	requireSourceDirty bool
	requireDestDirty   bool
	dirtyBeforeUpdate  bool
}

func (c *movePersistenceCatalogue) Records() ([]domain.CatalogueRecord, error) { return nil, nil }
func (c *movePersistenceCatalogue) Record(name string) (domain.CatalogueRecord, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	record, ok := c.records[name]
	return record, ok, nil
}
func (c *movePersistenceCatalogue) Create(domain.CatalogueRecord) error          { return nil }
func (c *movePersistenceCatalogue) Replace(string, domain.CatalogueRecord) error { return nil }
func (c *movePersistenceCatalogue) Rename(string, domain.CatalogueRecord) error  { return nil }
func (c *movePersistenceCatalogue) Delete(name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.records, name)
	c.events = append(c.events, "delete:"+name)
	return nil
}
func (c *movePersistenceCatalogue) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	sourceDirty := true
	if c.requireSourceDirty {
		c.source.snapshotMu.Lock()
		sourceDirty = c.source.snapDirty.Load() && c.source.snapshotGeneration > 0
		c.source.snapshotMu.Unlock()
	}
	destinationDirty := true
	if c.requireDestDirty {
		c.destination.snapshotMu.Lock()
		destinationDirty = c.destination.snapDirty.Load() && c.destination.snapshotGeneration > 0
		c.destination.snapshotMu.Unlock()
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.dirtyBeforeUpdate = c.dirtyBeforeUpdate && sourceDirty && destinationDirty
	c.events = append(c.events, "metadata:"+update.Name)
	return nil
}
func (c *movePersistenceCatalogue) Sync() error  { return nil }
func (c *movePersistenceCatalogue) Close() error { return nil }

type movePersistenceRepository struct {
	noOpSnapshotRepository
	mu           sync.Mutex
	events       []string
	publish      error
	daemon       *Daemon
	source       *session
	destination  *session
	moved        *tab
	pane         *pane
	outsideLocks bool
}

func (r *movePersistenceRepository) Publish(context.Context, ports.SnapshotPublication) error {
	outsideLocks := true
	unlocks := make([]func(), 0, 7)
	for _, candidate := range []struct {
		tryLock func() bool
		unlock  func()
	}{
		{r.daemon.mu.TryLock, r.daemon.mu.Unlock},
		{r.source.mu.TryLock, r.source.mu.Unlock},
		{r.destination.mu.TryLock, r.destination.mu.Unlock},
		{r.moved.mu.TryLock, r.moved.mu.Unlock},
		{r.pane.mu.TryLock, r.pane.mu.Unlock},
		{r.source.layoutApplyMu.TryLock, r.source.layoutApplyMu.Unlock},
		{r.destination.layoutApplyMu.TryLock, r.destination.layoutApplyMu.Unlock},
	} {
		if candidate.tryLock() {
			unlocks = append(unlocks, candidate.unlock)
		} else {
			outsideLocks = false
		}
	}
	for i := len(unlocks) - 1; i >= 0; i-- {
		unlocks[i]()
	}
	r.mu.Lock()
	r.events = append(r.events, "publish")
	r.outsideLocks = outsideLocks
	r.mu.Unlock()
	return r.publish
}

func (r *movePersistenceRepository) DeleteIncarnation(_ context.Context, _ domain.IncarnationID) error {
	r.mu.Lock()
	r.events = append(r.events, "purge")
	r.mu.Unlock()
	return nil
}

func TestMoveTabPersistenceMatrix(t *testing.T) {
	tests := []struct {
		name                      string
		sourceNamed               bool
		destinationNamed          bool
		finalSource               bool
		wantSourceGeneration      bool
		wantDestinationGeneration bool
		wantMetadata              []string
		wantPurge                 bool
	}{
		{name: "named to named", sourceNamed: true, destinationNamed: true, wantSourceGeneration: true, wantDestinationGeneration: true, wantMetadata: []string{"metadata:work", "metadata:destination"}},
		{name: "ephemeral to named", destinationNamed: true, wantDestinationGeneration: true, wantMetadata: []string{"metadata:destination"}},
		{name: "named to named final source", sourceNamed: true, destinationNamed: true, finalSource: true, wantDestinationGeneration: true, wantMetadata: []string{"metadata:destination"}, wantPurge: true},
		{name: "named to ephemeral final source", sourceNamed: true, finalSource: true, wantPurge: true},
		{name: "ephemeral to ephemeral", finalSource: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ptyCount := 2
			if tt.finalSource {
				ptyCount = 1
			}
			ptys := make([]ports.PTY, ptyCount)
			for i := range ptys {
				ptys[i] = newQuietPTY()
			}
			d, source, client, _ := newManualSessionWithPTYs(t, ptys...)
			if tt.finalSource {
				source.client = nil
				client.setSession(nil)
			}
			source.incarnation = domain.IncarnationID{1}
			source.ephemeral = !tt.sourceNamed
			source.snapEligible.Store(tt.sourceNamed)
			source.snapshotWake = d.snapshotWake
			moved := source.tabs[0]
			moved.stableID = "moved-tab"

			destination := addMoveTabTestSession(d, "destination", "destination-tab")
			destination.ephemeral = !tt.destinationNamed
			destination.snapEligible.Store(tt.destinationNamed)
			destination.snapshotWake = d.snapshotWake

			catalogue := &movePersistenceCatalogue{
				records:            map[string]domain.CatalogueRecord{},
				source:             source,
				destination:        destination,
				requireSourceDirty: tt.sourceNamed && !tt.finalSource,
				requireDestDirty:   tt.destinationNamed,
				dirtyBeforeUpdate:  true,
			}
			if tt.sourceNamed {
				catalogue.records[source.name] = domain.CatalogueRecord{Name: source.name, IncarnationID: source.incarnation}
			}
			if tt.destinationNamed {
				catalogue.records[destination.name] = domain.CatalogueRecord{Name: destination.name, IncarnationID: destination.incarnation}
			}
			repository := &movePersistenceRepository{}
			WithCatalogue(catalogue, nil)(d)
			WithSnapshotRepository(repository)(d)
			WithRecoveryCoordinator(recoveryusecase.NewCoordinator(catalogue, repository, nil))(d)

			require.NoError(t, d.moveTab(moveTabRequest{
				Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
				Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
			}))

			source.snapshotMu.Lock()
			sourceGeneration := source.snapshotGeneration
			source.snapshotMu.Unlock()
			destination.snapshotMu.Lock()
			destinationGeneration := destination.snapshotGeneration
			destination.snapshotMu.Unlock()
			require.Equal(t, tt.wantSourceGeneration, sourceGeneration > 0)
			require.Equal(t, tt.wantDestinationGeneration, destinationGeneration > 0)
			catalogue.mu.Lock()
			metadata := append([]string(nil), catalogue.events...)
			dirtyBeforeUpdate := catalogue.dirtyBeforeUpdate
			catalogue.mu.Unlock()
			require.True(t, dirtyBeforeUpdate, "snapshot dirtiness must be admitted before catalogue metadata")
			if tt.wantPurge {
				require.Equal(t, append(append([]string(nil), tt.wantMetadata...), "delete:"+source.name), metadata)
			} else {
				require.ElementsMatch(t, tt.wantMetadata, metadata)
			}
			repository.mu.Lock()
			purged := len(repository.events) == 1 && repository.events[0] == "purge"
			repository.mu.Unlock()
			require.Equal(t, tt.wantPurge, purged)
			require.Same(t, moved, destination.tabs[len(destination.tabs)-1])
		})
	}
}

func TestMoveSnapshotPublicationFailureReportsWithoutRollback(t *testing.T) {
	d, source, _, _ := newManualSessionWithPTYs(t, newQuietPTY(), newQuietPTY())
	source.ephemeral = true
	moved := source.tabs[0]
	moved.stableID = "moved-tab"
	destination := addMoveTabTestSession(d, "destination", "destination-tab")
	destination.ephemeral = false
	destination.snapEligible.Store(true)
	destination.snapshotWake = d.snapshotWake

	repository := &movePersistenceRepository{
		publish: errors.New("injected publication failure"),
		daemon:  d, source: source, destination: destination, moved: moved, pane: moved.focusedPane(),
	}
	WithSnapshotRepository(repository)(d)
	startSnapshotEncodeWorker(t, d)

	require.NoError(t, d.moveTab(moveTabRequest{
		Source: moveSessionLocator{ID: source.id, Incarnation: source.incarnation}, SourceTabID: domain.TabStableID(moved.stableID),
		Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
	}))
	d.scheduleEligibleRepositorySnapshots()
	awaitSnapshotIdle(t, destination)

	require.Same(t, moved, destination.tabs[len(destination.tabs)-1], "repository failure must not roll back live membership")
	require.Same(t, destination, moved.focusedPane().ownerSnapshot().session)
	require.True(t, destination.snapDirty.Load(), "failed publication remains scheduler-retryable")
	repository.mu.Lock()
	require.Equal(t, []string{"publish"}, repository.events, "move persistence uses only the existing snapshot publication format")
	require.True(t, repository.outsideLocks, "repository publication must run outside architecture locks")
	repository.mu.Unlock()
	history := d.notices.history()
	require.NotEmpty(t, history)
	require.Equal(t, domain.NoticeSnapshotWrite, history[len(history)-1].Code)
}
