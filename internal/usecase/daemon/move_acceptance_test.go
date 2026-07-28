package daemon

import (
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

var errMoveCatalogueLocked = errors.New("move catalogue called while architecture locks were held")

func catalogueUpdateNames(updates []domain.CatalogueMetadataUpdate) []string {
	names := make([]string, 0, len(updates))
	for _, update := range updates {
		names = append(names, update.Name)
	}
	return names
}

type moveCatalogueRecorder struct {
	daemon      *Daemon
	source      *session
	destination *session
	mu          sync.Mutex
	updates     []domain.CatalogueMetadataUpdate
}

func (c *moveCatalogueRecorder) Records() ([]domain.CatalogueRecord, error) { return nil, nil }
func (c *moveCatalogueRecorder) Record(string) (domain.CatalogueRecord, bool, error) {
	return domain.CatalogueRecord{}, false, nil
}
func (c *moveCatalogueRecorder) Create(domain.CatalogueRecord) error          { return nil }
func (c *moveCatalogueRecorder) Replace(string, domain.CatalogueRecord) error { return nil }
func (c *moveCatalogueRecorder) Rename(string, domain.CatalogueRecord) error  { return nil }
func (c *moveCatalogueRecorder) Delete(string) error                          { return nil }
func (c *moveCatalogueRecorder) Sync() error                                  { return nil }
func (c *moveCatalogueRecorder) Close() error                                 { return nil }
func (c *moveCatalogueRecorder) UpdateMetadata(update domain.CatalogueMetadataUpdate) error {
	if c.daemon.mu.TryLock() {
		c.daemon.mu.Unlock()
	} else {
		return errMoveCatalogueLocked
	}
	if c.source.mu.TryLock() {
		c.source.mu.Unlock()
	} else {
		return errMoveCatalogueLocked
	}
	if c.destination.mu.TryLock() {
		c.destination.mu.Unlock()
	} else {
		return errMoveCatalogueLocked
	}
	c.mu.Lock()
	c.updates = append(c.updates, update)
	c.mu.Unlock()
	return nil
}

func TestMovePaneAcceptanceRetainsSourceFocusAndDestinationActivity(t *testing.T) {
	movedPTY, releaseMoved := newBlockingPTY(t)
	remainingPTY, releaseRemaining := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	activePTY, releaseActive := newBlockingPTY(t)
	defer releaseMoved()
	defer releaseRemaining()
	defer releaseDestination()
	defer releaseActive()

	d, source, _, _ := newManualSessionWithPTYs(t, movedPTY)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	remaining := newPaneWithStableID("pane-2", "remaining-pane", remainingPTY, domain.Size{Cols: 80, Rows: 23})
	sourceTab.tree = &layout.Tree{
		Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
			layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2"),
		}},
		Focus: "pane-1",
	}
	sourceTab.panes[remaining.id] = remaining
	publishPaneOwner(remaining, source, sourceTab, 0)

	destinationActive := newTabWithStableID("destination-active", "active-pane", activePTY, domain.Size{Cols: 80, Rows: 23})
	destinationTab := newTabWithStableID("destination-target", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})
	destination := &session{
		id: "destination", name: "destination", ephemeral: true,
		tabs: []*tab{destinationActive, destinationTab}, active: 0,
	}
	publishTiledPaneOwners(destination, destinationActive)
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()
	source.ephemeral = false
	destination.ephemeral = false
	source.snapEligible.Store(true)
	destination.snapEligible.Store(true)
	catalogue := &moveCatalogueRecorder{daemon: d, source: source, destination: destination}
	d.catalogue = catalogue
	d.persistEnabled = true
	source.snapshotMu.Lock()
	beforeSourceSnapshot := source.snapshotGeneration
	source.snapshotMu.Unlock()
	destination.snapshotMu.Lock()
	beforeDestinationSnapshot := destination.snapshotGeneration
	destination.snapshotMu.Unlock()

	beforeSourceGeneration := sourceTab.layoutGeneration
	beforeDestinationGeneration := destinationTab.layoutGeneration
	beforeOwnerGeneration := moved.ownerGeneration

	require.NoError(t, d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	}))

	require.Len(t, source.tabs, 1)
	require.Same(t, sourceTab, source.tabs[0])
	require.Same(t, remaining, sourceTab.panes[remaining.id])
	require.Equal(t, layout.PaneID("pane-2"), sourceTab.tree.Focus, "source focus must use the valid fallback leaf")
	require.Same(t, destinationActive, destination.tabs[destination.active], "destination active tab must remain unchanged")
	require.Same(t, moved, destinationTab.panes[moved.id])
	require.Equal(t, destinationTab.tree.Focus, moved.id)
	require.Equal(t, beforeOwnerGeneration+1, moved.ownerGeneration, "move publishes exactly one new owner generation")
	require.Equal(t, beforeSourceGeneration+1, sourceTab.layoutGeneration)
	require.Equal(t, beforeDestinationGeneration+1, destinationTab.layoutGeneration)
	source.snapshotMu.Lock()
	require.GreaterOrEqual(t, source.snapshotGeneration, beforeSourceSnapshot+1)
	require.True(t, source.snapDirty.Load())
	source.snapshotMu.Unlock()
	destination.snapshotMu.Lock()
	require.GreaterOrEqual(t, destination.snapshotGeneration, beforeDestinationSnapshot+1)
	require.True(t, destination.snapDirty.Load())
	destination.snapshotMu.Unlock()
	catalogue.mu.Lock()
	updates := append([]domain.CatalogueMetadataUpdate(nil), catalogue.updates...)
	catalogue.mu.Unlock()
	require.ElementsMatch(t, []string{source.name, destination.name}, catalogueUpdateNames(updates))
}

func TestMovePaneRejectsStaleIncarnationWithoutMutation(t *testing.T) {
	movedPTY, releaseMoved := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	defer releaseMoved()
	defer releaseDestination()

	d, source, _, _ := newManualSessionWithPTYs(t, movedPTY)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	destination := &session{
		id: "destination", name: "destination", ephemeral: true,
		tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}, active: 0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	beforeSource := sourceTab.tree.Clone()
	beforeDestination := destinationTab.tree.Clone()
	beforeOwner := moved.ownerSnapshot()
	staleIncarnation := source.incarnation
	staleIncarnation[0]++
	err := d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id, Incarnation: staleIncarnation},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	})
	require.ErrorIs(t, err, errMovePaneInvalid)
	require.Equal(t, beforeSource, sourceTab.tree)
	require.Equal(t, beforeDestination, destinationTab.tree)
	require.Same(t, beforeOwner, moved.ownerSnapshot())
	require.Same(t, moved, sourceTab.panes[moved.id])
	require.NotSame(t, moved, destinationTab.panes[moved.id])
}

func TestMovePaneRejectsStaleLayoutGenerationBeforeMutation(t *testing.T) {
	movedPTY, releaseMoved := newBlockingPTY(t)
	destinationPTY, releaseDestination := newBlockingPTY(t)
	defer releaseMoved()
	defer releaseDestination()

	d, source, _, _ := newManualSessionWithPTYs(t, movedPTY)
	sourceTab := source.tabs[0]
	sourceTab.stableID = "source-tab"
	moved := sourceTab.focusedPane()
	destination := &session{
		id: "destination", name: "destination", ephemeral: true,
		tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}, active: 0,
	}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	beforeDestination := destinationTab.tree.Clone()
	beforeOwner := moved.ownerSnapshot()
	d.afterMovePaneSourceSnapshot = func() {
		sourceTab.mu.Lock()
		sourceTab.layoutGeneration++
		sourceTab.mu.Unlock()
	}
	defer func() { d.afterMovePaneSourceSnapshot = nil }()

	err := d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	})
	require.Error(t, err)
	require.Equal(t, beforeDestination, destinationTab.tree)
	require.Same(t, beforeOwner, moved.ownerSnapshot())
	require.Same(t, moved, sourceTab.panes[moved.id])
}
