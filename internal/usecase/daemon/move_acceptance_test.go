package daemon

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
)

func catalogueUpdateNames(updates []domain.CatalogueMetadataUpdate) []string {
	names := make([]string, 0, len(updates))
	for _, update := range updates {
		names = append(names, update.Name)
	}
	return names
}

type moveCatalogueRecorder struct {
	daemon        *Daemon
	source        *session
	destination   *session
	mu            sync.Mutex
	updates       []domain.CatalogueMetadataUpdate
	lockViolation bool
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
	violated := false
	if c.daemon.mu.TryLock() {
		c.daemon.mu.Unlock()
	} else {
		violated = true
	}
	if c.source.mu.TryLock() {
		c.source.mu.Unlock()
	} else {
		violated = true
	}
	if c.destination.mu.TryLock() {
		c.destination.mu.Unlock()
	} else {
		violated = true
	}
	c.mu.Lock()
	c.updates = append(c.updates, update)
	c.lockViolation = c.lockViolation || violated
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
	sourceTab.mu.Lock()
	sourceTab.tree = &layout.Tree{
		Root: &layout.Node{Kind: layout.Split, Dir: layout.Horizontal, Children: []*layout.Node{
			layout.NewLeaf("pane-1"), layout.NewLeaf("pane-2"),
		}},
		Focus: "pane-1",
	}
	sourceTab.panes[remaining.id] = remaining
	sourceTab.mu.Unlock()
	publishPaneOwner(remaining, source, sourceTab, 0)

	destinationActive := newTabWithStableID("destination-active", "active-pane", activePTY, domain.Size{Cols: 80, Rows: 23})
	destinationTab := newTabWithStableID("destination-target", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{destinationActive, destinationTab}}
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

	sourceTab.mu.Lock()
	beforeSourceGeneration := sourceTab.layoutGeneration
	sourceTab.mu.Unlock()
	destinationTab.mu.Lock()
	beforeDestinationGeneration := destinationTab.layoutGeneration
	destinationTab.mu.Unlock()
	moved.mu.Lock()
	beforeOwnerGeneration := moved.ownerGeneration
	moved.mu.Unlock()

	require.NoError(t, d.movePane(movePaneRequest{
		Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
		SourceTabID:      domain.TabStableID(sourceTab.stableID),
		SourcePaneID:     domain.PaneStableID(moved.stableID),
		Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
		DestinationTabID: domain.TabStableID(destinationTab.stableID),
	}))

	source.mu.Lock()
	sourceTabs := append([]*tab(nil), source.tabs...)
	source.mu.Unlock()
	require.Len(t, sourceTabs, 1)
	require.Same(t, sourceTab, sourceTabs[0])
	sourceTab.mu.Lock()
	remainingPane := sourceTab.panes[remaining.id]
	sourceFocus := sourceTab.tree.Focus
	sourceGeneration := sourceTab.layoutGeneration
	sourceTab.mu.Unlock()
	require.Same(t, remaining, remainingPane)
	require.Equal(t, layout.PaneID("pane-2"), sourceFocus, "source focus must use the valid fallback leaf")
	destination.mu.Lock()
	destinationActiveTab := destination.tabs[testAttachmentTabIndexLocked(destination)]
	destination.mu.Unlock()
	require.Same(t, destinationActive, destinationActiveTab, "destination active tab must remain unchanged")
	destinationTab.mu.Lock()
	destinationMovedPane := destinationTab.panes[moved.id]
	destinationFocus := destinationTab.tree.Focus
	destinationGeneration := destinationTab.layoutGeneration
	destinationTab.mu.Unlock()
	require.Same(t, moved, destinationMovedPane)
	require.Equal(t, destinationFocus, moved.id)
	moved.mu.Lock()
	ownerGeneration := moved.ownerGeneration
	moved.mu.Unlock()
	require.Equal(t, beforeOwnerGeneration+1, ownerGeneration, "move publishes exactly one new owner generation")
	require.Equal(t, beforeSourceGeneration+1, sourceGeneration)
	require.Equal(t, beforeDestinationGeneration+1, destinationGeneration)
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
	lockViolation := catalogue.lockViolation
	catalogue.mu.Unlock()
	require.False(t, lockViolation, "catalogue publication ran under an architecture lock")
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
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	sourceTab.mu.Lock()
	beforeSource := sourceTab.tree.Clone()
	sourceTab.mu.Unlock()
	destinationTab.mu.Lock()
	beforeDestination := destinationTab.tree.Clone()
	destinationTab.mu.Unlock()
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
	sourceTab.mu.Lock()
	afterSource := sourceTab.tree.Clone()
	sourceMovedPane := sourceTab.panes[moved.id]
	sourceTab.mu.Unlock()
	destinationTab.mu.Lock()
	afterDestination := destinationTab.tree.Clone()
	destinationMovedPane := destinationTab.panes[moved.id]
	destinationTab.mu.Unlock()
	require.Equal(t, beforeSource, afterSource)
	require.Equal(t, beforeDestination, afterDestination)
	require.Same(t, beforeOwner, moved.ownerSnapshot())
	require.Same(t, moved, sourceMovedPane)
	require.NotSame(t, moved, destinationMovedPane)
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
	destination := &session{sessionCore: sessionCore{id: "destination", name: "destination", ephemeral: true}, tabs: []*tab{newTabWithStableID("destination-tab", "destination-pane", destinationPTY, domain.Size{Cols: 80, Rows: 23})}}
	destinationTab := destination.tabs[0]
	publishTiledPaneOwners(destination, destinationTab)
	d.mu.Lock()
	d.sessions[destination.id] = destination
	d.mu.Unlock()

	destinationTab.mu.Lock()
	beforeDestination := destinationTab.tree.Clone()
	destinationTab.mu.Unlock()
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
	destinationTab.mu.Lock()
	afterDestination := destinationTab.tree.Clone()
	destinationTab.mu.Unlock()
	sourceTab.mu.Lock()
	sourceMovedPane := sourceTab.panes[moved.id]
	sourceTab.mu.Unlock()
	require.Equal(t, beforeDestination, afterDestination)
	require.Same(t, beforeOwner, moved.ownerSnapshot())
	require.Same(t, moved, sourceMovedPane)
}
