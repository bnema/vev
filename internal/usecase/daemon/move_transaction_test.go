package daemon

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

func TestMoveTransactionRejectsStalePublicationWithoutLeakingAuthority(t *testing.T) {
	tests := []struct {
		name        string
		installRace func(*Daemon, *tab)
		move        func(*Daemon, *session, *tab, *pane, *session, *tab) error
	}{
		{
			name: "pane",
			installRace: func(d *Daemon, movedTab *tab) {
				d.afterMovePaneSourceSnapshot = func() {
					movedTab.mu.Lock()
					movedTab.layoutGeneration++
					movedTab.mu.Unlock()
				}
			},
			move: func(d *Daemon, source *session, sourceTab *tab, moved *pane, destination *session, destinationTab *tab) error {
				return d.movePane(movePaneRequest{
					Source:           moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
					SourceTabID:      domain.TabStableID(sourceTab.stableID),
					SourcePaneID:     domain.PaneStableID(moved.stableID),
					Destination:      moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
					DestinationTabID: domain.TabStableID(destinationTab.stableID),
				})
			},
		},
		{
			name: "tab",
			installRace: func(d *Daemon, movedTab *tab) {
				d.afterMoveTabSourceSnapshot = func() {
					movedTab.mu.Lock()
					movedTab.layoutGeneration++
					movedTab.mu.Unlock()
				}
			},
			move: func(d *Daemon, source *session, sourceTab *tab, _ *pane, destination *session, _ *tab) error {
				return d.moveTab(moveTabRequest{
					Source:      moveSessionLocator{ID: source.id, Incarnation: source.incarnation},
					SourceTabID: domain.TabStableID(sourceTab.stableID),
					Destination: moveSessionLocator{ID: destination.id, Incarnation: destination.incarnation},
				})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, source, sourceTab, moved, destination, destinationTab, releasePTYs := newMoveGateRaceFixture(t)
			defer releasePTYs()

			source.mu.Lock()
			beforeSourceTabs := append([]*tab(nil), source.tabs...)
			sourceAttachments := source.snapshotAttachmentsLocked()
			source.mu.Unlock()
			destination.mu.Lock()
			beforeDestinationTabs := append([]*tab(nil), destination.tabs...)
			destination.mu.Unlock()
			beforeOwner := moved.ownerSnapshot()

			tt.installRace(d, sourceTab)
			err := tt.move(d, source, sourceTab, moved, destination, destinationTab)
			require.ErrorIs(t, err, errMoveStaleTarget)

			var rejection *moveRejection
			require.ErrorAs(t, err, &rejection)
			require.Equal(t, moveRejectionStaleTarget, rejection.Reason)

			source.mu.Lock()
			require.Equal(t, beforeSourceTabs, source.tabs)
			require.True(t, sameMoveAttachmentsLocked(source, sourceAttachments))
			source.mu.Unlock()
			destination.mu.Lock()
			require.Equal(t, beforeDestinationTabs, destination.tabs)
			destination.mu.Unlock()
			require.Same(t, beforeOwner, moved.ownerSnapshot())

			d.moveLifecycleMu.Lock()
			require.Zero(t, d.moveLifecycleActive)
			d.moveLifecycleMu.Unlock()
			for _, sess := range []*session{source, destination} {
				sess.teardownMu.Lock()
				require.Zero(t, sess.moveReservations)
				sess.teardownMu.Unlock()
			}
			for name, lock := range map[string]*sync.Mutex{
				"source dispatch":        &source.dispatchMu,
				"destination dispatch":   &destination.dispatchMu,
				"source layout":          &source.layoutApplyMu,
				"destination layout":     &destination.layoutApplyMu,
				"moved tab layout":       &sourceTab.layoutApplyMu,
				"destination tab layout": &destinationTab.layoutApplyMu,
				"moved pane resize":      &moved.resizeMu,
			} {
				require.True(t, lock.TryLock(), "%s authority leaked after rejection", name)
				lock.Unlock()
			}
			for _, attachment := range sourceAttachments {
				requireAttachmentEffectGateRetired(t, attachment)
			}
		})
	}
}
