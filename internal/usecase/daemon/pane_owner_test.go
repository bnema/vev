package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

func TestPaneOwnerPublicationAdvancesGenerationAndInvalidatesLease(t *testing.T) {
	p := newPaneWithStableID(layout.PaneID("pane-1"), "p_stable", nil, domain.Size{Cols: 80, Rows: 23})
	firstSession := &session{id: "first"}
	firstTab := newTab(nil, domain.Size{Cols: 80, Rows: 23})
	secondSession := &session{id: "second"}
	secondTab := newTab(nil, domain.Size{Cols: 80, Rows: 23})

	p.mu.Lock()
	oldLease, _ := p.publishOwnerLocked(firstSession, firstTab, 0)
	firstLease := p.effectLeaseLocked()
	p.mu.Unlock()
	require.False(t, oldLease.Current())
	require.True(t, firstLease.Current())
	require.Equal(t, uint64(1), firstLease.owner.generation)
	require.Same(t, firstSession, firstLease.owner.session)
	require.Same(t, firstTab, firstLease.owner.tab)

	p.mu.Lock()
	retiredLease, _ := p.publishOwnerLocked(secondSession, secondTab, 0)
	secondLease := p.effectLeaseLocked()
	p.mu.Unlock()
	require.Equal(t, firstLease, retiredLease)
	require.False(t, firstLease.Current())
	require.True(t, secondLease.Current())
	require.Equal(t, uint64(2), secondLease.owner.generation)
	require.Equal(t, "p_stable", p.stableID, "owner publication must preserve stable pane identity")

	p.mu.Lock()
	clearedLease := p.clearOwnerLocked()
	p.mu.Unlock()
	require.Equal(t, secondLease, clearedLease)
	require.False(t, secondLease.Current())
	require.Nil(t, p.ownerSnapshot())
	require.Equal(t, "p_stable", p.stableID, "terminal owner clearing must preserve stable pane identity")
	// Leases retain immutable owner records even after later publication and
	// terminal clearing.
	require.Same(t, firstSession, firstLease.owner.session)
	require.Same(t, firstTab, firstLease.owner.tab)
	require.Equal(t, uint64(1), firstLease.owner.generation)
}

func TestFloatingPaneOwnerLeaseRequiresCurrentSlotGeneration(t *testing.T) {
	p := newPaneWithStableID(layout.PaneID("floating"), "p_float", nil, domain.Size{Cols: 20, Rows: 8})
	sess := &session{id: "owner"}
	tb := newTab(nil, domain.Size{Cols: 80, Rows: 23})

	tb.mu.Lock()
	generation := tb.beginFloatingWarmLocked(true)
	p.mu.Lock()
	p.publishOwnerLocked(sess, tb, generation)
	lease := p.effectLeaseLocked()
	p.mu.Unlock()
	require.True(t, tb.installFloatingLocked(p, generation))
	tb.mu.Unlock()
	require.True(t, lease.Current())

	tb.mu.Lock()
	tb.floating.generation++
	tb.mu.Unlock()
	require.False(t, lease.Current(), "a reused floating slot must reject the old pane effect lease")
	require.Equal(t, "p_float", p.stableID)
}

func TestPublishTiledPaneOwnersInitializesAllPanesBeforePublication(t *testing.T) {
	sess := &session{id: "owner"}
	tb := newTabWithStableID("t_stable", "p_one", nil, domain.Size{Cols: 80, Rows: 23})
	second := newPaneWithStableID("pane-2", "p_two", nil, domain.Size{Cols: 40, Rows: 23})
	tb.panes[second.id] = second

	publishTiledPaneOwners(sess, tb)

	for _, p := range []*pane{tb.panes["pane-1"], second} {
		owner := p.ownerSnapshot()
		require.NotNil(t, owner)
		require.Same(t, sess, owner.session)
		require.Same(t, tb, owner.tab)
		require.Equal(t, uint64(1), owner.generation)
		require.Zero(t, owner.floatingSlotGeneration)
	}
	require.Equal(t, "p_one", tb.panes["pane-1"].stableID)
	require.Equal(t, "p_two", second.stableID)
}
