package daemon

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/usecase/layout"
)

func requireMutexLocked(t *testing.T, mu *sync.Mutex, message string) {
	t.Helper()
	require.Eventually(t, func() bool {
		if !mu.TryLock() {
			return true
		}
		mu.Unlock()
		return false
	}, time.Second, time.Millisecond, message)
}

func TestMoveResizeFencesAcquireSessionsByStableID(t *testing.T) {
	first := &session{id: "a-first"}
	second := &session{id: "z-second"}
	fences := newMoveResizeFences([]*session{second, first}, nil, nil)

	first.layoutApplyMu.Lock()
	acquired := make(chan bool, 1)
	go func() {
		acquired <- fences.acquire(func() bool { return true })
	}()

	require.True(t, second.layoutApplyMu.TryLock(), "acquisition reached the later session before the lowest SessionID")
	second.layoutApplyMu.Unlock()
	first.layoutApplyMu.Unlock()

	require.True(t, awaitTestValue(t, acquired, "ordered resize fences did not acquire"))
	fences.Release()
	require.True(t, first.layoutApplyMu.TryLock())
	first.layoutApplyMu.Unlock()
	require.True(t, second.layoutApplyMu.TryLock())
	second.layoutApplyMu.Unlock()
}

func TestMoveResizeFencesAcquireTabsByStableIDAfterSessions(t *testing.T) {
	sess := &session{id: "session"}
	first := &tab{stableID: "a-first"}
	second := &tab{stableID: "z-second"}
	fences := newMoveResizeFences([]*session{sess}, []*tab{second, first}, nil)

	first.layoutApplyMu.Lock()
	acquired := make(chan bool, 1)
	go func() {
		acquired <- fences.acquire(func() bool { return true })
	}()

	requireMutexLocked(t, &sess.layoutApplyMu, "session fence was not acquired before waiting on a tab fence")
	require.True(t, second.layoutApplyMu.TryLock(), "acquisition reached the later tab before the lowest stable tab ID")
	second.layoutApplyMu.Unlock()
	select {
	case <-acquired:
		t.Fatal("resize fences ignored the blocked first tab")
	default:
	}
	first.layoutApplyMu.Unlock()

	require.True(t, awaitTestValue(t, acquired, "ordered tab resize fences did not acquire"))
	fences.Release()
}

func TestMoveResizeFencesAcquirePanesByStableIDAfterTabs(t *testing.T) {
	sess := &session{id: "session"}
	tb := &tab{stableID: "tab"}
	first := &pane{stableID: "a-first"}
	second := &pane{stableID: "z-second"}
	fences := newMoveResizeFences([]*session{sess}, []*tab{tb}, []*pane{second, first})

	first.resizeMu.Lock()
	acquired := make(chan bool, 1)
	go func() {
		acquired <- fences.acquire(func() bool { return true })
	}()

	requireMutexLocked(t, &sess.layoutApplyMu, "session fence was not acquired before pane wait")
	requireMutexLocked(t, &tb.layoutApplyMu, "tab fence was not acquired before pane wait")
	require.True(t, second.resizeMu.TryLock(), "acquisition reached the later pane before the lowest stable pane ID")
	second.resizeMu.Unlock()
	select {
	case <-acquired:
		t.Fatal("resize fences ignored the blocked first pane")
	default:
	}
	first.resizeMu.Unlock()

	require.True(t, awaitTestValue(t, acquired, "ordered pane resize fences did not acquire"))
	fences.Release()
}

func TestMoveResizeFencesDeduplicateStableIdentities(t *testing.T) {
	sess := &session{id: "session"}
	tb := &tab{stableID: "tab"}
	p := &pane{stableID: "pane"}

	fences := newMoveResizeFences([]*session{sess, sess}, []*tab{tb, tb}, []*pane{p, p})

	require.Len(t, fences.sessions, 1)
	require.Len(t, fences.tabs, 1)
	require.Len(t, fences.panes, 1)
	require.True(t, fences.acquire(func() bool { return true }))
	fences.Release()
}

func TestMoveResizeFencesDeduplicateSessionIDWithoutReplacingResolvedOwner(t *testing.T) {
	resolved := &session{id: "same"}
	collision := &session{id: "same"}

	fences := newMoveResizeFences([]*session{resolved, collision}, nil, nil)

	require.Len(t, fences.sessions, 1)
	require.Same(t, resolved, fences.sessions[0])
}

func TestMoveResizeFencesDeduplicateTabIDWithoutReplacingResolvedOwner(t *testing.T) {
	resolved := &tab{stableID: "same"}
	collision := &tab{stableID: "same"}

	fences := newMoveResizeFences(nil, []*tab{resolved, collision}, nil)

	require.Len(t, fences.tabs, 1)
	require.Same(t, resolved, fences.tabs[0])
}

func TestMoveResizeFencesDeduplicatePaneIDWithoutReplacingResolvedOwner(t *testing.T) {
	resolved := &pane{stableID: "same"}
	collision := &pane{stableID: "same"}

	fences := newMoveResizeFences(nil, nil, []*pane{resolved, collision})

	require.Len(t, fences.panes, 1)
	require.Same(t, resolved, fences.panes[0])
}

func TestMoveResizeFencesRejectStaleMembershipAndCleanUp(t *testing.T) {
	sess := &session{id: "session"}
	tb := &tab{stableID: "tab"}
	p := &pane{stableID: "pane"}
	fences := newMoveResizeFences([]*session{sess}, []*tab{tb}, []*pane{p})

	membershipCurrent := true
	p.resizeMu.Lock()
	acquired := make(chan bool, 1)
	callbackCalled := make(chan struct{})
	go func() {
		acquired <- fences.acquire(func() bool {
			close(callbackCalled)
			return membershipCurrent
		})
	}()
	requireMutexLocked(t, &tb.layoutApplyMu, "resize fence acquisition did not reach the blocked pane")
	membershipCurrent = false
	p.resizeMu.Unlock()

	require.False(t, awaitTestValue(t, acquired, "stale resize fence acquisition did not return"))
	awaitTestCompletion(t, callbackCalled, "membership was not revalidated after resize fence acquisition")
	for _, lock := range []*sync.Mutex{&sess.layoutApplyMu, &tb.layoutApplyMu, &p.resizeMu} {
		require.True(t, lock.TryLock(), "rejected acquisition leaked a resize fence")
		lock.Unlock()
	}
}

func TestMoveResizeFencesCleanUpWhenPublicationPanics(t *testing.T) {
	sess := &session{id: "session"}
	tb := &tab{stableID: "tab"}
	p := &pane{stableID: "pane"}
	fences := newMoveResizeFences([]*session{sess}, []*tab{tb}, []*pane{p})

	require.Panics(t, func() {
		fences.acquire(func() bool { panic("publication failed") })
	})
	for _, lock := range []*sync.Mutex{&sess.layoutApplyMu, &tb.layoutApplyMu, &p.resizeMu} {
		require.True(t, lock.TryLock(), "panicked publication leaked a partial resize fence set")
		lock.Unlock()
	}
}

func TestMoveResizeFencesWaitBeforeTakingArchitectureLocks(t *testing.T) {
	d := &Daemon{}
	sess := &session{id: "session"}
	tb := &tab{stableID: "tab"}
	p := &pane{stableID: "pane"}
	fences := newMoveResizeFences([]*session{sess}, []*tab{tb}, []*pane{p})

	p.resizeMu.Lock()
	acquired := make(chan bool, 1)
	go func() {
		acquired <- fences.acquire(func() bool { return true })
	}()
	requireMutexLocked(t, &tb.layoutApplyMu, "resize fence acquisition did not reach the blocked pane")

	for name, lock := range map[string]*sync.Mutex{
		"daemon":  &d.mu,
		"session": &sess.mu,
		"tab":     &tb.mu,
	} {
		require.True(t, lock.TryLock(), "%s architecture lock was held while waiting on pane resize", name)
		lock.Unlock()
	}
	p.resizeMu.Unlock()

	require.True(t, awaitTestValue(t, acquired, "resize fences did not acquire after pane resize released"))
	fences.Release()
}

func TestMoveResizeFencesHoldThroughPublicationAndGenerationBumps(t *testing.T) {
	source := &session{id: "source"}
	destination := &session{id: "destination"}
	sourceTab := &tab{stableID: "a-source-tab"}
	destinationTab := &tab{stableID: "z-destination-tab"}
	p := &pane{stableID: "pane"}
	fences := newMoveResizeFences(
		[]*session{source, destination},
		[]*tab{sourceTab, destinationTab},
		[]*pane{p},
	)

	require.True(t, fences.acquire(func() bool {
		// This callback is the move's architecture-lock window: membership is
		// revalidated before owner publication and both affected generations bump.
		sourceTab.mu.Lock()
		destinationTab.mu.Lock()
		sourceTab.bumpLayoutGenerationLocked()
		destinationTab.bumpLayoutGenerationLocked()
		destinationTab.mu.Unlock()
		sourceTab.mu.Unlock()
		return true
	}))
	require.Equal(t, uint64(1), sourceTab.layoutGeneration)
	require.Equal(t, uint64(1), destinationTab.layoutGeneration)
	for _, lock := range []*sync.Mutex{&source.layoutApplyMu, &destination.layoutApplyMu, &sourceTab.layoutApplyMu, &destinationTab.layoutApplyMu, &p.resizeMu} {
		require.False(t, lock.TryLock(), "publication returned without retaining every resize fence")
	}

	// Move callers release here, before applyTabLayout or any other PTY I/O.
	fences.Release()
	for _, lock := range []*sync.Mutex{&source.layoutApplyMu, &destination.layoutApplyMu, &sourceTab.layoutApplyMu, &destinationTab.layoutApplyMu, &p.resizeMu} {
		require.True(t, lock.TryLock(), "publication fence did not release before layout application")
		lock.Unlock()
	}
}

func TestMovePaneResizeFencesCoverAffectedOwners(t *testing.T) {
	source := &session{id: "source"}
	destination := &session{id: "destination"}
	sourceTab := &tab{stableID: "source-tab"}
	destinationTab := &tab{stableID: "destination-tab"}
	moved := &pane{stableID: "moved-pane"}

	fences := newMovePaneResizeFences(source, destination, sourceTab, destinationTab, moved)

	require.ElementsMatch(t, []*session{source, destination}, fences.sessions)
	require.ElementsMatch(t, []*tab{sourceTab, destinationTab}, fences.tabs)
	require.Equal(t, []*pane{moved}, fences.panes)
}

func TestMoveTabResizeFencesCoverEveryContainedPane(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state floatingState
	}{{name: "hidden", state: floatingHidden}, {name: "visible", state: floatingVisible}} {
		t.Run(tc.name, func(t *testing.T) {
			source := &session{id: "source"}
			destination := &session{id: "destination"}
			first := &pane{stableID: "first"}
			second := &pane{stableID: "second"}
			floating := &pane{stableID: "floating"}
			movedTab := &tab{
				stableID: "moved-tab",
				panes: map[layout.PaneID]*pane{
					"pane-1": first,
					"pane-2": second,
				},
				floating: floatingSlot{state: tc.state, pane: floating, generation: 7},
			}

			fences := newMoveTabResizeFences(source, destination, movedTab)

			require.ElementsMatch(t, []*session{source, destination}, fences.sessions)
			require.Equal(t, []*tab{movedTab}, fences.tabs)
			require.ElementsMatch(t, []*pane{first, second, floating}, fences.panes)
		})
	}
}
