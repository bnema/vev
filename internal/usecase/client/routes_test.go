package client

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func routeTestTarget(index byte) ports.ExactSessionTarget {
	return ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{index + 1}, SessionName: "session-" + string(rune('a'+index%26)) + string(rune('0'+index/26))}
}

func routeTestCandidate(index byte, origin ports.RouteOrigin) routeCandidate {
	target := routeTestTarget(index)
	return routeCandidate{
		origin: origin,
		target: target,
		presentation: routePresentation{
			name:         target.SessionName,
			kind:         ports.RouteKindLocal,
			reachability: ports.RouteReachabilityReachable,
		},
		request: AttachRequest{
			Intent:      ports.IntentAttach,
			SessionName: target.SessionName,
			Origin:      origin,
		},
	}
}

func TestRouteLedgerBoundsAndImmutableSnapshot(t *testing.T) {
	ledger := newRouteLedger()
	var keys []routeKey
	for i := byte(0); i < maxRouteLedgerEntries+8; i++ {
		candidate := routeTestCandidate(i, ports.RouteOriginLocal)
		candidate.home = i == 0
		identity, err := ledger.commit(candidate)
		require.NoError(t, err)
		keys = append(keys, identity.key)
	}

	snapshot := ledger.snapshot()
	require.Len(t, snapshot.Entries, maxRouteLedgerEntries)
	require.Equal(t, uint64(keys[len(keys)-1]), snapshot.Active.Key)
	require.Equal(t, uint64(keys[len(keys)-2]), snapshot.Previous.Key)
	require.Equal(t, uint64(keys[0]), snapshot.Home.Key, "home is retained when older entries are evicted")

	for i := 1; i < len(snapshot.Entries); i++ {
		require.Greater(t, snapshot.Entries[i-1].Generation, snapshot.Entries[i].Generation)
	}
	originalName := snapshot.Entries[0].Name
	snapshot.Entries[0].Name = "mutated"
	fresh := ledger.snapshot()
	require.Equal(t, originalName, fresh.Entries[0].Name)

	old := ledger.activeRef()
	_, err := ledger.commit(routeTestCandidate(maxRouteLedgerEntries+9, ports.RouteOriginLocal))
	require.NoError(t, err)
	require.NotEqual(t, old, ledger.activeRef())
	require.Equal(t, old, ledger.previousRef(), "the old active route becomes the previous route")
}

func TestRouteLedgerUpsertChangesGenerationWithoutChangingOpaqueKey(t *testing.T) {
	ledger := newRouteLedger()
	candidate := routeTestCandidate(1, ports.RouteOriginRemote)
	first, err := ledger.commit(candidate)
	require.NoError(t, err)

	candidate.presentation.attention = true
	second, err := ledger.commit(candidate)
	require.NoError(t, err)
	require.Equal(t, first.key, second.key)
	require.Greater(t, second.generation, first.generation)
	_, ok := ledger.lookup(first.wire())
	require.False(t, ok, "old generation must not resolve")
}

func TestRouteLedgerDiscoveryCyclesAreIndependent(t *testing.T) {
	ledger := newRouteLedger()
	first := ledger.beginDiscoveryCycle()
	second := ledger.beginDiscoveryCycle()

	require.NotEqual(t, first, second)
	require.False(t, ledger.discoveryCycleCurrent(first))
	require.True(t, ledger.discoveryCycleCurrent(second))
}

type routeTestConnector struct {
	resumeErr   error
	attachErr   error
	resumeCalls int
	attachCalls int
}

func (c *routeTestConnector) Resume(context.Context, routeRecord) (routeTransitionCommit, error) {
	c.resumeCalls++
	if c.resumeErr != nil {
		return routeTransitionCommit{}, c.resumeErr
	}
	return routeTransitionCommit{}, errors.New("unexpected resume success")
}

func (c *routeTestConnector) Attach(_ context.Context, record routeRecord) (routeTransitionCommit, error) {
	c.attachCalls++
	if c.attachErr != nil {
		return routeTransitionCommit{}, c.attachErr
	}
	return routeTransitionCommit{
		identity: ports.CommittedRouteIdentity{Target: record.target},
		presentation: routePresentation{
			name:         record.presentation.name,
			kind:         record.presentation.kind,
			reachability: ports.RouteReachabilityReachable,
		},
		resumeToken: 123,
	}, nil
}

func TestRouteLedgerNavigateResumesThenFallsBackAndRestoresOrigin(t *testing.T) {
	ledger := newRouteLedger()
	candidate := routeTestCandidate(2, ports.RouteOriginDiscovery)
	candidate.resumeToken = 77
	identity, err := ledger.commit(candidate)
	require.NoError(t, err)

	connector := &routeTestConnector{resumeErr: errRouteResumeUnavailable}
	err = ledger.navigate(context.Background(), ports.RouteNavigationAction{Key: uint64(identity.key), Generation: uint64(identity.generation)}, connector)
	require.NoError(t, err)
	require.Equal(t, 1, connector.resumeCalls)
	require.Equal(t, 1, connector.attachCalls)

	active, ok := ledger.lookup(ledger.activeRef())
	require.True(t, ok)
	require.Equal(t, ports.RouteOriginDiscovery, active.origin)
	require.Equal(t, ports.RouteOriginDiscovery, active.request.Origin)
	require.Equal(t, candidate.target, active.target)
	require.Greater(t, active.identity.generation, identity.generation)
}

func TestRouteLedgerNavigateIsTransactionalOnFailureOrTargetChange(t *testing.T) {
	ledger := newRouteLedger()
	identity, err := ledger.commit(routeTestCandidate(3, ports.RouteOriginLocal))
	require.NoError(t, err)
	before := ledger.snapshot()

	connector := &routeTestConnector{attachErr: errors.New("offline")}
	err = ledger.navigate(context.Background(), identity.wireNavigationAction(), connector)
	require.Error(t, err)
	require.Equal(t, before, ledger.snapshot())

	connector = &routeTestConnector{}
	connector.attachErr = nil
	// Force a mismatched committed target through a small wrapper.
	mismatch := mismatchingRouteConnector{routeTestConnector: connector}
	err = ledger.navigate(context.Background(), identity.wireNavigationAction(), mismatch)
	require.ErrorIs(t, err, errRouteTargetChanged)
	require.Equal(t, before, ledger.snapshot())
}

type mismatchingRouteConnector struct{ *routeTestConnector }

func (c mismatchingRouteConnector) Attach(_ context.Context, record routeRecord) (routeTransitionCommit, error) {
	return routeTransitionCommit{
		identity:     ports.CommittedRouteIdentity{Target: routeTestTarget(99)},
		presentation: routePresentation{name: record.presentation.name, kind: record.presentation.kind},
	}, nil
}

func (i routeIdentity) wireNavigationAction() ports.RouteNavigationAction {
	return ports.RouteNavigationAction{Key: uint64(i.key), Generation: uint64(i.generation)}
}

func TestRouteLedgerConcurrentSnapshotsAndCommits(t *testing.T) {
	ledger := newRouteLedger()
	var wg sync.WaitGroup
	for worker := byte(0); worker < 4; worker++ {
		worker := worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := byte(0); i < 20; i++ {
				_, _ = ledger.commit(routeTestCandidate(worker*20+i, ports.RouteOriginLocal))
				_ = ledger.snapshot()
			}
		}()
	}
	wg.Wait()
	require.LessOrEqual(t, len(ledger.snapshot().Entries), maxRouteLedgerEntries)
}
