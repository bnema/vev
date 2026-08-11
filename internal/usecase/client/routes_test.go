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

func TestRouteCandidateRetainsRemoteOriginWithoutDiscoveryTarget(t *testing.T) {
	target := routeTestTarget(1)
	candidate := routeCandidateForAttach(AttachRequest{
		Intent: ports.IntentAttach, SessionName: target.SessionName, Remote: true,
		Origin: ports.RouteOriginRemote, OriginKey: "remote",
	}, ports.CommittedRouteIdentity{Target: target}, nil, 0)

	require.Nil(t, candidate.request.RemoteTarget)
	require.Equal(t, "remote", candidate.request.RemoteOrigin)
	require.Equal(t, "remote", candidate.presentation.hostLabel)
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
	require.Len(t, ledger.entries, maxRouteLedgerEntries)
	require.Len(t, snapshot.Entries, maxRouteLedgerEntries-1)
	require.Equal(t, uint64(keys[len(keys)-1]), snapshot.Active.Key)
	for _, entry := range snapshot.Entries {
		require.NotEqual(t, snapshot.Active, ports.RouteRef{Key: entry.Key, Generation: entry.Generation})
	}
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

func TestRouteLedgerRemembersTabsIndependentlyPerExactRoute(t *testing.T) {
	ledger := newRouteLedger()
	first := routeTestCandidate(1, ports.RouteOriginLocal)
	firstIdentity, err := ledger.commit(first)
	require.NoError(t, err)
	firstRef := ledger.activeRef()
	firstSnapshotGeneration := ledger.snapshot().Generation
	require.NoError(t, ledger.updateRoutePosition(ports.RoutePosition{Target: first.target, ActiveTabID: "tab-first"}))
	require.Equal(t, firstRef, ledger.activeRef(), "tab memory must not rotate route identity")
	require.Equal(t, firstSnapshotGeneration, ledger.snapshot().Generation, "tab memory must not stale navigation snapshots")

	second := routeTestCandidate(2, ports.RouteOriginRemote)
	secondIdentity, err := ledger.commit(second)
	require.NoError(t, err)
	secondRef := ledger.activeRef()
	secondSnapshotGeneration := ledger.snapshot().Generation
	require.NoError(t, ledger.updateRoutePosition(ports.RoutePosition{Target: second.target, ActiveTabID: "tab-second"}))
	require.Equal(t, secondRef, ledger.activeRef(), "tab memory must not rotate route identity")
	require.Equal(t, secondSnapshotGeneration, ledger.snapshot().Generation, "tab memory must not stale navigation snapshots")

	firstRecord, ok := ledger.lookup(firstIdentity.wire())
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("tab-first"), firstRecord.request.PreferredTabID)
	secondRecord, ok := ledger.lookup(secondIdentity.wire())
	require.True(t, ok)
	require.Equal(t, domain.TabStableID("tab-second"), secondRecord.request.PreferredTabID)
}

func TestRouteLedgerSamePeerHandoffRestoresRouteTab(t *testing.T) {
	ledger := newRouteLedger()
	first := routeTestCandidate(1, ports.RouteOriginLocal)
	first.originKey = "local"
	_, err := ledger.commit(first)
	require.NoError(t, err)
	require.NoError(t, ledger.updateRoutePosition(ports.RoutePosition{Target: first.target, ActiveTabID: "tab-first"}))

	second := routeTestCandidate(2, ports.RouteOriginLocal)
	second.originKey = "local"
	_, err = ledger.commit(second)
	require.NoError(t, err)

	recreated := first.target
	recreated.LifecycleID[0]++
	for _, tt := range []struct {
		name            string
		target          ports.ExactSessionTarget
		wantExactTarget *ports.ExactSessionTarget
		wantTab         domain.TabStableID
	}{
		{name: "matching lifecycle restores remembered tab", target: first.target, wantExactTarget: &first.target, wantTab: "tab-first"},
		{name: "recreated session does not reuse remembered tab", target: recreated, wantExactTarget: &recreated},
	} {
		t.Run(tt.name, func(t *testing.T) {
			request := ledger.samePeerHandoff(second.request, ports.AttachTarget{
				Session:           first.target.SessionName,
				Intent:            ports.IntentAttach,
				ExactTarget:       &tt.target,
				EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
			})
			require.Equal(t, first.target.SessionName, request.SessionName)
			require.Equal(t, tt.wantExactTarget, request.ExactTarget)
			require.Equal(t, tt.wantTab, request.PreferredTabID)
			require.Equal(t, ports.EnvironmentPolicyDaemonOwned, request.EnvironmentPolicy)
		})
	}
}

func TestRouteLedgerSamePeerHandoffDropsOriginalRemoteTarget(t *testing.T) {
	ledger := newRouteLedger()
	work := routeTestCandidate(1, ports.RouteOriginRemote)
	work.originKey = "remote"
	work.request.Remote = true
	work.request.OriginKey = "remote"
	work.request.EnvironmentPolicy = ports.EnvironmentPolicyDaemonOwned
	work.request.RemoteTarget = &domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: work.target.LifecycleID,
		SessionName: work.target.SessionName, LiveTabID: "tab-work",
	}
	_, err := ledger.commit(work)
	require.NoError(t, err)

	agents := routeTestCandidate(2, ports.RouteOriginRemote)
	agents.originKey = "remote"
	agents.request.Remote = true
	agents.request.OriginKey = "remote"
	agents.request.EnvironmentPolicy = ports.EnvironmentPolicyDaemonOwned

	request := ledger.samePeerHandoff(agents.request, ports.AttachTarget{
		Session:           work.target.SessionName,
		Intent:            ports.IntentAttach,
		ExactTarget:       &work.target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	})

	require.True(t, request.Remote)
	require.Equal(t, ports.RouteOriginRemote, request.Origin)
	require.Equal(t, "remote", request.OriginKey)
	require.Nil(t, request.RemoteTarget)
}

func TestRouteAttentionSubscriptionIncludesOnlyActiveOriginRoutes(t *testing.T) {
	ledger := newRouteLedger()
	first := routeTestCandidate(0, ports.RouteOriginRemote)
	first.originKey = "host-a"
	firstIdentity, err := ledger.commit(first)
	require.NoError(t, err)
	other := routeTestCandidate(1, ports.RouteOriginLocal)
	_, err = ledger.commit(other)
	require.NoError(t, err)
	otherRemote := routeTestCandidate(2, ports.RouteOriginRemote)
	otherRemote.originKey = "host-b"
	_, err = ledger.commit(otherRemote)
	require.NoError(t, err)
	active := routeTestCandidate(3, ports.RouteOriginRemote)
	active.originKey = "host-a"
	_, err = ledger.commit(active)
	require.NoError(t, err)

	subscription := ledger.attentionSubscription()

	require.Equal(t, []ports.RouteAttentionTarget{{
		Ref:    firstIdentity.wire(),
		Target: first.target,
	}}, subscription.Targets)
}

func TestCommittedIdentityRenamesActiveRouteInPlace(t *testing.T) {
	ledger := newRouteLedger()
	original := routeTestCandidate(0, ports.RouteOriginLocal)
	original.target = ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "0"}
	original.presentation.name = "0"
	original.request.SessionName = "0"
	identity, err := ledger.commit(original)
	require.NoError(t, err)
	before := ledger.snapshot()

	committed, err := ledger.commitCommittedIdentity(ports.CommittedRouteIdentity{
		Target:    ports.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "vps-infra"},
		Ephemeral: false,
	})
	require.NoError(t, err)

	snapshot := ledger.snapshot()
	require.Equal(t, identity, committed, "a rename must retain the route's process-local identity")
	require.Greater(t, snapshot.Generation, before.Generation, "a rename invalidates prior route snapshots")
	require.Empty(t, snapshot.Entries, "the old ephemeral label must not remain in route history")
	active, ok := ledger.lookup(snapshot.Active)
	require.True(t, ok)
	require.Equal(t, "vps-infra", active.presentation.name)
	require.Equal(t, "vps-infra", active.target.SessionName)
	require.False(t, active.presentation.ephemeral)
}

func TestCommittedIdentityDoesNotReassignHomeRoute(t *testing.T) {
	ledger := newRouteLedger()
	home := routeTestCandidate(1, ports.RouteOriginLocal)
	home.home = true
	homeIdentity, err := ledger.commit(home)
	require.NoError(t, err)
	committed, err := ledger.commitCommittedIdentity(ports.CommittedRouteIdentity{
		Target:    routeTestTarget(2),
		Ephemeral: true,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(homeIdentity.key), ledger.homeRef().Key)
	require.NotEqual(t, ledger.homeRef(), committed.wire())
	active, ok := ledger.lookup(ledger.activeRef())
	require.True(t, ok)
	require.Equal(t, committed, active.identity)

	_, err = ledger.commit(routeTestCandidate(1, ports.RouteOriginLocal))
	require.NoError(t, err)
	require.True(t, mustRouteRecord(t, ledger, ledger.activeRef()).home, "upserting the home route must retain its marker")
}

func TestRouteLedgerConcurrentInitialAttachmentsKeepOneHome(t *testing.T) {
	ledger := newRouteLedger()
	const attachments = 16
	errs := make(chan error, attachments)
	var wg sync.WaitGroup
	for i := byte(0); i < attachments; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := ledger.commitAttach(routeTestCandidate(i, ports.RouteOriginLocal))
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	ledger.mu.RLock()
	defer ledger.mu.RUnlock()
	homeCount := 0
	for _, entry := range ledger.entries {
		if entry.home {
			homeCount++
			require.Equal(t, entry.identity, ledger.home)
		}
	}
	require.Equal(t, 1, homeCount)
}

func TestRouteLedgerTransitionRejectsConcurrentCommit(t *testing.T) {
	ledger := newRouteLedger()
	selected, err := ledger.commit(routeTestCandidate(1, ports.RouteOriginLocal))
	require.NoError(t, err)
	_, err = ledger.commit(routeTestCandidate(2, ports.RouteOriginLocal))
	require.NoError(t, err)
	selection, ok := ledger.navigationSelection(selected.wireNavigationAction(routeGeneration(ledger.snapshot().Generation)))
	require.True(t, ok)

	_, err = ledger.commit(routeTestCandidate(3, ports.RouteOriginLocal))
	require.NoError(t, err)
	candidate := routeTestCandidate(4, ports.RouteOriginLocal)
	err = ledger.commitTransition(selected, selection.snapshotGeneration, selection.active, candidate)
	require.ErrorIs(t, err, errRouteStaleSelection)
	require.Equal(t, routeTestTarget(3), mustRouteRecord(t, ledger, ledger.activeRef()).target)
}

func mustRouteRecord(t *testing.T, ledger *routeLedger, ref ports.RouteRef) routeRecord {
	t.Helper()
	record, ok := ledger.lookup(ref)
	require.True(t, ok)
	return record
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
	resumeErr     error
	attachErr     error
	resumeCalls   int
	attachCalls   int
	restoreCalls  int
	restoreErr    error
	restoreTarget ports.ExactSessionTarget
}

func (c *routeTestConnector) Resume(context.Context, routeRecord) (routeTransitionCommit, error) {
	c.resumeCalls++
	if c.resumeErr != nil {
		return routeTransitionCommit{}, c.resumeErr
	}
	return routeTransitionCommit{}, errors.New("unexpected resume success")
}

func (c *routeTestConnector) Restore(_ context.Context, record routeRecord) error {
	c.restoreCalls++
	c.restoreTarget = record.target
	return c.restoreErr
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
	candidate.originKey = "daemon-discovered"
	candidate.resumeToken = 77
	identity, err := ledger.commit(candidate)
	require.NoError(t, err)
	_, err = ledger.commit(routeTestCandidate(4, ports.RouteOriginLocal))
	require.NoError(t, err)

	connector := &routeTestConnector{resumeErr: errRouteResumeUnavailable}
	action := identity.wireNavigationAction(routeGeneration(ledger.snapshot().Generation))
	err = ledger.navigate(context.Background(), action, connector)
	require.NoError(t, err)
	require.Equal(t, 1, connector.resumeCalls)
	require.Equal(t, 1, connector.attachCalls)
	require.Zero(t, connector.restoreCalls)

	active, ok := ledger.lookup(ledger.activeRef())
	require.True(t, ok)
	require.Equal(t, ports.RouteOriginDiscovery, active.origin)
	require.Equal(t, "daemon-discovered", active.originKey)
	require.Equal(t, ports.RouteOriginDiscovery, active.request.Origin)
	require.Equal(t, candidate.target, active.target)
	require.Greater(t, active.identity.generation, identity.generation)
}

func TestRouteLedgerNavigateIsTransactionalOnFailureOrTargetChange(t *testing.T) {
	ledger := newRouteLedger()
	identity, err := ledger.commit(routeTestCandidate(3, ports.RouteOriginLocal))
	require.NoError(t, err)
	_, err = ledger.commit(routeTestCandidate(5, ports.RouteOriginLocal))
	require.NoError(t, err)
	before := ledger.snapshot()
	action := identity.wireNavigationAction(routeGeneration(before.Generation))

	connector := &routeTestConnector{attachErr: errors.New("offline")}
	err = ledger.navigate(context.Background(), action, connector)
	require.Error(t, err)
	require.Equal(t, before, ledger.snapshot())
	require.Equal(t, routeTestTarget(5), connector.restoreTarget)
	require.Equal(t, 1, connector.restoreCalls)

	connector = &routeTestConnector{}
	// Force a mismatched committed target through a small wrapper.
	mismatch := mismatchingRouteConnector{routeTestConnector: connector}
	err = ledger.navigate(context.Background(), action, mismatch)
	require.ErrorIs(t, err, errRouteTargetChanged)
	require.Equal(t, before, ledger.snapshot())
	require.Equal(t, routeTestTarget(5), connector.restoreTarget)
	require.Equal(t, 1, connector.restoreCalls)
}

func TestRouteLedgerNavigationRejectsStaleActionsAndActiveNoOp(t *testing.T) {
	ledger := newRouteLedger()
	active, err := ledger.commit(routeTestCandidate(6, ports.RouteOriginLocal))
	require.NoError(t, err)
	firstSnapshot := ledger.snapshot()
	before := ledger.snapshot()

	require.NoError(t, ledger.navigate(context.Background(), active.wireNavigationAction(routeGeneration(firstSnapshot.Generation)), nil))
	require.Equal(t, before, ledger.snapshot())

	_, err = ledger.commit(routeTestCandidate(7, ports.RouteOriginLocal))
	require.NoError(t, err)
	stale := active.wireNavigationAction(routeGeneration(firstSnapshot.Generation))
	connector := &routeTestConnector{}
	require.ErrorIs(t, ledger.navigate(context.Background(), stale, connector), errRouteStaleSelection)
	require.Zero(t, connector.resumeCalls)
	require.Zero(t, connector.attachCalls)
}

type mismatchingRouteConnector struct{ *routeTestConnector }

func (c mismatchingRouteConnector) Attach(_ context.Context, record routeRecord) (routeTransitionCommit, error) {
	return routeTransitionCommit{
		identity:     ports.CommittedRouteIdentity{Target: routeTestTarget(99)},
		presentation: routePresentation{name: record.presentation.name, kind: record.presentation.kind},
	}, nil
}

func (i routeIdentity) wireNavigationAction(snapshotGeneration routeGeneration) ports.RouteNavigationAction {
	return ports.RouteNavigationAction{SnapshotGeneration: uint64(snapshotGeneration), Key: uint64(i.key), Generation: uint64(i.generation)}
}

func TestRouteLedgerConcurrentSnapshotsAndCommits(t *testing.T) {
	ledger := newRouteLedger()
	var wg sync.WaitGroup
	for worker := byte(0); worker < 4; worker++ {
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
	require.LessOrEqual(t, len(ledger.snapshot().Entries), maxRouteLedgerEntries-1)
}

func TestRouteLedgerSeparatesRemoteDaemonOrigins(t *testing.T) {
	ledger := newRouteLedger()
	firstCandidate := routeTestCandidate(40, ports.RouteOriginRemote)
	firstCandidate.originKey = "daemon-a"
	secondCandidate := firstCandidate
	secondCandidate.originKey = "daemon-b"

	first, err := ledger.commit(firstCandidate)
	require.NoError(t, err)
	second, err := ledger.commit(secondCandidate)
	require.NoError(t, err)

	require.NotEqual(t, first.key, second.key)
	require.Len(t, ledger.snapshot().Entries, 1)
}
