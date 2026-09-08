package client

import (
	"context"
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRouteLedgerNavigationAfterSamePeerSwitchDoesNotResumePreviousSessionCredential(t *testing.T) {
	ledger := newRouteLedger()
	first := routeTestCandidate(0, protocol.RouteOriginLocal)
	first.resumeToken = 42
	_, err := ledger.commit(first)
	require.NoError(t, err)
	second := routeTestTarget(1)
	_, err = ledger.commitCommittedIdentity(protocol.CommittedRouteIdentity{Target: second})
	require.NoError(t, err)
	remote := routeTestCandidate(2, protocol.RouteOriginRemote)
	remote.originKey = "remote"
	_, err = ledger.commit(remote)
	require.NoError(t, err)
	snapshot := ledger.snapshot()
	for _, entry := range snapshot.Entries {
		if entry.Target != first.target {
			continue
		}
		connector := &routeTestConnector{resumeErr: errRouteResumeUnavailable}
		err = ledger.navigate(context.Background(), protocol.RouteNavigationAction{SnapshotGeneration: snapshot.Generation, Key: entry.Key, Generation: entry.Generation}, connector)
		require.NoError(t, err)
		require.Zero(t, connector.resumeCalls, "a credential follows its attachment to the new session; old history must attach exactly")
		return
	}
	t.Fatal("previous session missing from history")
}
