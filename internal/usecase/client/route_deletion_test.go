package client

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRouteRetirementThenRecreationDoesNotDuplicateHistory(t *testing.T) {
	for _, origin := range []protocol.RouteOrigin{protocol.RouteOriginLocal, protocol.RouteOriginRemote} {
		t.Run(map[protocol.RouteOrigin]string{protocol.RouteOriginLocal: "local", protocol.RouteOriginRemote: "remote"}[origin], func(t *testing.T) {
			ledger := newRouteLedger()
			victim := routeTestCandidate(0, origin)
			victim.originKey = "source"
			id, err := ledger.commit(victim)
			require.NoError(t, err)
			active := routeTestCandidate(1, origin)
			active.originKey = "source"
			_, err = ledger.commit(active)
			require.NoError(t, err)
			sub := ledger.attentionSubscriptionFor(AttachRequest{Origin: origin, OriginKey: "source"})
			retired := protocol.RouteRetired{Ref: id.wire(), Target: victim.target}
			wrong := retired
			wrong.Target = active.target
			require.False(t, ledger.retireRoute(wrong, sub))
			require.False(t, ledger.retireRoute(retired, protocol.RouteAttentionSubscription{}))
			require.True(t, ledger.retireRoute(retired, sub))
			require.Empty(t, ledger.snapshot().Entries)
			replacement := victim
			replacement.target.LifecycleID[0] = 9
			_, err = ledger.commit(replacement)
			require.NoError(t, err)
			_, err = ledger.commit(active)
			require.NoError(t, err)
			require.False(t, ledger.retireRoute(retired, sub), "late deletion must not retire recreation")
			snapshot := ledger.snapshot()
			require.Len(t, snapshot.Entries, 1)
			require.Equal(t, replacement.target, snapshot.Entries[0].Target)
		})
	}
}

func TestHomePickerSubscribesToServingDaemonHistory(t *testing.T) {
	ledger := newRouteLedger()
	local := routeTestCandidate(0, protocol.RouteOriginLocal)
	local.originKey = "local"
	localID, err := ledger.commit(local)
	require.NoError(t, err)
	remote := routeTestCandidate(1, protocol.RouteOriginRemote)
	remote.originKey = "remote"
	_, err = ledger.commit(remote)
	require.NoError(t, err)

	// A transient home picker preserves the remote active route, but its
	// lifecycle observations must cover the local daemon it actually serves.
	subscription := ledger.attentionSubscriptionFor(AttachRequest{Origin: protocol.RouteOriginLocal, OriginKey: "local"})
	require.Contains(t, subscription.Targets, protocol.RouteAttentionTarget{
		Ref: localID.wire(), Target: local.target,
	})
}
