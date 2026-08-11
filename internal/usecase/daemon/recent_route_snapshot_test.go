package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestAttachmentStatusUsesClientRouteSnapshot(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation: 2,
		Active:     ports.RouteRef{Key: 3, Generation: 2},
		Entries: []ports.RecentRouteEntry{
			{Key: 2, Generation: 1, Name: "logs", HostLabel: "edge", Kind: ports.RouteKindRemote},
			{Key: 1, Generation: 1, Name: "work", Kind: ports.RouteKindLocal},
		},
	})

	state := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, ports.RecentRouteSnapshot{})

	require.Len(t, state.mru, 2)
	require.Equal(t, []string{"logs@edge", "work"}, []string{state.mru[0].name, state.mru[1].name})
}

func TestAttachmentStatusResolvesSubscribedRouteAttention(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	sess.mu.Lock()
	sess.incarnation = domain.SessionLifecycleID{9}
	sess.tabs[0].attention = true
	sess.mu.Unlock()
	ref := ports.RouteRef{Key: 2, Generation: 1}
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation: 2,
		Active:     ports.RouteRef{Key: 3, Generation: 2},
		Entries:    []ports.RecentRouteEntry{{Key: ref.Key, Generation: ref.Generation, Name: sess.name, Kind: ports.RouteKindLocal}},
	})
	ac.setRouteAttentionSubscription(ports.RouteAttentionSubscription{Targets: []ports.RouteAttentionTarget{{
		Ref: ref,
		Target: ports.ExactSessionTarget{
			LifecycleID: sess.incarnation,
			SessionName: sess.name,
		},
	}}})

	state := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, ports.RecentRouteSnapshot{})

	require.Len(t, state.mru, 1)
	require.True(t, state.mru[0].attention)
}

func TestAttachmentRouteSnapshotCopiesPublishedEntries(t *testing.T) {
	_, _, ac, _ := newManualSessionWithPTYs(t, nil)
	entries := []ports.RecentRouteEntry{{Key: 1, Generation: 2, Name: "before", Kind: ports.RouteKindLocal}}
	snapshot := ports.RecentRouteSnapshot{Generation: 3, Entries: entries}
	ac.setRouteSnapshot(snapshot)

	entries[0].Name = "after"
	got := ac.routeSnapshotCopy()
	require.Equal(t, "before", got.Entries[0].Name)
	got.Entries[0].Name = "mutated copy"
	require.Equal(t, "before", ac.routeSnapshotCopy().Entries[0].Name)
}

func TestPaletteRecentRouteSelectionSendsTypedClientAction(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.attachmentToken(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.publishAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	token.effect = effect

	exec := paletteExec{
		d: d, sess: source, attachment: source, ac: ac,
		routeSnapshot: ports.RecentRouteSnapshot{
			Generation: 4,
			Entries:    []ports.RecentRouteEntry{{Key: 2, Generation: 3, Name: "remote", Kind: ports.RouteKindRemote}},
		},
		effect: effect,
	}
	require.NoError(t, exec.JumpRecentSession(1))

	frames := transport.Sends()
	var action ports.RouteNavigationAction
	found := false
	for _, frame := range frames {
		if frame.Type != ports.MsgNavigateRecentRoute {
			continue
		}
		var err error
		action, err = ports.UnmarshalRouteNavigationAction(frame.Payload)
		require.NoError(t, err)
		found = true
	}
	require.True(t, found)
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 4, Key: 2, Generation: 3}, action)
}

func TestRecentRouteHintsRetainSnapshotSelectionIdentity(t *testing.T) {
	snapshot := ports.RecentRouteSnapshot{
		Generation: 8,
		Entries: []ports.RecentRouteEntry{{
			Key: 11, Generation: 7, Name: "logs", HostLabel: "edge", Kind: ports.RouteKindRemote,
		}},
	}

	hints := recentRouteHints(snapshot, nil)

	require.Len(t, hints.Recent, 1)
	require.Equal(t, "logs@edge", hints.Recent[0].Name)
	require.Equal(t, uint64(8), hints.Recent[0].SnapshotGeneration)
	require.Equal(t, uint64(11), hints.Recent[0].Key)
	require.Equal(t, uint64(7), hints.Recent[0].Generation)
}
