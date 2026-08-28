package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestAttachmentStatusUsesClientRouteSnapshot(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 2,
		Active:     protocol.RouteRef{Key: 3, Generation: 2},
		Entries: []protocol.RecentRouteEntry{
			{Key: 2, Generation: 1, Target: testRouteTarget("logs", 2), Name: "logs", HostLabel: "user@edge", Kind: protocol.RouteKindRemote},
			testRouteEntry(1, 1, "work", 1, protocol.RouteKindLocal),
		},
	})

	state := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})

	require.Len(t, state.mru, 2)
	require.Equal(t, []string{"logs@edge", "work"}, []string{state.mru[0].name, state.mru[1].name})
}

func TestAttachmentStatusResolvesSubscribedRouteAttention(t *testing.T) {
	for _, tt := range []struct {
		name      string
		targetID  domain.SessionLifecycleID
		attention bool
	}{
		{name: "matching lifecycle", targetID: domain.SessionLifecycleID{9}, attention: true},
		{name: "stale lifecycle", targetID: domain.SessionLifecycleID{8}, attention: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
			sess.mu.Lock()
			sess.incarnation = domain.SessionLifecycleID{9}
			sess.tabs[0].attention = true
			sess.mu.Unlock()
			ref := protocol.RouteRef{Key: 2, Generation: 1}
			ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
				Generation: 2,
				Active:     protocol.RouteRef{Key: 3, Generation: 2},
				Entries:    []protocol.RecentRouteEntry{{Key: ref.Key, Generation: ref.Generation, Target: protocol.ExactSessionTarget{LifecycleID: tt.targetID, SessionName: sess.name}, Name: sess.name, Kind: protocol.RouteKindLocal}},
			})
			ac.setRouteAttentionSubscription(protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{
				Ref: ref,
				Target: protocol.ExactSessionTarget{
					LifecycleID: tt.targetID,
					SessionName: sess.name,
				},
			}}})

			state := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})

			require.Len(t, state.mru, 1)
			require.Equal(t, tt.attention, state.mru[0].attention)
		})
	}
}

func TestRecentRouteSnapshotRepaintsWithoutDeferredIdentity(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})

	payload, err := ports.MarshalRecentRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 2})
	require.NoError(t, err)
	require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgRecentRouteSnapshot, Payload: payload}))

	awaitFrame(t, sends, ports.MsgOutput)
}

func TestAttachmentRouteSnapshotCopiesPublishedEntries(t *testing.T) {
	_, _, ac, _ := newManualSessionWithPTYs(t, nil)
	entries := []protocol.RecentRouteEntry{testRouteEntry(1, 2, "before", 1, protocol.RouteKindLocal)}
	snapshot := protocol.RecentRouteSnapshot{Generation: 3, Entries: entries}
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
	token := source.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()

	exec := paletteExec{
		d: d, sess: source, attachment: source, ac: ac,
		routeSnapshot: protocol.RecentRouteSnapshot{
			Generation: 4,
			Entries:    []protocol.RecentRouteEntry{testRouteEntry(2, 3, "remote", 2, protocol.RouteKindRemote)},
		},
		effect: effect,
	}
	require.NoError(t, exec.JumpRecentSession(1))

	frames := transport.Sends()
	var action protocol.RouteNavigationAction
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
	require.Equal(t, protocol.RouteNavigationAction{SnapshotGeneration: 4, Key: 2, Generation: 3}, action)
}

func TestRecentRouteHintsRetainSnapshotSelectionIdentity(t *testing.T) {
	snapshot := protocol.RecentRouteSnapshot{
		Generation: 8,
		Entries: []protocol.RecentRouteEntry{{
			Key: 11, Generation: 7, Target: testRouteTarget("logs", 11), Name: "logs", HostLabel: "edge", Kind: protocol.RouteKindRemote,
		}},
	}

	hints := recentRouteHints(snapshot, nil)

	require.Len(t, hints.Recent, 1)
	require.Equal(t, "logs@edge", hints.Recent[0].Name)
	require.Equal(t, uint64(8), hints.Recent[0].SnapshotGeneration)
	require.Equal(t, uint64(11), hints.Recent[0].Key)
	require.Equal(t, uint64(7), hints.Recent[0].Generation)
}
