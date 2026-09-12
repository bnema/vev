package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
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

func TestAttachmentStatusUsesClientPublishedAttention(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 2,
		Active:     protocol.RouteRef{Key: 3, Generation: 2},
		Entries: []protocol.RecentRouteEntry{{
			Key: 2, Generation: 1, Target: testRouteTarget("local", 9), Name: "local", Kind: protocol.RouteKindLocal, Attention: true,
		}},
	})

	state := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})

	require.Len(t, state.mru, 1)
	require.True(t, state.mru[0].attention)
}

func TestRecentRouteSnapshotRepaintsWithoutDeferredIdentity(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	token := sess.captureAttachmentCapability(ac, ac.transport())
	ac.installTestAttachmentCapability(token)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 1})

	payload, err := wire.MarshalRecentRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 2})
	require.NoError(t, err)
	require.False(t, d.handleAttachmentClientFrame(token, wire.Frame{Type: wire.MsgRecentRouteSnapshot, Payload: payload}))

	awaitFrame(t, sends, wire.MsgOutput)
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
		if frame.Type != wire.MsgNavigateRecentRoute {
			continue
		}
		var err error
		action, err = wire.UnmarshalRouteNavigationAction(frame.Payload)
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
