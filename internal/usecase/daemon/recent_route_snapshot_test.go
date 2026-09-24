package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/usecase/command"
	"github.com/stretchr/testify/require"
)

func TestAttachmentStatusUsesClientRouteSnapshot(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 2,
		Active:     protocol.RouteRef{Key: 3, Generation: 2},
		Entries: []protocol.RecentRouteEntry{
			{Key: 2, Generation: 1, Target: testRouteTarget("logs", 2), Name: "logs", HostLabel: "user@edge", Kind: protocol.RouteKindRemote, Visited: true},
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
			Key: 2, Generation: 1, Target: testRouteTarget("local", 9), Name: "local", Kind: protocol.RouteKindLocal, Attention: true, Visited: true,
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

	require.False(t, d.handleAttachmentClientFrame(token, mustClientEnvelope(protocol.RecentRouteSnapshot{Generation: 2})))

	awaitFrame(t, sends, "Output")
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

func TestPaletteJumpRecentSessionTargetsVisitedRank(t *testing.T) {
	unvisited := testRouteEntry(5, 1, "other", 5, protocol.RouteKindLocal)
	unvisited.Visited = false
	snapshot := protocol.RecentRouteSnapshot{
		Generation: 4,
		Entries:    []protocol.RecentRouteEntry{unvisited, testRouteEntry(2, 3, "remote", 2, protocol.RouteKindRemote)},
	}
	tests := []struct {
		name    string
		rank    int
		wantKey uint64
		wantErr error
	}{
		{name: "rank 1 skips the unvisited entry", rank: 1, wantKey: 2},
		{name: "rank beyond the visited prefix is refused", rank: 2, wantErr: command.ErrInvalidArguments},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
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

			exec := paletteExec{d: d, sess: source, attachment: source, ac: ac, routeSnapshot: snapshot, effect: effect}
			err := exec.JumpRecentSession(tt.rank)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			var action protocol.RouteNavigationAction
			found := false
			for _, frame := range transport.Sends() {
				if decoded, ok := decodeServerMessage(t, frame).(protocol.RouteNavigationAction); ok {
					action, found = decoded, true
				}
			}
			require.True(t, found)
			require.Equal(t, protocol.RouteNavigationAction{SnapshotGeneration: 4, Key: tt.wantKey, Generation: 3}, action)
			hints := recentRouteHints(snapshot, nil)
			require.Equal(t, action.Key, hints.Recent[tt.rank-1].Key, "hint and jump share the rank mapping")
		})
	}
}

func TestRecentRouteHintsRetainSnapshotSelectionIdentity(t *testing.T) {
	snapshot := protocol.RecentRouteSnapshot{
		Generation: 8,
		Entries: []protocol.RecentRouteEntry{{
			Key: 11, Generation: 7, Target: testRouteTarget("logs", 11), Name: "logs", HostLabel: "edge", Kind: protocol.RouteKindRemote, Visited: true,
		}},
	}

	hints := recentRouteHints(snapshot, nil)

	require.Len(t, hints.Recent, 1)
	require.Equal(t, "logs@edge", hints.Recent[0].Name)
	require.Equal(t, uint64(8), hints.Recent[0].SnapshotGeneration)
	require.Equal(t, uint64(11), hints.Recent[0].Key)
	require.Equal(t, uint64(7), hints.Recent[0].Generation)
}

func TestStatusHistoryShowsVisitedAndRingingRoutes(t *testing.T) {
	unvisited := testRouteEntry(4, 1, "other", 4, protocol.RouteKindLocal)
	unvisited.Visited = false
	ringingLocal := testRouteEntry(5, 1, "build", 5, protocol.RouteKindLocal)
	ringingLocal.Visited, ringingLocal.Attention, ringingLocal.AttentionSeq = false, true, 1
	ringingRemote := testRouteEntry(6, 1, "deploy", 6, protocol.RouteKindRemote)
	ringingRemote.Visited, ringingRemote.Attention, ringingRemote.AttentionSeq = false, true, 2
	tests := []struct {
		name    string
		entries []protocol.RecentRouteEntry
		want    []string
	}{
		{name: "fresh client shows no history", entries: []protocol.RecentRouteEntry{unvisited}, want: nil},
		{name: "visited routes keep their order", entries: []protocol.RecentRouteEntry{testRouteEntry(2, 1, "work", 2, protocol.RouteKindLocal), unvisited, testRouteEntry(3, 1, "logs", 3, protocol.RouteKindLocal)}, want: []string{"work", "logs"}},
		{name: "fresh client still shows ringing local and remote routes", entries: []protocol.RecentRouteEntry{unvisited, ringingLocal, ringingRemote}, want: []string{"build", "deploy@remote"}},
		{name: "ringing routes follow visited ones", entries: []protocol.RecentRouteEntry{testRouteEntry(2, 1, "work", 2, protocol.RouteKindLocal), unvisited, ringingLocal}, want: []string{"work", "build"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
			ac.setRouteSnapshot(protocol.RecentRouteSnapshot{Generation: 2, Entries: tt.entries})

			state := d.barStateForAttachmentPaletteHintsFor(sess, ac, "", nil, protocol.RecentRouteSnapshot{})

			var names []string
			for _, entry := range state.mru {
				names = append(names, entry.name)
				if entry.name == "build" || entry.name == "deploy@remote" {
					require.True(t, entry.attention, "%s must draw its bell", entry.name)
				}
			}
			require.Equal(t, tt.want, names)
			require.Equal(t, len(tt.want), len(recentRouteHints(ac.routeSnapshotCopy(), nil).Recent))
		})
	}
}
