package daemon

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func attentionRouteEntry(key uint64, name string, marker byte, seq uint64) protocol.RecentRouteEntry {
	entry := testRouteEntry(key, 1, name, marker, protocol.RouteKindRemote)
	entry.HostLabel = "box"
	entry.Attention = seq != 0
	entry.AttentionSeq = seq
	return entry
}

// TestOldestRouteAttention pins the cross-daemon half of jump-to-attention
// (Plan 003 C5): the oldest client-observed bell wins, and a lifecycle this
// daemon owns is never delegated to the client.
func TestOldestRouteAttention(t *testing.T) {
	owned := testRouteTarget("mine", 9).LifecycleID
	tests := []struct {
		name    string
		entries []protocol.RecentRouteEntry
		wantKey uint64
		wantOK  bool
	}{
		{name: "no bells", entries: []protocol.RecentRouteEntry{attentionRouteEntry(1, "a", 1, 0)}},
		{
			name:    "oldest onset wins over recency",
			entries: []protocol.RecentRouteEntry{attentionRouteEntry(1, "new", 1, 7), attentionRouteEntry(2, "old", 2, 3), attentionRouteEntry(3, "quiet", 3, 0)},
			wantKey: 2, wantOK: true,
		},
		{
			name:    "this daemon's own lifecycle is skipped",
			entries: []protocol.RecentRouteEntry{attentionRouteEntry(1, "mine", 9, 1), attentionRouteEntry(2, "theirs", 2, 4)},
			wantKey: 2, wantOK: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			snapshot := protocol.RecentRouteSnapshot{Generation: 5, Entries: tt.entries}
			require.NoError(t, snapshot.Validate())
			action, ok := oldestRouteAttention(snapshot, func(lifecycle domain.SessionLifecycleID) bool { return lifecycle == owned })
			require.Equal(t, tt.wantOK, ok)
			if tt.wantOK {
				require.Equal(t, protocol.RouteNavigationAction{SnapshotGeneration: 5, Key: tt.wantKey, Generation: 1}, action)
			}
		})
	}
}

// TestJumpAttentionDelegatesOtherDaemonsToTheClient drives the real jump:
// with no bell on this daemon, the oldest route bell becomes a client
// navigation request on the attachment.
func TestJumpAttentionDelegatesOtherDaemonsToTheClient(t *testing.T) {
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
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 6,
		Entries:    []protocol.RecentRouteEntry{attentionRouteEntry(4, "later", 4, 9), attentionRouteEntry(5, "first", 5, 2)},
	})

	require.NoError(t, d.jumpAttentionForAttachment(source, ac, effect))

	var actions []protocol.RouteNavigationAction
	for _, frame := range transport.Sends() {
		if action, ok := decodeServerMessage(t, frame).(protocol.RouteNavigationAction); ok {
			actions = append(actions, action)
		}
	}
	require.Equal(t, []protocol.RouteNavigationAction{{SnapshotGeneration: 6, Key: 5, Generation: 1}}, actions)
	require.Same(t, source, ac.currentSession(), "the client navigates; the daemon never switches")
}
