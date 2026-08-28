package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

func TestBackSessionUsesClientPreviousRouteAfterSnapshotPublication(t *testing.T) {
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
		Generation: 5,
		Active:     protocol.RouteRef{Key: 9, Generation: 5},
		Previous:   protocol.RouteRef{Key: 7, Generation: 3},
		Entries:    []protocol.RecentRouteEntry{testRouteEntry(7, 3, "previous", 7, protocol.RouteKindLocal)},
	})

	require.NoError(t, d.backSessionForAttachment(effect))
	frames := transport.Sends()
	require.NotEmpty(t, frames)
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
	require.Equal(t, protocol.RouteNavigationAction{SnapshotGeneration: 5, Key: 7, Generation: 3}, action)
}
