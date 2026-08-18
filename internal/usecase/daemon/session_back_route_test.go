package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

func TestBackSessionUsesClientPreviousRouteAfterSnapshotPublication(t *testing.T) {
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
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{
		Generation: 5,
		Active:     ports.RouteRef{Key: 9, Generation: 5},
		Previous:   ports.RouteRef{Key: 7, Generation: 3},
		Entries:    []ports.RecentRouteEntry{testRouteEntry(7, 3, "previous", 7, ports.RouteKindLocal)},
	})

	require.NoError(t, d.backSessionForAttachment(token))
	frames := transport.Sends()
	require.NotEmpty(t, frames)
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
	require.Equal(t, ports.RouteNavigationAction{SnapshotGeneration: 5, Key: 7, Generation: 3}, action)
}
