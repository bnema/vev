package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
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
	previous := protocol.RouteRef{Key: 7, Generation: 3}
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 5,
		Active:     protocol.RouteRef{Key: 9, Generation: 5},
		Previous:   previous,
		Entries:    []protocol.RecentRouteEntry{testRouteEntry(7, 3, "previous", 7, protocol.RouteKindLocal)},
	})
	ac.setRouteAttentionSubscription(protocol.RouteAttentionSubscription{
		Targets: []protocol.RouteAttentionTarget{{Ref: previous, Target: testRouteTarget("previous", 7)}},
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

func TestBackSessionOffersPreviousRouteOnCurrentDaemon(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, nil)
	target := &session{
		sessionCore: sessionCore{
			id: "target", name: "target", incarnation: testRouteTarget("target", 7).LifecycleID,
			attachments: make(map[*attachedClient]struct{}),
		},
		ctx: source.ctx, cancel: func() {},
		tabs: []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
	}
	publishTiledPaneOwners(target, target.tabs[0])
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()

	transport := &closeTrackingTransport{}
	ac.replaceTransport(transport)
	rc := d.attachCoordinator(source, nil, ac, true)
	token := source.captureAttachmentCapability(ac, transport)
	token.lease = rc.attachmentLease(ac)
	ac.installTestAttachmentCapability(token)
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	defer effect.End()
	previous := protocol.RouteRef{Key: 7, Generation: 3}
	exact := protocol.ExactSessionTarget{LifecycleID: target.incarnation, SessionName: target.name}
	ac.setRouteSnapshot(protocol.RecentRouteSnapshot{
		Generation: 5,
		Active:     protocol.RouteRef{Key: 9, Generation: 5},
		Previous:   previous,
		Entries:    []protocol.RecentRouteEntry{testRouteEntry(7, 3, target.name, 7, protocol.RouteKindRemote)},
	})
	ac.setRouteAttentionSubscription(protocol.RouteAttentionSubscription{
		Targets: []protocol.RouteAttentionTarget{{Ref: previous, Target: exact}},
	})

	require.NoError(t, d.backSessionForAttachment(effect))
	frames := transport.Sends()
	require.Len(t, frames, 1)
	require.Equal(t, wire.MsgAttachTarget, frames[0].Type)
	handoff, err := wire.UnmarshalAttachTarget(frames[0].Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.AttachTarget{
		Session: target.name, Intent: protocol.IntentAttach, ExactTarget: &exact,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned, SamePeer: true,
	}, handoff)
}
