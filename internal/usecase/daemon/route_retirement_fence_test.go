package daemon

import (
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

func TestRouteRetirementRequiresFetchStartedAfterSubscription(t *testing.T) {
	admitted := time.Unix(10, 0)
	target := protocol.RouteAttentionTarget{Ref: protocol.RouteRef{Key: 1, Generation: 1}, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "new"}, SourceKey: protocol.RemoteInventorySourceKey("remote")}
	sub := protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{target}}
	for _, tc := range []struct {
		name           string
		start, finish  int64
		checking, want bool
	}{
		{name: "old successful catalogue", start: 8, finish: 9},
		{name: "old fetch completes after admission", start: 9, finish: 11},
		{name: "new fetch still running", start: 11, finish: 9, checking: true},
		{name: "new successful absence", start: 11, finish: 12, want: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			inv := sessionInventory{hosts: []ports.RemoteHostSnapshot{{Endpoint: "remote", InventoryKnown: true, Availability: domain.RemoteAvailabilityReachable, LastAttempt: time.Unix(tc.start, 0), LastSuccess: time.Unix(tc.finish, 0), Checking: tc.checking}}}
			after := map[protocol.RouteAttentionTarget]time.Time{target: admitted}
			require.Equal(t, tc.want, len(retiredRoutes(sub, after, inv)) == 1)
		})
	}
	_, _, ac, _ := newManualSessionWithPTYs(t, nil)
	ac.setRouteAttentionSubscription(sub, ac.transportSnapshot(), admitted)
	ac.setRouteAttentionSubscription(sub, ac.transportSnapshot(), admitted.Add(time.Second))
	require.Equal(t, admitted, ac.routeObservationAfter[target], "unchanged publications must preserve the freshness fence")
}

func TestRouteRetirementDoesNotUseSubscriptionFromPreviousTransport(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, nil)
	ac.installTestAttachmentCapability(sess.captureAttachmentCapability(ac, ac.transport()))
	sub := protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{Ref: protocol.RouteRef{Key: 1, Generation: 1}, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{9}, SessionName: "deleted"}}}}
	ac.setRouteAttentionSubscription(sub, ac.transportSnapshot(), d.clock.Now())
	// Resume installs a transport while retaining the old subscription, before
	// Welcome. Reconciliation must emit nothing on the replacement connection.
	replacement := &closeTrackingTransport{}
	ac.replaceTransport(replacement)
	ac.installTestAttachmentCapability(sess.captureAttachmentCapability(ac, ac.transport()))
	d.reconcileRouteHistory(ac)
	require.Empty(t, replacement.Sends())
	ac.setRouteAttentionSubscription(sub, ac.transportSnapshot(), d.clock.Now())
	d.reconcileRouteHistory(ac)
	require.Len(t, replacement.Sends(), 1, "fresh post-Welcome subscription enables retirement")
}

func TestRouteRetirementDoesNotSendThroughFrozenAttachment(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, nil)
	ac.installTestAttachmentCapability(sess.captureAttachmentCapability(ac, ac.transport()))
	ac.setRouteAttentionSubscription(protocol.RouteAttentionSubscription{Targets: []protocol.RouteAttentionTarget{{Ref: protocol.RouteRef{Key: 1, Generation: 1}, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{9}, SessionName: "deleted"}}}}, ac.transportSnapshot(), d.clock.Now())
	frozen := freezeAttachmentEffectGates(ac)
	defer frozen.unfreeze()
	d.reconcileRouteHistory(ac)
	select {
	case message := <-sends:
		t.Fatalf("unexpected retirement during transition: %v", message.Type)
	default:
	}
}
