package daemon

import (
	"errors"
	"maps"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// retiredRoutes uses only authoritative observations. An unavailable remote
// source retains its history; a successful empty catalogue proves absence.
func retiredRoutes(subscription protocol.RouteAttentionSubscription, after map[protocol.RouteAttentionTarget]time.Time, inv sessionInventory) []protocol.RouteRetired {
	var retired []protocol.RouteRetired
	for _, target := range subscription.Targets {
		known, present := target.SourceKey == "", false
		if known {
			for _, item := range inv.live {
				present = present || item.view.incarnation == target.Target.LifecycleID
			}
			for _, stopped := range inv.stopped {
				present = present || stopped.incarnation == target.Target.LifecycleID
			}
		} else {
			for _, host := range inv.hosts {
				if protocol.RemoteInventorySourceKey(host.Endpoint) != target.SourceKey {
					continue
				}
				// A cached success (or a fetch already running at admission)
				// may predate creation of this lifecycle. Only a later-started,
				// completed successful observation can establish absence.
				admitted, fenced := after[target]
				known = fenced && host.InventoryKnown && host.Availability == domain.RemoteAvailabilityReachable &&
					!host.Checking && host.LastAttempt.After(admitted) && !host.LastSuccess.Before(host.LastAttempt)
				for _, session := range host.Sessions {
					present = present || session.LifecycleID == target.Target.LifecycleID
				}
				break
			}
		}
		if known && !present {
			retired = append(retired, protocol.RouteRetired{Ref: target.Ref, Target: target.Target})
		}
	}
	return retired
}

func (d *Daemon) reconcileRouteHistory(ac *attachedClient, admitted ...*attachmentEffect) {
	if ac == nil {
		return
	}
	ac.routeMu.RLock()
	expected := ac.routeSubscriptionTransport
	subscription := ac.routeAttentionSubscription
	subscription.Targets = append([]protocol.RouteAttentionTarget(nil), subscription.Targets...)
	after := maps.Clone(ac.routeObservationAfter)
	ac.routeMu.RUnlock()
	if expected.transport == nil || len(subscription.Targets) == 0 || !ac.transportSnapshotCurrent(expected) {
		return
	}
	var effect *attachmentEffect
	if len(admitted) != 0 {
		effect = admitted[0]
	} else {
		_, token, ok := d.currentAttachmentConnection(ac, expected.transport)
		if !ok {
			return
		}
		var okEffect bool
		effect, okEffect = ac.beginAttachmentEffect(token)
		if !okEffect {
			return
		}
		defer effect.End()
	}
	if effect == nil || effect.transport != expected || !effect.current() {
		return
	}
	retired := retiredRoutes(subscription, after, d.captureSessionInventory(viewOptions{}, false))
	for _, message := range retired {
		// Bind to the connection that supplied this subscription; a resumed
		// attachment must not receive observations from its old transport.
		_, err := d.boundedSendWith(expected.transport, func() error {
			return effect.sendControl(message)
		})
		if err != nil {
			if errors.Is(err, errSendTimedOut) {
				_ = ac.closeCapturedTransport(expected.transport)
			}
			return
		}
	}
}

func (d *Daemon) reconcileAllRouteHistories() {
	d.mu.Lock()
	sessions := d.sessionsSnapshotLocked()
	d.mu.Unlock()
	for _, sess := range sessions {
		for _, ac := range sess.snapshotAttachments() {
			d.reconcileRouteHistory(ac)
		}
	}
}
