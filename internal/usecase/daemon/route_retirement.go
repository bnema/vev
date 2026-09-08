package daemon

import (
	"errors"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

// retiredRoutes uses only authoritative observations. An unavailable remote
// source retains its history; a successful empty catalogue proves absence.
func retiredRoutes(subscription protocol.RouteAttentionSubscription, inv sessionInventory) []protocol.RouteRetired {
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
				known = host.InventoryKnown && host.Availability == domain.RemoteAvailabilityReachable
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

func (d *Daemon) reconcileRouteHistory(ac *attachedClient) {
	if ac == nil {
		return
	}
	expected := ac.transportSnapshot()
	if expected.transport == nil {
		return
	}
	ac.routeMu.RLock()
	subscription := ac.routeAttentionSubscription
	subscription.Targets = append([]protocol.RouteAttentionTarget(nil), subscription.Targets...)
	ac.routeMu.RUnlock()
	if len(subscription.Targets) == 0 {
		return
	}
	retired := retiredRoutes(subscription, d.captureSessionInventory(viewOptions{}, false))
	for _, message := range retired {
		// Bind to the connection that supplied this subscription; a resumed
		// attachment must not receive observations from its old transport.
		_, err := d.boundedSendWith(expected.transport, func() error {
			return ac.sendExpectedTransport(expected, message)
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
