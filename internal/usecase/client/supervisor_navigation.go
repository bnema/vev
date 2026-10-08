package client

import (
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// navigationState is the supervisor's run-goroutine navigation bookkeeping:
// the adopted connection's publication wake, the swap and in-place choices
// still waiting on a live attachment, and the client route ledger. It is only
// touched from the run goroutine, so it has no mutex. The zero value is ready.
type navigationState struct {
	// readySub is the adopted connection's subscription while the ready phase
	// runs, so the picker overlay over a live attachment keeps folding broker
	// publications. It is only touched from the run goroutine.
	readySub ports.BrokerSubscription
	// pendingSwap is the request the picker overlay or a daemon navigation
	// committed to another target while an attachment was live. It is only
	// touched from the run goroutine and consumed by settleAttachment.
	pendingSwap *pickerAttachmentTarget
	// pendingInPlace is the choice the live attachment is switching to in
	// place on its own daemon. A refusal turns it into pendingSwap. Only
	// touched from the run goroutine; cleared when the attachment settles.
	pendingInPlace *pendingInPlace
	// inPlaceSeq numbers in-place choices so a late outcome for an older one
	// is never applied to a newer one.
	inPlaceSeq uint64
	// routes is the client route ledger published to the serving daemon;
	// routesSent is the attachment that received its latest snapshot. Both are
	// only touched from the run goroutine.
	routes     *routeLedger
	routesSent AttachmentToken
}

// brokerChanged is the adopted connection's publication wake, or nil.
func (n *navigationState) brokerChanged() <-chan struct{} {
	if supervisorNil(n.readySub) {
		return nil
	}
	return n.readySub.Changed()
}

// setReady records the adopted connection's subscription.
func (n *navigationState) setReady(sub ports.BrokerSubscription) { n.readySub = sub }

// clearReady forgets the adopted connection's subscription.
func (n *navigationState) clearReady() { n.readySub = nil }

// setSwap commits the next attachment target.
func (n *navigationState) setSwap(target *pickerAttachmentTarget) { n.pendingSwap = target }

// hasSwap reports whether a swap target is already committed.
func (n *navigationState) hasSwap() bool { return n.pendingSwap != nil }

// takeSwap returns and clears the committed swap target.
func (n *navigationState) takeSwap() *pickerAttachmentTarget {
	swap := n.pendingSwap
	n.pendingSwap = nil
	return swap
}

// switchingInPlaceTo reports whether an in-place switch to target is in flight.
func (n *navigationState) switchingInPlaceTo(target protocol.ExactSessionTarget) bool {
	return n.pendingInPlace != nil && n.pendingInPlace.target.request.Target == target
}

// inPlaceIdle reports whether no in-place switch is in flight.
func (n *navigationState) inPlaceIdle() bool { return n.pendingInPlace == nil }

// nextInPlaceSeq numbers one new in-place choice.
func (n *navigationState) nextInPlaceSeq() uint64 {
	n.inPlaceSeq++
	return n.inPlaceSeq
}

// startInPlace records the in-place switch numbered seq as in flight.
func (n *navigationState) startInPlace(seq uint64, target pickerAttachmentTarget) {
	n.pendingInPlace = &pendingInPlace{seq: seq, target: target}
}

// clearInPlace forgets any in-place switch in flight.
func (n *navigationState) clearInPlace() { n.pendingInPlace = nil }

// takeInPlace returns and clears the in-flight in-place switch when it is the
// one numbered seq; an outcome for an older choice returns nil and changes
// nothing.
func (n *navigationState) takeInPlace(seq uint64) *pendingInPlace {
	pending := n.pendingInPlace
	if pending == nil || pending.seq != seq {
		return nil
	}
	n.pendingInPlace = nil
	return pending
}

// buildRoutes rebuilds the route ledger over the broker snapshot, creating it
// on first use.
func (n *navigationState) buildRoutes(snapshot ports.BrokerSnapshot, active routeActive) (protocol.RecentRouteSnapshot, bool) {
	if n.routes == nil {
		n.routes = newRouteLedger()
	}
	return n.routes.build(snapshot, active)
}

// routesSentTo reports whether token already received a route snapshot.
func (n *navigationState) routesSentTo(token AttachmentToken) bool { return n.routesSent == token }

// markRoutesSent records the attachment that received the latest snapshot.
func (n *navigationState) markRoutesSent(token AttachmentToken) { n.routesSent = token }

// resolveRoute resolves one route reference against the ledger.
func (n *navigationState) resolveRoute(ref protocol.RouteRef) (routeLedgerTarget, bool) {
	if n.routes == nil || ref.IsZero() {
		return routeLedgerTarget{}, false
	}
	return n.routes.resolve(ref)
}
