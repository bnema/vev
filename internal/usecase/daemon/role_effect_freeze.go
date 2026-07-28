package daemon

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/ports"
)

type roleEffectDrainDeadline struct {
	clock     ports.Clock
	done      chan struct{}
	stopCh    chan struct{}
	finished  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
}

func newRoleEffectDrainDeadline(clock ports.Clock) *roleEffectDrainDeadline {
	return &roleEffectDrainDeadline{
		clock: clock, done: make(chan struct{}), stopCh: make(chan struct{}), finished: make(chan struct{}),
	}
}

func (d *roleEffectDrainDeadline) stop() {
	if d == nil {
		return
	}
	d.stopOnce.Do(func() {
		// Claim an unstarted deadline without allocating a timer. Done is never
		// called after the owning freeze/drain operation returns.
		d.startOnce.Do(func() {})
		if !d.started.Load() {
			return
		}
		close(d.stopCh)
		<-d.finished
	})
}

func (d *roleEffectDrainDeadline) Done() <-chan struct{} {
	if d == nil {
		return nil
	}
	d.startOnce.Do(func() {
		d.started.Store(true)
		timer := d.clock.NewTimer(detachNotifyTimeout)
		go func() {
			defer close(d.finished)
			select {
			case <-timer.C():
				close(d.done)
			case <-d.stopCh:
				timer.Stop()
			}
		}()
	})
	return d.done
}

type frozenRoleEffectGates struct {
	clients       []*attachedClient
	interruptions map[*attachedClient]transportSnapshot
	acquired      bool
	drained       bool
}

func (f frozenRoleEffectGates) interrupted(ac *attachedClient, expected transportSnapshot) bool {
	interrupted, ok := f.interruptions[ac]
	return ok && interrupted.transport == expected.transport && interrupted.incarnation == expected.incarnation
}

type roleTransportInterrupt struct {
	ac        *attachedClient
	transport transportSnapshot
}

// freezeRoleEffectGates establishes transition priority in immutable attachment
// order, then drains earlier admissions. It must be called without daemon,
// routing, session, coordinator, send, overlay, router, or pane locks held.
func freezeRoleEffectGates(clients ...*attachedClient) frozenRoleEffectGates {
	return freezeRoleEffectGatesInterrupting(nil, clients...)
}

// freezeRoleEffectGatesInterrupting may close an exact captured transport only
// while an earlier admitted effect is inside that transport's Send. Closing and
// all drain waits happen without the gate mutex or an architecture lock held.
func freezeRoleEffectGatesInterrupting(interrupts []roleTransportInterrupt, clients ...*attachedClient) frozenRoleEffectGates {
	return freezeRoleEffectGatesInterruptingObserved(interrupts, nil, clients...)
}

func freezeRoleEffectGatesInterruptingObserved(interrupts []roleTransportInterrupt, afterFrozen func(*attachedClient), clients ...*attachedClient) frozenRoleEffectGates {
	return freezeRoleEffectGatesInterruptingObservedUntil(interrupts, nil, afterFrozen, clients...)
}

// freezeRoleEffectGatesInterruptingObservedUntil uses done as one overall bound
// for ordered acquisition and drain. Acquisition is all-or-nothing: if the
// bound expires behind another owner, only gates acquired by this call are
// rolled back, in reverse order. Once the complete set is owned, a terminal
// teardown may proceed when done closes during drain; late tickets then retire
// against an invalidated capability without pinning the session owner
// indefinitely.
func freezeRoleEffectGatesInterruptingObservedUntil(interrupts []roleTransportInterrupt, done func() <-chan struct{}, afterFrozen func(*attachedClient), clients ...*attachedClient) frozenRoleEffectGates {
	ordered := orderedRoleEffectClients(clients)
	frozen, acquired := acquireRoleEffectGates(ordered, done, afterFrozen)
	if !acquired {
		return frozenRoleEffectGates{}
	}

	interruptByClient := roleTransportInterruptsByClient(interrupts)
	frozen.interruptions = make(map[*attachedClient]transportSnapshot, len(interruptByClient))
	interruptFrozenRoleTransports(ordered, interruptByClient)
	frozen.acquired = true
	frozen.drained = drainFrozenRoleEffects(ordered, interruptByClient, frozen.interruptions, done)
	return frozen
}

// orderedRoleEffectClients snapshots unique participants in immutable lock
// order. Assigning a previously unused order does not freeze or wait on a gate.
func orderedRoleEffectClients(clients []*attachedClient) []*attachedClient {
	unique := make(map[*attachedClient]struct{}, len(clients))
	ordered := make([]*attachedClient, 0, len(clients))
	for _, ac := range clients {
		if ac == nil {
			continue
		}
		if _, exists := unique[ac]; exists {
			continue
		}
		unique[ac] = struct{}{}
		ac.roleEffects.immutableOrder()
		ordered = append(ordered, ac)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].roleEffects.order.Load() < ordered[j].roleEffects.order.Load()
	})
	return ordered
}

// waitForRoleEffectChangeLocked requires g.mu and returns with g.mu held. A
// true result means the overall deadline won and the caller must stop waiting.
func waitForRoleEffectChangeLocked(g *roleEffectGate, done func() <-chan struct{}) bool {
	if done == nil {
		g.cond.Wait()
		return false
	}
	changed := g.changed
	doneCh := done()
	g.mu.Unlock()
	select {
	case <-changed:
	case <-doneCh:
	}
	g.mu.Lock()
	select {
	case <-doneCh:
		return true
	default:
		return false
	}
}

// acquireRoleEffectGates establishes exclusive transition ownership in order.
// It must run without architecture locks. Deadline rollback releases only this
// call's partial acquisition and does so in reverse order.
func acquireRoleEffectGates(ordered []*attachedClient, done func() <-chan struct{}, afterFrozen func(*attachedClient)) (frozenRoleEffectGates, bool) {
	frozen := frozenRoleEffectGates{clients: make([]*attachedClient, 0, len(ordered))}
	for _, ac := range ordered {
		g := &ac.roleEffects
		g.mu.Lock()
		g.initLocked()
		for g.phase == roleEffectsFrozen {
			if waitForRoleEffectChangeLocked(g, done) {
				g.mu.Unlock()
				frozen.unfreeze()
				return frozenRoleEffectGates{}, false
			}
		}
		g.phase = roleEffectsFrozen
		g.mu.Unlock()
		frozen.clients = append(frozen.clients, ac)
		if afterFrozen != nil {
			afterFrozen(ac)
		}
	}
	return frozen, true
}

func roleTransportInterruptsByClient(interrupts []roleTransportInterrupt) map[*attachedClient]transportSnapshot {
	byClient := make(map[*attachedClient]transportSnapshot, len(interrupts))
	for _, interrupt := range interrupts {
		if interrupt.ac != nil && interrupt.transport.transport != nil {
			byClient[interrupt.ac] = interrupt.transport
		}
	}
	return byClient
}

// interruptFrozenRoleTransports snapshots every exact in-flight link while all
// gates are frozen, then closes the complete set before any drain wait. One
// uncooperative Send cannot prevent another participant from being interrupted.
func interruptFrozenRoleTransports(ordered []*attachedClient, interruptByClient map[*attachedClient]transportSnapshot) {
	toClose := make([]roleTransportInterrupt, 0, len(interruptByClient))
	for _, ac := range ordered {
		expected, mayInterrupt := interruptByClient[ac]
		if !mayInterrupt {
			continue
		}
		g := &ac.roleEffects
		g.mu.Lock()
		if gateHasTransportEffectLocked(g, expected) {
			toClose = append(toClose, roleTransportInterrupt{ac: ac, transport: expected})
		}
		g.mu.Unlock()
	}
	for _, interrupt := range toClose {
		_ = interrupt.ac.closeCapturedTransport(interrupt.transport.transport)
	}
}

// drainFrozenRoleEffects waits for admissions made before the freeze. A closed
// deadline permits terminal teardown to continue with the capability frozen;
// late tickets can then retire without pinning the owner indefinitely.
func drainFrozenRoleEffects(ordered []*attachedClient, interruptByClient, interruptions map[*attachedClient]transportSnapshot, done func() <-chan struct{}) bool {
	for _, ac := range ordered {
		g := &ac.roleEffects
		g.mu.Lock()
		for g.inFlight != 0 {
			if waitForRoleEffectChangeLocked(g, done) {
				g.mu.Unlock()
				return false
			}
		}
		if expected, mayInterrupt := interruptByClient[ac]; mayInterrupt &&
			g.failedTransport.transport == expected.transport && g.failedTransport.incarnation == expected.incarnation {
			interruptions[ac] = expected
		}
		g.mu.Unlock()
	}
	return true
}

func gateHasTransportEffectLocked(g *roleEffectGate, expected transportSnapshot) bool {
	for _, active := range g.transportEffects {
		if active.transport == expected.transport && active.incarnation == expected.incarnation {
			return true
		}
	}
	return false
}

func (f frozenRoleEffectGates) unfreeze() {
	for i := len(f.clients) - 1; i >= 0; i-- {
		g := &f.clients[i].roleEffects
		g.mu.Lock()
		g.initLocked()
		g.phase = roleEffectsStable
		g.signalLocked()
		g.mu.Unlock()
	}
}
