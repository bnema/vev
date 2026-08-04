package daemon

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/ports"
)

type attachmentEffectDrainDeadline struct {
	clock     ports.Clock
	done      chan struct{}
	stopCh    chan struct{}
	finished  chan struct{}
	startOnce sync.Once
	stopOnce  sync.Once
	started   atomic.Bool
}

func newAttachmentEffectDrainDeadline(clock ports.Clock) *attachmentEffectDrainDeadline {
	return &attachmentEffectDrainDeadline{
		clock: clock, done: make(chan struct{}), stopCh: make(chan struct{}), finished: make(chan struct{}),
	}
}

func (d *attachmentEffectDrainDeadline) stop() {
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

func (d *attachmentEffectDrainDeadline) Done() <-chan struct{} {
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

type frozenAttachmentEffectGates struct {
	clients       []*attachedClient
	interruptions map[*attachedClient]transportSnapshot
	acquired      bool
	drained       bool
}

type attachmentTransportInterrupt struct {
	ac        *attachedClient
	transport transportSnapshot
}

// attachmentEffectFreezeOptions controls one attachment-effect-gate freeze operation. A freeze
// must be called without daemon, routing, session, coordinator, send, overlay,
// router, or pane locks held.
type attachmentEffectFreezeOptions struct {
	interrupts  []attachmentTransportInterrupt
	done        func() <-chan struct{}
	afterFrozen func(*attachedClient)
	nonblocking bool
}

// freezeAttachmentEffectGates establishes transition priority in immutable attachment
// order, then drains earlier admissions.
func freezeAttachmentEffectGates(clients ...*attachedClient) frozenAttachmentEffectGates {
	return freezeAttachmentEffectGatesWith(attachmentEffectFreezeOptions{}, clients...)
}

// freezeAttachmentEffectGatesWith uses done as one overall bound for ordered
// acquisition and drain. Acquisition is all-or-nothing: if the bound expires
// behind another owner, only gates acquired by this call are rolled back, in
// reverse order. nonblocking is used by move reservations to avoid inverting
// the move/teardown protocols.
func freezeAttachmentEffectGatesWith(options attachmentEffectFreezeOptions, clients ...*attachedClient) frozenAttachmentEffectGates {
	ordered := orderedAttachmentEffectClients(clients)
	frozen, acquired := acquireAttachmentEffectGates(ordered, options.done, options.afterFrozen, options.nonblocking)
	if !acquired {
		return frozenAttachmentEffectGates{}
	}

	interruptByClient := attachmentTransportInterruptsByClient(options.interrupts)
	frozen.interruptions = make(map[*attachedClient]transportSnapshot, len(interruptByClient))
	interruptFrozenAttachmentTransports(ordered, interruptByClient)
	frozen.acquired = true
	frozen.drained = drainFrozenAttachmentEffects(ordered, interruptByClient, frozen.interruptions, options.done)
	return frozen
}

// orderedAttachmentEffectClients snapshots unique participants in immutable lock
// order. Assigning a previously unused order does not freeze or wait on a gate.
func orderedAttachmentEffectClients(clients []*attachedClient) []*attachedClient {
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
		ac.attachmentEffects.immutableOrder()
		ordered = append(ordered, ac)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return ordered[i].attachmentEffects.order.Load() < ordered[j].attachmentEffects.order.Load()
	})
	return ordered
}

// waitForAttachmentEffectChangeLocked requires g.mu and returns with g.mu held. A
// true result means the overall deadline won and the caller must stop waiting.
func waitForAttachmentEffectChangeLocked(g *attachmentEffectGate, done func() <-chan struct{}) bool {
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

// acquireAttachmentEffectGates establishes exclusive transition ownership in order.
// It must run without architecture locks. Deadline rollback releases only this
// call's partial acquisition and does so in reverse order.
func acquireAttachmentEffectGates(ordered []*attachedClient, done func() <-chan struct{}, afterFrozen func(*attachedClient), nonblocking bool) (frozenAttachmentEffectGates, bool) {
	frozen := frozenAttachmentEffectGates{clients: make([]*attachedClient, 0, len(ordered))}
	for _, ac := range ordered {
		g := &ac.attachmentEffects
		g.mu.Lock()
		g.initLocked()
		for g.phase == attachmentEffectsFrozen {
			if nonblocking {
				g.mu.Unlock()
				frozen.unfreeze()
				return frozenAttachmentEffectGates{}, false
			}
			if waitForAttachmentEffectChangeLocked(g, done) {
				g.mu.Unlock()
				frozen.unfreeze()
				return frozenAttachmentEffectGates{}, false
			}
		}
		g.phase = attachmentEffectsFrozen
		g.mu.Unlock()
		frozen.clients = append(frozen.clients, ac)
		if afterFrozen != nil {
			afterFrozen(ac)
		}
	}
	return frozen, true
}

func attachmentTransportInterruptsByClient(interrupts []attachmentTransportInterrupt) map[*attachedClient]transportSnapshot {
	byClient := make(map[*attachedClient]transportSnapshot, len(interrupts))
	for _, interrupt := range interrupts {
		if interrupt.ac != nil && interrupt.transport.transport != nil {
			byClient[interrupt.ac] = interrupt.transport
		}
	}
	return byClient
}

// interruptFrozenAttachmentTransports snapshots every exact in-flight link while all
// gates are frozen, then closes the complete set before any drain wait. One
// uncooperative Send cannot prevent another participant from being interrupted.
func interruptFrozenAttachmentTransports(ordered []*attachedClient, interruptByClient map[*attachedClient]transportSnapshot) {
	toClose := make([]attachmentTransportInterrupt, 0, len(interruptByClient))
	for _, ac := range ordered {
		expected, mayInterrupt := interruptByClient[ac]
		if !mayInterrupt {
			continue
		}
		g := &ac.attachmentEffects
		g.mu.Lock()
		if gateHasTransportEffectLocked(g, expected) {
			toClose = append(toClose, attachmentTransportInterrupt{ac: ac, transport: expected})
		}
		g.mu.Unlock()
	}
	for _, interrupt := range toClose {
		_ = interrupt.ac.closeCapturedTransport(interrupt.transport.transport)
	}
}

// drainFrozenAttachmentEffects waits for admissions made before the freeze. A closed
// deadline permits terminal teardown to continue with the capability frozen;
// late tickets can then retire without pinning the owner indefinitely.
func drainFrozenAttachmentEffects(ordered []*attachedClient, interruptByClient, interruptions map[*attachedClient]transportSnapshot, done func() <-chan struct{}) bool {
	for _, ac := range ordered {
		g := &ac.attachmentEffects
		g.mu.Lock()
		for g.inFlight != 0 {
			if waitForAttachmentEffectChangeLocked(g, done) {
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

func gateHasTransportEffectLocked(g *attachmentEffectGate, expected transportSnapshot) bool {
	for _, active := range g.transportEffects {
		if active.transport == expected.transport && active.incarnation == expected.incarnation {
			return true
		}
	}
	return false
}

func (f frozenAttachmentEffectGates) unfreeze() {
	for i := len(f.clients) - 1; i >= 0; i-- {
		g := &f.clients[i].attachmentEffects
		g.mu.Lock()
		g.initLocked()
		g.phase = attachmentEffectsStable
		g.signalLocked()
		g.mu.Unlock()
	}
}
