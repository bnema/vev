package daemon

import (
	"sort"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/ports"
)

type roleEffectPhase uint8

const (
	roleEffectsStable roleEffectPhase = iota
	roleEffectsFrozen
)

// roleCapability is the single attachment-local publication consumed by role
// effect admission. It deliberately contains no independently mutable state:
// a ticket is admitted only when every field matches the frame's token.
type roleCapability struct {
	sess       *session
	role       attachmentRole
	generation uint64
	transport  transportSnapshot
	lease      *attachmentLease
}

func capabilityFromToken(token attachmentRoleToken) roleCapability {
	return roleCapability{
		sess:       token.sess,
		role:       token.role,
		generation: token.generation,
		transport:  token.transport,
		lease:      token.lease,
	}
}

func (c roleCapability) matches(token attachmentRoleToken) bool {
	return token.ac != nil && c.sess == token.sess && c.role == token.role &&
		c.generation == token.generation && c.transport.transport == token.transport.transport &&
		c.transport.incarnation == token.transport.incarnation && c.lease == token.lease
}

type roleEffectGate struct {
	mu               sync.Mutex
	cond             *sync.Cond
	changed          chan struct{}
	phase            roleEffectPhase
	capability       roleCapability
	inFlight         uint64
	transportEffects map[*roleEffectTicket]transportSnapshot
	failedTransport  transportSnapshot
	order            atomic.Uint64
}

var nextRoleEffectGateOrder atomic.Uint64

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

func (g *roleEffectGate) initLocked() {
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
}

func (g *roleEffectGate) signalLocked() {
	g.initLocked()
	close(g.changed)
	g.changed = make(chan struct{})
	g.cond.Broadcast()
}

func (g *roleEffectGate) immutableOrder() uint64 {
	if order := g.order.Load(); order != 0 {
		return order
	}
	order := nextRoleEffectGateOrder.Add(1)
	if g.order.CompareAndSwap(0, order) {
		return order
	}
	return g.order.Load()
}

// roleEffectTicket reserves the exact published capability until End. End is
// idempotent so error paths can safely defer it before doing any observable I/O.
type roleEffectTicket struct {
	gate         *roleEffectGate
	token        attachmentRoleToken
	actionDaemon *Daemon
	action       string
	ended        atomic.Bool
}

func (t *roleEffectTicket) bindActionEnd(d *Daemon, action string) {
	if t == nil || t.ended.Load() {
		return
	}
	t.actionDaemon = d
	t.action = action
}

func (t *roleEffectTicket) roleToken() attachmentRoleToken {
	if t == nil {
		return attachmentRoleToken{}
	}
	token := t.token
	token.effect = t
	return token
}

func (t *roleEffectTicket) End() {
	if t == nil || t.gate == nil || !t.ended.CompareAndSwap(false, true) {
		return
	}
	g := t.gate
	g.mu.Lock()
	g.initLocked()
	if g.inFlight == 0 {
		g.mu.Unlock()
		panic("daemon: role effect ticket ended without admission")
	}
	delete(g.transportEffects, t)
	g.inFlight--
	g.signalLocked()
	g.mu.Unlock()
	if t.actionDaemon != nil && t.actionDaemon.afterActionRoleEffectEnded != nil {
		t.actionDaemon.afterActionRoleEffectEnded(t.action)
	}
}

func (d *Daemon) endActionRoleEffect(effect *roleEffectTicket, _ string) {
	if effect != nil {
		effect.End()
	}
}

// beginTransportSend marks only the interval spent in an external transport
// send as interruptible. A replacement may close this exact published link to
// release the send, but cannot retire an idle healthy snatched connection.
func (t *roleEffectTicket) beginTransportSend(expected transportSnapshot) bool {
	if t == nil || t.gate == nil || t.ended.Load() || expected.transport == nil {
		return false
	}
	g := t.gate
	g.mu.Lock()
	g.initLocked()
	if t.ended.Load() || g.capability.transport.transport != expected.transport ||
		g.capability.transport.incarnation != expected.incarnation {
		g.mu.Unlock()
		return false
	}
	if g.transportEffects == nil {
		g.transportEffects = make(map[*roleEffectTicket]transportSnapshot)
	}
	g.transportEffects[t] = expected
	g.signalLocked()
	g.mu.Unlock()
	return true
}

func (t *roleEffectTicket) reportTransportFailure(expected transportSnapshot) {
	if t == nil || t.gate == nil || expected.transport == nil {
		return
	}
	g := t.gate
	g.mu.Lock()
	if !t.ended.Load() && g.capability.transport.transport == expected.transport &&
		g.capability.transport.incarnation == expected.incarnation {
		g.failedTransport = expected
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (t *roleEffectTicket) endTransportSend() {
	if t == nil || t.gate == nil {
		return
	}
	g := t.gate
	g.mu.Lock()
	if _, exists := g.transportEffects[t]; exists {
		delete(g.transportEffects, t)
		g.signalLocked()
	}
	g.mu.Unlock()
}

// beginRoleEffect is the sole role-bound effect admission point. The gate is
// held only long enough to reserve the capability; the effect itself must run
// without the gate mutex.
func (ac *attachedClient) beginRoleEffect(token attachmentRoleToken) (*roleEffectTicket, bool) {
	if ac == nil || token.ac != ac {
		return nil, false
	}
	g := &ac.roleEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase != roleEffectsStable || !g.capability.matches(token) {
		g.mu.Unlock()
		return nil, false
	}
	g.inFlight++
	g.mu.Unlock()
	captured := token
	captured.effect = nil
	return &roleEffectTicket{gate: g, token: captured}, true
}

// publishRoleCapability is used by direct/headless setup and by transition
// publication while the gate is frozen. Production role changes publish only
// through publishFrozenRoleCapability.
func (ac *attachedClient) publishRoleCapability(token attachmentRoleToken) {
	if ac == nil {
		return
	}
	g := &ac.roleEffects
	g.mu.Lock()
	g.initLocked()
	g.capability = capabilityFromToken(token)
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}

// bootstrapRoleCapability supports direct/headless session construction. It
// never changes an existing or frozen production publication.
func (ac *attachedClient) bootstrapRoleCapability(token attachmentRoleToken) {
	if ac == nil || token.sess == nil || token.role == attachmentDetached {
		return
	}
	g := &ac.roleEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase == roleEffectsStable && g.capability.sess == nil {
		g.capability = capabilityFromToken(token)
		g.failedTransport = transportSnapshot{}
	}
	g.mu.Unlock()
}

func (ac *attachedClient) publishFrozenRoleCapability(token attachmentRoleToken) {
	g := &ac.roleEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase != roleEffectsFrozen {
		g.mu.Unlock()
		panic("daemon: publishing role capability without transition freeze")
	}
	g.capability = capabilityFromToken(token)
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}

func (ac *attachedClient) invalidateFrozenRoleCapability() {
	g := &ac.roleEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase != roleEffectsFrozen {
		g.mu.Unlock()
		panic("daemon: invalidating role capability without transition freeze")
	}
	g.capability = roleCapability{}
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
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

	frozenClients := make([]*attachedClient, 0, len(ordered))
	for _, ac := range ordered {
		g := &ac.roleEffects
		g.mu.Lock()
		g.initLocked()
		for g.phase == roleEffectsFrozen {
			if done == nil {
				g.cond.Wait()
				continue
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
				g.mu.Unlock()
				partial := frozenRoleEffectGates{clients: frozenClients}
				partial.unfreeze()
				return frozenRoleEffectGates{}
			default:
			}
		}
		g.phase = roleEffectsFrozen
		g.mu.Unlock()
		frozenClients = append(frozenClients, ac)
		if afterFrozen != nil {
			afterFrozen(ac)
		}
	}
	interruptByClient := make(map[*attachedClient]transportSnapshot, len(interrupts))
	for _, interrupt := range interrupts {
		if interrupt.ac != nil && interrupt.transport.transport != nil {
			interruptByClient[interrupt.ac] = interrupt.transport
		}
	}
	// Snapshot every exact in-flight link while all participant gates are frozen,
	// then close the complete set before waiting on any one ticket. One
	// uncooperative Send therefore cannot prevent another participant's Send
	// from being interrupted.
	interruptions := make(map[*attachedClient]transportSnapshot, len(interruptByClient))
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

	for _, ac := range ordered {
		g := &ac.roleEffects
		g.mu.Lock()
		for g.inFlight != 0 {
			if done == nil {
				g.cond.Wait()
				continue
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
				g.mu.Unlock()
				return frozenRoleEffectGates{clients: frozenClients, interruptions: interruptions, acquired: true}
			default:
			}
		}
		if expected, mayInterrupt := interruptByClient[ac]; mayInterrupt &&
			g.failedTransport.transport == expected.transport && g.failedTransport.incarnation == expected.incarnation {
			interruptions[ac] = expected
		}
		g.mu.Unlock()
	}
	return frozenRoleEffectGates{clients: frozenClients, interruptions: interruptions, acquired: true, drained: true}
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
