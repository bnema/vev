package daemon

import (
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

func (g *roleEffectGate) inFlightCount() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlight
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

// beginCurrentRoleEffect waits out a transition that already froze ac, then
// admits the role capability derived from the session registries. Handshakes
// use it after Welcome has completed so a replacement blocked behind that send
// can publish before readiness, first paint, or the snatched reset panel.
func (ac *attachedClient) beginCurrentRoleEffect(sess *session, tr ports.Transport) (attachmentRoleToken, *roleEffectTicket, bool) {
	if ac == nil || sess == nil || tr == nil {
		return attachmentRoleToken{}, nil, false
	}
	for {
		token := sess.attachmentToken(ac, tr)
		if token.role == attachmentDetached {
			return token, nil, false
		}
		if ticket, admitted := ac.beginRoleEffect(token); admitted {
			return token, ticket, true
		}

		g := &ac.roleEffects
		g.mu.Lock()
		g.initLocked()
		if g.phase == roleEffectsFrozen {
			changed := g.changed
			g.mu.Unlock()
			<-changed
			continue
		}
		g.mu.Unlock()
		// Publication may have completed between token capture and admission.
		// Re-read the registry and exact generation instead of using stale role
		// or lease authority.
	}
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
