package daemon

import (
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/ports"
)

type attachmentEffectPhase uint8

const (
	attachmentEffectsStable attachmentEffectPhase = iota
	attachmentEffectsFrozen
)

// attachmentCapability is the single attachment-local publication consumed by
// effect admission. It deliberately contains no independently mutable state:
// a ticket is admitted only when every field matches the frame's token.
type attachmentCapability struct {
	sess       attachmentSession
	generation uint64
	transport  transportSnapshot
	lease      *attachmentLease
}

func capabilityFromToken(token attachmentConnectionToken) attachmentCapability {
	return attachmentCapability{
		sess:       token.sess,
		generation: token.generation,
		transport:  token.transport,
		lease:      token.lease,
	}
}

func (c attachmentCapability) matches(token attachmentConnectionToken) bool {
	return token.ac != nil && c.sess == token.sess &&
		c.generation == token.generation && c.transport.transport == token.transport.transport &&
		c.transport.incarnation == token.transport.incarnation && c.lease == token.lease
}

type attachmentEffectGate struct {
	mu               sync.Mutex
	cond             *sync.Cond
	changed          chan struct{}
	phase            attachmentEffectPhase
	capability       attachmentCapability
	inFlight         uint64
	transportEffects map[*attachmentEffectTicket]transportSnapshot
	failedTransport  transportSnapshot
	order            atomic.Uint64
}

var nextAttachmentEffectGateOrder atomic.Uint64

func (g *attachmentEffectGate) initLocked() {
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
}

func (g *attachmentEffectGate) signalLocked() {
	g.initLocked()
	close(g.changed)
	g.changed = make(chan struct{})
	g.cond.Broadcast()
}

func (g *attachmentEffectGate) immutableOrder() uint64 {
	if order := g.order.Load(); order != 0 {
		return order
	}
	order := nextAttachmentEffectGateOrder.Add(1)
	if g.order.CompareAndSwap(0, order) {
		return order
	}
	return g.order.Load()
}

func (g *attachmentEffectGate) inFlightCount() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlight
}

// attachmentEffectTicket reserves the exact published capability until End. End is
// idempotent so error paths can safely defer it before doing any observable I/O.
type attachmentEffectTicket struct {
	gate         *attachmentEffectGate
	token        attachmentConnectionToken
	actionDaemon *Daemon
	action       string
	ended        atomic.Bool
}

func (t *attachmentEffectTicket) bindActionEnd(d *Daemon, action string) {
	if t == nil || t.ended.Load() {
		return
	}
	t.actionDaemon = d
	t.action = action
}

func (t *attachmentEffectTicket) connectionToken() attachmentConnectionToken {
	if t == nil {
		return attachmentConnectionToken{}
	}
	token := t.token
	token.effect = t
	return token
}

func (t *attachmentEffectTicket) End() {
	if t == nil || t.gate == nil || !t.ended.CompareAndSwap(false, true) {
		return
	}
	g := t.gate
	g.mu.Lock()
	g.initLocked()
	if g.inFlight == 0 {
		g.mu.Unlock()
		panic("daemon: attachment effect ticket ended without admission")
	}
	delete(g.transportEffects, t)
	g.inFlight--
	g.signalLocked()
	g.mu.Unlock()
	if t.actionDaemon != nil && t.actionDaemon.afterActionAttachmentEffectEnded != nil {
		t.actionDaemon.afterActionAttachmentEffectEnded(t.action)
	}
}

func (d *Daemon) endActionAttachmentEffect(effect *attachmentEffectTicket, _ string) {
	if effect != nil {
		effect.End()
	}
}

// beginTransportSend marks only the interval spent in an external transport
// send as interruptible. A replacement may close this exact published link to
// release the send without retiring an idle healthy attachment.
func (t *attachmentEffectTicket) beginTransportSend(expected transportSnapshot) bool {
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
		g.transportEffects = make(map[*attachmentEffectTicket]transportSnapshot)
	}
	g.transportEffects[t] = expected
	g.signalLocked()
	g.mu.Unlock()
	return true
}

func (t *attachmentEffectTicket) reportTransportFailure(expected transportSnapshot) {
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

func (t *attachmentEffectTicket) endTransportSend() {
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

// beginAttachmentEffect is the sole attachment-bound effect admission point. The gate is
// held only long enough to reserve the capability; the effect itself must run
// without the gate mutex.
func (ac *attachedClient) beginAttachmentEffect(token attachmentConnectionToken) (*attachmentEffectTicket, bool) {
	if ac == nil || token.ac != ac {
		return nil, false
	}
	g := &ac.attachmentEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsStable || !g.capability.matches(token) {
		g.mu.Unlock()
		return nil, false
	}
	g.inFlight++
	g.mu.Unlock()
	captured := token
	captured.effect = nil
	return &attachmentEffectTicket{gate: g, token: captured}, true
}

// beginCurrentAttachmentEffect waits out a transition that already froze ac, then
// admits the capability derived from the session registry. Handshakes use it
// after Welcome has completed so a replacement blocked behind that send can
// publish before readiness or first paint.
func (ac *attachedClient) beginCurrentAttachmentEffect(sess *session, tr ports.Transport) (attachmentConnectionToken, *attachmentEffectTicket, bool) {
	if ac == nil || sess == nil || tr == nil {
		return attachmentConnectionToken{}, nil, false
	}
	for {
		token := sess.attachmentToken(ac, tr)
		if token.ac == nil {
			return token, nil, false
		}
		if ticket, admitted := ac.beginAttachmentEffect(token); admitted {
			return token, ticket, true
		}

		g := &ac.attachmentEffects
		g.mu.Lock()
		g.initLocked()
		if g.phase == attachmentEffectsFrozen {
			changed := g.changed
			g.mu.Unlock()
			<-changed
			continue
		}
		g.mu.Unlock()
		// Publication may have completed between token capture and admission.
		// Re-read the registry and exact generation instead of using stale authority.
	}
}

// publishAttachmentCapability is used by direct/headless setup and by transition
// publication while the gate is frozen. Production attachment changes publish only
// through publishFrozenAttachmentCapability.
func (ac *attachedClient) publishAttachmentCapability(token attachmentConnectionToken) {
	if ac == nil {
		return
	}
	g := &ac.attachmentEffects
	g.mu.Lock()
	g.initLocked()
	g.capability = capabilityFromToken(token)
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}

// bootstrapAttachmentCapability supports direct/headless session construction. It
// never changes an existing or frozen production publication.
func (ac *attachedClient) bootstrapAttachmentCapability(token attachmentConnectionToken) {
	if ac == nil || token.sess == nil || token.ac != ac {
		return
	}
	g := &ac.attachmentEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase == attachmentEffectsStable && g.capability.sess == nil {
		g.capability = capabilityFromToken(token)
		g.failedTransport = transportSnapshot{}
	}
	g.mu.Unlock()
}

func (ac *attachedClient) publishFrozenAttachmentCapability(token attachmentConnectionToken) {
	g := &ac.attachmentEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsFrozen {
		g.mu.Unlock()
		panic("daemon: publishing attachment capability without transition freeze")
	}
	g.capability = capabilityFromToken(token)
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}

func (ac *attachedClient) invalidateFrozenAttachmentCapability() {
	g := &ac.attachmentEffects
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsFrozen {
		g.mu.Unlock()
		panic("daemon: invalidating attachment capability without transition freeze")
	}
	g.capability = attachmentCapability{}
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}
