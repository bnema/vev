package daemon

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/bnema/vev/internal/ports"
)

type attachmentEffectPhase uint8

const (
	attachmentEffectsStable attachmentEffectPhase = iota
	attachmentEffectsFrozen
)

// attachmentLifecycle owns the immutable committed capability and the
// linearization gate for every attachment-bound observable effect. Transitions
// freeze and drain it before changing membership, generation, Transport, or
// render lease identity.
type attachmentLifecycle struct {
	generation       atomic.Uint64
	mu               sync.Mutex
	cond             *sync.Cond
	changed          chan struct{}
	phase            attachmentEffectPhase
	capability       attachmentCapability
	inFlight         uint64
	transportEffects map[*attachmentEffect]transportSnapshot
	failedTransport  transportSnapshot
	order            atomic.Uint64
}

var nextAttachmentEffectGateOrder atomic.Uint64

func (g *attachmentLifecycle) generationValue() uint64 {
	if g == nil {
		return 0
	}
	return g.generation.Load()
}

func (g *attachmentLifecycle) initLocked() {
	if g.cond == nil {
		g.cond = sync.NewCond(&g.mu)
	}
	if g.changed == nil {
		g.changed = make(chan struct{})
	}
}

func (g *attachmentLifecycle) signalLocked() {
	g.initLocked()
	close(g.changed)
	g.changed = make(chan struct{})
	g.cond.Broadcast()
}

func (g *attachmentLifecycle) immutableOrder() uint64 {
	if order := g.order.Load(); order != 0 {
		return order
	}
	order := nextAttachmentEffectGateOrder.Add(1)
	if g.order.CompareAndSwap(0, order) {
		return order
	}
	return g.order.Load()
}

func (g *attachmentLifecycle) inFlightCount() uint64 {
	if g == nil {
		return 0
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.inFlight
}

// attachmentEffect is an explicit admission of one immutable capability. It
// remains valid until End because transitions freeze admission and drain all
// existing effects before changing lifecycle identity. End is idempotent.
type attachmentEffect struct {
	attachmentCapability
	lifecycle    *attachmentLifecycle
	actionDaemon *Daemon
	action       string
	ended        atomic.Bool
}

func (t *attachmentEffect) bindActionEnd(d *Daemon, action string) {
	if t == nil || t.ended.Load() {
		return
	}
	t.actionDaemon = d
	t.action = action
}

func (t *attachmentEffect) capability() attachmentCapability {
	if t == nil {
		return attachmentCapability{}
	}
	return t.attachmentCapability
}

func (t *attachmentEffect) current() bool {
	return t != nil && !t.ended.Load()
}

func (t *attachmentEffect) currentSessionLocked() bool {
	return t.current()
}

func (t *attachmentEffect) End() {
	if t == nil || t.lifecycle == nil || !t.ended.CompareAndSwap(false, true) {
		return
	}
	g := t.lifecycle
	g.mu.Lock()
	g.initLocked()
	if g.inFlight == 0 {
		g.mu.Unlock()
		panic("daemon: attachment effect ended without admission")
	}
	delete(g.transportEffects, t)
	g.inFlight--
	g.signalLocked()
	g.mu.Unlock()
	if t.actionDaemon != nil && t.actionDaemon.afterActionAttachmentEffectEnded != nil {
		t.actionDaemon.afterActionAttachmentEffectEnded(t.action)
	}
}

func (d *Daemon) endActionAttachmentEffect(effect *attachmentEffect, _ string) {
	if effect != nil {
		effect.End()
	}
}

// beginTransportSend marks only the interval spent in an external transport
// send as interruptible. A replacement may close this exact published link to
// release the send without retiring an idle healthy attachment.
func (t *attachmentEffect) beginTransportSend(expected transportSnapshot) bool {
	if t == nil || t.lifecycle == nil || t.ended.Load() || expected.transport == nil {
		return false
	}
	g := t.lifecycle
	g.mu.Lock()
	g.initLocked()
	if t.ended.Load() || g.capability.transport.transport != expected.transport ||
		g.capability.transport.incarnation != expected.incarnation {
		g.mu.Unlock()
		return false
	}
	if g.transportEffects == nil {
		g.transportEffects = make(map[*attachmentEffect]transportSnapshot)
	}
	g.transportEffects[t] = expected
	g.signalLocked()
	g.mu.Unlock()
	return true
}

func (t *attachmentEffect) reportTransportFailure(expected transportSnapshot) {
	if t == nil || t.lifecycle == nil || expected.transport == nil {
		return
	}
	g := t.lifecycle
	g.mu.Lock()
	if !t.ended.Load() && g.capability.transport.transport == expected.transport &&
		g.capability.transport.incarnation == expected.incarnation {
		g.failedTransport = expected
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (t *attachmentEffect) endTransportSend() {
	if t == nil || t.lifecycle == nil {
		return
	}
	g := t.lifecycle
	g.mu.Lock()
	if _, exists := g.transportEffects[t]; exists {
		delete(g.transportEffects, t)
		g.signalLocked()
	}
	g.mu.Unlock()
}

func (t *attachmentEffect) sendControl(frame ports.Frame) error {
	if !t.current() || t.ac == nil || t.transport.transport == nil {
		return errAttachmentTransition
	}
	t.ac.sendMu.Lock()
	defer t.ac.sendMu.Unlock()
	if !t.current() || !t.ac.transportSnapshotCurrent(t.transport) || !t.beginTransportSend(t.transport) {
		return errAttachmentTransition
	}
	err := t.transport.transport.Send(frame)
	if err != nil {
		t.reportTransportFailure(t.transport)
	}
	t.endTransportSend()
	return err
}

// beginAttachmentEffect is the sole attachment-bound effect admission point. The gate is
// held only long enough to reserve the capability; the effect itself must run
// without the gate mutex.
func (ac *attachedClient) beginAttachmentEffect(token attachmentCapability) (*attachmentEffect, bool) {
	if ac == nil || token.ac != ac {
		return nil, false
	}
	g := &ac.lifecycle
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsStable || !g.capability.sameIdentity(token) {
		g.mu.Unlock()
		return nil, false
	}
	g.inFlight++
	g.mu.Unlock()
	return &attachmentEffect{attachmentCapability: token, lifecycle: g}, true
}

// beginCurrentAttachmentEffect waits out a transition that already froze ac, then
// admits the capability derived from the session registry. Handshakes use it
// after Welcome has completed so a replacement blocked behind that send can
// publish before readiness or first paint.
func (ac *attachedClient) beginCurrentAttachmentEffect(sess *session, tr ports.Transport) (attachmentCapability, *attachmentEffect, bool) {
	//nolint:contextcheck // This compatibility wrapper deliberately delegates with a fresh context; cancellable callers use the context variant below.
	return ac.beginCurrentAttachmentEffectContext(context.Background(), sess, tr)
}

func (ac *attachedClient) beginCurrentAttachmentEffectContext(ctx context.Context, sess *session, tr ports.Transport) (attachmentCapability, *attachmentEffect, bool) {
	if ctx == nil {
		ctx = context.Background()
	}
	if ac == nil || sess == nil || tr == nil {
		return attachmentCapability{}, nil, false
	}
	for {
		token := sess.captureAttachmentCapability(ac, tr)
		if token.ac == nil {
			return token, nil, false
		}
		if ticket, admitted := ac.beginAttachmentEffect(token); admitted {
			return token, ticket, true
		}

		g := &ac.lifecycle
		g.mu.Lock()
		g.initLocked()
		if g.phase == attachmentEffectsFrozen {
			changed := g.changed
			g.mu.Unlock()
			select {
			case <-changed:
				continue
			case <-ctx.Done():
				return token, nil, false
			}
		}
		g.mu.Unlock()
		// A stable mismatch is only recoverable when a publication raced capability
		// capture; an already-stale capability proves that happened. A current capability
		// with a stable mismatch violates the publication invariant, so fail
		// closed instead of spinning forever.
		if token.current() {
			return token, nil, false
		}
		// Publication may have completed between token capture and admission.
		// Re-read the registry and exact generation instead of using stale authority.
	}
}

// installInitialCapability publishes only an empty lifecycle owner. It supports
// direct/headless construction without creating a second mutable publication.
func (g *attachmentLifecycle) installInitialCapability(capability attachmentCapability) {
	g.mu.Lock()
	g.initLocked()
	if g.phase == attachmentEffectsStable && g.capability.sess == nil {
		g.capability = capability
		g.failedTransport = transportSnapshot{}
	}
	g.mu.Unlock()
}

func (ac *attachedClient) publishFrozenAttachmentCapability(capability attachmentCapability) attachmentCapability {
	g := &ac.lifecycle
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsFrozen {
		g.mu.Unlock()
		panic("daemon: publishing attachment capability without transition freeze")
	}
	capability.generation = g.generation.Add(1)
	g.capability = capability
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
	return capability
}

// invalidateRetiredAttachmentCapability retires parked identity either while
// stable with no admitted effects or under a terminal teardown's existing
// freeze. Live membership transitions use invalidateFrozenAttachmentCapability.
func (ac *attachedClient) invalidateRetiredAttachmentCapability() {
	g := &ac.lifecycle
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsFrozen && (g.phase != attachmentEffectsStable || g.inFlight != 0) {
		g.mu.Unlock()
		panic("daemon: invalidating an active attachment capability")
	}
	g.generation.Add(1)
	g.capability = attachmentCapability{}
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}

func (ac *attachedClient) invalidateFrozenAttachmentCapability() {
	g := &ac.lifecycle
	g.mu.Lock()
	g.initLocked()
	if g.phase != attachmentEffectsFrozen {
		g.mu.Unlock()
		panic("daemon: invalidating attachment capability without transition freeze")
	}
	g.generation.Add(1)
	g.capability = attachmentCapability{}
	g.failedTransport = transportSnapshot{}
	g.mu.Unlock()
}
