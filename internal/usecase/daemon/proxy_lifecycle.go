package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

// Proxy lifecycle owns the dormant-expiry timer and the link-state publication
// of one remote attachment. It extends the package lock ordering documented at
// the top of client.go: every function in this file acquires
// d.mu -> p.sessionCore.mu -> p.mu, in that order and never any other, and
// holds them only to validate and publish state. Clock, Timer, transport,
// coordinator, and repaint work always runs after all three are released.

const proxyWarmDuration = 5 * time.Minute

// proxyWarmTimer is an exact lifecycle capability. Both pointer identity and
// generation must still match before its callback may remove a registry entry.
type proxyWarmTimer struct {
	generation uint64
	timer      ports.Timer
	cancel     chan struct{}
	done       chan struct{}
	cancelOnce sync.Once
}

func (t *proxyWarmTimer) stop() {
	if t == nil {
		return
	}
	if t.timer != nil {
		t.timer.Stop()
	}
	t.cancelOnce.Do(func() { close(t.cancel) })
}

// proxyAttachmentTransitionCommitted applies lifecycle policy only after the
// attachment publication has released every architecture lock. Each helper
// performs its own exact pointer/client revalidation before changing a timer.
func (d *Daemon) proxyAttachmentTransitionCommitted(source, target attachmentSession, ac *attachedClient, preserveRole bool) {
	if d == nil || preserveRole || source == target {
		return
	}
	if proxy, ok := source.(*proxySession); ok {
		d.armProxyWarm(proxy)
	}
	if proxy, ok := target.(*proxySession); ok {
		if d.cancelProxyWarmForAttachment(proxy, ac) {
			d.touchMRU(proxy)
		}
	}
}

// cancelProxyWarmForAttachment marks an exact published attachment as active.
// Timer operations occur only after d.mu, sessionCore.mu, and proxy.mu have all
// been released.
func (d *Daemon) cancelProxyWarmForAttachment(p *proxySession, ac *attachedClient) bool {
	if d == nil || p == nil || ac == nil {
		return false
	}
	d.mu.Lock()
	if d.sessions[p.id] != p {
		d.mu.Unlock()
		return false
	}
	p.sessionCore.mu.Lock()
	if !attachmentRegisteredLocked(attachmentSession(p), ac) {
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	p.mu.Lock()
	p.generation++
	old := p.warm
	p.warm = nil
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	old.stop()
	return true
}

// armProxyWarm reserves and publishes a fresh five-minute timer only while the
// exact proxy remains registered and headless. Clock and Timer methods are
// external operations and therefore run outside architecture locks.
//
// The lifecycle generation is bumped and the incumbent token released only in
// phase 2, atomically with publishing the replacement. A failed revalidation
// therefore leaves the already-armed token both installed and current, so the
// proxy can never be left registered without a path out of the live registry.
func (d *Daemon) armProxyWarm(p *proxySession) bool {
	if d == nil || p == nil || d.clock == nil {
		return false
	}
	d.mu.Lock()
	if d.sessions[p.id] != p || d.closing {
		d.mu.Unlock()
		return false
	}
	p.sessionCore.mu.Lock()
	if len(p.attachments) != 0 {
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	p.mu.Lock()
	generation := p.generation
	old := p.warm
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()

	timer := d.clock.NewTimer(proxyWarmDuration)
	if timer == nil {
		return false
	}
	token := &proxyWarmTimer{
		timer:  timer,
		cancel: make(chan struct{}),
		done:   make(chan struct{}),
	}

	d.mu.Lock()
	valid := d.sessions[p.id] == p && !d.closing
	if valid {
		p.sessionCore.mu.Lock()
		valid = len(p.attachments) == 0
		if valid {
			p.mu.Lock()
			valid = p.generation == generation && p.warm == old
			if valid {
				p.generation++
				token.generation = p.generation
				p.warm = token
			}
			p.mu.Unlock()
		}
		p.sessionCore.mu.Unlock()
	}
	d.mu.Unlock()
	if !valid {
		timer.Stop()
		close(token.done)
		return false
	}
	// The incumbent token is retired only once its successor owns the exact
	// lifecycle generation, never before.
	old.stop()

	go func() {
		defer close(token.done)
		select {
		case <-timer.C():
			d.expireWarmProxy(p, token)
		case <-token.cancel:
		}
	}()
	return true
}

// expireWarmProxy removes only the exact dormant proxy pointer and lifecycle
// generation. The canonical d.mu -> core.mu -> proxy.mu order is held only for
// validation/publication; cancellation, coordinator teardown, transport close,
// picker refresh, and daemon completion happen afterward.
func (d *Daemon) expireWarmProxy(p *proxySession, token *proxyWarmTimer) bool {
	if d == nil || p == nil || token == nil {
		return false
	}
	d.mu.Lock()
	if d.sessions[p.id] != p {
		d.mu.Unlock()
		return false
	}
	p.sessionCore.mu.Lock()
	if len(p.attachments) != 0 {
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	p.mu.Lock()
	if p.warm != token || p.generation != token.generation {
		p.mu.Unlock()
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	// Removal from the registry is what commits this expiry, so it is attempted
	// before any terminal lifecycle state is published. unregisterSessionLocked
	// only reads d.sessions, which this call already owns; it acquires no lock.
	if !d.unregisterSessionLocked(p) {
		p.mu.Unlock()
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	p.warm = nil
	p.generation++
	p.expired = true
	cancel := p.cancel
	transport := p.transport
	coordinator := p.coordinator.Load()
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	empty := len(d.sessions) == 0
	if empty {
		d.closing = true
	}
	d.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if transport != nil {
		_ = transport.Close()
	}
	if coordinator != nil {
		coordinator.beginSessionTeardown().finish()
		coordinator.waitForTimerWorkers()
	}
	p.finish()
	if empty {
		d.doneOnce.Do(func() { close(d.done) })
	} else {
		d.refreshRemoteOpenPickers()
	}
	return true
}

// withProxyLocked evaluates one lifecycle predicate under the canonical
// d.mu -> p.sessionCore.mu -> p.mu order, but only while p is still the exact
// registered session. update owns every state mutation it wants published and
// reports whether it applied. Repaint is deliberately after all architecture
// locks have been released.
func (d *Daemon) withProxyLocked(p *proxySession, update func() bool) bool {
	if d == nil || p == nil {
		return false
	}
	d.mu.Lock()
	if d.sessions[p.id] != p {
		d.mu.Unlock()
		return false
	}
	p.sessionCore.mu.Lock()
	p.mu.Lock()
	current := update()
	attachments := append([]*attachedClient(nil), p.sessionCore.snapshotAttachmentsLocked()...)
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	if current {
		for _, ac := range attachments {
			d.invalidateRender(p, ac, false, "proxy_lifecycle.go")
		}
	}
	return current
}

// updateProxyLinkState publishes one transport callback only if the exact proxy,
// link generation, and transport remain current.
func (d *Daemon) updateProxyLinkState(p *proxySession, generation uint64, transport ports.Transport, state ports.LinkState) bool {
	if d == nil || p == nil || transport == nil {
		return false
	}
	return d.withProxyLocked(p, func() bool {
		current := !p.expired && p.linkGeneration == generation && p.transport == transport
		if current {
			p.linkState = state
		}
		return current
	})
}

// updateProxyDisconnectedState publishes a state after the exact generation's
// transport has retired. It is used while resume dialing is pending or failed.
func (d *Daemon) updateProxyDisconnectedState(p *proxySession, generation uint64, state ports.LinkState) bool {
	if d == nil || p == nil {
		return false
	}
	return d.withProxyLocked(p, func() bool {
		current := !p.expired && p.linkGeneration == generation && p.transport == nil
		if current {
			p.linkState = state
		}
		return current
	})
}

func (d *Daemon) repaintProxyLifecycle(p *proxySession) {
	d.withProxyLocked(p, func() bool {
		return !p.expired && p.transport != nil
	})
}

// markProxyReplaced makes ReasonReplaced terminal for this exact remote link.
// Once published, no later state callback or resume attempt can revive it. An
// already-armed dormant expiry remains the lifecycle capability for this exact
// proxy and generation; replacement must not invalidate or clear that only path
// out of the live registry. A dormant proxy without a timer is armed afterward,
// outside every architecture lock.
func (d *Daemon) markProxyReplaced(p *proxySession, generation uint64, transport ports.Transport) bool {
	if d == nil || p == nil || transport == nil {
		return false
	}
	// Arming and repainting are mutually exclusive: needsWarm requires a headless
	// proxy, and the helper only repaints an attached one. Deferring the arm until
	// after the helper's repaint therefore preserves the published order.
	var needsWarm bool
	current := d.withProxyLocked(p, func() bool {
		current := p.linkGeneration == generation && p.transport == transport
		if current {
			p.expired = true
			p.linkState = ports.LinkStateDead
			p.generation++
			if p.warm != nil {
				// Rekey the already-published exact timer instead of stopping it. This
				// preserves its deadline while invalidating older lifecycle observers.
				p.warm.generation = p.generation
			}
		}
		needsWarm = current && len(p.attachments) == 0 && p.warm == nil
		return current
	})
	if needsWarm {
		d.armProxyWarm(p)
	}
	return current
}

func (p *proxySession) lifecycleDisplayName() string {
	if p == nil {
		return ""
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.expired {
		return p.name + " [expired]"
	}
	if p.linkState == ports.LinkStateConnected {
		return p.name
	}
	return p.name + " [" + p.linkState.String() + "]"
}
