package daemon

import (
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
)

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

// armProxyWarm reserves and publishes a fresh five-minute timer only while the
// exact proxy remains registered and headless. Clock and Timer methods are
// external operations and therefore run outside architecture locks.
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
	if p.client != nil {
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
	p.mu.Lock()
	p.generation++
	generation := p.generation
	old := p.warm
	p.warm = nil
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	old.stop()

	timer := d.clock.NewTimer(proxyWarmDuration)
	if timer == nil {
		return false
	}
	token := &proxyWarmTimer{
		generation: generation,
		timer:      timer,
		cancel:     make(chan struct{}),
		done:       make(chan struct{}),
	}

	d.mu.Lock()
	valid := d.sessions[p.id] == p && !d.closing
	if valid {
		p.sessionCore.mu.Lock()
		valid = p.client == nil
		if valid {
			p.mu.Lock()
			valid = p.generation == generation && p.warm == nil
			if valid {
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
// and daemon completion happen afterward.
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
	if p.client != nil {
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
	p.warm = nil
	p.generation++
	p.expired = true
	cancel := p.cancel
	transport := p.transport
	coordinator := p.coordinator.Load()
	p.mu.Unlock()
	if !d.unregisterSessionLocked(p) {
		p.sessionCore.mu.Unlock()
		d.mu.Unlock()
		return false
	}
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
	}
	return true
}

// updateProxyLinkState publishes one transport callback only if the exact proxy,
// link generation, and transport remain current. Repaint is deliberately after
// all architecture locks have been released.
func (d *Daemon) updateProxyLinkState(p *proxySession, generation uint64, transport ports.Transport, state ports.LinkState) bool {
	if d == nil || p == nil || transport == nil {
		return false
	}
	d.mu.Lock()
	if d.sessions[p.id] != p {
		d.mu.Unlock()
		return false
	}
	p.sessionCore.mu.Lock()
	p.mu.Lock()
	current := !p.expired && p.linkGeneration == generation && p.transport == transport
	if current {
		p.linkState = state
	}
	ac := p.client
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	if current && ac != nil {
		d.invalidateRender(p, ac, false, "proxy_lifecycle.go")
	}
	return current
}

// updateProxyDisconnectedState publishes a state after the exact generation's
// transport has retired. It is used while resume dialing is pending or failed.
func (d *Daemon) updateProxyDisconnectedState(p *proxySession, generation uint64, state ports.LinkState) bool {
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
	current := !p.expired && p.linkGeneration == generation && p.transport == nil
	if current {
		p.linkState = state
	}
	ac := p.client
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	if current && ac != nil {
		d.invalidateRender(p, ac, false, "proxy_lifecycle.go")
	}
	return current
}

func (d *Daemon) repaintProxyLifecycle(p *proxySession) {
	if d == nil || p == nil {
		return
	}
	d.mu.Lock()
	if d.sessions[p.id] != p {
		d.mu.Unlock()
		return
	}
	p.sessionCore.mu.Lock()
	p.mu.Lock()
	current := !p.expired && p.transport != nil
	ac := p.client
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	if current && ac != nil {
		d.invalidateRender(p, ac, false, "proxy_lifecycle.go")
	}
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
	d.mu.Lock()
	if d.sessions[p.id] != p {
		d.mu.Unlock()
		return false
	}
	p.sessionCore.mu.Lock()
	p.mu.Lock()
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
	ac := p.client
	needsWarm := current && ac == nil && p.warm == nil
	p.mu.Unlock()
	p.sessionCore.mu.Unlock()
	d.mu.Unlock()
	if needsWarm {
		d.armProxyWarm(p)
	}
	if current && ac != nil {
		d.invalidateRender(p, ac, false, "proxy_lifecycle.go")
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
