package daemon

import (
	"fmt"

	"github.com/bnema/vev/internal/ports"
)

// applyScreenUpdateForGeneration applies one semantic screen update while
// holding only the proxy leaf lock. A link generation fence prevents a stale
// receive goroutine from advancing a replacement link's screen.
func (p *proxySession) applyScreenUpdateForGeneration(generation uint64, update ports.ScreenUpdate) (ack uint64, requestReset, changed bool) {
	if p == nil {
		return 0, false, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.linkGeneration != generation || p.screen == nil {
		return 0, false, false
	}

	// The wire codec validates all semantic bounds. Geometry is still checked
	// here because it is attachment state, not merely a wire property: a
	// remote paint for the wrong content rectangle cannot be applied locally.
	if update.Size != p.contentSize {
		return 0, p.requestScreenResetLocked(), false
	}
	if update.Kind == ports.ScreenUpdateSnapshot {
		// A base-zero paint may only move the consumer forward. This floor is
		// retained across resume and resize, so resetRequested never authorizes
		// replaying a stale snapshot.
		if update.NewStateNum <= p.appliedState {
			return 0, p.requestScreenResetLocked(), false
		}
		before := p.screen.cursorOut
		if err := p.screen.Apply(update); err != nil {
			return 0, p.requestScreenResetLocked(), false
		}
		p.appliedState = update.NewStateNum
		p.screenReady = true
		p.resetRequested = false
		return update.NewStateNum, false, update.Kind == ports.ScreenUpdateSnapshot || before != update.Cursor || len(update.Spans) != 0
	}

	if !p.screenReady || p.resetRequested || update.BaseStateNum != p.appliedState || update.NewStateNum != update.BaseStateNum+1 {
		return 0, p.requestScreenResetLocked(), false
	}
	before := p.screen.cursorOut
	if err := p.screen.Apply(update); err != nil {
		return 0, p.requestScreenResetLocked(), false
	}
	p.appliedState = update.NewStateNum
	return update.NewStateNum, false, update.Scroll != nil || len(update.Spans) != 0 || before != update.Cursor
}

func (p *proxySession) requestScreenResetLocked() bool {
	if p.resetRequested {
		p.screenReady = false
		return false
	}
	p.screenReady = false
	p.resetRequested = true
	return true
}

// handleProxyScreenUpdate completes screen handling after apply has released
// proxy.mu. Transport I/O and render invalidation never run under that lock.
func (d *Daemon) handleProxyScreenUpdate(p *proxySession, generation uint64, update ports.ScreenUpdate) error {
	marks := d.newRuntimeMarkBatch()
	ack, requestReset, changed := p.applyScreenUpdateForGeneration(generation, update)
	if ack != 0 {
		if err := p.sendGeneration(generation, ports.Frame{
			Type:    ports.MsgAck,
			Payload: ports.MarshalAck(ports.Ack{AckedStateNum: ack}),
		}); err != nil {
			return fmt.Errorf("proxy screen ACK: %w: %w", errProxyLinkSend, err)
		}
	}
	if requestReset {
		if err := p.sendGeneration(generation, ports.Frame{
			Type:    ports.MsgOutputResetRequest,
			Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
		}); err != nil {
			return fmt.Errorf("proxy screen reset request: %w: %w", errProxyLinkSend, err)
		}
		marks.diagnostic(ports.RuntimeScreenResetRequested, 0, 0)
	}
	// The reset send released proxy sendMu and apply released proxy.mu before
	// observer I/O; a blocked sink cannot hold either protocol ownership lock.
	marks.flush()
	if !changed || !p.currentLinkGeneration(generation) {
		return nil
	}

	p.sessionCore.mu.Lock()
	ac := p.client
	p.sessionCore.mu.Unlock()
	d.invalidateRender(p, ac, false, "proxy_output.go")
	return nil
}

func (p *proxySession) resetOutputStateLocked() {
	p.appliedState = 0
	p.screenReady = false
	p.resetRequested = false
	if p.screen != nil {
		p.screen.stateNum = 0
	}
}

func (p *proxySession) currentLinkGeneration(generation uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.linkGeneration == generation
}
