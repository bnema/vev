package daemon

import (
	"fmt"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
)

// applyOutput applies one decoded remote output frame for the current link.
// Link readers use applyOutputForGeneration to bind processing to the exact
// transport generation that received the frame.
func (p *proxySession) applyOutput(out ports.Output) (ack uint64, requestReset, changed bool) {
	p.mu.Lock()
	generation := p.linkGeneration
	p.mu.Unlock()
	return p.applyOutputForGeneration(generation, out)
}

// applyOutputForGeneration applies one decoded remote output frame while
// holding only the proxy leaf lock. The supplied link generation prevents a
// receive goroutine from an old transport advancing the replacement link's VT
// state.
func (p *proxySession) applyOutputForGeneration(generation uint64, out ports.Output) (ack uint64, requestReset, changed bool) {
	// Screen.Write may retain a partial escape sequence, so do not retain a
	// transport-owned payload.
	data := append([]byte(nil), out.Data...)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.linkGeneration != generation {
		return 0, false, false
	}
	if p.screen == nil {
		return 0, false, false
	}
	if out.NewStateNum == 0 {
		p.screen.Write(data)
		return 0, false, len(data) != 0
	}
	if out.BaseStateNum == 0 {
		// A retransmitted full paint must not clobber a newer screen or regress
		// the applied state. Only a reset this proxy actually asked for may
		// replay a state it has already applied.
		if !p.awaitingReset && out.NewStateNum <= p.appliedState {
			return 0, false, false
		}
		p.screen = vt.NewScreen(p.screen.Frame.Width, p.screen.Frame.Height)
		p.screen.Write(data)
		p.appliedState = out.NewStateNum
		p.awaitingReset = false
		return out.NewStateNum, false, true
	}
	// Stateful frames are consecutive by construction. A duplicate or an
	// otherwise non-monotonic state is invalid just like a missing dependency.
	if p.awaitingReset || out.BaseStateNum != p.appliedState || out.NewStateNum != out.BaseStateNum+1 {
		if !p.awaitingReset {
			p.awaitingReset = true
			requestReset = true
		}
		return 0, requestReset, false
	}
	p.screen.Write(data)
	p.appliedState = out.NewStateNum
	return out.NewStateNum, false, len(data) != 0
}

// handleProxyOutput completes output handling after applyOutputForGeneration
// has released proxy.mu. In particular, transport I/O and local render
// invalidation must never run under the VT lock.
func (d *Daemon) handleProxyOutput(p *proxySession, generation uint64, out ports.Output) error {
	ack, requestReset, changed := p.applyOutputForGeneration(generation, out)
	if ack != 0 {
		if err := p.sendGeneration(generation, ports.Frame{
			Type:    ports.MsgAck,
			Payload: ports.MarshalAck(ports.Ack{AckedStateNum: ack}),
		}); err != nil {
			return fmt.Errorf("proxy output ACK: %w: %w", errProxyLinkSend, err)
		}
	}
	if requestReset {
		if err := p.sendGeneration(generation, ports.Frame{
			Type:    ports.MsgOutputResetRequest,
			Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
		}); err != nil {
			return fmt.Errorf("proxy output reset request: %w: %w", errProxyLinkSend, err)
		}
	}
	if !changed || !p.currentLinkGeneration(generation) {
		return nil
	}

	p.sessionCore.mu.Lock()
	ac := p.client
	p.sessionCore.mu.Unlock()
	d.invalidateRender(p, ac, false, "proxy_output.go")
	return nil
}

// resetOutputState discards the dependency chain after a local content resize.
// The next incremental remote frame is rejected and triggers a request for an
// authoritative base-zero paint.
func (p *proxySession) resetOutputState() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.resetOutputStateLocked()
}

func (p *proxySession) resetOutputStateLocked() {
	p.appliedState = 0
	p.awaitingReset = false
}

func (p *proxySession) currentLinkGeneration(generation uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.linkGeneration == generation
}
