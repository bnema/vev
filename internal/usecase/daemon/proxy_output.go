package daemon

import (
	"fmt"

	"github.com/bnema/vev/internal/ports"
)

// applyOutputForGeneration validates the remote ordinary output dependency
// chain without retaining a second terminal or renderer state. Bytes are
// forwarded only after a full frame establishes the current remote stream.
func (p *proxySession) applyOutputForGeneration(generation uint64, out ports.Output) (ack uint64, requestReset, changed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.linkGeneration != generation {
		return 0, false, false
	}
	invalid := func() (uint64, bool, bool) {
		if p.outputResetRequested {
			return 0, false, false
		}
		p.outputResetRequested = true
		return 0, true, false
	}
	if out.Epoch == 0 {
		return invalid()
	}
	if out.New == 0 {
		// New == 0 is an ordinary byte side effect, not a state transition.
		// Match the thin client: it is accepted before the first state-bearing
		// frame, and thereafter only within the current epoch.
		if out.Base != 0 || out.Full || (p.outputReady && out.Epoch != p.outputEpoch) {
			return invalid()
		}
		return 0, false, len(out.Data) != 0
	}
	if !p.outputReady {
		if out.Base != 0 || !out.Full {
			return invalid()
		}
		p.outputEpoch = out.Epoch
		p.outputState = out.New
		p.outputReady = true
		p.outputResetRequested = false
		return out.New, false, len(out.Data) != 0
	}
	if out.Epoch < p.outputEpoch {
		return invalid()
	}
	if out.Epoch == p.outputEpoch {
		if p.outputResetRequested || out.Full || out.Base != p.outputState || out.New != out.Base+1 {
			return invalid()
		}
		p.outputState = out.New
		return out.New, false, len(out.Data) != 0
	}
	if out.Base != 0 || !out.Full {
		return invalid()
	}
	p.outputEpoch = out.Epoch
	p.outputState = out.New
	p.outputResetRequested = false
	return out.New, false, len(out.Data) != 0
}

// handleProxyOutput forwards ordinary remote terminal bytes to the local
// attachment and acknowledges a state-bearing frame only after forwarding.
func (d *Daemon) handleProxyOutput(p *proxySession, generation uint64, out ports.Output) error {
	ack, requestReset, changed := p.applyOutputForGeneration(generation, out)
	if requestReset {
		if err := p.sendGeneration(generation, ports.Frame{
			Type:    ports.MsgOutputResetRequest,
			Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
		}); err != nil {
			return fmt.Errorf("proxy output reset request: %w: %w", errProxyLinkSend, err)
		}
	}
	if changed {
		if err := d.handleProxySideEffect(p, generation, out); err != nil {
			return err
		}
	}
	if ack != 0 {
		if err := p.sendGeneration(generation, ports.Frame{
			Type:    ports.MsgAck,
			Payload: ports.MarshalAck(ports.Ack{Epoch: out.Epoch, State: ack}),
		}); err != nil {
			return fmt.Errorf("proxy output ACK: %w: %w", errProxyLinkSend, err)
		}
	}
	return nil
}

func (p *proxySession) resetOutputStateLocked() {
	p.outputEpoch = 0
	p.outputState = 0
	p.outputReady = false
	p.outputResetRequested = false
}

func (p *proxySession) currentLinkGeneration(generation uint64) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.linkGeneration == generation
}
