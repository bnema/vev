package dgram

import "github.com/bnema/vev/internal/ports"

type dataSendJob struct {
	seq      uint64
	reliable bool
	frame    ports.Frame
	done     chan error
}

type queuedSend = dataSendJob

func (t *Transport) outputPaceLoop() {
	for {
		t.outboundMu.Lock()
		t.mu.Lock()
		if len(t.outputQueue) == 0 && !t.closed {
			wake := t.outputWake
			t.mu.Unlock()
			t.outboundMu.Unlock()
			select {
			case <-wake:
			case <-t.done:
				return
			}
			continue
		}
		if t.closed {
			t.mu.Unlock()
			t.outboundMu.Unlock()
			return
		}
		q := t.outputQueue[0]
		copy(t.outputQueue, t.outputQueue[1:])
		t.outputQueue = t.outputQueue[:len(t.outputQueue)-1]
		t.mu.Unlock()
		if err := t.queueDataJob(dataSendJob(q)); err != nil {
			t.removeQueuedPending([]queuedSend{q}, err)
			t.outboundMu.Unlock()
			return
		}
		t.outboundMu.Unlock()
	}
}

func (t *Transport) notifyOutputPacerLocked() {
	if t.outputWake == nil {
		return
	}
	close(t.outputWake)
	t.outputWake = make(chan struct{})
}

func shouldPaceOutput(f ports.Frame) bool {
	return f.Type == ports.MsgOutput
}

func (t *Transport) markPendingSent(seq uint64, reliable bool) {
	if !reliable {
		return
	}
	t.mu.Lock()
	if p := t.pending[seq]; p != nil {
		p.initialInFlight = true
	}
	t.mu.Unlock()
}

func (t *Transport) markPendingReady(seq uint64, reliable bool) {
	if !reliable {
		return
	}
	t.mu.Lock()
	if p := t.pending[seq]; p != nil {
		p.initialInFlight = false
	}
	t.mu.Unlock()
}

func (t *Transport) markPendingFinalWrite(seq uint64) {
	now := t.clock.Now()
	t.mu.Lock()
	if p := t.pending[seq]; p != nil {
		if p.first.IsZero() {
			p.first = now
		}
		p.last = now
	}
	t.mu.Unlock()
}
