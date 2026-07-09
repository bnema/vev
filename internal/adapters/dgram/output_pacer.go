package dgram

import (
	"time"

	"github.com/bnema/vev/internal/ports"
)

type queuedSend struct {
	seq      uint64
	reliable bool
	frame    ports.Frame
	done     chan error
}

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
		now := t.clock.Now()
		wait := t.outputNext.Sub(now)
		if wait > 0 {
			wake := t.outputWake
			timer := t.clock.NewTimer(wait)
			t.mu.Unlock()
			t.outboundMu.Unlock()
			select {
			case <-timer.C():
			case <-wake:
			case <-t.done:
				timer.Stop()
				return
			}
			timer.Stop()
			continue
		}
		limit := 0
		budget := t.mtu
		for limit < len(t.outputQueue) && limit < defaultOutputPaceBatch {
			cost := len(t.outputQueue[limit].frame.Payload) + dataRecordHeaderSize
			if limit > 0 && cost > budget {
				break
			}
			limit++
			budget -= min(cost, budget)
		}
		batch := append([]queuedSend(nil), t.outputQueue[:limit]...)
		copy(t.outputQueue, t.outputQueue[limit:])
		t.outputQueue = t.outputQueue[:len(t.outputQueue)-limit]
		t.outputNext = now.Add(t.outputPaceDelayLocked())
		t.mu.Unlock()
		for i, q := range batch {
			if err := t.sendQueuedData(q); err != nil {
				t.removeQueuedPending(batch[i+1:], err)
				t.closeWithError(err)
				t.outboundMu.Unlock()
				return
			}
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

func (t *Transport) outputPaceDelayLocked() time.Duration {
	if t.srtt <= 0 {
		return defaultOutputPaceMinDelay
	}
	d := t.srtt / 2
	if d < defaultOutputPaceMinCadence {
		d = defaultOutputPaceMinCadence
	}
	if d > defaultOutputPaceMaxDelay {
		d = defaultOutputPaceMaxDelay
	}
	return d
}

func shouldPaceOutput(f ports.Frame) bool {
	return f.Type == ports.MsgOutput
}

func (t *Transport) sendQueuedData(q queuedSend) error {
	t.markPendingSent(q.seq, q.reliable)
	err := t.sendData(q.seq, q.reliable, q.frame)
	if err != nil {
		t.removePending(q.seq, q.reliable)
	} else {
		t.markPendingReady(q.seq, q.reliable)
	}
	if q.done != nil {
		q.done <- err
		close(q.done)
	}
	return err
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
