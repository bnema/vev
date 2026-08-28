package dgram

import (
	"errors"
	"net"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
)

func (t *Transport) Send(f wire.Frame) error {
	end := t.beginRuntimeOperation(ports.RuntimeAdapterSendStart, uint64(len(f.Payload)))
	err := t.send(f, false)
	end(err == nil)
	return err
}

func (t *Transport) SendAsync(f wire.Frame) error {
	end := t.beginRuntimeOperation(ports.RuntimeAdapterSendStart, uint64(len(f.Payload)))
	err := t.send(f, true)
	end(err == nil)
	return err
}

// SendSynchronous owns queue reservation, fragment pacing, per-write deadlines,
// and close cancellation. Callers must not layer a shorter preflight timer over it.
func (t *Transport) SendSynchronous(f wire.Frame) error { return t.Send(f) }

func (t *Transport) send(f wire.Frame, async bool) error {
	reliable := true
	if err := t.lockOutboundSlot(reliable); err != nil {
		return err
	}
	unlockOutbound := true
	defer func() {
		if unlockOutbound {
			t.outboundMu.Unlock()
		}
	}()
	// lockOutboundSlot returns with both outboundMu and mu held.

	t.seq++
	seq := t.seq
	if reliable {
		now := t.clock.Now()
		if len(t.pending) == 0 {
			t.health.pendingStarted(now)
		}
		t.pending[seq] = &pending{frame: f, enqueued: now}
	}
	var done chan error
	if !async {
		done = make(chan error, 1)
	}
	job := dataSendJob{seq: seq, reliable: reliable, frame: f, done: done}
	if shouldPaceOutput(f) || len(t.outputQueue) > 0 {
		wasEmpty := len(t.outputQueue) == 0
		t.outputQueue = append(t.outputQueue, queuedSend(job))
		if wasEmpty {
			t.notifyOutputPacerLocked()
		}
		t.mu.Unlock()
		t.outboundMu.Unlock()
		unlockOutbound = false
		if async {
			return nil
		}
		return t.waitQueuedSend(done)
	}
	t.mu.Unlock()
	if err := t.queueDataJob(job); err != nil {
		t.removePending(seq, reliable)
		return err
	}
	t.outboundMu.Unlock()
	unlockOutbound = false
	if async {
		return nil
	}
	return t.waitQueuedSend(done)
}

func (t *Transport) queueDataJob(job dataSendJob) error {
	select {
	case t.dataSend <- job:
		return nil
	case <-t.done:
		return t.closedError()
	}
}

func (t *Transport) waitQueuedSend(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-t.done:
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return err
		}
		return errors.New("dgram: closed")
	}
}

func (t *Transport) lockOutboundSlot(reliable bool) error {
	var pendingWaitStarted time.Time
	for {
		t.outboundMu.Lock()
		t.mu.Lock()
		if t.closed {
			err := t.closeErr
			t.mu.Unlock()
			t.outboundMu.Unlock()
			if err != nil {
				return err
			}
			return errors.New("dgram: closed")
		}
		if !reliable || len(t.pending) < t.maxPending {
			return nil
		}
		if t.linkState != ports.LinkStateConnected {
			t.mu.Unlock()
			t.outboundMu.Unlock()
			return ErrPendingFull
		}
		if pendingWaitStarted.IsZero() {
			pendingWaitStarted = t.clock.Now()
		}
		remaining := t.maxPendingWait - t.clock.Now().Sub(pendingWaitStarted)
		if remaining <= 0 {
			t.mu.Unlock()
			t.outboundMu.Unlock()
			return ErrPendingFull
		}
		wake := t.sendWake
		timer := t.clock.NewTimer(remaining)
		t.mu.Unlock()
		t.outboundMu.Unlock()
		select {
		case <-timer.C():
		case <-wake:
		case <-t.done:
		}
		timer.Stop()
	}
}

func (t *Transport) removePending(seq uint64, reliable bool) {
	if !reliable {
		return
	}
	t.mu.Lock()
	removed := t.removePendingLocked(seq)
	if removed {
		t.notifySendWaitersLocked()
	}
	t.mu.Unlock()
}

func (t *Transport) removeQueuedPending(queued []queuedSend, err error) {
	t.mu.Lock()
	removed := false
	for _, q := range queued {
		if q.reliable {
			removed = t.removePendingLocked(q.seq) || removed
		}
		if q.done != nil {
			q.done <- err
			close(q.done)
		}
	}
	if removed {
		t.notifySendWaitersLocked()
	}
	t.mu.Unlock()
}

func (t *Transport) removePendingLocked(seq uint64) bool {
	if _, ok := t.pending[seq]; !ok {
		return false
	}
	delete(t.pending, seq)
	if len(t.pending) == 0 {
		t.health.pendingCleared()
	}
	return true
}

func (t *Transport) writePacket(pkt []byte, peer net.Addr) error {
	t.writeMu.Lock()
	defer t.writeMu.Unlock()

	t.mu.Lock()
	pc := t.pc
	writeTimeout := t.writeTimeout
	writeDeadlines := t.writeDeadlines
	t.mu.Unlock()

	return t.writeDatagram(pc, peer, pkt, writeDeadlines, writeTimeout)
}

func (t *Transport) writeDatagram(pc net.PacketConn, peer net.Addr, pkt []byte, deadlines *writeDeadlineState, timeout time.Duration) error {
	if timeout <= 0 {
		_, err := pc.WriteTo(pkt, peer)
		return err
	}
	ownDeadline := t.clock.Now().Add(timeout)
	finish, err := deadlines.begin(ownDeadline)
	if err != nil {
		return err
	}
	defer finish()
	for {
		_, err = pc.WriteTo(pkt, peer)
		if err == nil {
			return nil
		}
		var timeoutErr interface{ Timeout() bool }
		now := t.clock.Now()
		if !errors.As(err, &timeoutErr) || !timeoutErr.Timeout() || !now.Before(ownDeadline) {
			return err
		}
		deadlines.expire(now)
	}
}

func (t *Transport) nextCounter() uint64 { t.mu.Lock(); defer t.mu.Unlock(); t.ctr++; return t.ctr }

func (t *Transport) notifySendWaitersLocked() {
	if t.sendWake == nil {
		return
	}
	close(t.sendWake)
	t.sendWake = make(chan struct{})
}
