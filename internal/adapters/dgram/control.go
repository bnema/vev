package dgram

import (
	"encoding/binary"
	"errors"
	"net"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

const (
	controlQueueSize = 16
	maxACKDelay      = 25 * time.Millisecond
)

type controlRecord struct {
	kind   byte
	id     uint64
	result chan<- error
}

// queueACK retains the greatest cumulative acknowledgement without ever making
// packet receipt wait for a socket write.
func (t *Transport) queueACK(seq uint64) {
	t.controlMu.Lock()
	if !t.ackQueued || seq > t.ackSeq {
		t.ackSeq = seq
		t.ackQueued = true
	}
	t.controlMu.Unlock()
	if t.ackWake == nil {
		// Small unit-test transports may exercise record handling without the
		// goroutines installed by NewTransportWithOptions.
		go t.sendAck(seq)
		return
	}
	select {
	case t.ackWake <- struct{}{}:
		t.mu.Lock()
		hook := t.afterACKWakeAccepted
		t.mu.Unlock()
		if hook != nil {
			hook()
		}
	default:
	}
}

// queueControl is deliberately lossy when the bounded control queue is full:
// probes and pongs are advisory and must not stall authenticated receipt.
func (t *Transport) queueControl(kind byte, id uint64) bool {
	return t.queueControlRecord(controlRecord{kind: kind, id: id})
}

func (t *Transport) queueControlRecord(record controlRecord) bool {
	select {
	case t.control <- record:
		return true
	default:
		return false
	}
}

func (t *Transport) takeACK() (uint64, bool) {
	t.controlMu.Lock()
	defer t.controlMu.Unlock()
	seq, ok := t.ackSeq, t.ackQueued
	t.ackSeq = 0
	t.ackQueued = false
	return seq, ok
}

func (t *Transport) controlLoop() {
	go t.ackScheduleLoop()
	for {
		select {
		case record := <-t.control:
			err := t.sendControl(record.kind, record.id)
			if record.result != nil {
				select {
				case record.result <- err:
				default:
				}
			}
		case <-t.done:
			return
		}
	}
}

func (t *Transport) ackScheduleLoop() {
	var ackTimer ports.Timer
	var ackTimerC <-chan time.Time
	defer func() {
		if ackTimer != nil {
			ackTimer.Stop()
		}
	}()
	scheduleACK := func() {
		if ackTimerC == nil {
			if ackTimer == nil {
				ackTimer = t.clock.NewTimer(maxACKDelay)
			} else {
				ackTimer.Reset(maxACKDelay)
			}
			ackTimerC = ackTimer.C()
		}
		t.mu.Lock()
		hook := t.afterACKScheduled
		t.mu.Unlock()
		if hook != nil {
			hook()
		}
	}
	dispatchACK := func() {
		seq, ok := t.takeACK()
		if !ok {
			return
		}
		select {
		case t.ackSend <- seq:
			t.mu.Lock()
			hook := t.afterACKDispatched
			t.mu.Unlock()
			if hook != nil {
				hook()
			}
		default:
			// Preserve the cumulative maximum until the bounded ACK sender has
			// capacity again.
			t.queueACK(seq)
		}
	}

	for {
		// Check ACK scheduling explicitly so a sustained stream of advisory
		// controls cannot prevent the bounded coalescing deadline from starting
		// or expiring. Dispatch itself never writes to the socket.
		select {
		case <-ackTimerC:
			ackTimerC = nil
			dispatchACK()
			continue
		default:
		}
		select {
		case <-t.ackWake:
			scheduleACK()
			continue
		default:
		}

		select {
		case <-t.ackWake:
			scheduleACK()
		case <-ackTimerC:
			ackTimerC = nil
			dispatchACK()
		case <-t.done:
			return
		}
	}
}

func (t *Transport) ackSendLoop() {
	for {
		select {
		case seq := <-t.ackSend:
			_ = t.sendControl(recAck, seq)
		case <-t.done:
			return
		}
	}
}

func (t *Transport) sendControl(kind byte, id uint64) error {
	var payload [9]byte
	payload[0] = kind
	binary.BigEndian.PutUint64(payload[1:], id)
	frags, err := pdgram.FragmentPayload(t.nextCounter(), payload[:], t.mtu-pdgram.HeaderSize-t.codec.Overhead())
	if err != nil {
		return err
	}
	for _, frag := range frags {
		raw, err := pdgram.MarshalFragment(frag)
		if err != nil {
			return err
		}
		pkt := t.codec.Seal(t.sendDir, t.nextCounter(), raw, nil)
		t.mu.Lock()
		pc := t.pc
		peer := t.peer
		closed := t.closed
		closeErr := t.closeErr
		writeTimeout := t.writeTimeout
		writeDeadlines := t.writeDeadlines
		t.mu.Unlock()
		if closed {
			if closeErr != nil {
				return closeErr
			}
			return net.ErrClosed
		}
		if peer == nil {
			return errors.New("dgram: control peer unavailable")
		}
		// PacketConn permits concurrent callers. Control traffic deliberately
		// bypasses data's write lock so a retransmit cannot starve heartbeats.
		if err := t.writeDatagram(pc, peer, pkt, writeDeadlines, writeTimeout); err != nil {
			return err
		}
	}
	return nil
}
