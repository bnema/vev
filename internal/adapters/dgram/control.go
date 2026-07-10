package dgram

import (
	"encoding/binary"
	"runtime"

	pdgram "github.com/bnema/vev/pkg/dgram"
)

const controlQueueSize = 16

type controlRecord struct {
	kind byte
	id   uint64
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
	default:
	}
	// Let the dedicated writer claim queued control before delivery wakes a
	// consumer; this remains a non-blocking receive-path operation.
	runtime.Gosched()
}

// queueControl is deliberately lossy when the bounded control queue is full:
// probes and pongs are advisory and must not stall authenticated receipt.
func (t *Transport) queueControl(kind byte, id uint64) bool {
	select {
	case t.control <- controlRecord{kind: kind, id: id}:
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
	for {
		// Probes and pongs always win over an ACK that is ready to send.
		select {
		case record := <-t.control:
			t.sendControl(record.kind, record.id)
			continue
		default:
		}

		select {
		case record := <-t.control:
			t.sendControl(record.kind, record.id)
		case <-t.ackWake:
			if seq, ok := t.takeACK(); ok {
				t.sendControl(recAck, seq)
			}
		case <-t.done:
			return
		}
	}
}

func (t *Transport) sendControl(kind byte, id uint64) {
	var payload [9]byte
	payload[0] = kind
	binary.BigEndian.PutUint64(payload[1:], id)
	frags, err := pdgram.FragmentPayload(t.nextCounter(), payload[:], t.mtu-pdgram.HeaderSize-t.codec.Overhead())
	if err != nil {
		return
	}
	for _, frag := range frags {
		raw, err := pdgram.MarshalFragment(frag)
		if err != nil {
			return
		}
		pkt := t.codec.Seal(t.sendDir, t.nextCounter(), raw, nil)
		t.mu.Lock()
		pc := t.pc
		peer := t.peer
		closed := t.closed
		t.mu.Unlock()
		if closed || peer == nil {
			return
		}
		// PacketConn permits concurrent callers. Control traffic deliberately
		// bypasses data's write lock so a retransmit cannot starve heartbeats.
		if _, err := pc.WriteTo(pkt, peer); err != nil {
			return
		}
	}
}
