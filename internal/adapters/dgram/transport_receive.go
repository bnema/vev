package dgram

import (
	"encoding/binary"
	"net"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

func (t *Transport) readLoop(pc net.PacketConn) {
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := pc.ReadFrom(buf)
		if err != nil {
			t.mu.Lock()
			current := t.pc == pc
			closed := t.closed
			t.mu.Unlock()
			if current && !closed {
				t.closeWithError(err)
			}
			return
		}
		t.recvMu.Lock()
		counter, pt, err := t.codec.Open(append([]byte(nil), buf[:n]...), t.recvDir, nil, t.replay)
		if err != nil {
			t.recvMu.Unlock()
			continue
		}
		frag, err := pdgram.UnmarshalFragment(pt)
		if err != nil {
			t.recvMu.Unlock()
			t.notifyMalformedFragment()
			continue
		}
		payload, complete, err := t.reasm.Add(frag)
		inflight := t.reasm.Inflight()
		// Keep the diagnostic mirror ordered with reassembly mutations. Rebind can
		// overlap old and new read loops, so recvMu must remain held through commit.
		t.mu.Lock()
		t.reassemblyInflight = inflight
		var afterAuthenticated func()
		if err == nil {
			now := t.clock.Now()
			t.health.authenticatedPacket(now)
			t.lastAuthenticatedPacket = now
			if !t.peerCounterSet || counter > t.peerCounter {
				t.peer = addr
				t.peerCounter = counter
				t.peerCounterSet = true
			}
			afterAuthenticated = t.afterAuthenticatedPacket
		}
		t.mu.Unlock()
		t.recvMu.Unlock()
		if afterAuthenticated != nil {
			afterAuthenticated()
		}
		if err != nil {
			t.notifyMalformedFragment()
			t.notifyPacketProcessed()
			continue
		}
		if !complete {
			t.notifyPacketProcessed()
			continue
		}
		t.recordCompleteRecord()
		t.handleRecord(payload)
		t.checkSilence()
		t.notifyPacketProcessed()
	}
}

func (t *Transport) notifyPacketProcessed() {
	t.mu.Lock()
	afterPacketProcessed := t.afterPacketProcessed
	t.mu.Unlock()
	if afterPacketProcessed != nil {
		afterPacketProcessed()
	}
}

func (t *Transport) notifyMalformedFragment() {
	t.mu.Lock()
	afterMalformed := t.afterMalformedFragment
	t.mu.Unlock()
	if afterMalformed != nil {
		afterMalformed()
	}
}

func (t *Transport) recordCompleteRecord() {
	t.mu.Lock()
	now := t.clock.Now()
	t.health.completeRecord(now)
	t.lastCompleteRecord = now
	t.mu.Unlock()
}

func (t *Transport) handleRecord(p []byte) {
	if len(p) < 1 {
		return
	}
	switch p[0] {
	case recAck:
		if len(p) != 9 {
			return
		}
		seq := binary.BigEndian.Uint64(p[1:])
		t.mu.Lock()
		acked := false
		ackedBytes := 0
		for pendingSeq, p := range t.pending {
			if pendingSeq > seq {
				continue
			}
			if pendingSeq == seq && p != nil && !p.retransmitted {
				t.updateRTTLocked(t.clock.Now().Sub(p.first))
			}
			if p != nil {
				ackedBytes += p.wireBytes
			}
			delete(t.pending, pendingSeq)
			acked = true
		}
		if acked {
			t.congestion.onACK(ackedBytes)
			now := t.clock.Now()
			t.health.ackProgress(now)
			if len(t.pending) == 0 {
				t.health.pendingCleared()
			}
			t.lastACKProgress = now
			t.notifySendWaitersLocked()
		}
		t.mu.Unlock()
		if acked {
			t.setLinkState(ports.LinkStateConnected, nil)
			t.emitDiagnostic()
		}
	case recProbe:
		if len(p) != 9 {
			return
		}
		t.queueControl(recPong, binary.BigEndian.Uint64(p[1:]))
	case recPong:
		if len(p) != 9 {
			return
		}
		id := binary.BigEndian.Uint64(p[1:])
		t.mu.Lock()
		ch := t.probeWait[id]
		if ch != nil {
			delete(t.probeWait, id)
			close(ch)
		}
		t.mu.Unlock()
	case recData:
		seq, reliable, f, ok := decodeData(p)
		if !ok {
			return
		}
		if reliable {
			ackSeq, ack, queued := t.enqueueReliable(seq, f)
			if ack {
				t.queueACK(ackSeq)
			}
			if queued {
				t.signalDelivery()
			}
			return
		}
		t.deliver(f)
	}
}

func (t *Transport) enqueueReliable(seq uint64, f wire.Frame) (ackSeq uint64, ack bool, queued bool) {
	t.deliverMu.Lock()
	defer t.deliverMu.Unlock()
	if seq < t.nextRecvSeq {
		return t.highestContiguousRecvLocked(), true, false
	}
	if _, exists := t.recvBuf[seq]; exists {
		return t.highestContiguousRecvLocked(), true, false
	}
	if len(t.recvBuf) >= t.maxRecvBuffer && seq != t.nextRecvSeq {
		return 0, false, false
	}
	t.recvBuf[seq] = f
	return t.highestContiguousRecvLocked(), true, true
}

func (t *Transport) highestContiguousRecvLocked() uint64 {
	seq := t.nextRecvSeq
	for {
		if _, ok := t.recvBuf[seq]; ok {
			seq++
			continue
		}
		break
	}
	return seq - 1
}

func (t *Transport) signalDelivery() {
	t.deliverMu.Lock()
	t.deliverCond.Signal()
	t.deliverMu.Unlock()
}

func (t *Transport) deliveryLoop() {
	for {
		t.deliverMu.Lock()
		f, ok := t.recvBuf[t.nextRecvSeq]
		for !ok {
			if t.isClosed() {
				t.deliverMu.Unlock()
				return
			}
			t.deliverCond.Wait()
			f, ok = t.recvBuf[t.nextRecvSeq]
		}
		delete(t.recvBuf, t.nextRecvSeq)
		t.nextRecvSeq++
		t.deliverMu.Unlock()
		t.deliver(f)
	}
}

func (t *Transport) isClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

func (t *Transport) deliver(f wire.Frame) {
	select {
	case t.in <- f:
	case <-t.done:
	}
}

func encodeData(seq uint64, reliable bool, f wire.Frame) []byte {
	b := make([]byte, dataRecordHeaderSize+len(f.Payload))
	b[0] = recData
	binary.BigEndian.PutUint64(b[1:9], seq)
	if reliable {
		b[9] = 1
	}
	b[10] = byte(f.Type)
	copy(b[dataRecordHeaderSize:], f.Payload)
	return b
}
func decodeData(b []byte) (uint64, bool, wire.Frame, bool) {
	if len(b) < dataRecordHeaderSize || b[0] != recData {
		return 0, false, wire.Frame{}, false
	}
	if b[11] != 0 {
		return 0, false, wire.Frame{}, false
	}
	return binary.BigEndian.Uint64(b[1:9]), b[9] == 1, wire.Frame{Type: wire.MsgType(b[10]), Payload: append([]byte(nil), b[dataRecordHeaderSize:]...)}, true
}
