// Package dgram adapts authenticated UDP-style packet connections to ports.Transport.
package dgram

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

const (
	recData  byte = 1
	recAck   byte = 2
	recProbe byte = 3
	recPong  byte = 4

	defaultResend         = 40 * time.Millisecond
	defaultHeartbeat      = 3 * time.Second
	defaultSilenceTimeout = 10 * time.Second
	maxPendingReliable    = 1024
	maxRecvBuffer         = 1024
)

var (
	ErrPendingFull = errors.New("dgram: pending reliable queue full")
	ErrLinkDead    = errors.New("dgram: link dead")
)

type Transport struct {
	pc               net.PacketConn
	codec            *pdgram.Codec
	sendDir, recvDir uint32
	mtu              int

	mu        sync.Mutex
	peer      net.Addr
	ctr       uint64
	seq       uint64
	probeSeq  uint64
	pending   map[uint64]*pending
	closed    bool
	closeErr  error
	lastHeard time.Time
	heartbeat time.Duration
	silence   time.Duration
	probeWait map[uint64]chan struct{}

	replay *pdgram.ReplayWindow
	reasm  *pdgram.Reassembler
	in     chan ports.Frame
	done   chan struct{}

	deliverMu   sync.Mutex
	deliverCond *sync.Cond
	nextRecvSeq uint64
	recvBuf     map[uint64]ports.Frame
}

type pending struct {
	frame ports.Frame
	last  time.Time
}

func NewTransport(pc net.PacketConn, peer net.Addr, key []byte, sendDir, recvDir uint32) (*Transport, error) {
	c, err := pdgram.NewCodec(key)
	if err != nil {
		return nil, err
	}
	t := &Transport{pc: pc, codec: c, sendDir: sendDir, recvDir: recvDir, mtu: pdgram.DefaultMTU, peer: peer, pending: make(map[uint64]*pending), replay: pdgram.NewReplayWindow(), reasm: pdgram.NewReassembler(), in: make(chan ports.Frame, 32), done: make(chan struct{}), lastHeard: time.Now(), heartbeat: defaultHeartbeat, silence: defaultSilenceTimeout, probeWait: make(map[uint64]chan struct{}), nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}
	t.deliverCond = sync.NewCond(&t.deliverMu)
	go t.readLoop()
	go t.resendLoop()
	go t.deliveryLoop()
	return t, nil
}

func (t *Transport) Send(f ports.Frame) error {
	// Until full ACK-driven terminal state sync exists, every frame is reliable;
	// dropping MsgOutput can permanently desynchronize the client screen.
	reliable := true
	t.mu.Lock()
	if t.closed {
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return err
		}
		return errors.New("dgram: closed")
	}
	if reliable && len(t.pending) >= maxPendingReliable {
		t.mu.Unlock()
		return ErrPendingFull
	}
	t.seq++
	seq := t.seq
	if reliable {
		t.pending[seq] = &pending{frame: f}
	}
	t.mu.Unlock()
	return t.sendData(seq, reliable, f)
}

func (t *Transport) Recv() (ports.Frame, error) {
	select {
	case f := <-t.in:
		return f, nil
	case <-t.done:
		t.mu.Lock()
		err := t.closeErr
		t.mu.Unlock()
		if err != nil {
			return ports.Frame{}, err
		}
		return ports.Frame{}, errors.New("dgram: closed")
	}
}
func (t *Transport) Close() error {
	t.closeWithError(errors.New("dgram: closed"))
	return t.pc.Close()
}

func (t *Transport) Peer() net.Addr { t.mu.Lock(); defer t.mu.Unlock(); return t.peer }

// Probe sends an authenticated datagram and waits for the peer to authenticate a
// response or for ctx to expire. It is intended for UDP bootstrap/fallback checks.
func (t *Transport) Probe(ctx context.Context) error {
	id, ch, err := t.registerProbe()
	if err != nil {
		return err
	}
	defer t.unregisterProbe(id)
	if err := t.sendProbe(recProbe, id); err != nil {
		return err
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
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

func (t *Transport) registerProbe() (uint64, chan struct{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		if t.closeErr != nil {
			return 0, nil, t.closeErr
		}
		return 0, nil, errors.New("dgram: closed")
	}
	t.probeSeq++
	id := t.probeSeq
	ch := make(chan struct{})
	t.probeWait[id] = ch
	return id, ch, nil
}

func (t *Transport) unregisterProbe(id uint64) {
	t.mu.Lock()
	delete(t.probeWait, id)
	t.mu.Unlock()
}

func (t *Transport) sendData(seq uint64, reliable bool, f ports.Frame) error {
	p := encodeData(seq, reliable, f)
	return t.sendPayload(p)
}
func (t *Transport) sendAck(seq uint64) {
	var b [9]byte
	b[0] = recAck
	binary.BigEndian.PutUint64(b[1:], seq)
	_ = t.sendPayload(b[:])
}
func (t *Transport) sendProbe(kind byte, id uint64) error {
	var b [9]byte
	b[0] = kind
	binary.BigEndian.PutUint64(b[1:], id)
	return t.sendPayload(b[:])
}
func (t *Transport) sendPayload(p []byte) error {
	frags, err := pdgram.FragmentPayload(t.nextCounter(), p, t.mtu-pdgram.HeaderSize-t.codec.Overhead())
	if err != nil {
		return err
	}
	for _, f := range frags {
		raw, err := pdgram.MarshalFragment(f)
		if err != nil {
			return err
		}
		pkt := t.codec.Seal(t.sendDir, t.nextCounter(), raw, nil)
		t.mu.Lock()
		peer := t.peer
		closed := t.closed
		closeErr := t.closeErr
		t.mu.Unlock()
		if closed {
			if closeErr != nil {
				return closeErr
			}
			return errors.New("dgram: closed")
		}
		if peer == nil {
			return errors.New("dgram: no peer")
		}
		if _, err := t.pc.WriteTo(pkt, peer); err != nil {
			return err
		}
	}
	return nil
}
func (t *Transport) nextCounter() uint64 { t.mu.Lock(); defer t.mu.Unlock(); t.ctr++; return t.ctr }

func (t *Transport) readLoop() {
	buf := make([]byte, 64*1024)
	for {
		n, addr, err := t.pc.ReadFrom(buf)
		if err != nil {
			t.closeWithError(err)
			return
		}
		_, pt, err := t.codec.Open(append([]byte(nil), buf[:n]...), t.recvDir, nil, t.replay)
		if err != nil {
			continue
		}
		t.mu.Lock()
		t.peer = addr
		t.lastHeard = time.Now()
		t.mu.Unlock()
		frag, err := pdgram.UnmarshalFragment(pt)
		if err != nil {
			continue
		}
		payload, ok, err := t.reasm.Add(frag)
		if err != nil || !ok {
			continue
		}
		t.handleRecord(payload)
	}
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
		delete(t.pending, seq)
		t.mu.Unlock()
	case recProbe:
		if len(p) != 9 {
			return
		}
		_ = t.sendProbe(recPong, binary.BigEndian.Uint64(p[1:]))
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
			if t.enqueueReliable(seq, f) {
				t.sendAck(seq)
			}
			return
		}
		t.deliver(f)
	}
}

func (t *Transport) enqueueReliable(seq uint64, f ports.Frame) bool {
	t.deliverMu.Lock()
	defer t.deliverMu.Unlock()
	if seq < t.nextRecvSeq {
		return true
	}
	if _, exists := t.recvBuf[seq]; exists {
		return true
	}
	if len(t.recvBuf) >= maxRecvBuffer {
		if seq != t.nextRecvSeq {
			return false
		}
		for bufferedSeq := range t.recvBuf {
			delete(t.recvBuf, bufferedSeq)
			break
		}
	}
	t.recvBuf[seq] = f
	t.deliverCond.Signal()
	return true
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

func (t *Transport) deliver(f ports.Frame) {
	select {
	case t.in <- f:
	case <-t.done:
	}
}

func (t *Transport) resendLoop() {
	resend := time.NewTicker(defaultResend)
	defer resend.Stop()
	heartbeat := time.NewTicker(t.heartbeat)
	defer heartbeat.Stop()
	for {
		select {
		case <-resend.C:
			t.resendPending()
			t.checkSilence()
		case <-heartbeat.C:
			_ = t.sendProbe(recProbe, 0)
			t.checkSilence()
		case <-t.done:
			return
		}
	}
}

func (t *Transport) resendPending() {
	now := time.Now()
	var resend []struct {
		seq uint64
		f   ports.Frame
	}
	t.mu.Lock()
	for seq, p := range t.pending {
		if p.last.IsZero() || now.Sub(p.last) >= defaultResend {
			p.last = now
			resend = append(resend, struct {
				seq uint64
				f   ports.Frame
			}{seq, p.frame})
		}
	}
	t.mu.Unlock()
	for _, r := range resend {
		_ = t.sendData(r.seq, true, r.f)
	}
}

func (t *Transport) checkSilence() {
	t.mu.Lock()
	last := t.lastHeard
	silence := t.silence
	closed := t.closed
	t.mu.Unlock()
	if !closed && silence > 0 && time.Since(last) > silence {
		t.closeWithError(ErrLinkDead)
	}
}

func (t *Transport) closeWithError(err error) {
	t.mu.Lock()
	if t.closed {
		t.mu.Unlock()
		return
	}
	t.closed = true
	t.closeErr = err
	t.probeWait = make(map[uint64]chan struct{})
	close(t.done)
	t.mu.Unlock()
	t.deliverMu.Lock()
	t.deliverCond.Broadcast()
	t.deliverMu.Unlock()
}

func encodeData(seq uint64, reliable bool, f ports.Frame) []byte {
	b := make([]byte, 12+len(f.Payload))
	b[0] = recData
	binary.BigEndian.PutUint64(b[1:9], seq)
	if reliable {
		b[9] = 1
	}
	b[10] = byte(f.Type)
	b[11] = 0
	copy(b[12:], f.Payload)
	return b
}
func decodeData(b []byte) (uint64, bool, ports.Frame, bool) {
	if len(b) < 12 || b[0] != recData {
		return 0, false, ports.Frame{}, false
	}
	return binary.BigEndian.Uint64(b[1:9]), b[9] == 1, ports.Frame{Type: ports.MsgType(b[10]), Payload: append([]byte(nil), b[12:]...)}, true
}
