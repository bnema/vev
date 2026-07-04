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
	recData       byte = 1
	recAck        byte = 2
	defaultResend      = 40 * time.Millisecond
)

type Transport struct {
	pc               net.PacketConn
	codec            *pdgram.Codec
	sendDir, recvDir uint32
	mtu              int

	mu      sync.Mutex
	peer    net.Addr
	ctr     uint64
	seq     uint64
	pending map[uint64]*pending
	closed  bool

	replay       *pdgram.ReplayWindow
	reasm        *pdgram.Reassembler
	delivered    map[uint64]struct{}
	deliveredMax uint64
	in           chan ports.Frame
	done         chan struct{}
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
	t := &Transport{pc: pc, codec: c, sendDir: sendDir, recvDir: recvDir, mtu: pdgram.DefaultMTU, peer: peer, pending: make(map[uint64]*pending), replay: pdgram.NewReplayWindow(), reasm: pdgram.NewReassembler(), delivered: make(map[uint64]struct{}), in: make(chan ports.Frame, 32), done: make(chan struct{})}
	go t.readLoop()
	go t.resendLoop()
	return t, nil
}

func (t *Transport) Send(f ports.Frame) error {
	// Until full ACK-driven terminal state sync exists, every frame is reliable;
	// dropping MsgOutput can permanently desynchronize the client screen.
	reliable := true
	t.mu.Lock()
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
		return ports.Frame{}, errors.New("dgram: closed")
	}
}
func (t *Transport) Close() error {
	t.mu.Lock()
	if !t.closed {
		t.closed = true
		close(t.done)
	}
	t.mu.Unlock()
	return t.pc.Close()
}

func (t *Transport) Peer() net.Addr { t.mu.Lock(); defer t.mu.Unlock(); return t.peer }

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
		t.mu.Unlock()
		if closed {
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
			return
		}
		_, pt, err := t.codec.Open(append([]byte(nil), buf[:n]...), t.recvDir, nil, t.replay)
		if err != nil {
			continue
		}
		t.mu.Lock()
		t.peer = addr
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
	case recData:
		seq, reliable, f, ok := decodeData(p)
		if !ok {
			return
		}
		if reliable {
			t.sendAck(seq)
			if !t.markReliableDelivered(seq) {
				return
			}
		}
		go t.deliver(f)
	}
}

func (t *Transport) deliver(f ports.Frame) {
	select {
	case t.in <- f:
	case <-t.done:
	}
}

func (t *Transport) markReliableDelivered(seq uint64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.delivered[seq]; ok {
		return false
	}
	t.delivered[seq] = struct{}{}
	if seq > t.deliveredMax {
		t.deliveredMax = seq
	}
	const window = 1024
	if t.deliveredMax > window {
		cutoff := t.deliveredMax - window
		for s := range t.delivered {
			if s < cutoff {
				delete(t.delivered, s)
			}
		}
	}
	return true
}

func (t *Transport) resendLoop() {
	tick := time.NewTicker(defaultResend)
	defer tick.Stop()
	for {
		select {
		case <-tick.C:
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
		case <-t.done:
			return
		}
	}
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

type Dialer struct {
	PC               net.PacketConn
	Peer             net.Addr
	Key              []byte
	SendDir, RecvDir uint32
}

func (d Dialer) Dial(ctx context.Context) (ports.Transport, error) {
	return NewTransport(d.PC, d.Peer, d.Key, d.SendDir, d.RecvDir)
}
