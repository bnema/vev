package dgram

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	pdgram "github.com/bnema/vev/pkg/dgram"
)

type testAddr string

func (a testAddr) Network() string { return "mem" }
func (a testAddr) String() string  { return string(a) }

type packet struct {
	b    []byte
	addr net.Addr
}

// packetMeta is the deterministic simulator's view of a datagram before it is
// accepted by the link. It deliberately contains only link-level information.
type packetMeta struct {
	From, To   net.Addr
	Bytes      int
	At         time.Time
	QueueBytes int
	Retransmit bool
}

// packetPolicy makes loss and constrained links explicit in transport tests.
// A zero value delivers immediately, preserving newPair's historical behavior.
type packetPolicy struct {
	Drop           func(packetMeta) bool
	Delay          time.Duration
	MaxQueueBytes  int
	BytesPerSecond int
}

type scheduledPacket struct {
	packet
	due time.Time
}

// simulatedLink has a bounded byte queue. Tests advance its clock explicitly;
// it never starts a goroutine or relies on wall-clock time.
type simulatedLink struct {
	mu         sync.Mutex
	clock      interface{ Now() time.Time }
	policy     packetPolicy
	queue      []scheduledPacket
	queueBytes int
	next       time.Time
}

func newSimulatedLink(clock interface{ Now() time.Time }, policy packetPolicy) *simulatedLink {
	return &simulatedLink{clock: clock, policy: policy}
}

func (l *simulatedLink) enqueue(p *fakePC, peer *fakePC, b []byte) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	meta := packetMeta{From: p.addr, To: peer.addr, Bytes: len(b), At: l.clock.Now(), QueueBytes: l.queueBytes}
	if l.policy.Drop != nil && l.policy.Drop(meta) {
		return false
	}
	if l.policy.Delay == 0 && l.policy.BytesPerSecond == 0 {
		return l.deliverLocked(peer, packet{b: append([]byte(nil), b...), addr: p.addr})
	}
	if l.policy.MaxQueueBytes <= 0 || l.queueBytes+len(b) > l.policy.MaxQueueBytes {
		return false
	}
	due := meta.At.Add(l.policy.Delay)
	if l.policy.BytesPerSecond > 0 {
		if due.Before(l.next) {
			due = l.next
		}
		ns := int64(time.Second) * int64(len(b)) / int64(l.policy.BytesPerSecond)
		if ns == 0 {
			ns = 1
		}
		l.next = due.Add(time.Duration(ns))
	}
	l.queue = append(l.queue, scheduledPacket{packet: packet{b: append([]byte(nil), b...), addr: p.addr}, due: due})
	l.queueBytes += len(b)
	return true
}

func (l *simulatedLink) flush(peer *fakePC) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	for len(l.queue) > 0 && !l.queue[0].due.After(now) {
		q := l.queue[0]
		l.queue = l.queue[1:]
		l.queueBytes -= len(q.b)
		l.deliverLocked(peer, q.packet)
	}
}

func (l *simulatedLink) queuedBytes() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queueBytes
}

func (l *simulatedLink) deliverLocked(peer *fakePC, q packet) bool {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.closed {
		return false
	}
	select {
	case peer.in <- q:
		return true
	default:
		// The endpoint queue is intentionally bounded just like the link queue.
		return false
	}
}

type fakePC struct {
	addr       net.Addr
	mu         sync.Mutex
	closed     bool
	in         chan packet
	peers      map[string]*fakePC
	drop       func([]byte, net.Addr) bool
	afterWrite func()
	link       *simulatedLink
}

func newPair() (*fakePC, *fakePC) { return newPairWithLink(nil) }

func newPairWithLink(link *simulatedLink) (*fakePC, *fakePC) {
	a := &fakePC{addr: testAddr("a"), in: make(chan packet, 100), peers: map[string]*fakePC{}, link: link}
	b := &fakePC{addr: testAddr("b"), in: make(chan packet, 100), peers: map[string]*fakePC{}, link: link}
	a.peers["b"] = b
	b.peers["a"] = a
	return a, b
}
func (p *fakePC) ReadFrom(b []byte) (int, net.Addr, error) {
	q, ok := <-p.in
	if !ok {
		return 0, nil, errors.New("closed")
	}
	copy(b, q.b)
	return len(q.b), q.addr, nil
}
func (p *fakePC) WriteTo(b []byte, addr net.Addr) (int, error) {
	p.mu.Lock()
	closed := p.closed
	p.mu.Unlock()
	if closed {
		return 0, errors.New("closed")
	}
	if p.drop != nil && p.drop(b, addr) {
		return len(b), nil
	}
	peer := p.peers[addr.String()]
	if peer == nil {
		return 0, errors.New("unknown peer")
	}
	if p.link != nil {
		p.link.enqueue(p, peer, b)
	} else {
		peer.mu.Lock()
		if peer.closed {
			peer.mu.Unlock()
			return 0, errors.New("peer closed")
		}
		peer.in <- packet{append([]byte(nil), b...), p.addr}
		peer.mu.Unlock()
	}
	if p.afterWrite != nil {
		p.afterWrite()
	}
	return len(b), nil
}
func (p *fakePC) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.in)
	}
	return nil
}
func (p *fakePC) LocalAddr() net.Addr              { return p.addr }
func (p *fakePC) SetDeadline(time.Time) error      { return nil }
func (p *fakePC) SetReadDeadline(time.Time) error  { return nil }
func (p *fakePC) SetWriteDeadline(time.Time) error { return nil }

func key() []byte { return bytes.Repeat([]byte{1}, pdgram.KeySize) }

type floodMetrics struct {
	packetContact   int
	completedRecord int
	ackProgress     int
	queueBytes      int
	retransmits     int
}

// TestTransportFloodClassification documents the three distinct symptoms that
// used to be conflated as a healthy UDP link: authenticated datagrams arriving,
// complete records arriving, and ACKs retiring sender work.
func TestTransportFloodClassification(t *testing.T) {
	const records = 32
	payload := make([]byte, 24*pdgram.DefaultMTU)
	fragments, err := pdgram.FragmentPayload(1, payload, pdgram.DefaultMTU)
	if err != nil {
		t.Fatal(err)
	}
	if len(fragments) < 24 {
		t.Fatalf("fragments=%d, want at least 24", len(fragments))
	}
	packets := make([][]byte, len(fragments))
	for i, fragment := range fragments {
		packets[i], err = pdgram.MarshalFragment(fragment)
		if err != nil {
			t.Fatal(err)
		}
	}

	tests := []struct {
		name  string
		run   func(*manualClock) floodMetrics
		check func(*testing.T, floodMetrics)
	}{
		{
			name: "one fragment per record loss has packet contact without record or ACK progress",
			run: func(clk *manualClock) floodMetrics {
				attempt := 0
				link := newSimulatedLink(clk, packetPolicy{Drop: func(packetMeta) bool {
					drop := attempt%len(fragments) == len(fragments)-1
					attempt++
					return drop
				}})
				a, b := newPairWithLink(link)
				m := floodMetrics{}
				for range records {
					delivered := 0
					for _, fragment := range packets {
						_, _ = a.WriteTo(fragment, b.addr)
						select {
						case <-b.in:
							delivered++
							m.packetContact++
						default:
						}
					}
					if delivered == len(fragments) {
						m.completedRecord++
						m.ackProgress++
					}
				}
				return m
			},
			check: func(t *testing.T, m floodMetrics) {
				t.Helper()
				if m.packetContact == 0 || m.completedRecord != 0 || m.ackProgress != 0 {
					t.Fatalf("metrics=%+v, want packet contact only", m)
				}
			},
		},
		{
			name: "bounded bandwidth queue has no false progress and stays bounded",
			run: func(clk *manualClock) floodMetrics {
				limit := 2 * pdgram.DefaultMTU
				link := newSimulatedLink(clk, packetPolicy{Delay: time.Second, MaxQueueBytes: limit, BytesPerSecond: pdgram.DefaultMTU})
				a, b := newPairWithLink(link)
				for range records {
					for _, fragment := range packets {
						_, _ = a.WriteTo(fragment, b.addr)
					}
				}
				return floodMetrics{queueBytes: link.queuedBytes()}
			},
			check: func(t *testing.T, m floodMetrics) {
				t.Helper()
				if m.packetContact != 0 || m.completedRecord != 0 || m.ackProgress != 0 || m.queueBytes <= 0 || m.queueBytes > 2*pdgram.DefaultMTU {
					t.Fatalf("metrics=%+v, want bounded queued bytes without progress", m)
				}
			},
		},
		{
			name: "retransmit storm has record progress but no ACK progress",
			run: func(clk *manualClock) floodMetrics {
				link := newSimulatedLink(clk, packetPolicy{})
				a, b := newPairWithLink(link)
				m := floodMetrics{}
				for range records {
					for attempt := 0; attempt < 3; attempt++ {
						for _, fragment := range packets {
							_, _ = a.WriteTo(fragment, b.addr)
							<-b.in
							m.packetContact++
						}
						if attempt == 0 {
							m.completedRecord++
						} else {
							m.retransmits++
						}
					}
				}
				// ACK packets are intentionally lost in this scenario.
				return m
			},
			check: func(t *testing.T, m floodMetrics) {
				t.Helper()
				if m.packetContact == 0 || m.completedRecord != records || m.ackProgress != 0 || m.retransmits == 0 {
					t.Fatalf("metrics=%+v, want record contact with retransmit-only pressure", m)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
			metrics := tt.run(clk)
			tt.check(t, metrics)
		})
	}
}
