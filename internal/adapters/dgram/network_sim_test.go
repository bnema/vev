package dgram

import (
	"bytes"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
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
	read       chan struct{}
	peers      map[string]*fakePC
	drop       func([]byte, net.Addr) bool
	afterWrite func()
	link       *simulatedLink
}

func newPair() (*fakePC, *fakePC) { return newPairWithLink(nil) }

func newPairWithLink(link *simulatedLink) (*fakePC, *fakePC) {
	a := &fakePC{addr: testAddr("a"), in: make(chan packet, 100), read: make(chan struct{}, 100), peers: map[string]*fakePC{}, link: link}
	b := &fakePC{addr: testAddr("b"), in: make(chan packet, 100), read: make(chan struct{}, 100), peers: map[string]*fakePC{}, link: link}
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
	select {
	case p.read <- struct{}{}:
	default:
	}
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
		select {
		case peer.in <- packet{append([]byte(nil), b...), p.addr}:
		default:
		}
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

const fixtureWaitTimeout = time.Second

// awaitSignal gives fixture barriers a diagnostic deadline. The deadline is a
// hang guard only; tests advance behavior with the manual clock and barriers.
func awaitSignal(t *testing.T, ch <-chan struct{}, context string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(fixtureWaitTimeout):
		t.Fatalf("timed out waiting for %s", context)
	}
}

func awaitResult[T any](t *testing.T, ch <-chan T, context string) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(fixtureWaitTimeout):
		t.Fatalf("timed out waiting for %s", context)
		var zero T
		return zero
	}
}

// TestTransportFloodClassification drives authenticated datagrams through real
// transports. The metrics are transport state, not simulator bookkeeping.
func TestTransportFloodClassification(t *testing.T) {
	t.Run("one real fragment per large record loss contacts without completing", func(t *testing.T) {
		clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
		aPC, bPC := newPair()
		inspect, err := pdgram.NewCodec(key())
		if err != nil {
			t.Fatal(err)
		}
		dropped := map[uint64]bool{}
		aPC.drop = func(pkt []byte, _ net.Addr) bool {
			_, raw, err := inspect.Open(pkt, 1, nil, nil)
			if err != nil {
				return false
			}
			frag, err := pdgram.UnmarshalFragment(raw)
			if err != nil || frag.Index != frag.Count-1 || dropped[frag.Seq] {
				return false
			}
			dropped[frag.Seq] = true
			return true
		}
		a, b := newFloodTransports(t, aPC, bPC, clk, 128)
		defer closeFloodTransports(a, b)
		contacted := make(chan struct{}, 2)
		b.mu.Lock()
		b.afterAuthenticatedPacket = func() { contacted <- struct{}{} }
		b.mu.Unlock()
		start := clk.Now()
		clk.advance(time.Nanosecond)
		for range 2 {
			if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: make([]byte, 200)}); err != nil {
				t.Fatal(err)
			}
		}
		awaitSignal(t, contacted, "first authenticated fragment")
		awaitSignal(t, contacted, "second authenticated fragment")
		state := floodState(b)
		if !state.packet || state.record || floodState(a).ack {
			t.Fatalf("contact=%v complete=%v ack=%v, want contact only", state.packet, state.record, floodState(a).ack)
		}
		if !floodState(b).packet || !clk.Now().After(start) {
			t.Fatal("real authenticated fragments did not update contact")
		}
	})

	t.Run("bounded byte queue advances and releases on fake clock", func(t *testing.T) {
		clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
		limit := 2 * 128
		link := newSimulatedLink(clk, packetPolicy{Delay: time.Second, MaxQueueBytes: limit, BytesPerSecond: 128})
		aPC, bPC := newPairWithLink(link)
		a, b := newFloodTransports(t, aPC, bPC, clk, 128)
		defer closeFloodTransports(a, b)
		contacted := make(chan struct{}, 1)
		b.mu.Lock()
		b.afterAuthenticatedPacket = func() { contacted <- struct{}{} }
		b.mu.Unlock()
		a.mu.Lock()
		a.rto = time.Hour // Keep the fake-clock queue case about bandwidth, not retries.
		a.mu.Unlock()
		if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: make([]byte, 200)}); err != nil {
			t.Fatal(err)
		}
		queued := link.queuedBytes()
		if queued <= 0 || queued > limit {
			t.Fatalf("queued bytes=%d, want 1..%d", queued, limit)
		}
		clk.advance(10 * time.Second)
		link.flush(bPC)
		awaitSignal(t, contacted, "queued authenticated fragment")
		if got := link.queuedBytes(); got != 0 {
			t.Fatalf("queued bytes after fake-clock release=%d, want 0", got)
		}
		if floodState(b).record || floodState(a).ack {
			t.Fatal("dropped queued fragment produced false record or ACK progress")
		}
	})

	t.Run("control stays responsive while fresh output and large retransmits contend", func(t *testing.T) {
		clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
		aPC, bPC := newPair()
		inspect, err := pdgram.NewCodec(key())
		if err != nil {
			t.Fatal(err)
		}

		acksDropped := make(chan struct{}, 2)
		bPC.drop = func(pkt []byte, _ net.Addr) bool {
			_, raw, err := inspect.Open(pkt, 2, nil, nil)
			if err != nil {
				return false
			}
			frag, err := pdgram.UnmarshalFragment(raw)
			if err != nil || frag.Count != 1 || len(frag.Data) != 9 || frag.Data[0] != recAck {
				return false
			}
			acksDropped <- struct{}{}
			return true
		}

		retransmitBlocked := make(chan struct{}, 1)
		releaseRetransmit := make(chan struct{})
		probeSent := make(chan struct{}, 1)
		freshAtPace := make(chan struct{})
		allowFreshPace := make(chan struct{})
		freshReturned := make(chan error, 1)
		writes := make(map[uint64]int)
		var writesMu sync.Mutex
		aPC.drop = func(pkt []byte, _ net.Addr) bool {
			_, raw, err := inspect.Open(pkt, 1, nil, nil)
			if err != nil {
				return false
			}
			frag, err := pdgram.UnmarshalFragment(raw)
			if err != nil {
				return false
			}
			if frag.Count == 1 && len(frag.Data) == 9 && frag.Data[0] == recProbe {
				probeSent <- struct{}{}
				return false
			}
			if frag.Index != 0 {
				return false
			}
			seq, _, _, ok := decodeData(frag.Data)
			if !ok {
				return false
			}
			writesMu.Lock()
			writes[seq]++
			secondWrite := writes[seq] == 2
			writesMu.Unlock()
			if secondWrite {
				retransmitBlocked <- struct{}{}
				awaitSignal(t, releaseRetransmit, "retransmit release")
			}
			return false
		}

		opts := Options{Clock: clk, MTU: 128, ResendAfter: 10 * time.Millisecond, Heartbeat: 11 * time.Millisecond}
		a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, opts)
		if err != nil {
			t.Fatal(err)
		}
		b, err := NewTransportWithOptions(bPC, aPC.addr, key(), 2, 1, opts)
		if err != nil {
			_ = a.Close()
			t.Fatal(err)
		}
		defer closeFloodTransports(a, b)
		for range 6 { // Both transports install retransmit, health, and heartbeat timers.
			awaitSignal(t, clk.timerCreated, "transport timer creation")
		}

		// Two fresh, fragmented output records become overdue while their real
		// cumulative ACKs are lost. Disable only test pacing so the fixture can
		// isolate resend-loop serialization rather than pacing delay.
		a.dataPaceMu.Lock()
		a.dataPaceRemaining = 100
		a.dataPaceNext = clk.Now().Add(time.Hour)
		a.dataPaceMu.Unlock()
		for range 2 {
			a.mu.Lock()
			a.outputNext = time.Time{}
			a.mu.Unlock()
			if err := a.Send(ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 500)}); err != nil {
				t.Fatal(err)
			}
		}
		awaitSignal(t, clk.timerCreated, "ACK coalescing timer creation")
		clk.advance(maxACKDelay)
		awaitSignal(t, acksDropped, "dropped cumulative ACK")

		awaitSignal(t, retransmitBlocked, "retransmit pacing barrier")
		if got := floodState(a).retransmits; got < 2 {
			t.Fatalf("retransmits=%d, want both overdue large records selected", got)
		}

		// Start a third large output at this same fake-clock instant. The pacing
		// barrier proves its real Send reaches the shared pacing gate while the
		// retransmit owns it inside WriteTo.
		a.mu.Lock()
		a.outputNext = time.Time{}
		a.beforeDataPace = func() {
			close(freshAtPace)
			awaitSignal(t, allowFreshPace, "fresh pacing release")
		}
		a.mu.Unlock()
		go func() {
			freshReturned <- a.Send(ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 500)})
		}()
		awaitSignal(t, freshAtPace, "fresh output pacing barrier")
		a.mu.Lock()
		a.beforeDataPace = nil
		a.mu.Unlock()
		close(allowFreshPace)
		select {
		case err := <-freshReturned:
			t.Fatalf("fresh output completed while retransmit held pacing: %v", err)
		default:
		}

		awaitSignal(t, probeSent, "heartbeat probe during output contention")

		close(releaseRetransmit)
		if err := awaitResult(t, freshReturned, "fresh output completion"); err != nil {
			t.Fatal(err)
		}
		eventually(t, time.Second, func() bool {
			writesMu.Lock()
			defer writesMu.Unlock()
			return writes[1] >= 2 && writes[2] >= 2
		})
		writesMu.Lock()
		defer writesMu.Unlock()
		if writes[1] < 2 || writes[2] < 2 || writes[3] == 0 {
			t.Fatalf("writes=%v, want fresh large traffic and retransmissions for both overdue records", writes)
		}
	})
}

type floodTransportState struct {
	packet, record, ack bool
	retransmits         uint64
}

func floodState(t *Transport) floodTransportState {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.clock.Now()
	return floodTransportState{
		packet:      t.lastAuthenticatedPacket.Equal(now),
		record:      t.lastCompleteRecord.Equal(now),
		ack:         t.lastACKProgress.Equal(now),
		retransmits: t.retransmits,
	}
}

func newFloodTransports(t *testing.T, aPC, bPC *fakePC, clk *manualClock, mtu int) (*Transport, *Transport) {
	t.Helper()
	opts := Options{Clock: clk, MTU: mtu, ResendAfter: 10 * time.Millisecond, Heartbeat: time.Hour}
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, opts)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransportWithOptions(bPC, aPC.addr, key(), 2, 1, opts)
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	return a, b
}

func closeFloodTransports(a, b *Transport) {
	_ = a.Close()
	_ = b.Close()
}

func TestMalformedAuthenticatedFragmentDoesNotCountAsContact(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	a, b := newFloodTransports(t, aPC, bPC, clk, 128)
	defer closeFloodTransports(a, b)
	b.mu.Lock()
	startPacket, startRecord := b.lastAuthenticatedPacket, b.lastCompleteRecord
	b.mu.Unlock()
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	malformedRejected := make(chan struct{}, 1)
	b.mu.Lock()
	b.afterMalformedFragment = func() { malformedRejected <- struct{}{} }
	b.mu.Unlock()
	clk.advance(time.Second)
	if _, err := aPC.WriteTo(codec.Seal(1, 999, []byte("not a fragment"), nil), bPC.addr); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, malformedRejected, "malformed fragment rejection")
	b.mu.Lock()
	gotPacket, gotRecord := b.lastAuthenticatedPacket, b.lastCompleteRecord
	b.mu.Unlock()
	if gotPacket != startPacket || gotRecord != startRecord {
		t.Fatal("malformed authenticated plaintext counted as packet contact")
	}
}
