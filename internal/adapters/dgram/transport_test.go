package dgram

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
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
type fakePC struct {
	addr   net.Addr
	mu     sync.Mutex
	closed bool
	in     chan packet
	peers  map[string]*fakePC
	drop   func([]byte, net.Addr) bool
}

func newPair() (*fakePC, *fakePC) {
	a := &fakePC{addr: testAddr("a"), in: make(chan packet, 100), peers: map[string]*fakePC{}}
	b := &fakePC{addr: testAddr("b"), in: make(chan packet, 100), peers: map[string]*fakePC{}}
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
	peer.mu.Lock()
	defer peer.mu.Unlock()
	if peer.closed {
		return 0, errors.New("peer closed")
	}
	peer.in <- packet{append([]byte(nil), b...), p.addr}
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

func TestReliableDuplicateAckedButNotDeliveredTwice(t *testing.T) {
	aPC, bPC := newPair()
	var droppedAck atomic.Bool
	bPC.drop = func(_ []byte, addr net.Addr) bool {
		return addr.String() == "a" && droppedAck.CompareAndSwap(false, true)
	}
	a, err := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("close a: %v", err)
		}
	}()
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("close b: %v", err)
		}
	}()
	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("typed")}); err != nil {
		t.Fatal(err)
	}
	got := recvWithin(t, b, time.Second)
	if got.Type != ports.MsgInput || string(got.Payload) != "typed" || !droppedAck.Load() {
		t.Fatalf("got=%+v droppedAck=%v", got, droppedAck.Load())
	}
	if got, ok := recvMaybe(b, 3*defaultResend); ok {
		t.Fatalf("duplicate reliable frame delivered: %+v", got)
	}
}

func TestReliableInputRetransmitsUntilAck(t *testing.T) {
	aPC, bPC := newPair()
	var dropped atomic.Bool
	aPC.drop = func(_ []byte, addr net.Addr) bool {
		return addr.String() == "b" && dropped.CompareAndSwap(false, true)
	}
	a, err := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("close a: %v", err)
		}
	}()
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("close b: %v", err)
		}
	}()
	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("typed")}); err != nil {
		t.Fatal(err)
	}
	got := recvWithin(t, b, time.Second)
	if got.Type != ports.MsgInput || string(got.Payload) != "typed" || !dropped.Load() {
		t.Fatalf("got=%+v dropped=%v", got, dropped.Load())
	}
}

func TestAuthenticatedPeerChangeUpdatesPeerAndEmitsConnected(t *testing.T) {
	aPC, bPC := newPair()
	a, err := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("close a: %v", err)
		}
	}()
	evilPC := &fakePC{addr: testAddr("evil"), in: make(chan packet, 100), peers: map[string]*fakePC{}}
	aPC.peers["evil"] = evilPC
	_ = bPC
	old := a.Peer().String()
	a.setLinkState(ports.LinkStateOffline, nil)
	<-a.LinkEvents()
	aPC.in <- packet{[]byte("not authenticated"), testAddr("evil")}
	time.Sleep(30 * time.Millisecond)
	if a.Peer().String() != old {
		t.Fatalf("unauthenticated packet changed peer to %v", a.Peer())
	}
	c, _ := pdgram.NewCodec(key())
	rec := encodeData(7, true, ports.Frame{Type: ports.MsgPing})
	frags, _ := pdgram.FragmentPayload(9, rec, pdgram.DefaultMTU)
	raw, _ := pdgram.MarshalFragment(frags[0])
	aPC.in <- packet{c.Seal(2, 99, raw, nil), testAddr("evil")}
	time.Sleep(30 * time.Millisecond)
	if a.Peer().String() != "evil" {
		t.Fatalf("authenticated packet did not rehome: %v", a.Peer())
	}
	select {
	case ev := <-a.LinkEvents():
		if ev.State != ports.LinkStateConnected {
			t.Fatalf("event state=%v, want connected", ev.State)
		}
	case <-time.After(time.Second):
		t.Fatal("missing connected event after authenticated rehome")
	}
	select {
	case pkt := <-evilPC.in:
		if pkt.addr.String() != "a" {
			t.Fatalf("ack source=%v, want a", pkt.addr)
		}
	case <-time.After(time.Second):
		t.Fatal("missing ACK to authenticated new peer")
	}
}

func TestPortHopPreservesPendingReliableMessages(t *testing.T) {
	aPC, bPC := newPair()
	var hopped atomic.Bool
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		ResendAfter:  20 * time.Millisecond,
		OfflineAfter: time.Millisecond,
		DeadAfter:    time.Hour,
		RebindPacketConn: func(old net.PacketConn) (net.PacketConn, error) {
			newPC := &fakePC{addr: testAddr("a2"), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}
			bPC.peers["a2"] = newPC
			hopped.Store(true)
			return newPC, nil
		},
	})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("survive")}); err != nil {
		t.Fatal(err)
	}
	a.mu.Lock()
	a.lastHeard = time.Now().Add(-time.Second)
	a.mu.Unlock()
	a.checkSilence()
	if !hopped.Load() {
		t.Fatal("expected packet conn rebind")
	}
	aPC.drop = nil
	got := recvWithin(t, b, time.Second)
	if got.Type != ports.MsgInput || string(got.Payload) != "survive" {
		t.Fatalf("got=%+v, want pending reliable message", got)
	}
	select {
	case <-a.done:
		t.Fatal("transport closed during port hop")
	default:
	}
}

func TestPortHopRetriesAfterRebindFailure(t *testing.T) {
	aPC, bPC := newPair()
	var attempts atomic.Int32
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		ResendAfter:  time.Hour,
		Heartbeat:    time.Hour,
		OfflineAfter: time.Millisecond,
		DeadAfter:    time.Hour,
		RebindPacketConn: func(old net.PacketConn) (net.PacketConn, error) {
			if attempts.Add(1) == 1 {
				return nil, errors.New("temporary rebind failure")
			}
			newPC := &fakePC{addr: testAddr("a2"), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}
			bPC.peers["a2"] = newPC
			return newPC, nil
		},
	})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	a.mu.Lock()
	a.lastHeard = time.Now().Add(-time.Second)
	a.mu.Unlock()
	a.checkSilence()
	a.mu.Lock()
	firstPC := a.pc
	a.mu.Unlock()
	if attempts.Load() != 1 || firstPC != aPC {
		t.Fatalf("first failed hop attempts=%d pc=%v, want one attempt and original pc", attempts.Load(), firstPC.LocalAddr())
	}

	a.checkSilence()
	a.mu.Lock()
	secondPC := a.pc
	a.mu.Unlock()
	if attempts.Load() != 2 || secondPC == aPC {
		t.Fatalf("second hop attempts=%d pc=%v, want retry with new pc", attempts.Load(), secondPC.LocalAddr())
	}
}

func TestServerWithoutRebindDoesNotHopPorts(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{OfflineAfter: time.Millisecond, DeadAfter: time.Hour})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	a.mu.Lock()
	a.lastHeard = time.Now().Add(-time.Second)
	a.mu.Unlock()
	a.checkSilence()
	if a.pc != aPC {
		t.Fatal("server/proxy transport hopped without rebind hook")
	}
}

func TestReliableReceiveBufferDoesNotDropAckedFutureFrame(t *testing.T) {
	tr := &Transport{maxRecvBuffer: 1, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}
	ack, queued := tr.enqueueReliable(2, ports.Frame{Type: ports.MsgOutput, Payload: []byte("future")})
	if !ack || !queued {
		t.Fatalf("future enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	ack, queued = tr.enqueueReliable(1, ports.Frame{Type: ports.MsgOutput, Payload: []byte("next")})
	if !ack || !queued {
		t.Fatalf("next enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if _, ok := tr.recvBuf[2]; !ok {
		t.Fatal("acked future frame was dropped when next frame arrived")
	}
}

func TestOutputRetransmitsUntilAck(t *testing.T) {
	aPC, bPC := newPair()
	var dropped atomic.Bool
	aPC.drop = func(_ []byte, addr net.Addr) bool {
		return addr.String() == "b" && dropped.CompareAndSwap(false, true)
	}
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() {
		if err := a.Close(); err != nil {
			t.Errorf("close a: %v", err)
		}
	}()
	defer func() {
		if err := b.Close(); err != nil {
			t.Errorf("close b: %v", err)
		}
	}()
	if err := a.Send(ports.Frame{Type: ports.MsgOutput, Payload: []byte("state1")}); err != nil {
		t.Fatal(err)
	}
	got := recvWithin(t, b, time.Second)
	if got.Type != ports.MsgOutput || string(got.Payload) != "state1" || !dropped.Load() {
		t.Fatalf("got=%+v dropped=%v", got, dropped.Load())
	}
}

func TestReliableDeliveryPreservesSequenceOrder(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := 0; i < 100; i++ {
		if err := a.Send(ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 100; i++ {
		got := recvWithin(t, b, time.Second)
		if got.Type != ports.MsgOutput || len(got.Payload) != 1 || got.Payload[0] != byte(i) {
			t.Fatalf("frame %d delivered out of order: %+v", i, got)
		}
	}
}

func TestAckProcessingContinuesWhenConsumerBackpressured(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := 0; i < cap(b.in)+20; i++ {
		if err := a.Send(ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		a.mu.Lock()
		pending := len(a.pending)
		a.mu.Unlock()
		if pending == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.mu.Lock()
	pending := len(a.pending)
	a.mu.Unlock()
	t.Fatalf("acks blocked behind undrained consumer; pending=%d", pending)
}

func TestProbeTimeoutAndSuccess(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	if err := a.Probe(ctx); err != nil {
		cancel()
		t.Fatalf("probe success: %v", err)
	}
	cancel()

	bPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "a" }
	ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := a.Probe(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("probe timeout err=%v, want context deadline", err)
	}
}

func TestRecvUnblocksWhenPeerDeadAfter(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{ResendAfter: 25 * time.Millisecond, DeadAfter: 75 * time.Millisecond})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	a.mu.Lock()
	a.lastHeard = time.Now().Add(-time.Second)
	a.mu.Unlock()

	_, err := recvErrWithin(t, a, time.Second)
	if !errors.Is(err, ErrLinkDead) {
		t.Fatalf("Recv err=%v, want ErrLinkDead", err)
	}
}

func TestPendingReliableQueueBounded(t *testing.T) {
	aPC, bPC := newPair()
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := 0; i < maxPendingReliable; i++ {
		if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatalf("send %d: %v", i, err)
		}
	}
	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("full")}); !errors.Is(err, ErrPendingFull) {
		t.Fatalf("send past bound err=%v, want ErrPendingFull", err)
	}
	a.mu.Lock()
	pending := len(a.pending)
	a.mu.Unlock()
	if pending > maxPendingReliable {
		t.Fatalf("pending=%d, want <= %d", pending, maxPendingReliable)
	}
}

func TestReliableRecvBufferBoundedForFarFutureSequences(t *testing.T) {
	tr := &Transport{nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame), maxRecvBuffer: maxRecvBuffer, done: make(chan struct{})}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)
	for i := 0; i < maxRecvBuffer+100; i++ {
		tr.enqueueReliable(uint64(1000+i), ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}})
	}
	if got := len(tr.recvBuf); got != maxRecvBuffer {
		t.Fatalf("recvBuf len=%d, want %d", got, maxRecvBuffer)
	}
	tr.enqueueReliable(1, ports.Frame{Type: ports.MsgOutput, Payload: []byte("next")})
	if _, ok := tr.recvBuf[1]; !ok {
		t.Fatalf("next expected sequence was dropped when far-future buffer was full")
	}
	if got := len(tr.recvBuf); got > maxRecvBuffer+1 {
		t.Fatalf("recvBuf len=%d, want <= %d", got, maxRecvBuffer+1)
	}
}

func TestReliableFullRecvBufferDoesNotAckDroppedFarFutureFrame(t *testing.T) {
	aPC, bPC := newPair()
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	tr := &Transport{pc: aPC, peer: testAddr("b"), codec: codec, sendDir: 1, mtu: pdgram.DefaultMTU, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame), maxRecvBuffer: maxRecvBuffer, clock: realClock{}, done: make(chan struct{})}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)
	defer func() { _ = aPC.Close() }()
	defer func() { _ = bPC.Close() }()

	for i := 0; i < maxRecvBuffer; i++ {
		tr.recvBuf[uint64(1000+i)] = ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}
	}
	tr.handleRecord(encodeData(9999, true, ports.Frame{Type: ports.MsgOutput, Payload: []byte("dropped")}))
	select {
	case pkt := <-bPC.in:
		t.Fatalf("unexpected ACK packet for dropped frame: %x", pkt.b)
	case <-time.After(25 * time.Millisecond):
	}

	tr.handleRecord(encodeData(1, true, ports.Frame{Type: ports.MsgOutput, Payload: []byte("next")}))
	select {
	case <-bPC.in:
	case <-time.After(time.Second):
		t.Fatal("expected ACK for buffered contiguous frame")
	}
}

func recvMaybe(tr *Transport, d time.Duration) (ports.Frame, bool) {
	ch := make(chan ports.Frame, 1)
	go func() { f, _ := tr.Recv(); ch <- f }()
	select {
	case f := <-ch:
		return f, true
	case <-time.After(d):
		return ports.Frame{}, false
	}
}

func recvWithin(t *testing.T, tr *Transport, d time.Duration) ports.Frame {
	t.Helper()
	ch := make(chan ports.Frame, 1)
	go func() { f, _ := tr.Recv(); ch <- f }()
	select {
	case f := <-ch:
		return f
	case <-time.After(d):
		t.Fatal("timeout")
		return ports.Frame{}
	}
}

func recvErrWithin(t *testing.T, tr *Transport, d time.Duration) (ports.Frame, error) {
	t.Helper()
	type result struct {
		f   ports.Frame
		err error
	}
	ch := make(chan result, 1)
	go func() {
		f, err := tr.Recv()
		ch <- result{f: f, err: err}
	}()
	select {
	case r := <-ch:
		return r.f, r.err
	case <-time.After(d):
		t.Fatal("timeout")
		return ports.Frame{}, nil
	}
}

func TestLinkStateTransitionsFromConfigurableSilence(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{DegradedAfter: time.Second, ProbeAfter: 2 * time.Second, OfflineAfter: 3 * time.Second, DeadAfter: 4 * time.Second})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	tests := []struct {
		name string
		age  time.Duration
		want ports.LinkState
	}{
		{"degraded", time.Second, ports.LinkStateDegraded},
		{"probing", 2 * time.Second, ports.LinkStateProbing},
		{"offline", 3 * time.Second, ports.LinkStateOffline},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a.mu.Lock()
			a.lastHeard = time.Now().Add(-tt.age)
			a.mu.Unlock()
			a.checkSilence()
			if got := a.LinkState(); got != tt.want {
				t.Fatalf("LinkState()=%v, want %v", got, tt.want)
			}
			select {
			case ev := <-a.LinkEvents():
				if ev.State != tt.want {
					t.Fatalf("event state=%v, want %v", ev.State, tt.want)
				}
			default:
				t.Fatalf("missing link event for %v", tt.want)
			}
		})
	}
}

func TestShortSilenceDoesNotKillTransport(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{DegradedAfter: 50 * time.Millisecond, DeadAfter: time.Hour})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	a.mu.Lock()
	a.lastHeard = time.Now().Add(-time.Second)
	a.mu.Unlock()
	a.checkSilence()
	if got := a.LinkState(); got != ports.LinkStateDegraded {
		t.Fatalf("LinkState()=%v, want degraded", got)
	}
	select {
	case <-a.done:
		t.Fatal("transport closed before DeadAfter")
	default:
	}
}
