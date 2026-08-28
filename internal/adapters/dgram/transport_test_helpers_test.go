package dgram

import (
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

type timeoutErr struct{}

func (timeoutErr) Error() string { return "i/o timeout" }
func (timeoutErr) Timeout() bool { return true }

type blockingWritePC struct {
	addr net.Addr

	mu       sync.Mutex
	closed   bool
	deadline time.Time
	done     chan struct{}
}

func newBlockingWritePC() *blockingWritePC {
	return &blockingWritePC{addr: testAddr("blocked"), done: make(chan struct{})}
}

func (p *blockingWritePC) ReadFrom([]byte) (int, net.Addr, error) {
	<-p.done
	return 0, nil, errors.New("closed")
}

func (p *blockingWritePC) WriteTo([]byte, net.Addr) (int, error) {
	for {
		p.mu.Lock()
		closed := p.closed
		deadline := p.deadline
		p.mu.Unlock()
		if closed {
			return 0, errors.New("closed")
		}
		if !deadline.IsZero() {
			wait := time.Until(deadline)
			if wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-timer.C:
				case <-p.done:
					timer.Stop()
					return 0, errors.New("closed")
				}
			}
			return 0, timeoutErr{}
		}
		select {
		case <-time.After(time.Millisecond):
		case <-p.done:
			return 0, errors.New("closed")
		}
	}
}

func (p *blockingWritePC) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.closed {
		p.closed = true
		close(p.done)
	}
	return nil
}

func (p *blockingWritePC) LocalAddr() net.Addr { return p.addr }
func (p *blockingWritePC) SetDeadline(t time.Time) error {
	p.mu.Lock()
	p.deadline = t
	p.mu.Unlock()
	return nil
}
func (p *blockingWritePC) SetReadDeadline(time.Time) error { return nil }
func (p *blockingWritePC) SetWriteDeadline(t time.Time) error {
	p.mu.Lock()
	p.deadline = t
	p.mu.Unlock()
	return nil
}

type fixedClock struct{ now time.Time }

func (c fixedClock) Now() time.Time { return c.now }
func (c fixedClock) NewTimer(d time.Duration) ports.Timer {
	return realTimer{t: time.NewTimer(d)}
}

type manualClock struct {
	mu           sync.Mutex
	now          time.Time
	timers       map[*manualTimer]struct{}
	timerCreated chan struct{}
}

func newManualClock(now time.Time) *manualClock {
	return &manualClock{
		now:          now,
		timers:       make(map[*manualTimer]struct{}),
		timerCreated: make(chan struct{}, 64),
	}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	tm := &manualTimer{clock: c, c: make(chan time.Time, 1), deadline: c.now.Add(d), active: true}
	c.timers[tm] = struct{}{}
	c.mu.Unlock()
	select {
	case c.timerCreated <- struct{}{}:
	default:
	}
	return tm
}

func (c *manualClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	timers := make([]*manualTimer, 0, len(c.timers))
	for tm := range c.timers {
		timers = append(timers, tm)
	}
	c.mu.Unlock()
	for _, tm := range timers {
		tm.fireIfDue(now)
	}
}

type manualTimer struct {
	clock    *manualClock
	c        chan time.Time
	deadline time.Time
	active   bool
}

func (t *manualTimer) C() <-chan time.Time { return t.c }
func (t *manualTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	wasActive := t.active
	t.deadline = t.clock.now.Add(d)
	t.active = true
	t.clock.timers[t] = struct{}{}
	t.clock.mu.Unlock()
	return wasActive
}
func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	t.active = false
	delete(t.clock.timers, t)
	return wasActive
}
func (t *manualTimer) fireIfDue(now time.Time) bool {
	t.clock.mu.Lock()
	if !t.active {
		delete(t.clock.timers, t)
		t.clock.mu.Unlock()
		return false
	}
	if now.Before(t.deadline) {
		t.clock.mu.Unlock()
		return false
	}
	t.active = false
	delete(t.clock.timers, t)
	t.clock.mu.Unlock()
	select {
	case t.c <- now:
	default:
	}
	return true
}

func waitForManualTimers(t *testing.T, clk *manualClock, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		clk.mu.Lock()
		active := len(clk.timers)
		clk.mu.Unlock()
		if active >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	clk.mu.Lock()
	active := len(clk.timers)
	clk.mu.Unlock()
	t.Fatalf("manual timers=%d, want at least %d", active, n)
}

type deadlineCapturePC struct {
	addr     net.Addr
	deadline atomic.Value
	done     chan struct{}
}

func (p *deadlineCapturePC) ReadFrom([]byte) (int, net.Addr, error) {
	<-p.done
	return 0, nil, errors.New("closed")
}
func (p *deadlineCapturePC) WriteTo(b []byte, _ net.Addr) (int, error) { return len(b), nil }
func (p *deadlineCapturePC) Close() error {
	select {
	case <-p.done:
	default:
		close(p.done)
	}
	return nil
}
func (p *deadlineCapturePC) LocalAddr() net.Addr             { return p.addr }
func (p *deadlineCapturePC) SetDeadline(time.Time) error     { return nil }
func (p *deadlineCapturePC) SetReadDeadline(time.Time) error { return nil }
func (p *deadlineCapturePC) SetWriteDeadline(t time.Time) error {
	if !t.IsZero() {
		p.deadline.Store(t)
	}
	return nil
}

func eventuallyPacketCount(t *testing.T, pc *fakePC, want int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(pc.in) >= want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("datagrams=%d, want at least %d", len(pc.in), want)
}

func eventually(t *testing.T, timeout time.Duration, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition was not met before timeout")
}

func newResendTestTransport(t *testing.T, maxResendPerTick int) (*Transport, *atomic.Int32) {
	t.Helper()
	aPC, _ := newPair()
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	var writes atomic.Int32
	aPC.drop = func(_ []byte, addr net.Addr) bool {
		if addr.String() == "b" {
			writes.Add(1)
			return true
		}
		return false
	}
	return &Transport{
		pc:               aPC,
		peer:             testAddr("b"),
		codec:            codec,
		sendDir:          1,
		mtu:              pdgram.DefaultMTU,
		pending:          make(map[uint64]*pending),
		resendAfter:      100 * time.Millisecond,
		maxResendAfter:   500 * time.Millisecond,
		maxResendPerTick: maxResendPerTick,
		rto:              100 * time.Millisecond,
		clock:            realClock{},
		congestion:       newCongestionController(pdgram.DefaultMTU),
		done:             make(chan struct{}),
	}, &writes
}

func ackRecord(seq uint64) []byte {
	var b [9]byte
	b[0] = recAck
	binary.BigEndian.PutUint64(b[1:], seq)
	return b[:]
}

func probeRecord(kind byte, id uint64) []byte {
	var b [9]byte
	b[0] = kind
	binary.BigEndian.PutUint64(b[1:], id)
	return b[:]
}

func TestReliableRecvBufferBoundedForFarFutureSequences(t *testing.T) {
	tr := &Transport{nextRecvSeq: 1, recvBuf: make(map[uint64]wire.Frame), maxRecvBuffer: maxRecvBuffer, done: make(chan struct{})}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)
	for i := range maxRecvBuffer + 100 {
		tr.enqueueReliable(uint64(1000+i), wire.Frame{Type: wire.MsgOutput, Payload: []byte{byte(i)}})
	}
	if got := len(tr.recvBuf); got != maxRecvBuffer {
		t.Fatalf("recvBuf len=%d, want %d", got, maxRecvBuffer)
	}
	tr.enqueueReliable(1, wire.Frame{Type: wire.MsgOutput, Payload: []byte("next")})
	if _, ok := tr.recvBuf[1]; !ok {
		t.Fatalf("next expected sequence was dropped when far-future buffer was full")
	}
	if got := len(tr.recvBuf); got > maxRecvBuffer+1 {
		t.Fatalf("recvBuf len=%d, want <= %d", got, maxRecvBuffer+1)
	}
}

func TestOutputPrerequisitesRemainInFullReceiveBuffer(t *testing.T) {
	tr := &Transport{
		nextRecvSeq:   1,
		recvBuf:       map[uint64]wire.Frame{2: {Type: wire.MsgOutput, Payload: []byte("stale")}},
		maxRecvBuffer: 1,
		done:          make(chan struct{}),
	}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)

	ackSeq, ack, queued := tr.enqueueReliable(3, wire.Frame{Type: wire.MsgOutput, Payload: []byte("replacement")})
	if ack || queued || ackSeq != 0 {
		t.Fatalf("replacement ackSeq=%d ack=%v queued=%v, want output rejected while prerequisite buffer is full", ackSeq, ack, queued)
	}
	if _, ok := tr.recvBuf[2]; !ok {
		t.Fatal("prerequisite output was discarded from full receive buffer")
	}
	if _, ok := tr.recvBuf[3]; ok {
		t.Fatal("far-future replacement bypassed full receive buffer")
	}
}

func TestReliableFullRecvBufferDoesNotAckDroppedFarFutureFrame(t *testing.T) {
	aPC, bPC := newPair()
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	tr := &Transport{pc: aPC, peer: testAddr("b"), codec: codec, sendDir: 1, mtu: pdgram.DefaultMTU, nextRecvSeq: 1, recvBuf: make(map[uint64]wire.Frame), maxRecvBuffer: maxRecvBuffer, clock: realClock{}, done: make(chan struct{})}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)
	defer func() { _ = aPC.Close() }()
	defer func() { _ = bPC.Close() }()

	for i := range maxRecvBuffer {
		tr.recvBuf[uint64(1000+i)] = wire.Frame{Type: wire.MsgOutput, Payload: []byte{byte(i)}}
	}
	tr.handleRecord(encodeData(9999, true, wire.Frame{Type: wire.MsgOutput, Payload: []byte("dropped")}))
	select {
	case pkt := <-bPC.in:
		t.Fatalf("unexpected ACK packet for dropped frame: %x", pkt.b)
	case <-time.After(25 * time.Millisecond):
	}

	tr.handleRecord(encodeData(1, true, wire.Frame{Type: wire.MsgOutput, Payload: []byte("next")}))
	select {
	case <-bPC.in:
	case <-time.After(time.Second):
		t.Fatal("expected ACK for buffered contiguous frame")
	}
}

func recvMaybe(tr *Transport, d time.Duration) (wire.Frame, bool) {
	ch := make(chan wire.Frame, 1)
	go func() { f, _ := tr.Recv(); ch <- f }()
	select {
	case f := <-ch:
		return f, true
	case <-time.After(d):
		return wire.Frame{}, false
	}
}

func recvWithin(t *testing.T, tr *Transport, d time.Duration) wire.Frame {
	t.Helper()
	ch := make(chan wire.Frame, 1)
	go func() { f, _ := tr.Recv(); ch <- f }()
	select {
	case f := <-ch:
		return f
	case <-time.After(d):
		t.Fatal("timeout")
		return wire.Frame{}
	}
}

func waitPeer(t *testing.T, tr *Transport, want string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if tr.Peer().String() == want {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("peer=%v, want %s", tr.Peer(), want)
}

func recvErrWithin(t *testing.T, tr *Transport, d time.Duration) (wire.Frame, error) {
	t.Helper()
	type result struct {
		f   wire.Frame
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
		return wire.Frame{}, nil
	}
}
