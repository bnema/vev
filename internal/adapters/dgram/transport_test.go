package dgram

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
	"github.com/stretchr/testify/require"
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

func TestCongestionControllerBoundsAndIntegerGrowth(t *testing.T) {
	const mtu = 1200
	c := newCongestionController(mtu)
	if got, want := c.cwndBytes, initialCongestionPackets*mtu; got != want {
		t.Fatalf("initial cwnd=%d, want %d", got, want)
	}
	if got, want := c.burstBytes(), dataBurstPackets*mtu; got != want {
		t.Fatalf("initial burst=%d, want %d", got, want)
	}
	if got, want := c.bytesPerSecond(), initialCongestionPackets*mtu*int(time.Second/initialPacingRTT); got != want {
		t.Fatalf("initial pacing rate=%d, want %d", got, want)
	}

	c.onLoss()
	if got, want := c.cwndBytes, initialCongestionPackets*mtu/2; got != want {
		t.Fatalf("cwnd after first loss=%d, want %d", got, want)
	}
	c.cwndBytes = 3 * mtu
	c.onLoss()
	if got, want := c.cwndBytes, minimumCongestionPackets*mtu; got != want {
		t.Fatalf("minimum cwnd=%d, want %d", got, want)
	}

	c.cwndBytes = 10 * mtu
	c.onACK(1)
	if got, want := c.cwndBytes, 10*mtu+1; got != want {
		t.Fatalf("minimum additive increase cwnd=%d, want %d", got, want)
	}
	c.onACK(10 * mtu)
	if got, want := c.cwndBytes, 10*mtu+1+(mtu*10*mtu)/(10*mtu+1); got != want {
		t.Fatalf("integer additive increase cwnd=%d, want %d", got, want)
	}
	c.cwndBytes = maximumCongestionBytes
	c.onACK(maximumCongestionBytes)
	if got := c.cwndBytes; got != maximumCongestionBytes {
		t.Fatalf("maximum cwnd=%d, want %d", got, maximumCongestionBytes)
	}

	c.onRTT(500 * time.Microsecond)
	if got, want := c.bytesPerSecond(), maximumCongestionBytes*1000; got != want {
		t.Fatalf("sub-millisecond pacing rate=%d, want %d", got, want)
	}
}

func TestBytePacerUsesBurstThenFakeClockRate(t *testing.T) {
	const mtu = 1200
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	c := newCongestionController(mtu)
	p := bytePacer{clk: clk}
	done := make(chan struct{})
	limits := func() (int, int) { return c.bytesPerSecond(), c.burstBytes() }

	for range dataBurstPackets {
		if err := p.wait(done, mtu, limits); err != nil {
			t.Fatal(err)
		}
	}
	wait := make(chan error, 1)
	go func() { wait <- p.wait(done, mtu, limits) }()
	awaitSignal(t, clk.timerCreated, "byte pacer timer")
	clk.advance(24 * time.Millisecond)
	select {
	case err := <-wait:
		t.Fatalf("third MTU released early: %v", err)
	default:
	}
	clk.advance(time.Millisecond)
	if err := awaitResult(t, wait, "third paced MTU"); err != nil {
		t.Fatal(err)
	}

	c.onLoss()
	next := make(chan error, 1)
	go func() { next <- p.wait(done, mtu, limits) }()
	awaitSignal(t, clk.timerCreated, "reduced-cwnd byte pacer timer")
	clk.advance(49 * time.Millisecond)
	select {
	case err := <-next:
		t.Fatalf("reduced cwnd did not lengthen wait: %v", err)
	default:
	}
	clk.advance(time.Millisecond)
	if err := awaitResult(t, next, "reduced-cwnd paced MTU"); err != nil {
		t.Fatal(err)
	}
}

func TestBytePacerMaximumRateRefillsAfterMultiSecondFakeClockJump(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	p := bytePacer{clk: clk}
	done := make(chan struct{})
	burst := int(^uint(0) >> 1)
	limits := func() (int, int) { return burst, burst }

	if err := p.wait(done, burst, limits); err != nil {
		t.Fatal(err)
	}
	clk.advance(3 * time.Second)
	if err := p.wait(done, burst, limits); err != nil {
		t.Fatal(err)
	}

	wait := make(chan error, 1)
	go func() { wait <- p.wait(done, burst, limits) }()
	awaitSignal(t, clk.timerCreated, "maximum-rate byte pacer timer")
	clk.advance(time.Second - time.Nanosecond)
	select {
	case err := <-wait:
		t.Fatalf("maximum-rate burst released early: %v", err)
	default:
	}
	clk.advance(time.Nanosecond)
	if err := awaitResult(t, wait, "maximum-rate burst"); err != nil {
		t.Fatal(err)
	}
}

func TestBytePacerCancellation(t *testing.T) {
	const mtu = 1200
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	p := bytePacer{clk: clk}
	done := make(chan struct{})
	limits := func() (int, int) { return mtu, mtu }
	if err := p.wait(done, mtu, limits); err != nil {
		t.Fatal(err)
	}
	wait := make(chan error, 1)
	go func() { wait <- p.wait(done, mtu, limits) }()
	awaitSignal(t, clk.timerCreated, "cancelled byte pacer timer")
	close(done)
	if err := awaitResult(t, wait, "byte pacer cancellation"); !errors.Is(err, errPacerClosed) {
		t.Fatalf("cancellation error=%v, want %v", err, errPacerClosed)
	}
}

func BenchmarkCongestionControllerACK(b *testing.B) {
	c := newCongestionController(defaultMTU)
	b.ReportAllocs()
	for b.Loop() {
		c.onACK(defaultMTU)
		if c.cwndBytes == c.maxBytes {
			c = newCongestionController(defaultMTU)
		}
	}
}

func TestPendingWireBytesMatchSealedDatagrams(t *testing.T) {
	const mtu = 128
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		MTU: mtu, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	maxFragmentData := mtu - pdgram.HeaderSize - a.codec.Overhead() - 16
	frame := ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, maxFragmentData-dataRecordHeaderSize+1)}
	if err := a.Send(frame); err != nil {
		t.Fatal(err)
	}

	want := 0
	for len(bPC.in) > 0 {
		want += len((<-bPC.in).b)
	}
	a.mu.Lock()
	got := a.pending[1].wireBytes
	initialCwnd := a.congestion.cwndBytes
	a.mu.Unlock()
	if got != want {
		t.Fatalf("pending wire bytes=%d, want sealed datagram bytes=%d", got, want)
	}

	ack := make([]byte, 9)
	ack[0] = recAck
	binary.BigEndian.PutUint64(ack[1:], 1)
	a.handleRecord(ack)
	a.mu.Lock()
	gotCwnd := a.congestion.cwndBytes
	a.mu.Unlock()
	wantCwnd := initialCwnd + max(1, mtu*want/initialCwnd)
	if gotCwnd != wantCwnd {
		t.Fatalf("cwnd after ACK=%d, want %d from %d acknowledged sealed bytes", gotCwnd, wantCwnd, want)
	}
}

func TestSendAsyncTransfersFragmentedRecordToBytePacedSender(t *testing.T) {
	const mtu = 128
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, MTU: mtu, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	maxFragmentData := mtu - pdgram.HeaderSize - a.codec.Overhead()
	frame := ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 2*maxFragmentData)}
	if err := a.SendAsync(frame); err != nil {
		t.Fatal(err)
	}
	eventuallyPacketCount(t, bPC, dataBurstPackets)
	if got := len(bPC.in); got != dataBurstPackets {
		t.Fatalf("immediate datagrams=%d, want %d MTU burst", got, dataBurstPackets)
	}
	awaitSignal(t, clk.timerCreated, "shared data sender pacing timer")
	clk.advance(initialPacingRTT)
	eventuallyPacketCount(t, bPC, 3)
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

func TestSmallOutputsShareInitialByteBurst(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	for i := range 3 {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	eventuallyPacketCount(t, bPC, 3)
	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("next")}); err != nil {
		t.Fatal(err)
	}
	eventuallyPacketCount(t, bPC, 4)
}

func TestOutputPacingSendsAtMostOneOversizedFramePerTick(t *testing.T) {
	aPC, _ := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("prime")}); err != nil {
		t.Fatal(err)
	}
	large := ports.MarshalOutput(ports.Output{BaseStateNum: 1, NewStateNum: 2, Data: make([]byte, 24*a.mtu)})
	for range 2 {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: large}); err != nil {
			t.Fatal(err)
		}
	}
	waitForManualTimers(t, clk, 4)
	previousPackets := len(aPC.in)
	for tick := 0; tick < 128; tick++ {
		clk.advance(initialPacingRTT)
		time.Sleep(time.Millisecond)
		packets := len(aPC.in)
		if delta := packets - previousPackets; delta > initialCongestionPackets {
			t.Fatalf("tick %d emitted %d packets, cwnd packets %d", tick, delta, initialCongestionPackets)
		}
		previousPackets = packets
		a.mu.Lock()
		last := a.pending[3]
		done := len(a.outputQueue) == 0 && last != nil && !last.initialInFlight
		a.mu.Unlock()
		if done {
			return
		}
	}
	t.Fatal("oversized queued frames did not complete within pacing ticks")
}

func TestPacedInitialSendTimestampsFinalFragmentCompletion(t *testing.T) {
	aPC, _ := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, ResendAfter: 500 * time.Millisecond, MaxResendAfter: time.Second, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	large := ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 24*a.mtu)}
	done := make(chan error, 1)
	go func() { done <- a.Send(large) }()
	start := clk.Now()
	for tick := 0; tick < 128; tick++ {
		a.mu.Lock()
		p := a.pending[1]
		if p != nil && p.initialInFlight && !p.last.IsZero() {
			a.mu.Unlock()
			t.Fatal("pending last timestamp set before final fragment")
		}
		a.mu.Unlock()
		clk.advance(initialPacingRTT)
		time.Sleep(time.Millisecond)
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			a.mu.Lock()
			completed := a.pending[1].last
			a.mu.Unlock()
			if elapsed := completed.Sub(start); elapsed <= initialPacingRTT {
				t.Fatalf("large send completed too early after %v", elapsed)
			}
			before := len(aPC.in)
			clk.advance(499 * time.Millisecond)
			a.resendPending()
			if got := len(aPC.in); got != before {
				t.Fatalf("resend emitted before one RTO after final fragment: %d -> %d", before, got)
			}
			a.handleRecord(ackRecord(1))
			a.mu.Lock()
			srtt := a.srtt
			a.mu.Unlock()
			require.Equal(t, 499*time.Millisecond, srtt, "RTT sample must start at final-fragment completion")
			return
		default:
		}
	}
	t.Fatal("large initial send did not complete")
}

func TestOwnedSynchronousMaxSideEffectCompletesThroughConcurrentPacedWork(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, ResendAfter: 250 * time.Millisecond, MaxResendAfter: time.Second, Heartbeat: time.Hour,
	})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	a.mu.Lock()
	a.srtt = 500 * time.Millisecond
	a.mu.Unlock()
	for i := range 8 {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	now := clk.Now()
	a.mu.Lock()
	for _, seq := range []uint64{100, 101} {
		a.pending[seq] = &pending{
			frame:    ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 20*a.mtu)},
			enqueued: now.Add(-time.Second), first: now.Add(-time.Second), last: now.Add(-time.Second),
		}
	}
	a.mu.Unlock()
	go a.resendPending()
	sideEffect := ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{Data: make([]byte, 100*1024)})}
	done := make(chan error, 1)
	go func() { done <- a.SendSynchronous(sideEffect) }()

	for tick := 0; tick < 512; tick++ {
		clk.advance(initialPacingRTT)
		time.Sleep(time.Millisecond)
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			if a.isClosed() {
				t.Fatal("healthy owned synchronous send force-closed transport")
			}
			return
		default:
		}
	}
	t.Fatal("maximum side effect did not complete through queued/retransmit pacing")
}

func TestResendSkipsQueuedOutputBeforeFirstWireAttempt(t *testing.T) {
	tr, writes := newResendTestTransport(t, 10)
	tr.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("queued")}}

	tr.resendPending()

	if got := writes.Load(); got != 0 {
		t.Fatalf("writes=%d, want queued output excluded from resend", got)
	}
	if !tr.pending[1].first.IsZero() || !tr.pending[1].last.IsZero() {
		t.Fatal("resend scan marked queued output as sent")
	}
}

func TestResendSkipsOutputDuringFirstWireAttempt(t *testing.T) {
	tr, writes := newResendTestTransport(t, 10)
	now := tr.clock.Now()
	tr.pending[1] = &pending{
		frame:           ports.Frame{Type: ports.MsgOutput, Payload: []byte("sending")},
		first:           now.Add(-time.Second),
		last:            now.Add(-time.Second),
		initialInFlight: true,
	}

	tr.resendPending()
	if got := writes.Load(); got != 0 {
		t.Fatalf("writes=%d, want first wire attempt excluded from resend", got)
	}
	tr.pending[1].initialInFlight = false
	tr.resendPending()
	if got := writes.Load(); got != 1 {
		t.Fatalf("writes=%d, want resend after first wire attempt completed", got)
	}
}

func TestInputWaitsForDequeuedOutputBatch(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	a.writeMu.Lock()
	for _, payload := range []string{"first", "queued"} {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("typed")}) }()
	select {
	case err := <-done:
		a.writeMu.Unlock()
		t.Fatalf("input completed before dequeued output batch: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	a.writeMu.Unlock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("input did not complete after output batch was released")
	}
	if got := len(bPC.in); got != 3 {
		t.Fatalf("datagrams when input completed=%d, want output batch before input", got)
	}
}

func TestPacedControlQueueFailureClosesTransport(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	if err := bPC.Close(); err != nil {
		t.Fatal(err)
	}
	inputDone := make(chan error, 1)
	go func() { inputDone <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("typed")}) }()
	if err := <-inputDone; err == nil {
		t.Fatal("data send succeeded after paced wire failure")
	}
	select {
	case <-a.done:
	case <-time.After(time.Second):
		t.Fatal("transport remained open after paced control queue write failed")
	}
}

func TestRemoveQueuedPendingResolvesSynchronousWaiters(t *testing.T) {
	tr, _ := newResendTestTransport(t, 10)
	wantErr := errors.New("paced send failed")
	done := make(chan error, 1)

	tr.mu.Lock()
	tr.pending[7] = &pending{}
	tr.mu.Unlock()
	tr.removeQueuedPending([]queuedSend{{seq: 7, reliable: true, done: done}}, wantErr)

	require.ErrorIs(t, <-done, wantErr)
	_, ok := <-done
	require.False(t, ok, "completion channel must be closed")
	tr.mu.Lock()
	_, pending := tr.pending[7]
	tr.mu.Unlock()
	require.False(t, pending)
}

func TestCloseUnblocksQueuedSynchronousFrames(t *testing.T) {
	frames := []struct {
		name  string
		frame ports.Frame
	}{
		{name: "control", frame: ports.Frame{Type: ports.MsgPing}},
		{name: "terminal side effect", frame: ports.Frame{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{Data: []byte("osc")})}},
	}
	for _, tt := range frames {
		t.Run(tt.name, func(t *testing.T) {
			aPC, _ := newPair()
			clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
			a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
			if err != nil {
				t.Fatal(err)
			}
			if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 3*a.mtu)}); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- a.Send(tt.frame) }()
			eventually(t, time.Second, func() bool {
				a.mu.Lock()
				defer a.mu.Unlock()
				return len(a.pending) == 2
			})
			_ = a.Close()
			if err := <-done; err == nil {
				t.Fatal("queued synchronous Send returned nil after close")
			}
		})
	}
}

func TestOutputPaceFailureClosesTransportWithWriteError(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	if err := bPC.Close(); err != nil {
		t.Fatal(err)
	}
	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("fails")}); err != nil {
		t.Fatal(err)
	}

	select {
	case <-a.done:
	case <-time.After(time.Second):
		t.Fatal("transport remained open after paced output write failed")
	}
	if err := a.Send(ports.Frame{Type: ports.MsgInput}); err == nil || err.Error() != "peer closed" {
		t.Fatalf("send after paced write failure err=%v, want peer closed", err)
	}
}

func TestInputAndControlRemainOrderedInBoundedPacedQueue(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()

	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("output")}); err != nil {
		t.Fatal(err)
	}
	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("queued")}); err != nil {
		t.Fatal(err)
	}
	eventuallyPacketCount(t, bPC, 2)
	inputDone := make(chan error, 1)
	go func() { inputDone <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("typed")}) }()
	eventuallyPacketCount(t, bPC, 3)
	if err := <-inputDone; err != nil {
		t.Fatal(err)
	}
	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("queued-before-control")}); err != nil {
		t.Fatal(err)
	}
	controlDone := make(chan error, 1)
	go func() { controlDone <- a.Send(ports.Frame{Type: ports.MsgPing}) }()
	eventuallyPacketCount(t, bPC, 5)
	if err := <-controlDone; err != nil {
		t.Fatal(err)
	}
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

func TestSendWriteTimeoutBoundsBlockedPacketConn(t *testing.T) {
	pc := newBlockingWritePC()
	tr, err := NewTransportWithOptions(pc, testAddr("peer"), key(), 1, 2, Options{
		WriteTimeout: 10 * time.Millisecond,
		ResendAfter:  time.Hour,
		Heartbeat:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	done := make(chan error, 1)
	go func() { done <- tr.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("blocked")}) }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Send returned nil, want write timeout")
		}
		var timeout interface{ Timeout() bool }
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("Send err=%v, want timeout error", err)
		}
		tr.mu.Lock()
		pending := len(tr.pending)
		tr.mu.Unlock()
		if pending != 0 {
			t.Fatalf("pending after failed initial send=%d, want 0", pending)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("Send blocked without honoring WriteTimeout")
	}
}

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
	if got.Type != ports.MsgInput || string(got.Payload) != "typed" {
		t.Fatalf("got=%+v", got)
	}
	eventually(t, time.Second, droppedAck.Load)
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
	if a.Peer().String() != old {
		t.Fatalf("unauthenticated packet changed peer to %v", a.Peer())
	}
	c, _ := pdgram.NewCodec(key())
	rec := encodeData(7, true, ports.Frame{Type: ports.MsgPing})
	frags, _ := pdgram.FragmentPayload(9, rec, pdgram.DefaultMTU)
	raw, _ := pdgram.MarshalFragment(frags[0])
	aPC.in <- packet{c.Seal(2, 99, raw, nil), testAddr("evil")}
	waitPeer(t, a, "evil")
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

func TestAuthenticatedFragmentSeparatesContactProgressAndPeerEligibility(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	defer func() { _ = bPC.Close() }()

	a.setLinkState(ports.LinkStateDegraded, nil)
	<-a.LinkEvents()
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	authenticated := make(chan struct{}, 8)
	malformed := make(chan struct{}, 8)
	a.mu.Lock()
	a.afterAuthenticatedPacket = func() { authenticated <- struct{}{} }
	a.afterMalformedFragment = func() { malformed <- struct{}{} }
	initialPacket := a.health.lastPacket
	initialRecord := a.health.lastRecord
	a.mu.Unlock()

	sealFragment := func(counter uint64, frag pdgram.Fragment) []byte {
		t.Helper()
		raw, marshalErr := pdgram.MarshalFragment(frag)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		return codec.Seal(2, counter, raw, nil)
	}
	waitSignal := func(ch <-chan struct{}, what string) {
		t.Helper()
		select {
		case <-ch:
		case <-time.After(time.Second):
			t.Fatalf("timeout waiting for %s", what)
		}
	}
	assertHealth := func(wantPacket, wantRecord time.Time, wantPeer string, wantState ports.LinkState) {
		t.Helper()
		a.mu.Lock()
		gotPacket := a.health.lastPacket
		gotRecord := a.health.lastRecord
		a.mu.Unlock()
		if !gotPacket.Equal(wantPacket) || !gotRecord.Equal(wantRecord) {
			t.Fatalf("health packet/record=(%v,%v), want (%v,%v)", gotPacket, gotRecord, wantPacket, wantRecord)
		}
		if got := a.Peer().String(); got != wantPeer {
			t.Fatalf("peer=%s, want %s", got, wantPeer)
		}
		if got := a.LinkState(); got != wantState {
			t.Fatalf("state=%v, want %v", got, wantState)
		}
	}

	// Neither failed authentication nor an authenticated but invalid fragment
	// crosses the contact or roaming trust boundary.
	badAEAD := codec.Seal(2, 1, []byte("not a fragment"), nil)
	badAEAD[len(badAEAD)-1] ^= 0xff
	aPC.in <- packet{b: badAEAD, addr: testAddr("roam1")}
	aPC.in <- packet{b: codec.Seal(2, 2, []byte("not a fragment"), nil), addr: testAddr("roam1")}
	waitSignal(malformed, "malformed fragment")
	assertHealth(initialPacket, initialRecord, "b", ports.LinkStateDegraded)

	clk.advance(time.Second)
	partial := pdgram.Fragment{Seq: 100, Index: 0, Count: 2, Data: []byte("half")}
	acceptedPacket := clk.Now()
	acceptedRaw := sealFragment(10, partial)
	aPC.in <- packet{b: acceptedRaw, addr: testAddr("roam1")}
	waitSignal(authenticated, "accepted incomplete fragment")
	assertHealth(acceptedPacket, initialRecord, "roam1", ports.LinkStateDegraded)

	// A replay and an Add-time fragment inconsistency cannot refresh contact or
	// migrate the peer. Reassembly diagnostics still update on the Add error.
	aPC.in <- packet{b: acceptedRaw, addr: testAddr("roam2")}
	aPC.in <- packet{b: codec.Seal(2, 11, []byte("still not a fragment"), nil), addr: testAddr("roam2")}
	waitSignal(malformed, "replay barrier")
	assertHealth(acceptedPacket, initialRecord, "roam1", ports.LinkStateDegraded)

	aPC.in <- packet{b: sealFragment(12, pdgram.Fragment{Seq: 100, Index: 1, Count: 3, Data: []byte("mismatch")}), addr: testAddr("roam2")}
	waitSignal(malformed, "reassembly rejection")
	assertHealth(acceptedPacket, initialRecord, "roam1", ports.LinkStateDegraded)

	// Fresh reordered packets refresh contact, but only the greatest accepted
	// AEAD counter is eligible to move the roaming peer. Contact alone must not
	// re-arm socket hopping while stream progress remains stalled.
	a.mu.Lock()
	a.hoppedOffline = true
	a.mu.Unlock()
	clk.advance(time.Second)
	reorderedPacket := clk.Now()
	aPC.in <- packet{b: sealFragment(9, pdgram.Fragment{Seq: 101, Index: 0, Count: 2, Data: []byte("older")}), addr: testAddr("roam2")}
	waitSignal(authenticated, "fresh reordered fragment")
	assertHealth(reorderedPacket, initialRecord, "roam1", ports.LinkStateDegraded)
	a.mu.Lock()
	if !a.hoppedOffline {
		a.mu.Unlock()
		t.Fatal("fragment-only contact re-armed socket hopping")
	}
	a.mu.Unlock()

	clk.advance(time.Second)
	newestPacket := clk.Now()
	aPC.in <- packet{b: sealFragment(13, pdgram.Fragment{Seq: 102, Index: 0, Count: 2, Data: []byte("newer")}), addr: testAddr("roam2")}
	waitSignal(authenticated, "new greatest fragment")
	assertHealth(newestPacket, initialRecord, "roam2", ports.LinkStateDegraded)

	clk.advance(time.Second)
	completeAt := clk.Now()
	aPC.in <- packet{b: sealFragment(14, pdgram.Fragment{Seq: 103, Index: 0, Count: 1, Data: probeRecord(recPong, 999)}), addr: testAddr("roam2")}
	waitSignal(authenticated, "complete record")
	eventually(t, time.Second, func() bool { return a.LinkState() == ports.LinkStateConnected })
	assertHealth(completeAt, completeAt, "roam2", ports.LinkStateConnected)
}

func TestPortHopPreservesPendingReliableMessages(t *testing.T) {
	aPC, bPC := newPair()
	var hopped atomic.Bool
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		ResendAfter: 20 * time.Millisecond,
		ProbeAfter:  time.Millisecond,
		DeadAfter:   time.Hour,
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
	a.health.lastPacket = time.Now().Add(-time.Second)
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
		ResendAfter: time.Hour,
		Heartbeat:   time.Hour,
		ProbeAfter:  time.Millisecond,
		DeadAfter:   time.Hour,
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
	a.health.lastPacket = time.Now().Add(-time.Second)
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
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{ProbeAfter: time.Millisecond, DeadAfter: time.Hour})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	a.mu.Lock()
	a.health.lastPacket = time.Now().Add(-time.Second)
	a.mu.Unlock()
	a.checkSilence()
	if a.pc != aPC {
		t.Fatal("server/proxy transport hopped without rebind hook")
	}
}

func TestReliableReceiveBufferDoesNotDropAckedFutureFrame(t *testing.T) {
	tr := &Transport{maxRecvBuffer: 1, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}
	_, ack, queued := tr.enqueueReliable(2, ports.Frame{Type: ports.MsgOutput, Payload: []byte("future")})
	if !ack || !queued {
		t.Fatalf("future enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	_, ack, queued = tr.enqueueReliable(1, ports.Frame{Type: ports.MsgOutput, Payload: []byte("next")})
	if !ack || !queued {
		t.Fatalf("next enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if _, ok := tr.recvBuf[2]; !ok {
		t.Fatal("acked future frame was dropped when next frame arrived")
	}
}

func TestDataRecordWireEncoding(t *testing.T) {
	got := encodeData(5, true, ports.Frame{Type: ports.MsgOutput, Payload: []byte("x")})
	want := []byte{recData, 0, 0, 0, 0, 0, 0, 0, 5, 1, byte(ports.MsgOutput), 0, 'x'}
	if !bytes.Equal(got, want) {
		t.Fatalf("encoded data=%v, want %v", got, want)
	}
	seq, reliable, frame, ok := decodeData(got)
	if !ok || seq != 5 || !reliable || frame.Type != ports.MsgOutput || string(frame.Payload) != "x" {
		t.Fatalf("decoded seq=%d reliable=%v frame=%+v ok=%v", seq, reliable, frame, ok)
	}
}

func TestDataRecordWireDecodeRejectsUnknownFlags(t *testing.T) {
	withTrailingGarbage := []byte{recData, 0, 0, 0, 0, 0, 0, 0, 5, 1, byte(ports.MsgOutput), 1, 'x'}
	if _, _, _, ok := decodeData(withTrailingGarbage); ok {
		t.Fatal("decodeData accepted unknown data-record flags with trailing garbage")
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
	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("state1")}); err != nil {
		t.Fatal(err)
	}
	got := recvWithin(t, b, time.Second)
	if got.Type != ports.MsgOutput || string(got.Payload) != "state1" || !dropped.Load() {
		t.Fatalf("got=%+v dropped=%v", got, dropped.Load())
	}
}

func TestImmediateLoopbackAckSamplesStampedFinalWrite(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	aPC.afterWrite = func() {
		deadline := time.Now().Add(time.Second)
		for time.Now().Before(deadline) {
			a.mu.Lock()
			_, pending := a.pending[1]
			a.mu.Unlock()
			if !pending {
				return
			}
			time.Sleep(time.Microsecond)
		}
		t.Error("ACK did not retire pending frame before WriteTo returned")
	}

	if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte("state")}); err != nil {
		t.Fatal(err)
	}
	eventually(t, time.Second, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.srtt > 0
	})
	a.mu.Lock()
	srtt, rto := a.srtt, a.rto
	a.mu.Unlock()
	if srtt <= 0 || srtt > time.Second {
		t.Fatalf("srtt=%v, want finite immediate-loopback sample", srtt)
	}
	if rto < a.resendAfter || rto > a.maxResendAfter {
		t.Fatalf("rto=%v outside [%v,%v]", rto, a.resendAfter, a.maxResendAfter)
	}
}

func TestReliableDeliveryPreservesInputSequenceOrder(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := range 100 {
		if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte{byte(i)}}); err != nil {
			t.Fatal(err)
		}
	}
	for i := range 100 {
		got := recvWithin(t, b, time.Second)
		if got.Type != ports.MsgInput || len(got.Payload) != 1 || got.Payload[0] != byte(i) {
			t.Fatalf("frame %d delivered out of order: %+v", i, got)
		}
	}
}

func TestOutputDependencyChainRetransmitsDroppedPredecessors(t *testing.T) {
	aPC, bPC := newPair()
	var sendAttempts atomic.Int32
	aPC.drop = func(_ []byte, addr net.Addr) bool {
		return addr.String() == "b" && sendAttempts.Add(1) <= 2
	}
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for _, payload := range []string{"state1", "state2", "state3"} {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte(payload)}); err != nil {
			t.Fatal(err)
		}
	}

	for _, want := range []string{"state1", "state2", "state3"} {
		got := recvWithin(t, b, time.Second)
		if got.Type != ports.MsgOutput || string(got.Payload) != want {
			t.Fatalf("got=%+v, want ordered prerequisite output %q", got, want)
		}
	}
	if sendAttempts.Load() < 3 {
		t.Fatalf("send attempts=%d, want first two output sends dropped before newest arrived", sendAttempts.Load())
	}
}

func TestAckProcessingContinuesWhenConsumerBackpressured(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransport(aPC, testAddr("b"), key(), 1, 2)
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	for i := range cap(b.in) + 20 {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}); err != nil {
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

func TestResendPendingCapsBurst(t *testing.T) {
	tr, writes := newResendTestTransport(t, 2)
	now := tr.clock.Now()
	for i := range 5 {
		tr.pending[uint64(i+1)] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(i)}}, last: now.Add(-time.Second)}
	}

	tr.resendPending()
	if got := writes.Load(); got != 2 {
		t.Fatalf("resend writes=%d, want capped burst of 2", got)
	}
}

func TestRetransmitBatchReducesCongestionWindowOnce(t *testing.T) {
	tr, writes := newResendTestTransport(t, 10)
	now := tr.clock.Now()
	for seq := uint64(1); seq <= 3; seq++ {
		tr.pending[seq] = &pending{
			frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(seq)}},
			last:  now.Add(-time.Second),
		}
	}
	initialCwnd := tr.congestion.cwndBytes

	tr.resendPending()

	if got := writes.Load(); got != 3 {
		t.Fatalf("retransmit writes=%d, want 3", got)
	}
	if got, want := tr.congestion.cwndBytes, initialCwnd/2; got != want {
		t.Fatalf("cwnd after one retransmit batch=%d, want %d", got, want)
	}
}

func TestACKBeforeQueuedRetransmitDoesNotReduceCongestionWindow(t *testing.T) {
	tr, writes := newResendTestTransport(t, 1)
	now := tr.clock.Now()
	tr.pending[1] = &pending{
		frame:     ports.Frame{Type: ports.MsgOutput, Payload: []byte("acked")},
		first:     now.Add(-time.Second),
		last:      now.Add(-time.Second),
		wireBytes: tr.mtu,
	}
	initialCwnd := tr.congestion.cwndBytes
	tr.mu.Lock()
	batch, _ := tr.selectRetransmitsLocked(now)
	tr.mu.Unlock()
	if len(batch) != 1 {
		t.Fatalf("selected retransmits=%d, want 1", len(batch))
	}
	tr.handleRecord(ackRecord(1))
	cwndAfterACK := initialCwnd + max(1, tr.mtu*tr.mtu/initialCwnd)
	counterAfterACK := tr.ctr

	tr.runRetransmitBatch(&bytePacer{clk: tr.clock}, batch)

	if got := tr.ctr; got != counterAfterACK {
		t.Fatalf("packet counter after ACK=%d, want unchanged %d", got, counterAfterACK)
	}
	if got := writes.Load(); got != 0 {
		t.Fatalf("writes after ACK=%d, want 0", got)
	}
	if got := tr.congestion.cwndBytes; got != cwndAfterACK {
		t.Fatalf("cwnd after ACK race=%d, want ACK-only growth %d", got, cwndAfterACK)
	}
}

func TestOversizedRetransmitHonorsPacketBudget(t *testing.T) {
	tr, writes := newResendTestTransport(t, 1)
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	tr.clock = clk
	tr.done = make(chan struct{})
	tr.pending[1] = &pending{
		frame: ports.Frame{Type: ports.MsgOutput, Payload: make([]byte, 24*tr.mtu)},
		last:  clk.now.Add(-time.Second),
	}
	done := make(chan struct{})
	go func() {
		tr.resendPending()
		close(done)
	}()

	previous := int32(0)
	for tick := 0; tick < 128; tick++ {
		clk.advance(initialPacingRTT)
		time.Sleep(time.Millisecond)
		current := writes.Load()
		if delta := current - previous; delta > dataBurstPackets {
			t.Fatalf("tick %d retransmitted %d packets, burst %d", tick, delta, dataBurstPackets)
		}
		previous = current
		select {
		case <-done:
			return
		default:
		}
	}
	t.Fatal("oversized retransmit did not complete within pacing ticks")
}

func TestResendPendingUsesExponentialBackoffAndMaxDelay(t *testing.T) {
	tr, writes := newResendTestTransport(t, 10)
	now := tr.clock.Now()
	tr.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("too-soon")}, last: now.Add(-150 * time.Millisecond), attempts: 1}
	tr.pending[2] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("ready")}, last: now.Add(-250 * time.Millisecond), attempts: 1}
	tr.pending[3] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("capped")}, last: now.Add(-time.Second), attempts: 10}

	tr.resendPending()
	if got := writes.Load(); got != 2 {
		t.Fatalf("resend writes=%d, want ready and capped frames only", got)
	}
	if attempts := tr.pending[1].attempts; attempts != 1 {
		t.Fatalf("too-soon attempts=%d, want unchanged", attempts)
	}
	if attempts := tr.pending[2].attempts; attempts != 2 {
		t.Fatalf("ready attempts=%d, want incremented", attempts)
	}
	if attempts := tr.pending[3].attempts; attempts != 11 {
		t.Fatalf("capped attempts=%d, want incremented", attempts)
	}
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
	a.health.lastPacket = time.Now().Add(-time.Second)
	a.mu.Unlock()

	_, err := recvErrWithin(t, a, time.Second)
	if !errors.Is(err, ErrLinkDead) {
		t.Fatalf("Recv err=%v, want ErrLinkDead", err)
	}
}

func TestCumulativeAckReleasesAllEarlierPendingFrames(t *testing.T) {
	tr, _ := newResendTestTransport(t, 10)
	now := tr.clock.Now()
	for seq := uint64(1); seq <= 3; seq++ {
		tr.pending[seq] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte{byte(seq)}}, first: now, last: now}
	}

	tr.handleRecord(ackRecord(3))

	if got := len(tr.pending); got != 0 {
		t.Fatalf("pending after cumulative ack=%d, want 0", got)
	}
}

func TestReliableReceiverAcksOnlyHighestContiguousSequence(t *testing.T) {
	tr := &Transport{maxRecvBuffer: maxRecvBuffer, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame)}

	ackSeq, ack, queued := tr.enqueueReliable(3, ports.Frame{Type: ports.MsgOutput, Payload: []byte("third")})
	if !ack || !queued {
		t.Fatalf("out-of-order enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if got, want := ackSeq, uint64(0); got != want {
		t.Fatalf("out-of-order ack=%d, want highest contiguous %d", got, want)
	}

	ackSeq, ack, queued = tr.enqueueReliable(1, ports.Frame{Type: ports.MsgOutput, Payload: []byte("first")})
	if !ack || !queued {
		t.Fatalf("first enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if got, want := ackSeq, uint64(1); got != want {
		t.Fatalf("first contiguous ack=%d, want %d", got, want)
	}

	ackSeq, ack, queued = tr.enqueueReliable(2, ports.Frame{Type: ports.MsgOutput, Payload: []byte("second")})
	if !ack || !queued {
		t.Fatalf("gap fill enqueue ack=%v queued=%v, want true/true", ack, queued)
	}
	if got, want := ackSeq, uint64(3); got != want {
		t.Fatalf("filled gap ack=%d, want %d", got, want)
	}
}

func TestRTTEstimatorUpdatesRTOFromAckSample(t *testing.T) {
	clk := fixedClock{now: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}
	tr, writes := newResendTestTransport(t, 10)
	tr.clock = clk
	tr.resendAfter = 100 * time.Millisecond
	tr.maxResendAfter = time.Second
	tr.rto = tr.resendAfter
	tr.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput}, first: clk.now.Add(-200 * time.Millisecond), last: clk.now.Add(-200 * time.Millisecond)}

	tr.handleRecord(ackRecord(1))

	if tr.rto <= tr.resendAfter {
		t.Fatalf("rto=%v, want above initial resend after RTT sample", tr.rto)
	}
	tr.pending[2] = &pending{frame: ports.Frame{Type: ports.MsgOutput}, first: clk.now, last: clk.now.Add(-(tr.rto - time.Millisecond))}
	tr.resendPending()
	if got := writes.Load(); got != 0 {
		t.Fatalf("writes before rto=%d, want 0", got)
	}
	tr.pending[2].last = clk.now.Add(-tr.rto)
	tr.resendPending()
	if got := writes.Load(); got != 1 {
		t.Fatalf("writes at rto=%d, want 1", got)
	}
}

func TestRTTEstimatorIgnoresRetransmittedFramesAfterRecovery(t *testing.T) {
	clk := fixedClock{now: time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)}
	tr, _ := newResendTestTransport(t, 10)
	tr.clock = clk
	tr.resendAfter = 100 * time.Millisecond
	tr.maxResendAfter = time.Second
	tr.rto = tr.resendAfter
	tr.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput}, first: clk.now.Add(-200 * time.Millisecond), last: clk.now.Add(-tr.resendAfter)}
	tr.resendPending()
	tr.linkState = ports.LinkStateOffline

	tr.setLinkState(ports.LinkStateConnected, nil)
	tr.handleRecord(ackRecord(1))

	if tr.rto != tr.resendAfter || tr.srtt != 0 || tr.rttvar != 0 {
		t.Fatalf("rtt sampled retransmitted frame after recovery: rto=%v srtt=%v rttvar=%v", tr.rto, tr.srtt, tr.rttvar)
	}
}

func TestPendingReliableQueueBackpressuresUntilAck(t *testing.T) {
	aPC, bPC := newPair()
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{MaxPending: 1})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("second")}) }()

	select {
	case err := <-done:
		t.Fatalf("send returned before an ack released pending capacity: %v", err)
	case <-time.After(25 * time.Millisecond):
	}

	a.handleRecord(ackRecord(1))
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("send after ack: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not resume after ack released pending capacity")
	}
	a.mu.Lock()
	pending := len(a.pending)
	a.mu.Unlock()
	if pending > 1 {
		t.Fatalf("pending=%d, want <= 1", pending)
	}
}

func TestPendingReliableQueueReturnsErrPendingFullWhileConnected(t *testing.T) {
	aPC, bPC := newPair()
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{MaxPending: 1, MaxPendingWait: 50 * time.Millisecond, Clock: clk})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("second")}) }()
	waitForManualTimers(t, clk, 4)

	clk.advance(50 * time.Millisecond)
	select {
	case err := <-done:
		if !errors.Is(err, ErrPendingFull) {
			t.Fatalf("send err=%v, want ErrPendingFull", err)
		}
	case <-time.After(time.Second):
		t.Fatal("send did not return after clock advanced past MaxPendingWait")
	}
}

func TestPendingAttemptsResetWhenLinkRecovers(t *testing.T) {
	tr, _ := newResendTestTransport(t, 10)
	tr.linkState = ports.LinkStateDegraded
	tr.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("stale")}, attempts: 4}
	tr.pending[2] = &pending{frame: ports.Frame{Type: ports.MsgOutput, Payload: []byte("stale")}, attempts: 7}

	tr.setLinkState(ports.LinkStateConnected, nil)

	for seq, p := range tr.pending {
		if p.attempts != 0 {
			t.Fatalf("pending[%d] attempts=%d, want reset on connected recovery", seq, p.attempts)
		}
	}
}

func TestWritePacketDeadlineUsesTransportClock(t *testing.T) {
	wantNow := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	pc := &deadlineCapturePC{addr: testAddr("a"), done: make(chan struct{})}
	tr, err := NewTransportWithOptions(pc, testAddr("b"), key(), 1, 2, Options{
		Clock:        fixedClock{now: wantNow},
		WriteTimeout: 250 * time.Millisecond,
		ResendAfter:  time.Hour,
		Heartbeat:    time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	if err := tr.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("deadline")}); err != nil {
		t.Fatal(err)
	}
	got, _ := pc.deadline.Load().(time.Time)
	if want := wantNow.Add(250 * time.Millisecond); !got.Equal(want) {
		t.Fatalf("write deadline=%v, want transport clock deadline %v", got, want)
	}
}

func TestProbeReplyDoesNotBlockOnWriteMu(t *testing.T) {
	aPC, bPC := newPair()
	tr, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	defer func() { _ = bPC.Close() }()

	tr.writeMu.Lock()
	done := make(chan struct{})
	go func() {
		for id := uint64(1); id <= probeReplyBufferSize*4; id++ {
			tr.handleRecord(probeRecord(recProbe, id))
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		tr.writeMu.Unlock()
		<-done
		t.Fatal("probe reply burst blocked behind writeMu")
	}
	tr.writeMu.Unlock()
}

func TestPendingReliableQueueReturnsWhenLinkDegrades(t *testing.T) {
	aPC, bPC := newPair()
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	clk := newManualClock(time.Now())
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{MaxPending: 1, MaxPendingWait: time.Hour, Clock: clk})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	if err := a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	go func() { done <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("second")}) }()
	go func() { done <- a.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("third")}) }()
	waitForManualTimers(t, clk, 2)
	a.setLinkState(ports.LinkStateDegraded, nil)
	for i := range 2 {
		select {
		case err := <-done:
			if !errors.Is(err, ErrPendingFull) {
				t.Fatalf("send %d err=%v, want ErrPendingFull", i, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("send %d did not unblock after link degraded", i)
		}
	}
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
	tr := &Transport{nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame), maxRecvBuffer: maxRecvBuffer, done: make(chan struct{})}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)
	for i := range maxRecvBuffer + 100 {
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

func TestOutputPrerequisitesRemainInFullReceiveBuffer(t *testing.T) {
	tr := &Transport{
		nextRecvSeq:   1,
		recvBuf:       map[uint64]ports.Frame{2: {Type: ports.MsgOutput, Payload: []byte("stale")}},
		maxRecvBuffer: 1,
		done:          make(chan struct{}),
	}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)

	ackSeq, ack, queued := tr.enqueueReliable(3, ports.Frame{Type: ports.MsgOutput, Payload: []byte("replacement")})
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
	tr := &Transport{pc: aPC, peer: testAddr("b"), codec: codec, sendDir: 1, mtu: pdgram.DefaultMTU, nextRecvSeq: 1, recvBuf: make(map[uint64]ports.Frame), maxRecvBuffer: maxRecvBuffer, clock: realClock{}, done: make(chan struct{})}
	tr.deliverCond = sync.NewCond(&tr.deliverMu)
	defer func() { _ = aPC.Close() }()
	defer func() { _ = bPC.Close() }()

	for i := range maxRecvBuffer {
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
			staleAt := time.Now().Add(-tt.age)
			a.health = healthTracker{lastPacket: staleAt, lastRecord: staleAt, lastProgress: staleAt}
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

func TestLinkHealthSeparatesPacketContactFromUsefulProgress(t *testing.T) {
	now := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)
	threshold := func(seconds int) time.Duration { return time.Duration(seconds) * time.Second }
	tests := []struct {
		name       string
		packetAge  time.Duration
		recordAge  time.Duration
		ackAge     time.Duration
		hasPending bool
		wantState  ports.LinkState
		wantHop    bool
		wantDead   bool
	}{
		{name: "fresh contact without useful progress degrades", packetAge: 0, recordAge: threshold(10), ackAge: threshold(10), wantState: ports.LinkStateDegraded},
		{name: "pending round trip stall probes and hops", packetAge: 0, recordAge: threshold(20), ackAge: threshold(20), hasPending: true, wantState: ports.LinkStateProbing, wantHop: true},
		{name: "fragment-only contact never becomes offline", packetAge: 0, recordAge: threshold(30), ackAge: threshold(30), wantState: ports.LinkStateDegraded},
		{name: "packet silence probes and hops", packetAge: threshold(20), recordAge: threshold(20), ackAge: threshold(20), wantState: ports.LinkStateProbing, wantHop: true},
		{name: "packet silence becomes offline", packetAge: threshold(30), recordAge: threshold(30), ackAge: threshold(30), wantState: ports.LinkStateOffline},
		{name: "packet silence becomes dead", packetAge: threshold(60), recordAge: threshold(60), ackAge: threshold(60), wantState: ports.LinkStateDead, wantDead: true},
		{name: "complete record cannot mask pending stall", packetAge: 0, recordAge: 0, ackAge: threshold(30), hasPending: true, wantState: ports.LinkStateProbing, wantHop: true},
		{name: "complete record restores connected without pending data", packetAge: 0, recordAge: 0, ackAge: threshold(30), wantState: ports.LinkStateConnected},
		{name: "ack progress restores connected", packetAge: 0, recordAge: threshold(30), ackAge: 0, hasPending: true, wantState: ports.LinkStateConnected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			health := healthTracker{
				lastPacket:   now.Add(-tt.packetAge),
				lastRecord:   now.Add(-tt.recordAge),
				lastProgress: now.Add(-tt.ackAge),
			}
			state, hop, dead := health.decide(now, tt.hasPending, threshold(10), threshold(20), threshold(30), threshold(60))
			if state != tt.wantState || hop != tt.wantHop || dead != tt.wantDead {
				t.Fatalf("decide()=(%v,%v,%v), want (%v,%v,%v)", state, hop, dead, tt.wantState, tt.wantHop, tt.wantDead)
			}
		})
	}
}

func TestLinkHealthPendingStallWithFreshContactProbesOnceWithoutDying(t *testing.T) {
	aPC, bPC := newPair()
	var hops atomic.Int32
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		ResendAfter: time.Hour, Heartbeat: time.Hour, ProbeAfter: 20 * time.Second, DeadAfter: 60 * time.Second,
		RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
			hops.Add(1)
			return &fakePC{addr: testAddr("a2"), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}, nil
		},
	})
	defer func() { _ = a.Close() }()
	defer func() { _ = bPC.Close() }()

	now := time.Now()
	a.mu.Lock()
	a.health = healthTracker{lastPacket: now, lastRecord: now.Add(-time.Minute), lastProgress: now.Add(-time.Minute)}
	a.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput}, enqueued: now.Add(-time.Minute), attempts: 3}
	a.mu.Unlock()

	a.checkSilence()
	a.checkSilence()
	if got := a.LinkState(); got != ports.LinkStateProbing {
		t.Fatalf("state=%v, want probing", got)
	}
	if got := hops.Load(); got != 1 {
		t.Fatalf("socket hops=%d, want one", got)
	}
	select {
	case <-a.done:
		t.Fatal("fresh authenticated contact must prevent pending-stall death")
	default:
	}
}

func TestLinkHealthHeartbeatControlCannotMaskPendingACKStall(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	var hops atomic.Int32
	tr, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
		RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
			attempt := hops.Add(1)
			return &fakePC{addr: testAddr(fmt.Sprintf("a%d", attempt+1)), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	defer func() { _ = bPC.Close() }()
	if err := tr.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("unacked")}); err != nil {
		t.Fatal(err)
	}

	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	authenticated := make(chan struct{}, 32)
	tr.mu.Lock()
	tr.afterAuthenticatedPacket = func() { authenticated <- struct{}{} }
	tr.mu.Unlock()
	var counter uint64
	sendHeartbeatPong := func() {
		t.Helper()
		counter++
		frag := pdgram.Fragment{Seq: counter, Index: 0, Count: 1, Data: probeRecord(recPong, counter)}
		raw, marshalErr := pdgram.MarshalFragment(frag)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		tr.mu.Lock()
		actual := tr.pc
		pc, ok := actual.(*fakePC)
		tr.mu.Unlock()
		if !ok {
			t.Fatalf("transport packet conn = %T, want *fakePC", actual)
		}
		pc.in <- packet{b: codec.Seal(2, counter, raw, nil), addr: testAddr("b")}
		select {
		case <-authenticated:
		case <-time.After(time.Second):
			t.Fatal("heartbeat pong was not authenticated")
		}
	}

	for elapsed := 3 * time.Second; elapsed <= 66*time.Second; elapsed += 3 * time.Second {
		clk.advance(3 * time.Second)
		sendHeartbeatPong()
		eventually(t, time.Second, func() bool {
			tr.mu.Lock()
			defer tr.mu.Unlock()
			return tr.health.lastRecord.Equal(clk.Now())
		})
		if elapsed >= 20*time.Second {
			eventually(t, time.Second, func() bool {
				tr.mu.Lock()
				defer tr.mu.Unlock()
				return tr.pc != aPC
			})
		}
	}
	if got := tr.LinkState(); got != ports.LinkStateProbing {
		t.Fatalf("state=%v, want probing despite fresh heartbeat control traffic", got)
	}
	if got := hops.Load(); got != 1 {
		t.Fatalf("socket hops=%d, want one pending round-trip recovery hop", got)
	}
	select {
	case <-tr.done:
		t.Fatal("fresh authenticated control contact must prevent offline/dead closure")
	default:
	}
}

func TestLinkHealthOnlyAckProgressThatRemovesPendingRestoresConnected(t *testing.T) {
	tr, _ := newResendTestTransport(t, 10)
	now := tr.clock.Now()
	tr.linkState = ports.LinkStateDegraded
	tr.health = healthTracker{lastPacket: now, lastRecord: now.Add(-time.Minute), lastProgress: now.Add(-time.Minute)}
	tr.pending[1] = &pending{frame: ports.Frame{Type: ports.MsgOutput}, first: now, last: now}

	tr.handleRecord(ackRecord(1))
	if got := tr.LinkState(); got != ports.LinkStateConnected {
		t.Fatalf("state after advancing ACK=%v, want connected", got)
	}
	tr.mu.Lock()
	if tr.health.lastProgress.Before(now) {
		tr.mu.Unlock()
		t.Fatalf("last progress=%v, want at or after %v", tr.health.lastProgress, now)
	}
	tr.mu.Unlock()

	tr.setLinkState(ports.LinkStateDegraded, nil)
	tr.handleRecord(ackRecord(1))
	if got := tr.LinkState(); got != ports.LinkStateDegraded {
		t.Fatalf("duplicate ACK state=%v, want degraded", got)
	}
}

func TestLinkHealthFreshProgressInvalidatesStaleDecision(t *testing.T) {
	tests := []struct {
		name      string
		staleAge  time.Duration
		freshen   func(*Transport)
		wantState ports.LinkState
	}{
		{
			name:     "fresh packet contact prevents stale dead close",
			staleAge: time.Minute,
			freshen: func(tr *Transport) {
				tr.mu.Lock()
				now := tr.clock.Now()
				tr.health.authenticatedPacket(now)
				tr.lastAuthenticatedPacket = now
				tr.mu.Unlock()
			},
			wantState: ports.LinkStateDegraded,
		},
		{
			name:     "complete record prevents stale probing state and hop",
			staleAge: 20 * time.Second,
			freshen: func(tr *Transport) {
				tr.mu.Lock()
				now := tr.clock.Now()
				tr.health.authenticatedPacket(now)
				tr.health.completeRecord(now)
				tr.lastAuthenticatedPacket = now
				tr.lastCompleteRecord = now
				tr.mu.Unlock()
				tr.setLinkState(ports.LinkStateConnected, nil)
			},
			wantState: ports.LinkStateConnected,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			aPC, bPC := newPair()
			var hops atomic.Int32
			tr, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
				ResendAfter: time.Hour,
				Heartbeat:   time.Hour,
				RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
					hops.Add(1)
					return &fakePC{addr: testAddr("a2"), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}, nil
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tr.Close() }()
			defer func() { _ = bPC.Close() }()

			now := tr.clock.Now()
			tr.mu.Lock()
			staleAt := now.Add(-tt.staleAge)
			tr.health = healthTracker{lastPacket: staleAt, lastRecord: staleAt, lastProgress: staleAt}
			decisionReady := make(chan struct{})
			resume := make(chan struct{})
			tr.afterHealthDecision = func() {
				close(decisionReady)
				<-resume
			}
			tr.mu.Unlock()

			done := make(chan struct{})
			go func() {
				tr.checkSilence()
				close(done)
			}()
			select {
			case <-decisionReady:
			case <-time.After(time.Second):
				t.Fatal("health decision hook was not reached")
			}
			tt.freshen(tr)
			close(resume)
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("health check did not finish")
			}

			if got := tr.LinkState(); got != tt.wantState {
				t.Fatalf("state=%v, want %v after fresh health evidence", got, tt.wantState)
			}
			if got := hops.Load(); got != 0 {
				t.Fatalf("socket hops=%d, want none after recovery", got)
			}
			for len(tr.linkEvents) > 0 {
				ev := <-tr.linkEvents
				if ev.State == ports.LinkStateDead || ev.State == ports.LinkStateProbing {
					t.Fatalf("stale link event emitted after recovery: %v", ev.State)
				}
			}
			select {
			case <-tr.done:
				t.Fatal("transport closed from stale health decision")
			default:
			}
		})
	}
}

func TestLinkHealthStaleRebindRestoresFutureHopAllowance(t *testing.T) {
	aPC, bPC := newPair()
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	rebindStarted := make(chan struct{})
	resumeRebind := make(chan struct{})
	var rebinds atomic.Int32
	tr, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
		RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
			attempt := rebinds.Add(1)
			pc := &fakePC{addr: testAddr(fmt.Sprintf("a%d", attempt+1)), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}
			if attempt == 1 {
				close(rebindStarted)
				<-resumeRebind
			}
			return pc, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	defer func() { _ = bPC.Close() }()

	tr.mu.Lock()
	staleAt := clk.Now().Add(-20 * time.Second)
	tr.health = healthTracker{lastPacket: staleAt, lastRecord: staleAt, lastProgress: staleAt}
	tr.mu.Unlock()
	firstDone := make(chan struct{})
	go func() {
		tr.checkSilence()
		close(firstDone)
	}()
	select {
	case <-rebindStarted:
	case <-time.After(time.Second):
		t.Fatal("first rebind did not start")
	}

	tr.mu.Lock()
	now := clk.Now()
	tr.health.authenticatedPacket(now)
	tr.lastAuthenticatedPacket = now
	tr.mu.Unlock()
	close(resumeRebind)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("stale rebind did not finish")
	}
	tr.mu.Lock()
	latched := tr.hoppedOffline
	pcAfterStale := tr.pc
	tr.mu.Unlock()
	if latched {
		t.Fatal("stale rebind consumed the future hop allowance")
	}
	if pcAfterStale != aPC {
		t.Fatal("stale rebind replaced the active packet conn")
	}

	clk.advance(20 * time.Second)
	tr.checkSilence()
	tr.mu.Lock()
	pcAfterRetry := tr.pc
	tr.mu.Unlock()
	if got := rebinds.Load(); got != 2 {
		t.Fatalf("rebind attempts=%d, want a later eligible retry", got)
	}
	if pcAfterRetry == aPC {
		t.Fatal("later stale interval did not hop the packet conn")
	}
}

func TestLinkHealthStateEventsCannotPublishAfterNewerRecovery(t *testing.T) {
	aPC, bPC := newPair()
	tr, err := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{ResendAfter: time.Hour, Heartbeat: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	defer func() { _ = bPC.Close() }()

	probingCommitted := make(chan struct{})
	resumeProbing := make(chan struct{})
	tr.mu.Lock()
	tr.linkState = ports.LinkStateDegraded
	tr.afterLinkStateCommit = func(state ports.LinkState) {
		if state != ports.LinkStateProbing {
			return
		}
		close(probingCommitted)
		<-resumeProbing
	}
	tr.mu.Unlock()

	probingDone := make(chan struct{})
	go func() {
		tr.setLinkState(ports.LinkStateProbing, nil)
		close(probingDone)
	}()
	select {
	case <-probingCommitted:
	case <-time.After(time.Second):
		t.Fatal("probing commit hook was not reached")
	}
	tr.setLinkState(ports.LinkStateConnected, nil)
	close(resumeProbing)
	select {
	case <-probingDone:
	case <-time.After(time.Second):
		t.Fatal("probing publisher did not finish")
	}

	if got := tr.LinkState(); got != ports.LinkStateConnected {
		t.Fatalf("state=%v, want connected", got)
	}
	select {
	case ev := <-tr.LinkEvents():
		if ev.State != ports.LinkStateConnected {
			t.Fatalf("first event=%v, want connected", ev.State)
		}
	default:
		t.Fatal("missing connected recovery event")
	}
	select {
	case ev := <-tr.LinkEvents():
		t.Fatalf("stale event published after recovery: %v", ev.State)
	default:
	}
}

func TestHopFiresAtProbingNotDegraded(t *testing.T) {
	aPC, bPC := newPair()
	var hopped atomic.Bool
	aPC.drop = func(_ []byte, addr net.Addr) bool { return addr.String() == "b" }
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{
		DegradedAfter: time.Second, ProbeAfter: 2 * time.Second, OfflineAfter: 3 * time.Second, DeadAfter: time.Hour,
		RebindPacketConn: func(net.PacketConn) (net.PacketConn, error) {
			newPC := &fakePC{addr: testAddr("a2"), in: make(chan packet, 100), peers: map[string]*fakePC{"b": bPC}}
			bPC.peers["a2"] = newPC
			hopped.Store(true)
			return newPC, nil
		},
	})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()

	a.mu.Lock()
	staleAt := time.Now().Add(-time.Second)
	a.health = healthTracker{lastPacket: staleAt, lastRecord: staleAt, lastProgress: staleAt}
	a.mu.Unlock()
	a.checkSilence()
	if hopped.Load() {
		t.Fatal("socket hopped at degraded silence")
	}

	a.mu.Lock()
	a.health.lastPacket = time.Now().Add(-2 * time.Second)
	a.mu.Unlock()
	a.checkSilence()
	if !hopped.Load() {
		t.Fatal("socket did not hop at probing silence")
	}
}

func TestShortSilenceDoesNotKillTransport(t *testing.T) {
	aPC, bPC := newPair()
	a, _ := NewTransportWithOptions(aPC, testAddr("b"), key(), 1, 2, Options{DegradedAfter: 50 * time.Millisecond, DeadAfter: time.Hour})
	b, _ := NewTransport(bPC, testAddr("a"), key(), 2, 1)
	defer func() { _ = a.Close() }()
	defer func() { _ = b.Close() }()
	a.mu.Lock()
	staleAt := time.Now().Add(-time.Second)
	a.health = healthTracker{lastPacket: staleAt, lastRecord: staleAt, lastProgress: staleAt}
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
