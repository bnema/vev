package dgram

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

type stagedTimeoutPC struct {
	mu       sync.Mutex
	deadline time.Time
	calls    int
	started  chan struct{}
	release  chan struct{}
}

func (p *stagedTimeoutPC) ReadFrom([]byte) (int, net.Addr, error) {
	return 0, nil, net.ErrClosed
}
func (p *stagedTimeoutPC) WriteTo(b []byte, _ net.Addr) (int, error) {
	p.mu.Lock()
	p.calls++
	call := p.calls
	p.mu.Unlock()
	if call <= 2 {
		p.started <- struct{}{}
		<-p.release
		return 0, timeoutErr{}
	}
	return len(b), nil
}
func (p *stagedTimeoutPC) Close() error                    { return nil }
func (p *stagedTimeoutPC) LocalAddr() net.Addr             { return testAddr("staged") }
func (p *stagedTimeoutPC) SetDeadline(time.Time) error     { return nil }
func (p *stagedTimeoutPC) SetReadDeadline(time.Time) error { return nil }
func (p *stagedTimeoutPC) SetWriteDeadline(d time.Time) error {
	p.mu.Lock()
	p.deadline = d
	p.mu.Unlock()
	return nil
}

func TestReadLoopDoesNotWriteACKSynchronously(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransportWithOptions(bPC, aPC.addr, key(), 2, 1, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	releaseACK := make(chan struct{})
	defer close(releaseACK)
	defer closeFloodTransports(a, b)

	ackStarted := make(chan struct{}, 1)
	aPC.drop = func(_ []byte, _ net.Addr) bool {
		ackStarted <- struct{}{}
		<-releaseACK
		return true
	}
	contacts := make(chan struct{}, 2)
	a.mu.Lock()
	a.afterAuthenticatedPacket = func() { contacts <- struct{}{} }
	// Keep diagnostics local to this test so each snapshot has an explicit
	// causative event instead of depending on observer scheduling.
	a.diagnosticCh = make(chan Diagnostic, 1)
	a.mu.Unlock()

	if err := b.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, contacts, "first authenticated record")
	for range 7 {
		awaitSignal(t, clk.timerCreated, "transport and ACK timer creation")
	}
	clk.advance(maxACKDelay)
	awaitSignal(t, ackStarted, "blocked ACK")

	clk.advance(time.Second)
	if err := b.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, contacts, "second authenticated record while ACK write is blocked")
	wantComplete := clk.Now()
	eventually(t, time.Second, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		return a.lastCompleteRecord.Equal(wantComplete)
	})
	a.emitDiagnostic()
	diagnostic := awaitResult(t, a.diagnosticCh, "second-record diagnostic")
	if diagnostic.SinceCompleteRecord != 0 {
		t.Fatalf("second record age=%v, want 0 before ACK release", diagnostic.SinceCompleteRecord)
	}
}

func TestCumulativeACKCoalescesForBoundedDelay(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	for range 3 {
		awaitSignal(t, clk.timerCreated, "transport timer creation")
	}

	acks := make(chan uint64, 2)
	aPC.drop = func(pkt []byte, _ net.Addr) bool {
		_, raw, err := a.codec.Open(pkt, a.sendDir, nil, nil)
		if err != nil {
			t.Errorf("open outbound ACK: %v", err)
			return true
		}
		frag, err := pdgram.UnmarshalFragment(raw)
		if err != nil {
			t.Errorf("unmarshal outbound ACK: %v", err)
			return true
		}
		if frag.Count == 1 && len(frag.Data) == 9 && frag.Data[0] == recAck {
			acks <- binary.BigEndian.Uint64(frag.Data[1:])
		}
		return true
	}

	a.queueACK(1)
	a.queueACK(2)
	awaitSignal(t, clk.timerCreated, "ACK coalescing timer creation")
	clk.advance(maxACKDelay - time.Millisecond)
	select {
	case seq := <-acks:
		t.Fatalf("ACK %d sent before coalescing delay", seq)
	default:
	}
	clk.advance(time.Millisecond)
	if seq := awaitResult(t, acks, "coalesced ACK"); seq != 2 {
		t.Fatalf("ACK=%d, want cumulative maximum 2", seq)
	}
	select {
	case seq := <-acks:
		t.Fatalf("extra ACK %d sent for coalesced records", seq)
	default:
	}
}

func TestProbeDoesNotBlockOnDataWriter(t *testing.T) {
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := NewTransportWithOptions(bPC, aPC.addr, key(), 2, 1, Options{
		ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		_ = a.Close()
		t.Fatal(err)
	}
	defer closeFloodTransports(a, b)

	a.writeMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Probe(ctx) }()
	select {
	case err := <-done:
		a.writeMu.Unlock()
		if err != nil {
			t.Fatalf("Probe failed: %v", err)
		}
	case <-time.After(50 * time.Millisecond):
		a.writeMu.Unlock()
		<-done
		t.Fatal("Probe blocked behind data writer")
	}
}

func TestProbeDoesNotBlockBehindACKWrite(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		Clock: clk, ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	for range 3 {
		awaitSignal(t, clk.timerCreated, "transport timer creation")
	}

	ackStarted := make(chan struct{}, 1)
	probeStarted := make(chan struct{}, 1)
	releaseACK := make(chan struct{})
	defer close(releaseACK)
	aPC.drop = func(pkt []byte, _ net.Addr) bool {
		_, raw, err := a.codec.Open(pkt, a.sendDir, nil, nil)
		if err != nil {
			return true
		}
		frag, err := pdgram.UnmarshalFragment(raw)
		if err != nil || frag.Count != 1 || len(frag.Data) != 9 {
			return true
		}
		switch frag.Data[0] {
		case recAck:
			ackStarted <- struct{}{}
			<-releaseACK
		case recProbe:
			probeStarted <- struct{}{}
		}
		return true
	}

	a.queueACK(1)
	awaitSignal(t, clk.timerCreated, "ACK coalescing timer creation")
	clk.advance(maxACKDelay)
	awaitSignal(t, ackStarted, "blocked ACK")
	if !a.queueControl(recProbe, 1) {
		t.Fatal("probe control queue unexpectedly full")
	}
	awaitSignal(t, probeStarted, "probe while ACK write is blocked")
}

func TestControlWriteTimeoutBoundsBlockedPacketConn(t *testing.T) {
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
	go func() { done <- tr.sendControl(recAck, 1) }()
	select {
	case err := <-done:
		var timeout interface{ Timeout() bool }
		if !errors.As(err, &timeout) || !timeout.Timeout() {
			t.Fatalf("control write err=%v, want timeout", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("control write blocked without honoring WriteTimeout")
	}
}

func TestConcurrentWriterCannotClearEarlierDeadline(t *testing.T) {
	pc := &deadlineCapturePC{addr: testAddr("a"), done: make(chan struct{})}
	state := newWriteDeadlineState(pc)
	earlier := time.Now().Add(time.Second)
	later := earlier.Add(time.Second)
	finishEarlier, err := state.begin(earlier)
	if err != nil {
		t.Fatal(err)
	}
	defer finishEarlier()
	finishLater, err := state.begin(later)
	if err != nil {
		t.Fatal(err)
	}
	finishLater()

	got, _ := pc.deadline.Load().(time.Time)
	if !got.Equal(earlier) {
		t.Fatalf("deadline after concurrent writer completed=%v, want earlier active deadline %v", got, earlier)
	}
}

func TestFreshControlWriteRetriesAfterInheritedDeadline(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	pc := &stagedTimeoutPC{started: make(chan struct{}, 2), release: make(chan struct{})}
	tr := &Transport{clock: clk}
	deadlines := newWriteDeadlineState(pc)
	oldDone := make(chan error, 1)
	go func() {
		oldDone <- tr.writeDatagram(pc, testAddr("peer"), []byte("old"), deadlines, 10*time.Millisecond)
	}()
	awaitSignal(t, pc.started, "older blocked write")
	clk.advance(5 * time.Millisecond)
	freshDone := make(chan error, 1)
	go func() {
		freshDone <- tr.writeDatagram(pc, testAddr("peer"), []byte("fresh"), deadlines, 100*time.Millisecond)
	}()
	awaitSignal(t, pc.started, "fresh write sharing earlier deadline")
	clk.advance(5 * time.Millisecond)
	close(pc.release)
	if err := awaitResult(t, oldDone, "older write timeout"); err == nil {
		t.Fatal("older write succeeded, want timeout")
	}
	if err := awaitResult(t, freshDone, "fresh write retry"); err != nil {
		t.Fatalf("fresh write failed at inherited deadline: %v", err)
	}
}

func TestProbeReturnsLocalWriteError(t *testing.T) {
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		ResendAfter: time.Hour, Heartbeat: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	if err := bPC.Close(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := a.Probe(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Probe err=%v, want immediate local write error", err)
	}
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("Probe returned local write error after %v", elapsed)
	}
}

func TestHeartbeatContinuesDuringRetransmitStorm(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	const heartbeat = 11 * time.Millisecond
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		Clock: clk, MTU: 128, ResendAfter: 10 * time.Millisecond, Heartbeat: heartbeat,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	for range 3 {
		awaitSignal(t, clk.timerCreated, "retransmit, health, and heartbeat timer creation")
	}

	retransmitStarted := make(chan struct{}, 1)
	releaseRetransmit := make(chan struct{})
	defer close(releaseRetransmit)
	heartbeats := make(chan struct{}, 2)
	aPC.drop = func(pkt []byte, _ net.Addr) bool {
		_, raw, err := a.codec.Open(pkt, a.sendDir, nil, nil)
		if err != nil {
			t.Errorf("open outbound packet: %v", err)
			return true
		}
		frag, err := pdgram.UnmarshalFragment(raw)
		if err != nil {
			t.Errorf("unmarshal outbound fragment: %v", err)
			return true
		}
		if frag.Count == 1 && len(frag.Data) == 9 && frag.Data[0] == recProbe {
			heartbeats <- struct{}{}
			return true
		}
		select {
		case retransmitStarted <- struct{}{}:
			<-releaseRetransmit
		default:
		}
		return true
	}

	now := clk.Now()
	a.mu.Lock()
	for seq := uint64(1); seq <= 64; seq++ {
		a.pending[seq] = &pending{
			frame:    ports.Frame{Type: ports.MsgInput, Payload: make([]byte, 24*a.mtu)},
			enqueued: now.Add(-time.Second), first: now.Add(-time.Second), last: now.Add(-time.Second),
		}
	}
	a.mu.Unlock()

	clk.advance(10 * time.Millisecond)
	awaitSignal(t, retransmitStarted, "first retransmit in 64-record storm")
	for interval := 1; interval <= 2; interval++ {
		clk.advance(heartbeat)
		awaitSignal(t, heartbeats, "heartbeat during retransmit storm")
	}
}

func TestHealthChecksContinueDuringRetransmitStorm(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	a, err := NewTransportWithOptions(aPC, bPC.addr, key(), 1, 2, Options{
		Clock: clk, MTU: 128, ResendAfter: 10 * time.Millisecond,
		Heartbeat: time.Hour, DegradedAfter: 10 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = a.Close() }()
	for range 3 {
		awaitSignal(t, clk.timerCreated, "transport timer creation")
	}

	retransmitStarted := make(chan struct{}, 1)
	releaseRetransmit := make(chan struct{})
	defer close(releaseRetransmit)
	aPC.drop = func(_ []byte, _ net.Addr) bool {
		select {
		case retransmitStarted <- struct{}{}:
			<-releaseRetransmit
		default:
		}
		return true
	}

	now := clk.Now()
	a.mu.Lock()
	a.pending[1] = &pending{
		frame:    ports.Frame{Type: ports.MsgInput, Payload: make([]byte, 24*a.mtu)},
		enqueued: now.Add(-time.Second), first: now.Add(-time.Second), last: now.Add(-time.Second),
	}
	a.mu.Unlock()

	clk.advance(10 * time.Millisecond)
	awaitSignal(t, retransmitStarted, "blocked retransmit")
	select {
	case event := <-a.LinkEvents():
		if event.State != ports.LinkStateDegraded {
			t.Fatalf("link state=%s, want degraded", event.State)
		}
	case <-time.After(time.Second):
		t.Fatal("health check did not run while retransmit sender was blocked")
	}
}
