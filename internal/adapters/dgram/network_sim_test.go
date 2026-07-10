package dgram

import (
	"bytes"
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
	dropped    int
	maxDelay   time.Duration
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
		l.dropped++
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
	l.maxDelay = max(l.maxDelay, due.Sub(meta.At))
	l.queue = append(l.queue, scheduledPacket{packet: packet{b: append([]byte(nil), b...), addr: p.addr}, due: due})
	l.queueBytes += len(b)
	return true
}

func (l *simulatedLink) flush(peer *fakePC) int {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	delivered := 0
	for len(l.queue) > 0 && !l.queue[0].due.After(now) {
		q := l.queue[0]
		l.queue = l.queue[1:]
		l.queueBytes -= len(q.b)
		if l.deliverLocked(peer, q.packet) {
			delivered++
		}
	}
	return delivered
}

func (l *simulatedLink) queuedBytes() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.queueBytes
}

func (l *simulatedLink) droppedPackets() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropped
}

func (l *simulatedLink) maximumQueueDelay() time.Duration {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.maxDelay
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

func newPair() (*fakePC, *fakePC) { return newPairWithCapacity(nil, 100) }

func newFloodPair(link *simulatedLink) (*fakePC, *fakePC) {
	return newPairWithCapacity(link, floodRecordCount*floodRecordMTUs*2)
}

func newPairWithCapacity(link *simulatedLink, queuePackets int) (*fakePC, *fakePC) {
	a := &fakePC{addr: testAddr("a"), in: make(chan packet, queuePackets), read: make(chan struct{}, queuePackets), peers: map[string]*fakePC{}, link: link}
	b := &fakePC{addr: testAddr("b"), in: make(chan packet, queuePackets), read: make(chan struct{}, queuePackets), peers: map[string]*fakePC{}, link: link}
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
	afterWrite := p.afterWrite
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
	if afterWrite != nil {
		afterWrite()
	}
	return len(b), nil
}
func (p *fakePC) replaceAfterWrite(afterWrite func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.afterWrite = afterWrite
}

func (p *fakePC) addAfterWrite(afterWrite func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	previous := p.afterWrite
	p.afterWrite = func() {
		if previous != nil {
			previous()
		}
		afterWrite()
	}
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

const (
	floodRecordCount = 32
	floodRecordMTUs  = 24
)

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

func flushAndAwait(t *testing.T, link *simulatedLink, peer *fakePC, processed <-chan struct{}) int {
	t.Helper()
	delivered := link.flush(peer)
	for range delivered {
		awaitSignal(t, processed, "flushed packet processing")
	}
	return delivered
}

// TestTransportFloodClassification drives authenticated datagrams through real
// transports. The metrics are transport state, not simulator bookkeeping.
func TestTransportFloodClassification(t *testing.T) {
	t.Run("one real fragment per large record loss contacts without completing", func(t *testing.T) {
		clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
		aPC, bPC := newFloodPair(nil)
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
		const mtu = 128
		a, b := newFloodTransports(t, aPC, bPC, clk, mtu)
		defer closeFloodTransports(a, b)
		contacted := make(chan struct{}, 1)
		b.mu.Lock()
		b.afterAuthenticatedPacket = func() {
			select {
			case contacted <- struct{}{}:
			default:
			}
		}
		b.mu.Unlock()
		start := clk.Now()
		clk.advance(time.Nanosecond)
		sendFloodOutputs(t, a, floodRecordCount, mtu)
		awaitSignal(t, contacted, "first authenticated fragment")
		state := floodState(a, b, nil)
		if len(dropped) != floodRecordCount {
			t.Fatalf("flood state = %+v, dropped records=%d, want exactly one fragment from each of %d records", state, len(dropped), floodRecordCount)
		}
		if state.packetAge > initialPacingRTT || state.recordAge <= 0 || state.ackAge <= 0 {
			t.Fatalf("flood state = %+v, want fresh packet contact without record or ACK progress", state)
		}
		if !clk.Now().After(start) {
			t.Fatal("real authenticated fragments did not update contact")
		}
	})

	t.Run("bandwidth_queue", func(t *testing.T) {
		clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
		const (
			mtu             = 128
			oneWayDelay     = 50 * time.Millisecond
			bandwidth       = 16 * mtu
			queueLimit      = 4 * mtu
			outputCount     = 3
			outputDataBytes = 6 * mtu
		)
		var attempts, deterministicDrops atomic.Int64
		forward := newSimulatedLink(clk, packetPolicy{
			Delay:          oneWayDelay,
			MaxQueueBytes:  queueLimit,
			BytesPerSecond: bandwidth,
			Drop: func(packetMeta) bool {
				attempt := attempts.Add(1)
				if attempt%20 != 0 {
					return false
				}
				deterministicDrops.Add(1)
				return true
			},
		})
		reverse := newSimulatedLink(clk, packetPolicy{
			Delay:          oneWayDelay,
			MaxQueueBytes:  queueLimit,
			BytesPerSecond: bandwidth,
		})
		aPC, bPC := newFloodPair(nil)
		aPC.link = forward
		bPC.link = reverse
		const probeAfter = defaultProbe
		opts := Options{
			Clock: clk, MTU: mtu, ResendAfter: 200 * time.Millisecond, MaxResendAfter: 800 * time.Millisecond,
			Heartbeat: 500 * time.Millisecond, DegradedAfter: defaultDegraded,
			ProbeAfter: probeAfter, OfflineAfter: defaultOffline, DeadAfter: defaultDead,
		}
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
		waitForManualTimers(t, clk, 6)
		var contacted atomic.Bool
		processed := make(chan struct{})
		primerWritten := make(chan struct{}, 1)
		primerACKScheduled := make(chan struct{}, 1)
		primerAcknowledged := make(chan struct{}, 1)
		ackQueued := make(chan struct{}, 128)
		ackScheduled := make(chan struct{}, 128)
		ackWritten := make(chan struct{}, 128)
		aPC.replaceAfterWrite(func() {
			select {
			case primerWritten <- struct{}{}:
			default:
			}
		})
		bPC.replaceAfterWrite(func() {
			select {
			case primerAcknowledged <- struct{}{}:
			default:
			}
			select {
			case ackWritten <- struct{}{}:
			default:
			}
		})
		a.mu.Lock()
		a.afterPacketProcessed = func() { processed <- struct{}{} }
		a.mu.Unlock()
		b.mu.Lock()
		b.afterAuthenticatedPacket = func() { contacted.Store(true) }
		b.afterPacketProcessed = func() { processed <- struct{}{} }
		b.afterACKScheduled = func() {
			select {
			case primerACKScheduled <- struct{}{}:
			default:
			}
			select {
			case ackScheduled <- struct{}{}:
			default:
			}
		}
		b.afterACKQueued = func() { ackQueued <- struct{}{} }
		b.mu.Unlock()
		primer := ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("primer")})
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: primer}); err != nil {
			t.Fatal(err)
		}
		awaitSignal(t, primerWritten, "primer write")
		primerDrained := false
		awaitingPrimerACK := false
		for range 40 {
			clk.advance(25 * time.Millisecond)
			if awaitingPrimerACK {
				awaitSignal(t, primerAcknowledged, "primer ACK write")
				awaitingPrimerACK = false
			}
			if flushAndAwait(t, forward, bPC, processed) > 0 {
				awaitSignal(t, primerACKScheduled, "primer ACK scheduling")
				awaitingPrimerACK = true
			}
			flushAndAwait(t, reverse, aPC, processed)
			a.mu.Lock()
			primerDrained = len(a.pending) == 0
			a.mu.Unlock()
			if primerDrained {
				break
			}
		}
		if !primerDrained {
			t.Fatal("clean RTT primer did not drain")
		}
		<-ackQueued
		<-ackScheduled
		a.mu.Lock()
		initialCwnd := a.congestion.cwndBytes
		a.mu.Unlock()
		dataPaceStarted := make(chan struct{}, 128)
		dataWritten := make(chan struct{}, 1)
		a.mu.Lock()
		a.beforeDataPace = func() { dataPaceStarted <- struct{}{} }
		a.mu.Unlock()
		aPC.addAfterWrite(func() {
			select {
			case dataWritten <- struct{}{}:
			default:
			}
		})
		for state := 1; state < outputCount; state++ {
			payload := ports.MarshalOutput(ports.Output{
				BaseStateNum: uint64(state),
				NewStateNum:  uint64(state + 1),
				Data:         bytes.Repeat([]byte{byte(state + 1)}, outputDataBytes),
			})
			if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: payload}); err != nil {
				t.Fatal(err)
			}
		}
		awaitSignal(t, dataPaceStarted, "output pacing")
		awaitSignal(t, dataWritten, "first paced output write")

		var maxContactAge time.Duration
		pendingACKWrites := 0
		lossReducedCwnd := false
		latestState := uint64(0)
		probed := false
		var probeAt time.Duration
		probeWho := ""
		for range 2000 {
			clk.advance(25 * time.Millisecond)
			for range pendingACKWrites {
				awaitSignal(t, ackWritten, "cumulative ACK write")
			}
			pendingACKWrites = 0
			flushAndAwait(t, forward, bPC, processed)
			for {
				select {
				case <-ackQueued:
					awaitSignal(t, ackScheduled, "cumulative ACK scheduling")
					pendingACKWrites++
				default:
					goto ackRecordsDrained
				}
			}
		ackRecordsDrained:
			flushAndAwait(t, reverse, aPC, processed)
			a.mu.Lock()
			queuedOutput := len(a.outputQueue) > 0
			a.mu.Unlock()
			if queuedOutput {
				awaitSignal(t, dataPaceStarted, "output pacing")
			}

			if forward.queuedBytes() > queueLimit || reverse.queuedBytes() > queueLimit {
				t.Fatalf("queue exceeded bound: forward=%d reverse=%d limit=%d", forward.queuedBytes(), reverse.queuedBytes(), queueLimit)
			}
			if contacted.Load() {
				b.mu.Lock()
				age := clk.Now().Sub(b.lastAuthenticatedPacket)
				b.mu.Unlock()
				maxContactAge = max(maxContactAge, age)
			}
			for i, tr := range []*Transport{a, b} {
				if tr.LinkState() == ports.LinkStateProbing {
					if !probed {
						probeAt = clk.Now().Sub(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
						probeWho = []string{"sender", "receiver"}[i]
					}
					probed = true
				}
				select {
				case event := <-tr.LinkEvents():
					if event.State == ports.LinkStateProbing && !probed {
						probeAt = event.At.Sub(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
						probeWho = []string{"sender", "receiver"}[i]
					}
					probed = probed || event.State == ports.LinkStateProbing
				default:
				}
			}
			for {
				select {
				case frame := <-b.in:
					output, err := ports.UnmarshalOutput(frame.Payload)
					if err != nil {
						t.Fatal(err)
					}
					latestState = max(latestState, output.NewStateNum)
				default:
					goto drainedOutput
				}
			}
		drainedOutput:
			a.mu.Lock()
			pending := len(a.pending)
			lossReducedCwnd = lossReducedCwnd || a.congestion.cwndBytes < initialCwnd
			a.mu.Unlock()
			if pending == 0 && latestState == outputCount && forward.queuedBytes() == 0 && reverse.queuedBytes() == 0 {
				break
			}
		}

		a.mu.Lock()
		pending := len(a.pending)
		srtt := a.srtt
		a.mu.Unlock()
		if got, want := deterministicDrops.Load(), attempts.Load()/20; got == 0 || got != want {
			t.Fatalf("deterministic loss=%d/%d, want exactly one in twenty attempts (%d)", got, attempts.Load(), want)
		}
		if forward.droppedPackets() == 0 {
			t.Fatal("bounded bandwidth queue never overflowed")
		}
		if delay := max(forward.maximumQueueDelay(), reverse.maximumQueueDelay()); delay < 2*oneWayDelay || delay >= probeAfter {
			t.Fatalf("maximum queue delay=%v, want in [%v,%v)", delay, 2*oneWayDelay, probeAfter)
		}
		if srtt < 2*oneWayDelay || srtt >= probeAfter {
			t.Fatalf("smoothed RTT=%v, want in [%v,%v)", srtt, 2*oneWayDelay, probeAfter)
		}
		if !contacted.Load() || maxContactAge >= probeAfter {
			t.Fatalf("authenticated contact=%v maximum age=%v, want fresh below %v", contacted.Load(), maxContactAge, probeAfter)
		}
		if probed {
			t.Fatalf("healthy lossy transfer entered probing: %s at %v pending=%d latest=%d attempts=%d loss=%d overflow=%d", probeWho, probeAt, pending, latestState, attempts.Load(), deterministicDrops.Load(), forward.droppedPackets())
		}
		if !lossReducedCwnd {
			t.Fatal("deterministic loss did not reduce congestion window")
		}
		if pending != 0 || latestState != outputCount {
			t.Fatalf("convergence pending=%d latest state=%d, want 0 and %d", pending, latestState, outputCount)
		}
	})

	t.Run("control stays responsive while fresh output and large retransmits contend", func(t *testing.T) {
		clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
		aPC, bPC := newFloodPair(nil)
		inspect, err := pdgram.NewCodec(key())
		if err != nil {
			t.Fatal(err)
		}

		acksDropped := make(chan struct{}, floodRecordCount)
		bPC.drop = func(pkt []byte, _ net.Addr) bool {
			_, raw, err := inspect.Open(pkt, 2, nil, nil)
			if err != nil {
				return false
			}
			frag, err := pdgram.UnmarshalFragment(raw)
			if err != nil || frag.Count != 1 || len(frag.Data) != 9 || frag.Data[0] != recAck {
				return false
			}
			select {
			case acksDropped <- struct{}{}:
			default:
			}
			return true
		}

		retransmitBlocked := make(chan struct{}, 1)
		releaseRetransmit := make(chan struct{})
		lastRetransmitted := make(chan struct{}, 1)
		probeSent := make(chan struct{}, 1)
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
				select {
				case retransmitBlocked <- struct{}{}:
				default:
				}
				awaitSignal(t, releaseRetransmit, "retransmit release")
				if seq == floodRecordCount {
					lastRetransmitted <- struct{}{}
				}
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
		a.mu.Lock()
		a.rto = time.Hour
		a.mu.Unlock()

		// Fresh, fragmented output records become overdue while their real
		// cumulative ACKs are lost. Disabling pacing intentionally forces overload
		// to classify contention; it does not claim production pacing creates it.
		sendFloodOutputs(t, a, floodRecordCount, 128)
		awaitSignal(t, clk.timerCreated, "ACK coalescing timer creation")
		clk.advance(maxACKDelay)
		awaitSignal(t, acksDropped, "dropped cumulative ACK")
		a.mu.Lock()
		a.rto = 10 * time.Millisecond
		a.mu.Unlock()
		a.queueRetransmits()
		a.mu.Lock()
		a.rto = time.Hour
		a.mu.Unlock()
		clk.advance(initialPacingRTT)

		awaitSignal(t, retransmitBlocked, "retransmit pacing barrier")
		if state := floodState(a, b, nil); state.retransmits < floodRecordCount {
			t.Fatalf("flood state = %+v, want all %d overdue records selected", state, floodRecordCount)
		}
		select {
		case <-probeSent:
		default:
		}

		// A fresh record queues behind the sole data sender while control traffic
		// remains independent of congestion tokens and the blocked data write.
		go func() {
			freshReturned <- a.Send(ports.Frame{Type: ports.MsgOutput, Payload: floodOutputPayload(floodRecordCount, 128)})
		}()
		select {
		case err := <-freshReturned:
			t.Fatalf("fresh output completed while retransmit held pacing: %v", err)
		default:
		}

		clk.advance(11 * time.Millisecond)
		awaitSignal(t, probeSent, "heartbeat probe during output contention")

		close(releaseRetransmit)
		for range 1024 {
			clk.advance(initialPacingRTT)
			time.Sleep(time.Millisecond)
			select {
			case err := <-freshReturned:
				if err != nil {
					t.Fatal(err)
				}
				awaitSignal(t, lastRetransmitted, "last overdue record retransmission")
				writesMu.Lock()
				defer writesMu.Unlock()
				if writes[1] < 2 || writes[floodRecordCount] < 2 || writes[floodRecordCount+1] == 0 {
					t.Fatalf("flood state = %+v, writes=%v, want fresh large traffic and retransmissions across %d overdue records", floodState(a, b, nil), writes, floodRecordCount)
				}
				return
			default:
			}
		}
		writesMu.Lock()
		defer writesMu.Unlock()
		t.Fatalf("fresh output did not complete after paced retransmissions: writes=%v state=%+v", writes, floodState(a, b, nil))
	})
}

type floodTransportState struct {
	packetAge, recordAge, ackAge time.Duration
	queuedBytes                  int
	retransmits                  uint64
}

func floodState(sender, receiver *Transport, link *simulatedLink) floodTransportState {
	now := sender.clock.Now()
	receiver.mu.Lock()
	packetAge := now.Sub(receiver.lastAuthenticatedPacket)
	recordAge := now.Sub(receiver.lastCompleteRecord)
	receiver.mu.Unlock()
	sender.mu.Lock()
	ackAge := now.Sub(sender.lastACKProgress)
	retransmits := sender.retransmits
	sender.mu.Unlock()
	queuedBytes := 0
	if link != nil {
		queuedBytes = link.queuedBytes()
	}
	return floodTransportState{packetAge: packetAge, recordAge: recordAge, ackAge: ackAge, queuedBytes: queuedBytes, retransmits: retransmits}
}

func floodOutputPayload(baseState, mtu int) []byte {
	return ports.MarshalOutput(ports.Output{
		BaseStateNum: uint64(baseState),
		NewStateNum:  uint64(baseState + 1),
		Data:         make([]byte, floodRecordMTUs*mtu),
	})
}

func TestFloodOutputPayloadIsIncrementalStateBearingOutput(t *testing.T) {
	const mtu = 128
	for state := range floodRecordCount {
		payload := floodOutputPayload(state, mtu)
		output, err := ports.UnmarshalOutput(payload)
		if err != nil {
			t.Fatalf("state %d: %v", state, err)
		}
		if output.BaseStateNum != uint64(state) || output.NewStateNum != uint64(state+1) {
			t.Fatalf("output state = %d -> %d, want %d -> %d", output.BaseStateNum, output.NewStateNum, state, state+1)
		}
		minimumBytes := floodRecordMTUs * mtu
		if len(output.Data) < minimumBytes || len(payload) < minimumBytes {
			t.Fatalf("state %d: data bytes=%d encoded bytes=%d, want each at least %d", state, len(output.Data), len(payload), minimumBytes)
		}
	}
}

func sendFloodOutputs(t *testing.T, sender *Transport, count, mtu int) {
	t.Helper()
	for state := range count {
		if err := sender.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: floodOutputPayload(state, mtu)}); err != nil {
			t.Fatal(err)
		}
	}
	clk, ok := sender.clock.(*manualClock)
	if !ok {
		return
	}
	for range count * floodRecordMTUs * 2 {
		clk.advance(initialPacingRTT)
		time.Sleep(time.Millisecond)
		sender.mu.Lock()
		complete := len(sender.outputQueue) == 0 && len(sender.dataSend) == 0
		for _, p := range sender.pending {
			complete = complete && !p.initialInFlight && !p.last.IsZero()
		}
		sender.mu.Unlock()
		if complete {
			return
		}
	}
	t.Fatal("flood output pacing did not drain")
}

func newFloodTransports(t *testing.T, aPC, bPC *fakePC, clk *manualClock, mtu int) (*Transport, *Transport) {
	t.Helper()
	opts := Options{
		Clock: clk, MTU: mtu, ResendAfter: time.Hour, Heartbeat: time.Hour,
		DegradedAfter: time.Hour, ProbeAfter: time.Hour, OfflineAfter: time.Hour, DeadAfter: time.Hour,
	}
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

func TestInconsistentAuthenticatedFragmentDoesNotCountAsContact(t *testing.T) {
	clk := newManualClock(time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC))
	aPC, bPC := newPair()
	a, b := newFloodTransports(t, aPC, bPC, clk, 128)
	defer closeFloodTransports(a, b)
	codec, err := pdgram.NewCodec(key())
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{}, 1)
	rejected := make(chan struct{}, 1)
	b.mu.Lock()
	b.afterAuthenticatedPacket = func() { accepted <- struct{}{} }
	b.afterMalformedFragment = func() { rejected <- struct{}{} }
	b.mu.Unlock()

	writeFragment := func(counter uint64, frag pdgram.Fragment) {
		t.Helper()
		raw, err := pdgram.MarshalFragment(frag)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := aPC.WriteTo(codec.Seal(1, counter, raw, nil), bPC.addr); err != nil {
			t.Fatal(err)
		}
	}
	writeFragment(999, pdgram.Fragment{Seq: 42, Index: 0, Count: 2, Data: []byte("first")})
	awaitSignal(t, accepted, "accepted incomplete fragment")
	b.mu.Lock()
	lastContact := b.lastAuthenticatedPacket
	acceptedInflight := b.reassemblyInflight
	b.mu.Unlock()
	if acceptedInflight != 1 {
		t.Fatalf("reassembly inflight after accepted fragment=%d, want 1", acceptedInflight)
	}

	clk.advance(time.Second)
	writeFragment(1000, pdgram.Fragment{Seq: 42, Index: 1, Count: 3, Data: []byte("inconsistent")})
	awaitSignal(t, rejected, "inconsistent fragment rejection")
	b.mu.Lock()
	gotContact := b.lastAuthenticatedPacket
	gotInflight := b.reassemblyInflight
	b.mu.Unlock()
	if gotContact != lastContact {
		t.Fatalf("rejected inconsistent fragment refreshed contact from %v to %v", lastContact, gotContact)
	}
	if gotInflight != 0 {
		t.Fatalf("reassembly inflight after rejected inconsistent fragment=%d, want 0", gotInflight)
	}
	diagnostic := b.diagnosticSnapshot()
	if diagnostic.ReassemblyInflight != 0 {
		t.Fatalf("diagnostic reassembly inflight after rejected inconsistent fragment=%d, want 0", diagnostic.ReassemblyInflight)
	}
}
