package dgram

import (
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
)

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
