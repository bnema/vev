package dgram

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
	"github.com/stretchr/testify/require"
)

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
	large := mustMarshalOutput(ports.Output{Epoch: 1, Base: 1, New: 2, Size: domain.Size{Cols: 1, Rows: 1}, Data: make([]byte, 24*a.mtu)})
	for range 2 {
		if err := a.SendAsync(ports.Frame{Type: ports.MsgOutput, Payload: large}); err != nil {
			t.Fatal(err)
		}
	}
	waitForManualTimers(t, clk, 4)
	previousPackets := len(aPC.in)
	for tick := range 128 {
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
	for range 128 {
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
	sideEffect := ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{Epoch: 1, Size: domain.Size{Cols: 1, Rows: 1}, Data: make([]byte, 100*1024)})}
	done := make(chan error, 1)
	go func() { done <- a.SendSynchronous(sideEffect) }()

	for range 512 {
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
		{name: "terminal side effect", frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{Epoch: 1, Size: domain.Size{Cols: 1, Rows: 1}, Data: []byte("osc")})}},
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
