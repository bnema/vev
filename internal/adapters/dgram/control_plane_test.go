package dgram

import (
	"net"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

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
	awaitSignal(t, ackStarted, "blocked ACK")

	clk.advance(time.Second)
	if err := b.Send(ports.Frame{Type: ports.MsgInput, Payload: []byte("second")}); err != nil {
		t.Fatal(err)
	}
	awaitSignal(t, contacts, "second authenticated record while ACK write is blocked")
	a.emitDiagnostic()
	diagnostic := awaitResult(t, a.diagnosticCh, "second-record diagnostic")
	if diagnostic.SinceCompleteRecord != 0 {
		t.Fatalf("second record age=%v, want 0 before ACK release", diagnostic.SinceCompleteRecord)
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
	for range 2 {
		awaitSignal(t, clk.timerCreated, "resend and heartbeat timer creation")
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
