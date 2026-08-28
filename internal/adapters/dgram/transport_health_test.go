package dgram

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/wire"
	pdgram "github.com/bnema/vev/pkg/dgram"
)

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

	if err := a.Send(wire.Frame{Type: wire.MsgInput, Payload: []byte("survive")}); err != nil {
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
	if got.Type != wire.MsgInput || string(got.Payload) != "survive" {
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

func TestPendingAttemptsResetWhenLinkRecovers(t *testing.T) {
	tr, _ := newResendTestTransport(t, 10)
	tr.linkState = ports.LinkStateDegraded
	tr.pending[1] = &pending{frame: wire.Frame{Type: wire.MsgOutput, Payload: []byte("stale")}, attempts: 4}
	tr.pending[2] = &pending{frame: wire.Frame{Type: wire.MsgOutput, Payload: []byte("stale")}, attempts: 7}

	tr.setLinkState(ports.LinkStateConnected, nil)

	for seq, p := range tr.pending {
		if p.attempts != 0 {
			t.Fatalf("pending[%d] attempts=%d, want reset on connected recovery", seq, p.attempts)
		}
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

	if err := a.Send(wire.Frame{Type: wire.MsgInput, Payload: []byte("first")}); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 2)
	go func() { done <- a.Send(wire.Frame{Type: wire.MsgInput, Payload: []byte("second")}) }()
	go func() { done <- a.Send(wire.Frame{Type: wire.MsgInput, Payload: []byte("third")}) }()
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
	a.pending[1] = &pending{frame: wire.Frame{Type: wire.MsgOutput}, enqueued: now.Add(-time.Minute), attempts: 3}
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
	if err := tr.Send(wire.Frame{Type: wire.MsgInput, Payload: []byte("unacked")}); err != nil {
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
	tr.pending[1] = &pending{frame: wire.Frame{Type: wire.MsgOutput}, first: now, last: now}

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
