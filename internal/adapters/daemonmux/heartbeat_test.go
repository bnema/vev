package daemonmux

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

var hbTestConfig = HeartbeatConfig{Interval: 5 * time.Second, Timeout: 10 * time.Second, ResumeTimeout: 3 * time.Second}

// hbClock drives the heartbeat through generated ports mocks. Now() returns a
// test-controlled wall time (built with time.Date, so it has no monotonic
// reading, exactly like a Round(0) value). The tick and reply-deadline timers
// are MockTimers whose channels the test feeds to "fire" them; every Reset
// and Stop is reported on a channel so tests synchronize on the heartbeat
// goroutine's own actions instead of sleeping. A suspend is a wall jump handed
// to fireTick/fireDead that the timers' planned periods do not account for.
type hbClock struct {
	clock *portsmocks.MockClock

	mu  sync.Mutex
	now time.Time

	tickCh, deadCh         chan time.Time
	tickResets, deadResets chan time.Duration
	deadStops              chan struct{}
}

func newHBClock(t *testing.T) *hbClock {
	t.Helper()
	c := &hbClock{
		now: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		// Unbuffered, so firing a timer returns only once the heartbeat took
		// the event, and it handles it before it can select another one.
		tickCh:     make(chan time.Time),
		deadCh:     make(chan time.Time),
		tickResets: make(chan time.Duration, 64),
		deadResets: make(chan time.Duration, 64),
		deadStops:  make(chan struct{}, 64),
	}
	tick := portsmocks.NewMockTimer(t)
	tick.EXPECT().C().Return(c.tickCh).Maybe()
	tick.EXPECT().Reset(mock.Anything).RunAndReturn(func(d time.Duration) bool { c.tickResets <- d; return true }).Maybe()
	tick.EXPECT().Stop().Return(false).Maybe()
	dead := portsmocks.NewMockTimer(t)
	dead.EXPECT().C().Return(c.deadCh).Maybe()
	dead.EXPECT().Reset(mock.Anything).RunAndReturn(func(d time.Duration) bool { c.deadResets <- d; return true }).Maybe()
	dead.EXPECT().Stop().RunAndReturn(func() bool { c.deadStops <- struct{}{}; return false }).Maybe()

	c.clock = portsmocks.NewMockClock(t)
	c.clock.EXPECT().Now().RunAndReturn(c.wall).Maybe()
	c.clock.EXPECT().NewTimer(hbTestConfig.Interval).Return(tick).Maybe()
	c.clock.EXPECT().NewTimer(hbTestConfig.Timeout).Return(dead).Maybe()
	return c
}

func (c *hbClock) wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *hbClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// fireTick moves the wall clock forward by elapsed and fires the tick timer.
// It returns once the heartbeat re-armed the tick, which it does last, so any
// ping for this tick is already queued; probed reports whether it armed the
// reply deadline and deadline is the value it armed.
func (c *hbClock) fireTick(elapsed time.Duration) (deadline time.Duration, probed bool) {
	c.advance(elapsed)
	c.tickCh <- c.wall()
	<-c.tickResets
	select {
	case deadline = <-c.deadResets:
		return deadline, true
	default:
		return 0, false
	}
}

// fireDead moves the wall clock forward by elapsed and fires the reply
// deadline timer.
func (c *hbClock) fireDead(elapsed time.Duration) {
	c.advance(elapsed)
	c.deadCh <- c.wall()
}

// hbHarness is a started broker-side pump (inbound DirectionServer) running the
// heartbeat on the mock clock.
type hbHarness struct {
	*hbClock
	pump    *Pump
	carrier *fakeCarrier
}

func newHBHarness(t *testing.T) *hbHarness {
	t.Helper()
	clk := newHBClock(t)
	carrier := newFakeCarrier()
	pump, err := NewPump(carrier, DirectionServer, DefaultMuxCeilings(), WithPumpHeartbeat(clk.clock, hbTestConfig))
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	pump.Start(context.Background())
	<-clk.deadStops // Start parks the reply-deadline timer.
	return &hbHarness{hbClock: clk, pump: pump, carrier: carrier}
}

// tickPing fires one tick and requires a ping with the given nonce on the wire
// and the given reply deadline armed.
func (h *hbHarness) tickPing(t *testing.T, elapsed time.Duration, nonce uint64, wantDeadline time.Duration) {
	t.Helper()
	deadline, probed := h.fireTick(elapsed)
	require.True(t, probed, "tick must probe")
	require.Equal(t, wantDeadline, deadline)
	h.written(t, nonce, wantDeadline)
}

// written requires the ping with the given nonce on the wire and the reply
// deadline restarted for it: the heartbeat arms the reply deadline when the
// writer handed the ping to the carrier.
func (h *hbHarness) written(t *testing.T, nonce uint64, wantDeadline time.Duration) {
	t.Helper()
	requireSentPing(t, h.carrier, nonce)
	require.Equal(t, wantDeadline, <-h.deadResets)
}

// reply delivers one inbound frame and waits until the heartbeat observed it
// and cleared the reply deadline.
func (h *hbHarness) reply(t *testing.T, frame ServerMessage) {
	t.Helper()
	deliverServerFrame(t, h.carrier, frame)
	<-h.deadStops
}

func (h *hbHarness) requireAlive(t *testing.T) {
	t.Helper()
	require.False(t, channelClosed(h.pump.Done()))
	require.NoError(t, h.pump.Err())
}

func (h *hbHarness) requireTimedOut(t *testing.T) {
	t.Helper()
	<-h.pump.Done()
	require.ErrorIs(t, h.pump.Err(), ErrHeartbeatTimeout)
	require.Equal(t, domain.RemoteFailureTimeout, h.pump.FailureKind())
	<-h.pump.hbDone
}

// openStream admits and confirms one local stream so data can be sent on it.
func (h *hbHarness) openStream(t *testing.T, id PhysicalStreamID) {
	t.Helper()
	require.NoError(t, h.pump.Engine().Open(openFor(id)))
	require.NoError(t, h.pump.Engine().Opened(Opened{Ref: testRef(id)}))
}

func requireSentPing(t *testing.T, carrier *fakeCarrier, nonce uint64) {
	t.Helper()
	message, err := DecodeClient(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Ping{Nonce: nonce}, message)
}

func requireSentPong(t *testing.T, carrier *fakeCarrier, nonce uint64) {
	t.Helper()
	message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Pong{Nonce: nonce}, message)
}

func TestHeartbeatConfigResolve(t *testing.T) {
	tests := []struct {
		name    string
		cfg     HeartbeatConfig
		want    HeartbeatConfig
		wantErr bool
	}{
		{name: "zero selects defaults", cfg: HeartbeatConfig{}, want: HeartbeatConfig{Interval: 5 * time.Second, Timeout: 10 * time.Second, ResumeTimeout: 3 * time.Second}},
		{name: "explicit values kept", cfg: hbTestConfig, want: hbTestConfig},
		{name: "partial fills the rest", cfg: HeartbeatConfig{Timeout: time.Second}, want: HeartbeatConfig{Interval: 5 * time.Second, Timeout: time.Second, ResumeTimeout: 3 * time.Second}},
		{name: "negative interval refused", cfg: HeartbeatConfig{Interval: -1}, wantErr: true},
		{name: "negative timeout refused", cfg: HeartbeatConfig{Timeout: -1}, wantErr: true},
		{name: "negative resume refused", cfg: HeartbeatConfig{ResumeTimeout: -1}, wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.cfg.resolve()
			if tc.wantErr {
				require.ErrorIs(t, err, ErrPumpConfig)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
	require.Equal(t, HeartbeatConfig{Interval: 5 * time.Second, Timeout: 10 * time.Second, ResumeTimeout: 3 * time.Second}, DefaultHeartbeatConfig())
}

func TestNewPumpHeartbeatValidation(t *testing.T) {
	clock := portsmocks.NewMockClock(t)
	tests := []struct {
		name    string
		inbound EnvelopeDirection
		opt     PumpOption
		wantErr bool
	}{
		{name: "broker side accepts", inbound: DirectionServer, opt: WithPumpHeartbeat(clock, hbTestConfig)},
		{name: "daemon side refuses", inbound: DirectionClient, opt: WithPumpHeartbeat(clock, hbTestConfig), wantErr: true},
		{name: "nil clock refused", inbound: DirectionServer, opt: WithPumpHeartbeat(nil, hbTestConfig), wantErr: true},
		{name: "negative config refused", inbound: DirectionServer, opt: WithPumpHeartbeat(clock, HeartbeatConfig{Interval: -1}), wantErr: true},
		{name: "nil option ignored", inbound: DirectionClient, opt: nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pump, err := NewPump(newFakeCarrier(), tc.inbound, DefaultMuxCeilings(), tc.opt)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrPumpConfig)
				return
			}
			require.NoError(t, err)
			require.NoError(t, pump.Close())
		})
	}
}

// TestPumpAnswersPingWithPong proves the daemon side echoes every nonce.
func TestPumpAnswersPingWithPong(t *testing.T) {
	tests := []struct {
		name   string
		nonces []uint64
	}{
		{name: "single", nonces: []uint64{1}},
		{name: "multibyte varint", nonces: []uint64{300}},
		{name: "max nonce", nonces: []uint64{math.MaxUint64}},
		{name: "every ping answered in order", nonces: []uint64{1, 2, 3}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pump, carrier := newTestPump(t, DirectionClient)
			pump.Start(context.Background())
			for _, nonce := range tc.nonces {
				deliverClientFrame(t, carrier, Ping{Nonce: nonce})
				requireSentPong(t, carrier, nonce)
			}
			require.False(t, channelClosed(pump.Done()))
			require.NoError(t, pump.Err())
		})
	}
}

// TestPumpPongBypassesFullScheduler proves a Pong is neither refused nor
// queued behind stream frames when the stream scheduler is full: it is written
// next, ahead of every scheduled frame, and the slot stays bounded to the
// newest unsent pong.
func TestPumpPongBypassesFullScheduler(t *testing.T) {
	pump, carrier := newTestPump(t, DirectionClient)
	pump.Start(context.Background())

	for _, id := range []PhysicalStreamID{1, 2} {
		deliverClientFrame(t, carrier, openFor(id))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == 2 })
	for _, id := range []PhysicalStreamID{1, 2} {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
	}

	// Park the writer inside the first data frame, queue two more behind it,
	// then make the scheduler refuse further frames (overflowing the sibling
	// stream, so stream 1's queue stays intact).
	hold := make(chan struct{})
	carrier.setHold(hold)
	require.NoError(t, pump.SendData(1, []byte("a"), nil))
	require.Eventually(t, func() bool { return carrier.enteredSends() >= 1 }, 5*time.Second, time.Millisecond)
	require.NoError(t, pump.SendData(1, []byte("b"), nil))
	require.NoError(t, pump.SendData(1, []byte("c"), nil))
	pump.scheduler.maxAggregateBytes = pump.scheduler.AggregateBytes()
	require.ErrorIs(t, pump.scheduler.EnqueueServer(Data{Physical: 2, Data: []byte("d")}), ErrSchedulerFull)

	// Two pings while the writer is parked: the slot keeps only the newest.
	deliverClientFrame(t, carrier, Ping{Nonce: 41})
	deliverClientFrame(t, carrier, Ping{Nonce: 42})
	require.Eventually(t, func() bool {
		pump.control.mu.Lock()
		defer pump.control.mu.Unlock()
		return pump.control.pong != nil && decodePongNonce(t, pump.control.pong) == 42
	}, 5*time.Second, time.Millisecond)
	carrier.release(hold)

	data, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Data{Physical: 1, Data: []byte("a")}, data)
	requireSentPong(t, carrier, 42)
	for _, want := range []string{"b", "c"} {
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, Data{Physical: 1, Data: []byte(want)}, message)
	}
	carrier.requireNoSent(t, 20*time.Millisecond)
}

func decodePongNonce(t *testing.T, raw []byte) uint64 {
	t.Helper()
	message, err := DecodeServer(raw, testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	pong, ok := message.(Pong)
	require.True(t, ok)
	return pong.Nonce
}

// TestPumpHeartbeatAnyInboundFrameIsLiveness proves a frame of any kind, not
// only the matching pong, clears the pending reply deadline; the next tick then
// probes afresh with the normal deadline and the connection stays up.
func TestPumpHeartbeatAnyInboundFrameIsLiveness(t *testing.T) {
	tests := []struct {
		name  string
		frame ServerMessage
	}{
		{name: "matching pong", frame: Pong{Nonce: 1}},
		{name: "stale pong", frame: Pong{Nonce: 99}},
		{name: "stream frame for an unknown stream", frame: Close{Physical: 7}},
		{name: "window update for an unknown stream", frame: WindowUpdate{Physical: 7, Credit: 1}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHBHarness(t)
			h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)
			h.reply(t, tc.frame)
			h.tickPing(t, hbTestConfig.Interval, 2, hbTestConfig.Timeout)
			h.requireAlive(t)
		})
	}
}

// TestPumpHeartbeatTimeoutSettlesFailure proves silence for Timeout after a
// ping fails the physical connection as a timeout, terminalizes its streams,
// closes the carrier, and stops the heartbeat goroutine.
func TestPumpHeartbeatTimeoutSettlesFailure(t *testing.T) {
	h := newHBHarness(t)
	require.NoError(t, h.pump.Engine().Open(openFor(1)))
	status, ok := h.pump.Engine().Status(1)
	require.True(t, ok)

	h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)
	h.requireAlive(t)
	h.fireDead(hbTestConfig.Timeout)

	h.requireTimedOut(t)
	<-status.Done
	final, ok := h.pump.Engine().Status(1)
	require.True(t, ok)
	require.ErrorIs(t, final.Err, ErrHeartbeatTimeout)
	require.Equal(t, domain.RemoteFailureTimeout, final.FailureKind)
	require.ErrorIs(t, h.pump.Send(Close{Physical: 1}), ErrPhysicalClosed)
	<-h.pump.writerDone
	require.Equal(t, 1, h.carrier.closeCalls())
}

// TestPumpHeartbeatSuspend proves a wall jump the timers' monotonic periods do
// not account for pings at once with the short resume deadline, a jump within
// the slack does not, and a reply after a resume restores the normal cadence.
func TestPumpHeartbeatSuspend(t *testing.T) {
	tests := []struct {
		name         string
		suspend      time.Duration
		wantDeadline time.Duration
		reply        bool
	}{
		{name: "long sleep, silence", suspend: time.Hour, wantDeadline: hbTestConfig.ResumeTimeout},
		{name: "just over the slack, silence", suspend: suspendSlack + time.Nanosecond, wantDeadline: hbTestConfig.ResumeTimeout},
		{name: "exactly the slack is not a suspend", suspend: suspendSlack, wantDeadline: hbTestConfig.Timeout},
		{name: "small jitter is not a suspend", suspend: time.Second, wantDeadline: hbTestConfig.Timeout},
		{name: "long sleep, prompt reply", suspend: time.Hour, wantDeadline: hbTestConfig.ResumeTimeout, reply: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHBHarness(t)
			// A healthy exchange first, so the suspend hits an idle link.
			h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)
			h.reply(t, Pong{Nonce: 1})

			h.tickPing(t, hbTestConfig.Interval+tc.suspend, 2, tc.wantDeadline)
			if tc.reply {
				h.reply(t, Pong{Nonce: 2})
				h.tickPing(t, hbTestConfig.Interval, 3, hbTestConfig.Timeout)
				h.requireAlive(t)
				return
			}
			h.fireDead(tc.wantDeadline)
			h.requireTimedOut(t)
		})
	}
}

// TestPumpHeartbeatSuspendWithPendingPing proves a suspend while a ping is
// already outstanding replaces the long deadline with the short one: either the
// next tick sees the jump, or the reply deadline itself fires across it and
// re-probes instead of failing a link that never had a chance to answer.
func TestPumpHeartbeatSuspendWithPendingPing(t *testing.T) {
	tests := []struct {
		name string
		fire func(h *hbHarness) (deadline time.Duration, probed bool)
	}{
		{name: "tick after suspend", fire: func(h *hbHarness) (time.Duration, bool) {
			return h.fireTick(hbTestConfig.Interval + time.Hour)
		}},
		{name: "reply deadline fires across suspend", fire: func(h *hbHarness) (time.Duration, bool) {
			h.fireDead(hbTestConfig.Timeout + time.Hour)
			return <-h.deadResets, true
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHBHarness(t)
			h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)

			deadline, probed := tc.fire(h)
			require.True(t, probed)
			require.Equal(t, hbTestConfig.ResumeTimeout, deadline)
			h.written(t, 2, hbTestConfig.ResumeTimeout)
			h.requireAlive(t)

			// With no answer to the re-probe either, the short deadline kills it.
			h.fireDead(hbTestConfig.ResumeTimeout)
			h.requireTimedOut(t)
		})
	}
}

// TestPumpHeartbeatReplyRacesDeadline proves a reply that races the reply
// deadline never fails the connection and clears the outstanding ping, whichever
// the heartbeat observes first.
func TestPumpHeartbeatReplyRacesDeadline(t *testing.T) {
	tests := []struct {
		name string
		race func(t *testing.T, h *hbHarness)
	}{
		{name: "deadline fires after the reply was already processed", race: func(t *testing.T, h *hbHarness) {
			h.reply(t, Pong{Nonce: 1})
			h.fireDead(0) // only the ordering matters, so the wall clock stays put
		}},
		{name: "deadline fires with a reply token pending", race: func(_ *testing.T, h *hbHarness) {
			h.pump.noteLive()
			h.fireDead(0)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHBHarness(t)
			h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)
			tc.race(t, h)

			// The ping is no longer outstanding: the next tick probes afresh.
			h.tickPing(t, hbTestConfig.Interval, 2, hbTestConfig.Timeout)
			h.requireAlive(t)
		})
	}
}

// TestPumpHeartbeatOutboundWrites proves outbound progress is never liveness
// (buffered transports complete writes on a dead link), and that the reply
// deadline starts when the ping is written, not while it waits behind a write.
func TestPumpHeartbeatOutboundWrites(t *testing.T) {
	sendData := func(t *testing.T, h *hbHarness, value byte) {
		t.Helper()
		require.NoError(t, h.pump.SendData(1, []byte{value}, nil))
		message, err := DecodeClient(h.carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, Data{Physical: 1, Data: []byte{value}}, message)
	}
	tests := []struct {
		name string
		run  func(t *testing.T, h *hbHarness)
	}{
		{name: "writes completing instantly on a dead link time out at the reply deadline", run: func(t *testing.T, h *hbHarness) {
			h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)
			for value := byte(1); value <= 3; value++ {
				sendData(t, h, value)
			}
			h.requireAlive(t)
			h.fireDead(hbTestConfig.Timeout)
			h.requireTimedOut(t)
		}},
		{name: "ping stuck behind a blocked write past the timeout is dead", run: func(t *testing.T, h *hbHarness) {
			hold := make(chan struct{})
			h.carrier.setHold(hold)
			require.NoError(t, h.pump.SendData(1, []byte("x"), nil))
			require.Eventually(t, func() bool { return h.carrier.enteredSends() >= 1 }, 5*time.Second, time.Millisecond)

			// The ping queues behind the blocked write and is never written;
			// the first deadline bounds that wait.
			deadline, probed := h.fireTick(hbTestConfig.Interval)
			require.True(t, probed)
			require.Equal(t, hbTestConfig.Timeout, deadline)
			h.requireAlive(t)
			h.fireDead(hbTestConfig.Timeout)
			h.requireTimedOut(t)
		}},
		{name: "ping delayed by a slow write gets a fresh reply deadline once written", run: func(t *testing.T, h *hbHarness) {
			hold := make(chan struct{})
			h.carrier.setHold(hold)
			require.NoError(t, h.pump.SendData(1, []byte{1}, nil))
			require.Eventually(t, func() bool { return h.carrier.enteredSends() >= 1 }, 5*time.Second, time.Millisecond)
			_, probed := h.fireTick(hbTestConfig.Interval)
			require.True(t, probed)

			h.carrier.release(hold) // the slow write finishes, then the ping goes out
			message, err := DecodeClient(h.carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			require.Equal(t, Data{Physical: 1, Data: []byte{1}}, message)
			h.written(t, 1, hbTestConfig.Timeout)
			h.requireAlive(t)
			h.fireDead(hbTestConfig.Timeout)
			h.requireTimedOut(t)
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newHBHarness(t)
			h.openStream(t, 1)
			tc.run(t, h)
		})
	}
}

// TestPumpHeartbeatStopsOnTerminal proves the heartbeat goroutine is joined and
// its timers released on every terminal path.
func TestPumpHeartbeatStopsOnTerminal(t *testing.T) {
	tests := []struct {
		name string
		stop func(t *testing.T, h *hbHarness, cancel context.CancelFunc)
	}{
		{name: "close", stop: func(t *testing.T, h *hbHarness, _ context.CancelFunc) { require.NoError(t, h.pump.Close()) }},
		{name: "context cancel", stop: func(_ *testing.T, _ *hbHarness, cancel context.CancelFunc) { cancel() }},
		{name: "carrier loss", stop: func(t *testing.T, h *hbHarness, _ context.CancelFunc) {
			h.carrier.deliver(t, fakeFrame{err: errFakeCarrierClosed})
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := newHBClock(t)
			carrier := newFakeCarrier()
			pump, err := NewPump(carrier, DirectionServer, DefaultMuxCeilings(), WithPumpHeartbeat(clk.clock, hbTestConfig))
			require.NoError(t, err)
			t.Cleanup(func() { _ = pump.Close() })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			pump.Start(ctx)
			<-clk.deadStops
			h := &hbHarness{hbClock: clk, pump: pump, carrier: carrier}
			h.tickPing(t, hbTestConfig.Interval, 1, hbTestConfig.Timeout)

			tc.stop(t, h, cancel)
			<-pump.hbDone
			<-pump.readerDone
			<-pump.writerDone
			require.NoError(t, pump.Close())
			require.Equal(t, 1, carrier.closeCalls())
			require.NotErrorIs(t, pump.Err(), ErrHeartbeatTimeout)
			// The stopped heartbeat no longer pings, whatever the timers do.
			carrier.requireNoSent(t, 20*time.Millisecond)
		})
	}
}

// TestPumpWithoutHeartbeatNeverPings proves the default pump has no heartbeat,
// so only the opted-in broker side pings.
func TestPumpWithoutHeartbeatNeverPings(t *testing.T) {
	pump, carrier := newTestPump(t, DirectionServer)
	pump.Start(context.Background())
	require.Nil(t, pump.hb)
	carrier.requireNoSent(t, 20*time.Millisecond)
	require.False(t, channelClosed(pump.Done()))
}

// TestPumpFlushIgnoresControlFrames proves Flush is a barrier for stream
// traffic only: a pending or in-flight heartbeat frame neither delays it nor
// changes its result, before or after Close.
func TestPumpFlushIgnoresControlFrames(t *testing.T) {
	tests := []struct {
		name string
		run  func(t *testing.T, pump *Pump, carrier *fakeCarrier)
	}{
		{name: "pending pong", run: func(t *testing.T, pump *Pump, _ *fakeCarrier) {
			pump.queuePong(1)
			require.NoError(t, pump.Flush(context.Background()))
		}},
		{name: "pending pong then close", run: func(t *testing.T, pump *Pump, _ *fakeCarrier) {
			pump.queuePong(1)
			require.NoError(t, pump.Close())
			require.NoError(t, pump.Flush(context.Background()))
		}},
		{name: "pong blocked in the carrier", run: func(t *testing.T, pump *Pump, carrier *fakeCarrier) {
			carrier.setHold(make(chan struct{}))
			pump.Start(context.Background())
			deliverClientFrame(t, carrier, Ping{Nonce: 1})
			require.Eventually(t, func() bool { return carrier.enteredSends() >= 1 }, 5*time.Second, time.Millisecond)
			require.NoError(t, pump.Flush(context.Background()))
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			pump, carrier := newTestPump(t, DirectionClient)
			tc.run(t, pump, carrier)
		})
	}
}

// TestEndpointConnectorHeartbeatEndToEnd proves the connector wires the
// heartbeat into the broker pump and a real daemon-side pump answers it: every
// ping is pong'd, so the reply deadline is cleared each round and the
// connection survives well past the timeout.
func TestEndpointConnectorHeartbeatEndToEnd(t *testing.T) {
	binding := mustServerBinding(t)
	server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
	server.serve()

	clk := newHBClock(t)
	connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings(), WithHeartbeat(clk.clock, hbTestConfig))
	require.NoError(t, err)
	physical, err := connector.Connect(context.Background(), remoteMuxEndpoint(binding.Identity(), binding.Policy(), "raw://heartbeat"))
	require.NoError(t, err)
	require.NoError(t, server.awaitAdoption(t))
	t.Cleanup(func() { _ = physical.Close() })
	<-clk.deadStops

	for range 6 {
		deadline, probed := clk.fireTick(hbTestConfig.Interval)
		require.True(t, probed)
		require.Equal(t, hbTestConfig.Timeout, deadline)
		<-clk.deadStops // the daemon's pong cleared the reply deadline (the ping's own re-arm Reset stays buffered)
	}
	require.False(t, channelClosed(physical.Done()))
	require.NoError(t, physical.Err())
}

// remoteMuxEndpoint is muxEndpoint with a remote fence.
func remoteMuxEndpoint(identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy, address string) ports.BrokerDialTarget {
	target := muxEndpoint(identity, policy, address)
	target.Fence = ports.BrokerEndpointFence{Registration: testRegistration()}
	return target
}

// TestEndpointConnectorHeartbeatOnlyOnRemoteTargets proves the heartbeat is
// chosen per Connect from the dial target's fence: a local Unix link cannot die
// silently and never pings, while a remote physical connection does.
func TestEndpointConnectorHeartbeatOnlyOnRemoteTargets(t *testing.T) {
	tests := []struct {
		name      string
		target    func(binding ServerBinding) ports.BrokerDialTarget
		wantHeart bool
	}{
		{name: "local target has no heartbeat", target: func(b ServerBinding) ports.BrokerDialTarget {
			return muxEndpoint(b.Identity(), b.Policy(), "raw://local")
		}},
		{name: "remote target has a heartbeat", wantHeart: true, target: func(b ServerBinding) ports.BrokerDialTarget {
			return remoteMuxEndpoint(b.Identity(), b.Policy(), "raw://remote")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			binding := mustServerBinding(t)
			server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
			server.serve()
			clk := newHBClock(t)
			connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings(), WithHeartbeat(clk.clock, hbTestConfig))
			require.NoError(t, err)

			physical, err := connector.Connect(context.Background(), tc.target(binding))
			require.NoError(t, err)
			require.NoError(t, server.awaitAdoption(t))
			t.Cleanup(func() { _ = physical.Close() })

			conn, ok := physical.(*PhysicalConnection)
			require.True(t, ok)
			require.Equal(t, tc.wantHeart, conn.pump.hb != nil)
		})
	}
}

func TestEndpointConnectorHeartbeatOptionValidation(t *testing.T) {
	dial := func(context.Context, ports.BrokerDialTarget) (RawFramedTransport, error) { return nil, nil }
	clock := portsmocks.NewMockClock(t)
	tests := []struct {
		name    string
		opts    []ConnectorOption
		wantErr bool
	}{
		{name: "nil clock", opts: []ConnectorOption{WithHeartbeat(nil, hbTestConfig)}, wantErr: true},
		{name: "negative config", opts: []ConnectorOption{WithHeartbeat(clock, HeartbeatConfig{Timeout: -1})}, wantErr: true},
		{name: "nil option ignored", opts: []ConnectorOption{nil, WithHeartbeat(clock, HeartbeatConfig{})}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewEndpointConnector(dial, DefaultMuxCeilings(), tc.opts...)
			if tc.wantErr {
				require.ErrorIs(t, err, ErrConnectorConfig)
				return
			}
			require.NoError(t, err)
		})
	}
}
