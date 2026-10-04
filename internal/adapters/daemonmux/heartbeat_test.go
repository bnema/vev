package daemonmux

import (
	"bytes"
	"context"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

// hbClock is a generated MockClock handing out one MockTimer whose channel the
// test feeds to fire a tick. Every Reset is reported on resets, so a test
// synchronizes on the heartbeat goroutine's own action instead of sleeping.
type hbClock struct {
	clock  *portsmocks.MockClock
	tickCh chan time.Time
	resets chan time.Duration
}

func newHBClock(t *testing.T) *hbClock {
	t.Helper()
	c := &hbClock{tickCh: make(chan time.Time), resets: make(chan time.Duration, 64)}
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return((<-chan time.Time)(c.tickCh)).Maybe()
	timer.EXPECT().Reset(heartbeatInterval).RunAndReturn(func(d time.Duration) bool { c.resets <- d; return true }).Maybe()
	timer.EXPECT().Stop().Return(true).Maybe()
	c.clock = portsmocks.NewMockClock(t)
	c.clock.EXPECT().NewTimer(heartbeatInterval).Return(timer).Maybe()
	return c
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

// awaitLive waits until the reader recorded an inbound frame.
func awaitLive(t *testing.T, pump *Pump) {
	t.Helper()
	require.Eventually(t, func() bool { return len(pump.live) == 1 }, 5*time.Second, time.Millisecond)
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
			require.NoError(t, pump.Err())
		})
	}
}

// TestPumpPongBypassesFullScheduler proves a Pong is neither refused nor queued
// behind stream frames when the stream scheduler is full: it is written next,
// ahead of every scheduled frame.
func TestPumpPongBypassesFullScheduler(t *testing.T) {
	// The aggregate floor holds about one maximum chunk, so a near-maximum frame
	// queued behind the in-flight one leaves no room for another.
	ceilings := DefaultMuxCeilings()
	ceilings.MaxAggregateBytes = MinMuxAggregateBytes
	pump, carrier := newTestPumpWithCeilings(t, DirectionClient, ceilings)
	pump.Start(context.Background())

	for _, id := range []PhysicalStreamID{1, 2} {
		deliverClientFrame(t, carrier, openFor(id))
	}
	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == 2 })
	for _, id := range []PhysicalStreamID{1, 2} {
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
	}

	// Park the writer inside the first data frame, queue two more behind it,
	// then make the scheduler refuse further frames.
	hold := make(chan struct{})
	carrier.setHold(hold)
	require.NoError(t, pump.SendData(1, []byte("a"), nil))
	require.Eventually(t, func() bool { return carrier.enteredSends() >= 1 }, 5*time.Second, time.Millisecond)
	full := bytes.Repeat([]byte("b"), int(MaxMuxChunkBytes)-1024)
	require.NoError(t, pump.SendData(1, full, nil))
	require.ErrorIs(t, pump.scheduler.EnqueueServer(Data{Physical: 2, Data: bytes.Repeat([]byte("d"), 2048)}), ErrSchedulerFull)

	deliverClientFrame(t, carrier, Ping{Nonce: 42})
	require.Eventually(t, func() bool {
		pump.flushMu.Lock()
		defer pump.flushMu.Unlock()
		return pump.control != nil
	}, 5*time.Second, time.Millisecond)
	carrier.release(hold)

	data, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Data{Physical: 1, Data: []byte("a")}, data)
	requireSentPong(t, carrier, 42)
	data, err = DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Data{Physical: 1, Data: full}, data)
}

// TestPumpHeartbeat drives the broker-side loop tick by tick. A step optionally
// delivers one inbound frame, fires one tick, and then expects either the next
// ping (nonce counts pings) or, on the last step, a heartbeat timeout.
func TestPumpHeartbeat(t *testing.T) {
	pong := Pong{Nonce: 1}
	tests := []struct {
		name      string
		frames    []ServerMessage // one per tick; nil = silence
		outbound  bool            // write stream data before every tick
		wantAlive bool
	}{
		{name: "pong each interval", frames: []ServerMessage{nil, pong, pong, pong}, wantAlive: true},
		{name: "stale pong is liveness", frames: []ServerMessage{nil, Pong{Nonce: 99}, Pong{Nonce: 99}}, wantAlive: true},
		{name: "stream frame is liveness", frames: []ServerMessage{nil, Close{Physical: 7}, WindowUpdate{Physical: 7, Credit: 1}}, wantAlive: true},
		{name: "one missed tick recovers", frames: []ServerMessage{nil, nil, pong, nil}, wantAlive: true},
		{name: "silence times out", frames: []ServerMessage{nil, nil, nil}},
		{name: "silence after traffic times out", frames: []ServerMessage{pong, nil, nil}},
		{name: "outbound writes are not liveness", frames: []ServerMessage{nil, nil, nil}, outbound: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clk := newHBClock(t)
			carrier := newFakeCarrier()
			pump, err := NewPump(carrier, DirectionServer, DefaultMuxCeilings(), clk.clock)
			require.NoError(t, err)
			t.Cleanup(func() { _ = pump.Close() })
			require.NoError(t, pump.Engine().Open(openFor(1)))
			status, ok := pump.Engine().Status(1)
			require.True(t, ok)
			require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
			pump.Start(context.Background())

			var pings uint64
			for i, frame := range tc.frames {
				if frame != nil {
					deliverServerFrame(t, carrier, frame)
					awaitLive(t, pump)
				}
				if tc.outbound {
					require.NoError(t, pump.SendData(1, []byte{byte(i)}, nil))
					requireSentData(t, carrier, byte(i))
				}
				clk.tickCh <- time.Time{}
				if i == len(tc.frames)-1 && !tc.wantAlive {
					break
				}
				require.Equal(t, heartbeatInterval, <-clk.resets)
				pings++
				requireSentPing(t, carrier, pings)
				require.NoError(t, pump.Err())
			}
			if tc.wantAlive {
				require.False(t, channelClosed(pump.Done()))
				return
			}
			<-pump.Done()
			require.ErrorIs(t, pump.Err(), ErrHeartbeatTimeout)
			require.Equal(t, domain.RemoteFailureTimeout, pump.FailureKind())
			<-pump.hbDone
			<-status.Done
			final, _ := pump.Engine().Status(1)
			require.ErrorIs(t, final.Err, ErrHeartbeatTimeout)
			require.ErrorIs(t, pump.Send(Close{Physical: 1}), ErrPhysicalClosed)
			<-pump.writerDone
			require.Equal(t, 1, carrier.closeCalls())
		})
	}
}

func requireSentData(t *testing.T, carrier *fakeCarrier, value byte) {
	t.Helper()
	message, err := DecodeClient(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	require.Equal(t, Data{Physical: 1, Data: []byte{value}}, message)
}

// TestPumpFlushIgnoresControlFrames proves Flush is a barrier for stream
// traffic only: a pending or in-flight pong neither delays it nor changes its
// result, before or after Close.
func TestPumpFlushIgnoresControlFrames(t *testing.T) {
	queuePong := func(pump *Pump) {
		pump.queueControl(EncodeServer(Pong{Nonce: 1}, testEnvelopeCeiling, testChunkCeiling))
	}
	tests := []struct {
		name string
		run  func(t *testing.T, pump *Pump, carrier *fakeCarrier)
	}{
		{name: "pending pong", run: func(t *testing.T, pump *Pump, _ *fakeCarrier) {
			queuePong(pump)
			require.NoError(t, pump.Flush(context.Background()))
		}},
		{name: "pending pong then close", run: func(t *testing.T, pump *Pump, _ *fakeCarrier) {
			queuePong(pump)
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

// remoteMuxEndpoint is muxEndpoint with a remote fence.
func remoteMuxEndpoint(identity ports.BrokerDaemonIdentity, policy ports.BrokerPolicy, address string) ports.BrokerDialTarget {
	target := muxEndpoint(identity, policy, address)
	target.Fence = ports.BrokerEndpointFence{Registration: testRegistration()}
	return target
}

// TestEndpointConnectorHeartbeatOnlyOnRemoteTargets proves the heartbeat is
// chosen per Connect from the dial target's fence: a local Unix link never
// pings, while a remote one pings and the real daemon-side pump answers.
func TestEndpointConnectorHeartbeatOnlyOnRemoteTargets(t *testing.T) {
	tests := []struct {
		name      string
		target    func(binding ServerBinding) ports.BrokerDialTarget
		wantHeart bool
	}{
		{name: "local target has no heartbeat", target: func(b ServerBinding) ports.BrokerDialTarget {
			return muxEndpoint(b.Identity(), b.Policy(), "raw://local")
		}},
		{name: "remote target pings and the daemon answers", wantHeart: true, target: func(b ServerBinding) ports.BrokerDialTarget {
			return remoteMuxEndpoint(b.Identity(), b.Policy(), "raw://remote")
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			binding := mustServerBinding(t)
			server := newSuperviseServer(t, binding, DefaultMuxCeilings(), 0, nil)
			server.serve()
			clk := newHBClock(t)
			connector, err := NewEndpointConnector(server.dial(), DefaultMuxCeilings(), WithHeartbeat(clk.clock))
			require.NoError(t, err)

			physical, err := connector.Connect(context.Background(), tc.target(binding))
			require.NoError(t, err)
			require.NoError(t, server.awaitAdoption(t))
			t.Cleanup(func() { _ = physical.Close() })

			pump := physical.(*PhysicalConnection).pump
			require.Equal(t, tc.wantHeart, pump.hbClock != nil)
			if !tc.wantHeart {
				return
			}
			clk.tickCh <- time.Time{}
			require.Equal(t, heartbeatInterval, <-clk.resets)
			awaitLive(t, pump) // the daemon's pong
			require.NoError(t, physical.Err())
		})
	}
}
