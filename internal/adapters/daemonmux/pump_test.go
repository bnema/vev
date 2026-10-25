package daemonmux

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
)

// errFakeCarrierClosed is the synthetic carrier error a carrierFixture returns
// once Close released its blocked I/O. It mirrors the adapter prompt-close
// contract: Close unblocks an in-flight Send or Receive.
var errFakeCarrierClosed = errors.New("carrierFixture: closed")

// fakeFrame is one inbound carrier event: a complete envelope payload, or the
// error the next Receive must report.
type fakeFrame struct {
	payload []byte
	err     error
}

// carrierFixture is the deterministic FramedCarrier the pump tests drive. Inbound
// frames are delivered in order through a buffered channel; outbound frames are
// recorded in the order the writer sent them. Close closes a channel that every
// blocked Receive and Send observes, so it unblocks both callers exactly once.
type carrierFixture struct {
	mock  *MockFramedCarrier
	mu    sync.Mutex
	calls int
	// hold, when non-nil, makes Send wait for its release before recording.
	hold      chan struct{}
	holdOnce  sync.Once
	closeErr  error
	closeOnce sync.Once
	entered   atomic.Int64

	inbound chan fakeFrame
	sent    chan []byte
	closed  chan struct{}
}

func newCarrierFixture(t *testing.T) *carrierFixture {
	c := &carrierFixture{mock: NewMockFramedCarrier(t), inbound: make(chan fakeFrame, 1024), sent: make(chan []byte, 1024), closed: make(chan struct{})}
	c.mock.EXPECT().Receive(mock.Anything).RunAndReturn(func(ctx context.Context) ([]byte, error) {
		select {
		case frame := <-c.inbound:
			if frame.err != nil {
				return nil, frame.err
			}
			return frame.payload, nil
		case <-c.closed:
			return nil, errFakeCarrierClosed
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}).Maybe()
	c.mock.EXPECT().Send(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, payload []byte) error {
		c.entered.Add(1)
		c.mu.Lock()
		hold := c.hold
		c.mu.Unlock()
		if hold != nil {
			select {
			case <-hold:
			case <-c.closed:
				return errFakeCarrierClosed
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		select {
		case c.sent <- append([]byte(nil), payload...):
			return nil
		case <-c.closed:
			return errFakeCarrierClosed
		case <-ctx.Done():
			return ctx.Err()
		}
	}).Maybe()
	c.mock.EXPECT().Close().RunAndReturn(func() error {
		c.closeOnce.Do(func() {
			c.mu.Lock()
			c.calls++
			c.mu.Unlock()
			close(c.closed)
		})
		c.mu.Lock()
		defer c.mu.Unlock()
		return c.closeErr
	}).Maybe()
	return c
}

func (c *carrierFixture) closeCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

// setHold makes the next Send wait until release is called.
func (c *carrierFixture) setHold(hold chan struct{}) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hold = hold
}

// release unblocks a held Send exactly once.
func (c *carrierFixture) release(hold chan struct{}) {
	c.holdOnce.Do(func() { close(hold) })
}

// enteredSends reports how many Send calls the writer has started.
func (c *carrierFixture) enteredSends() int64 { return c.entered.Load() }

// deliver queues one inbound frame for the next Receive, in order.
func (c *carrierFixture) deliver(t *testing.T, frame fakeFrame) {
	t.Helper()
	select {
	case c.inbound <- frame:
	}
}

// nextSent returns the next outbound frame the writer recorded, in order.
func (c *carrierFixture) nextSent(t *testing.T) []byte {
	t.Helper()
	payload := <-c.sent
	return payload
}

// requireNoSentSettled fails when the writer recorded an outbound frame once
// every bubble goroutine is blocked.
func (c *carrierFixture) requireNoSentSettled(t *testing.T) {
	t.Helper()
	synctest.Wait()
	select {
	case payload := <-c.sent:
		t.Fatalf("carrierFixture: unexpected outbound frame %q", payload)
	default:
	}
}

// newTestPump builds a pump over a fresh carrierFixture under the default
// ceilings and closes it when the test ends.
func newTestPump(t *testing.T, inbound EnvelopeDirection) (*Pump, *carrierFixture) {
	t.Helper()
	return newTestPumpWithCeilings(t, inbound, DefaultMuxCeilings())
}

func newTestPumpWithCeilings(t *testing.T, inbound EnvelopeDirection, ceilings MuxCeilings) (*Pump, *carrierFixture) {
	t.Helper()
	carrier := newCarrierFixture(t)
	pump, err := NewPump(carrier.mock, inbound, ceilings)
	require.NoError(t, err)
	t.Cleanup(func() { _ = pump.Close() })
	return pump, carrier
}

// openFor returns one complete, encodable attachment Open for a physical
// identity: the wire codec validates the whole request, not just the reference.
func openFor(id PhysicalStreamID) Open {
	open := testOpen()
	open.Ref = testRef(id)
	return open
}

func deliverClientFrame(t *testing.T, carrier *carrierFixture, message ClientMessage) {
	t.Helper()
	carrier.deliver(t, fakeFrame{payload: mustEncodeClient(t, message)})
}

func deliverServerFrame(t *testing.T, carrier *carrierFixture, message ServerMessage) {
	t.Helper()
	carrier.deliver(t, fakeFrame{payload: mustEncodeServer(t, message)})
}

// requireEngineSettled waits until the engine satisfies cond.
func requireEngineSettled(t *testing.T, pump *Pump, cond func(*StreamEngine) bool, msgAndArgs ...any) {
	t.Helper()
	requireSettled(t, func() bool { return cond(pump.Engine()) }, msgAndArgs...)
}

// requireStreamTerminalSettled waits until one stream has reached its
// terminal state with Done closed, then returns that settled read.
func requireStreamTerminalSettled(t *testing.T, pump *Pump, id PhysicalStreamID) StreamStatus {
	t.Helper()
	var status StreamStatus
	requireSettled(t, func() bool {
		current, ok := pump.Engine().Status(id)
		if !ok {
			return false
		}
		status = current
		return current.State == StreamTerminal && channelClosed(current.Done)
	}, "stream %d did not reach terminal", id)
	return status
}

// requireStreamClosingSettled waits until one undrained stream has settled
// into closing with its preserved chunk still queued and charged and Done still
// open, then returns that settled read. The transition is stable until the
// chunk is drained, so the returned read cannot be a stale intermediate state.
func requireStreamClosingSettled(t *testing.T, pump *Pump, id PhysicalStreamID) StreamStatus {
	t.Helper()
	var status StreamStatus
	requireSettled(t, func() bool {
		current, ok := pump.Engine().Status(id)
		if !ok {
			return false
		}
		status = current
		return current.State == StreamClosing &&
			current.QueuedChunks == 1 &&
			current.QueuedBytes == 1 &&
			!channelClosed(current.Done)
	}, "stream %d did not settle into closing with a preserved chunk", id)
	return status
}

// takeSettled waits until one stream's inbound queue yields a chunk.
func takeSettled(t *testing.T, pump *Pump, id PhysicalStreamID) ([]byte, bool) {
	t.Helper()
	var chunk []byte
	requireSettled(t, func() bool {
		taken, ok := pump.Take(id)
		if ok {
			chunk = taken
		}
		return ok
	})
	return chunk, chunk != nil
}

// decodeReset decodes one outbound Reset frame in the given direction.
func decodeReset(t *testing.T, payload []byte, direction EnvelopeDirection) Reset {
	t.Helper()
	var message any
	var err error
	if direction == DirectionServer {
		message, err = DecodeServer(payload, testEnvelopeCeiling, testChunkCeiling)
	} else {
		message, err = DecodeClient(payload, testEnvelopeCeiling, testChunkCeiling)
	}
	require.NoError(t, err)
	reset, ok := message.(Reset)
	require.True(t, ok, "outbound frame is not a Reset")
	return reset
}

func TestNewPumpValidation(t *testing.T) {
	t.Run("missing carrier", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, err := NewPump(nil, DirectionClient, DefaultMuxCeilings())
			require.ErrorIs(t, err, ErrPumpConfig)
			require.Nil(t, pump)
		})
	})
	t.Run("unknown inbound direction", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			for _, direction := range []EnvelopeDirection{0, EnvelopeDirection(200)} {
				pump, err := NewPump(newCarrierFixture(t).mock, direction, DefaultMuxCeilings())
				require.ErrorIs(t, err, ErrPumpConfig)
				require.Nil(t, pump)
			}
		})
	})
	t.Run("invalid ceilings", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, err := NewPump(newCarrierFixture(t).mock, DirectionClient, MuxCeilings{})
			require.ErrorIs(t, err, ErrInvalidCeilings)
			require.Nil(t, pump)
		})
	})
	t.Run("valid", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, carrier := newTestPumpWithCeilings(t, DirectionServer, MuxCeilings{
				MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
				StreamChunkLimit:        MinMuxChunkBytes,
				MaxStreams:              MinMuxStreams,
				MaxAggregateBytes:       MinMuxAggregateBytes,
			})
			require.NotNil(t, pump.Engine())
			require.False(t, channelClosed(pump.Done()))
			require.NoError(t, pump.Err())
			require.Equal(t, domain.RemoteFailureNone, pump.FailureKind())
			require.NoError(t, pump.Close())
			require.True(t, channelClosed(pump.Done()))
			require.Equal(t, 1, carrier.closeCalls())
			require.ErrorIs(t, pump.SendData(1, []byte("x"), nil), ErrPhysicalClosed)
			require.ErrorIs(t, pump.Send(Data{Physical: 1, Data: []byte("x")}), ErrDataNeedsCredit, "stream data never bypasses credit")
			require.ErrorIs(t, pump.Send(&Data{Physical: 1, Data: []byte("x")}), ErrDataNeedsCredit, "nor in pointer form")
		})
	})
}

// TestPumpTwoAndHundredStreams drives inbound and outbound traffic over two
// and a hundred concurrent streams: every inbound envelope is applied, every
// locally admitted stream's frame is written, and closing every stream leaves
// the physical connection open.
func TestPumpTwoAndHundredStreams(t *testing.T) {
	for _, count := range []int{2, 100} {
		t.Run(fmt.Sprintf("%d streams", count), func(t *testing.T) {
			inBubble(t, func(t *testing.T) {
				pump, carrier := newTestPump(t, DirectionClient)
				pump.Start(context.Background())

				ids := make([]PhysicalStreamID, count)
				for i := range ids {
					ids[i] = PhysicalStreamID(i + 1)
					deliverClientFrame(t, carrier, openFor(ids[i]))
				}
				requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == count })

				for _, id := range ids {
					require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
					deliverClientFrame(t, carrier, Data{Physical: id, Data: []byte{byte(id)}})
				}
				requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.AggregateBytes() == count })

				taken, ok := pump.Take(ids[0])
				require.True(t, ok)
				require.Equal(t, []byte{byte(ids[0])}, taken)
				require.Equal(t, count-1, pump.Engine().AggregateBytes())

				for _, id := range ids {
					require.NoError(t, pump.SendData(id, []byte("out"), nil))
				}
				seen := make(map[PhysicalStreamID]int, count)
				for i := 0; i < count; i++ {
					message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
					require.NoError(t, err)
					data, ok := message.(Data)
					require.True(t, ok)
					require.Equal(t, []byte("out"), data.Data)
					seen[data.Physical]++
				}
				require.Len(t, seen, count)

				for _, id := range ids {
					deliverClientFrame(t, carrier, Close{Physical: id})
				}
				// The drained stream settles at once; every other stream is closing
				// and still holds its one unread, charged chunk until it is drained.
				// The reader applies the closes asynchronously and in order, so each
				// stream is observed on its own eventual transition: a global Live
				// precondition would pass as soon as the drained stream settles,
				// before a later stream has applied its own close.
				terminal := requireStreamTerminalSettled(t, pump, ids[0])
				require.Equal(t, StreamTerminal, terminal.State)
				for _, id := range ids[1:] {
					status := requireStreamClosingSettled(t, pump, id)
					require.Equal(t, 1, status.QueuedChunks)
					require.Equal(t, 1, status.QueuedBytes)
				}
				require.Equal(t, count-1, pump.Engine().Live())
				require.Equal(t, count-1, pump.Engine().AggregateBytes())
				require.False(t, pump.Engine().Closed())
				require.False(t, channelClosed(pump.Done()))

				// Draining every preserved chunk releases its bytes and terminalizes
				// the stream; the physical connection stays open.
				for _, id := range ids[1:] {
					chunk, ok := takeSettled(t, pump, id)
					require.True(t, ok)
					require.Equal(t, []byte{byte(id)}, chunk)
				}
				requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 0 })
				require.Zero(t, pump.Engine().AggregateBytes())
				require.False(t, pump.Engine().Closed())
				require.False(t, channelClosed(pump.Done()))
				for _, id := range ids {
					status := mustStatus(t, pump.Engine(), id)
					require.Equal(t, StreamTerminal, status.State)
					require.True(t, channelClosed(status.Done))
					require.NoError(t, status.Err)
				}
			})
		})
	}
}

// TestPumpBlockedConsumerDoesNotBlockReaderOrSibling proves a logical consumer
// that never drains one stream's inbound queue neither blocks the reader nor a
// sibling: the stalled stream is reset by its own bound while the sibling keeps
// receiving.
func TestPumpBlockedConsumerDoesNotBlockReaderOrSibling(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())

		const siblings = 2
		for i := 1; i <= siblings; i++ {
			deliverClientFrame(t, carrier, openFor(PhysicalStreamID(i)))
		}
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == siblings })
		for i := 1; i <= siblings; i++ {
			require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(PhysicalStreamID(i))}))
		}

		// A peer ignores flow control and fills stream 1's whole window while the
		// consumer never Takes it.
		chunk, full := floodWindow(pump.Ceilings())
		for i := 0; i < full; i++ {
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: chunk})
		}
		requireEngineSettled(t, pump, func(e *StreamEngine) bool {
			status, ok := e.Status(1)
			return ok && status.QueuedChunks == full
		})

		// One more chunk exceeds the granted window; a sibling chunk still lands,
		// so neither the reader nor the sibling was blocked.
		deliverClientFrame(t, carrier, Data{Physical: 1, Data: chunk})
		deliverClientFrame(t, carrier, Data{Physical: 2, Data: []byte("sibling")})

		taken, ok := takeSettled(t, pump, 2)
		require.True(t, ok)
		require.Equal(t, []byte("sibling"), taken)

		stalled := mustStatus(t, pump.Engine(), 1)
		require.Equal(t, StreamTerminal, stalled.State)
		require.ErrorIs(t, stalled.Err, ErrStreamQueueFull)
		require.Equal(t, domain.RemoteFailureInvalidResponse, stalled.FailureKind)
		require.Zero(t, stalled.QueuedChunks)
		require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State)
		require.False(t, pump.Engine().Closed())
		require.False(t, channelClosed(pump.Done()))

		reset := decodeReset(t, carrier.nextSent(t), DirectionServer)
		require.Equal(t, PhysicalStreamID(1), reset.Physical)
		require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)
	})
}

// TestPumpBlockedWriterDoesNotBlockReader proves the reader keeps applying
// inbound frames while the writer is blocked inside a slow carrier Send.
func TestPumpBlockedWriterDoesNotBlockReader(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())

		deliverClientFrame(t, carrier, openFor(1))
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

		hold := make(chan struct{})
		carrier.setHold(hold)
		require.NoError(t, pump.Send(Reset{Physical: 1}))
		requireSettled(t, func() bool { return carrier.enteredSends() >= 1 })

		for i := 0; i < 3; i++ {
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte{byte(i)}})
		}
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.AggregateBytes() == 3 })

		// Nothing reaches the wire until the carrier releases the blocked write.
		carrier.requireNoSentSettled(t)
		carrier.release(hold)

		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		_, ok := message.(Reset)
		require.True(t, ok)
	})
}

// TestPumpFairWriterProgress proves the writer preserves the scheduler's
// deterministic byte-deficit round-robin order and each stream's own order.
func TestPumpFairWriterProgress(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())

		const count = 4
		ids := make([]PhysicalStreamID, count)
		for i := range ids {
			ids[i] = PhysicalStreamID(i + 1)
			deliverClientFrame(t, carrier, openFor(ids[i]))
		}
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == count })

		const rounds = 2
		for round := 0; round < rounds; round++ {
			for _, id := range ids {
				require.NoError(t, pump.SendData(id, []byte{byte(id), byte(round)}, nil))
			}
		}

		seen := make(map[PhysicalStreamID]int, count)
		for served := 0; served < rounds*count; served++ {
			want := ids[served%count]
			message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			data, ok := message.(Data)
			require.True(t, ok)
			require.Equal(t, want, data.Physical, "dequeue %d", served)
			require.Equal(t, []byte{byte(want), byte(seen[want])}, data.Data, "per-stream order preserved")
			seen[want]++
		}
		for _, id := range ids {
			require.Equal(t, rounds, seen[id])
		}
	})
}

// TestPumpResetIsolation proves one stream-local refusal schedules exactly one
// Reset for its own stream, settles it locally, and leaves the physical
// connection and every sibling healthy.
func TestPumpResetIsolation(t *testing.T) {
	t.Run("semantic refusal", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, carrier := newTestPump(t, DirectionClient)
			pump.Start(context.Background())

			for _, id := range []PhysicalStreamID{1, 2, 3} {
				deliverClientFrame(t, carrier, openFor(id))
			}
			requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 3 })
			for _, id := range []PhysicalStreamID{2, 3} {
				require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
			}

			// Stream 1 is still opening: inbound data is a stream-local state
			// refusal.
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("early")})
			requireEngineSettled(t, pump, func(e *StreamEngine) bool {
				status, ok := e.Status(1)
				return ok && status.State == StreamTerminal
			})
			settled := mustStatus(t, pump.Engine(), 1)
			require.ErrorContains(t, settled.Err, "invalid stream state")
			require.Equal(t, domain.RemoteFailureInvalidResponse, settled.FailureKind)

			deliverClientFrame(t, carrier, Data{Physical: 2, Data: []byte("sibling")})
			taken, ok := takeSettled(t, pump, 2)
			require.True(t, ok)
			require.Equal(t, []byte("sibling"), taken)

			reset := decodeReset(t, carrier.nextSent(t), DirectionServer)
			require.Equal(t, PhysicalStreamID(1), reset.Physical)
			require.True(t, reset.HasError)
			require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)

			// Exactly one Reset: a later frame for the settled stream is
			// discarded, so no second frame is scheduled.
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("late")})
			carrier.requireNoSentSettled(t)

			require.False(t, pump.Engine().Closed())
			require.False(t, channelClosed(pump.Done()))
			require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State)
			require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 3).State)
			require.Equal(t, 2, pump.Engine().Live())
		})
	})

	t.Run("queue overflow", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, carrier := newTestPump(t, DirectionClient)
			pump.Start(context.Background())

			for _, id := range []PhysicalStreamID{1, 2} {
				deliverClientFrame(t, carrier, openFor(id))
			}
			requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 2 })
			for _, id := range []PhysicalStreamID{1, 2} {
				require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
			}

			chunk, full := floodWindow(pump.Ceilings())
			for i := 0; i < full; i++ {
				deliverClientFrame(t, carrier, Data{Physical: 1, Data: chunk})
			}
			requireEngineSettled(t, pump, func(e *StreamEngine) bool {
				status, ok := e.Status(1)
				return ok && status.QueuedChunks == full
			})
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: chunk})
			requireEngineSettled(t, pump, func(e *StreamEngine) bool {
				status, ok := e.Status(1)
				return ok && status.State == StreamTerminal
			})

			deliverClientFrame(t, carrier, Data{Physical: 2, Data: []byte("sibling")})
			taken, ok := takeSettled(t, pump, 2)
			require.True(t, ok)
			require.Equal(t, []byte("sibling"), taken)

			reset := decodeReset(t, carrier.nextSent(t), DirectionServer)
			require.Equal(t, PhysicalStreamID(1), reset.Physical)
			require.Equal(t, domain.RemoteFailureInvalidResponse, reset.Error.FailureKind)
			require.False(t, pump.Engine().Closed())
		})
	})
}

// TestPumpMalformedPhysicalFatal proves a malformed outer envelope is
// physical-fatal: Done closes before every stream is terminalized, the carrier
// is closed, and both goroutines stop.
func TestPumpMalformedPhysicalFatal(t *testing.T) {
	tight := MuxCeilings{
		MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
		StreamChunkLimit:        MinMuxChunkBytes,
		MaxStreams:              MinMuxStreams,
		MaxAggregateBytes:       MinMuxAggregateBytes,
	}
	cases := []struct {
		name     string
		ceilings MuxCeilings
		payload  []byte
	}{
		{"garbage payload", DefaultMuxCeilings(), []byte{0xde, 0xad, 0xbe, 0xef}},
		{"empty payload", DefaultMuxCeilings(), nil},
		{"wrong direction", DefaultMuxCeilings(), mustEncodeServer(t, Opened{Ref: testRef(1)})},
		{"chunk above negotiated ceiling", tight, mustEncodeClient(t, Data{Physical: 1, Data: []byte("ab")})},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inBubble(t, func(t *testing.T) {
				pump, carrier := newTestPumpWithCeilings(t, DirectionClient, tc.ceilings)
				pump.Start(context.Background())
				deliverClientFrame(t, carrier, openFor(1))
				requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })

				carrier.deliver(t, fakeFrame{payload: tc.payload})
				requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Closed() })

				require.Error(t, pump.Err())
				require.Equal(t, domain.RemoteFailureInvalidResponse, pump.FailureKind())
				require.True(t, channelClosed(pump.Engine().Done()))
				status := mustStatus(t, pump.Engine(), 1)
				require.Equal(t, StreamTerminal, status.State)
				require.True(t, channelClosed(status.Done))
				require.Equal(t, domain.RemoteFailureInvalidResponse, status.FailureKind)

				requireSettled(t, func() bool {
					return channelClosed(pump.readerDone) && channelClosed(pump.writerDone)
				})
				requireSettled(t, func() bool { return carrier.closeCalls() == 1 })
			})
		})
	}
}

// TestPumpCarrierLossFanoutOrdering proves a carrier error is physical-fatal,
// that the physical Done closes before every stream Done, and that every live
// stream observes the same transport cause.
func TestPumpCarrierLossFanoutOrdering(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())

		const count = 3
		for i := 1; i <= count; i++ {
			deliverClientFrame(t, carrier, openFor(PhysicalStreamID(i)))
		}
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == count })

		// Each watcher records whether the physical Done was already closed when
		// its stream's Done fired.
		ordered := make(chan bool, count)
		for i := 1; i <= count; i++ {
			done := mustStatus(t, pump.Engine(), PhysicalStreamID(i)).Done
			go func(streamDone <-chan struct{}) {
				<-streamDone
				<-pump.Done()
				ordered <- true
			}(done)
		}

		cause := errors.New("carrier lost")
		carrier.deliver(t, fakeFrame{err: cause})
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Closed() })

		for i := 0; i < count; i++ {
			require.True(t, <-ordered, "stream Done fired before the physical Done")
		}
		require.ErrorIs(t, pump.Err(), cause)
		require.Equal(t, domain.RemoteFailureTransport, pump.FailureKind())
		require.Zero(t, pump.Engine().Live())
		for i := 1; i <= count; i++ {
			status := mustStatus(t, pump.Engine(), PhysicalStreamID(i))
			require.Equal(t, StreamTerminal, status.State)
			require.True(t, channelClosed(status.Done))
			require.ErrorIs(t, status.Err, cause)
			require.Equal(t, domain.RemoteFailureTransport, status.FailureKind)
		}
		requireSettled(t, func() bool {
			return channelClosed(pump.readerDone) && channelClosed(pump.writerDone)
		})
		require.Equal(t, 1, carrier.closeCalls())
	})
}

// TestPumpCancellationNoLeak proves cancelling the run context is an orderly
// close: the physical connection settles with a nil cause, streams settle with
// nil, the carrier is closed, and no pump goroutine survives.
func TestPumpCancellationNoLeak(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		before := runtime.NumGoroutine()
		pump, carrier := newTestPump(t, DirectionClient)
		ctx, cancel := context.WithCancel(context.Background())
		pump.Start(ctx)

		for _, id := range []PhysicalStreamID{1, 2} {
			deliverClientFrame(t, carrier, openFor(id))
		}
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 2 })

		cancel()
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Closed() })
		require.NoError(t, pump.Err())
		require.Equal(t, domain.RemoteFailureNone, pump.FailureKind())
		for _, id := range []PhysicalStreamID{1, 2} {
			status := mustStatus(t, pump.Engine(), id)
			require.Equal(t, StreamTerminal, status.State)
			require.True(t, channelClosed(status.Done))
			require.NoError(t, status.Err)
			require.Equal(t, domain.RemoteFailureNone, status.FailureKind)
		}
		requireSettled(t, func() bool { return channelClosed(pump.readerDone) })
		requireSettled(t, func() bool { return channelClosed(pump.writerDone) })
		requireSettled(t, func() bool { return carrier.closeCalls() == 1 })
		requireSettled(t, func() bool { return runtime.NumGoroutine() <= before+2 }, "pump goroutines leaked after cancellation")
	})
}

// TestPumpConcurrentClose proves concurrent Close calls are idempotent, each
// returns the same result, the carrier is closed exactly once, and Close
// unblocks a writer blocked inside the carrier and joins both goroutines
// within the adapter prompt-close contract.
func TestPumpConcurrentClose(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())

		deliverClientFrame(t, carrier, openFor(1))
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })

		hold := make(chan struct{})
		carrier.setHold(hold)
		require.NoError(t, pump.Send(Reset{Physical: 1}))
		requireSettled(t, func() bool { return carrier.enteredSends() >= 1 })

		const closers = 12
		errs := make([]error, closers)
		var wg sync.WaitGroup
		for i := 0; i < closers; i++ {
			wg.Add(1)
			go func(index int) {
				defer wg.Done()
				errs[index] = pump.Close()
			}(i)
		}
		joined := make(chan struct{})
		go func() {
			wg.Wait()
			close(joined)
		}()
		<-joined

		for i, err := range errs {
			require.NoError(t, err, "closer %d", i)
		}
		require.True(t, channelClosed(pump.Done()))
		require.NoError(t, pump.Err())
		require.Equal(t, domain.RemoteFailureNone, pump.FailureKind())
		require.Equal(t, 1, carrier.closeCalls())
		require.True(t, channelClosed(pump.readerDone))
		require.True(t, channelClosed(pump.writerDone))
		require.ErrorIs(t, pump.SendData(1, []byte("x"), nil), ErrPhysicalClosed)
	})
}

// TestPumpCloseBeforeStart proves Close never starts or joins goroutines that
// were never launched, and that Start after Close is a no-op.
func TestPumpCloseBeforeStart(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		require.NoError(t, pump.Close())
		require.True(t, channelClosed(pump.Done()))
		require.NoError(t, pump.Err())
		require.Equal(t, 1, carrier.closeCalls())

		pump.Start(context.Background())
		require.False(t, channelClosed(pump.readerDone))
		require.False(t, channelClosed(pump.writerDone))
		require.NoError(t, pump.Close())
		require.Equal(t, 1, carrier.closeCalls())
	})
}

// TestPumpSendDirection proves Send refuses a message whose direction does not
// match the pump's local side before it encodes or queues anything.
func TestPumpSendDirection(t *testing.T) {
	t.Run("daemon side writes server messages", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, carrier := newTestPump(t, DirectionClient)
			pump.Start(context.Background())
			deliverClientFrame(t, carrier, openFor(1))
			requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })
			require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

			require.ErrorIs(t, pump.Send(Open{Ref: testRef(2)}), ErrWrongDirection)
			require.ErrorIs(t, pump.Send(nil), ErrInvalidMessage)
			require.NoError(t, pump.Send(Opened{Ref: testRef(1)}))
			message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			_, ok := message.(Opened)
			require.True(t, ok)
			carrier.requireNoSentSettled(t)
		})
	})

	t.Run("client side writes client messages", func(t *testing.T) {
		inBubble(t, func(t *testing.T) {
			pump, carrier := newTestPump(t, DirectionServer)
			pump.Start(context.Background())
			require.NoError(t, pump.Engine().Open(Open{Ref: testRef(1)}))

			require.ErrorIs(t, pump.Send(Opened{Ref: testRef(1)}), ErrWrongDirection)
			require.NoError(t, pump.Send(openFor(1)))
			message, err := DecodeClient(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			_, ok := message.(Open)
			require.True(t, ok)

			// The client side reads daemon frames: an inbound Opened confirms the
			// locally admitted stream through the reader.
			deliverServerFrame(t, carrier, Opened{Ref: testRef(1)})
			requireEngineSettled(t, pump, func(e *StreamEngine) bool {
				status, ok := e.Status(1)
				return ok && status.State == StreamOpen
			})
		})
	})
}

// TestPumpFlushWaitsForInflightSend proves Flush does not return while a send
// is in flight inside the carrier, and returns once the writer finished it.
func TestPumpFlushWaitsForInflightSend(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())
		require.NoError(t, pump.Engine().Open(Open{Ref: testRef(1)}))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

		hold := make(chan struct{})
		carrier.setHold(hold)
		require.NoError(t, pump.Send(ServerMessage(Close{Physical: 1})))
		requireSettled(t, func() bool { return carrier.enteredSends() >= 1 })

		flushed := make(chan error, 1)
		go func() { flushed <- pump.Flush(context.Background()) }()
		synctest.Wait()
		select {
		case <-flushed:
			t.Fatal("Flush returned while a send was still in flight")
		default:
		}

		carrier.release(hold)
		err := <-flushed
		require.NoError(t, err)
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		closed, ok := message.(Close)
		require.True(t, ok)
		require.Equal(t, PhysicalStreamID(1), closed.Physical)
	})
}

// TestPumpFlushPublishesFinalClose proves a flushes-before-close caller makes
// the final Close observable on the wire: Send accepted means queued only, and
// Flush is the explicit barrier that drains it.
func TestPumpFlushPublishesFinalClose(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())
		require.NoError(t, pump.Engine().Open(Open{Ref: testRef(1)}))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

		require.NoError(t, pump.Send(ServerMessage(Close{Physical: 1})))
		require.NoError(t, pump.Flush(context.Background()))
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		closed, ok := message.(Close)
		require.True(t, ok)
		require.Equal(t, PhysicalStreamID(1), closed.Physical)

		// Flush of an already-drained pump returns immediately.
		require.NoError(t, pump.Flush(context.Background()))
	})
}

// TestPumpFlushHonoursContext proves Flush returns the context error when the
// deadline wins before the scheduler drains.
func TestPumpFlushHonoursContext(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		require.NoError(t, pump.Engine().Open(Open{Ref: testRef(1)}))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
		// No writer is started, so the queued frame can never drain.
		require.NoError(t, pump.SendData(1, []byte("queued"), nil))

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, pump.Flush(ctx), context.DeadlineExceeded)

		// Starting the pump drains the frame and a fresh Flush succeeds.
		pump.Start(context.Background())
		require.NoError(t, pump.Flush(context.Background()))
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		data, ok := message.(Data)
		require.True(t, ok)
		require.Equal(t, []byte("queued"), data.Data)
	})
}

// TestPumpFlushCancellationMayDrop proves Close is not a flush: a frame queued
// behind a blocked write is dropped when the context is cancelled rather than
// drained, so a caller that must publish a final frame flushes first.
func TestPumpFlushCancellationMayDrop(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		ctx, cancel := context.WithCancel(context.Background())
		pump.Start(ctx)
		require.NoError(t, pump.Engine().Open(Open{Ref: testRef(1)}))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

		hold := make(chan struct{})
		carrier.setHold(hold)
		require.NoError(t, pump.SendData(1, []byte("first"), nil))
		requireSettled(t, func() bool { return carrier.enteredSends() >= 1 })
		// The final Close is queued behind the blocked write.
		require.NoError(t, pump.Send(ServerMessage(Close{Physical: 1})))

		cancel()
		requireSettled(t, func() bool { return channelClosed(pump.writerDone) })
		carrier.release(hold)
		carrier.requireNoSentSettled(t)
	})
}

// TestPumpSendDataBackpressure proves a sender that exhausted its stream
// credit waits instead of overflowing or resetting the stream, resumes once
// the peer returns credit, and that the stream stays open throughout.
func TestPumpSendDataBackpressure(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())
		deliverClientFrame(t, carrier, openFor(1))
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

		chunk, full := floodWindow(pump.Ceilings())
		for i := 0; i < full; i++ {
			require.NoError(t, pump.SendData(1, chunk, nil))
		}
		for i := 0; i < full; i++ {
			carrier.nextSent(t)
		}

		sent := make(chan error, 1)
		go func() { sent <- pump.SendData(1, chunk, nil) }()
		synctest.Wait()
		select {
		case err := <-sent:
			t.Fatalf("SendData returned %v without credit", err)
		default:
		}
		require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 1).State, "a slow peer never resets the stream")

		deliverClientFrame(t, carrier, WindowUpdate{Physical: 1, Credit: chunkCredit(len(chunk))})
		require.NoError(t, <-sent)
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, Data{Physical: 1, Data: chunk}, message)
		require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 1).State)
	})
}

// TestPumpSendDataCancel proves a sender blocked on credit returns when its
// cancel channel closes or its stream settles.
func TestPumpSendDataCancel(t *testing.T) {
	cases := []struct {
		name   string
		settle func(*testing.T, *Pump, *carrierFixture, chan struct{})
		want   error
	}{
		{"cancel", func(_ *testing.T, _ *Pump, _ *carrierFixture, cancel chan struct{}) { close(cancel) }, ErrLogicalClosed},
		{"peer reset", func(t *testing.T, _ *Pump, carrier *carrierFixture, _ chan struct{}) {
			deliverClientFrame(t, carrier, Reset{Physical: 1})
		}, errStreamSettled},
		{"peer close with unread data", func(t *testing.T, pump *Pump, carrier *carrierFixture, _ chan struct{}) {
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("unread")})
			deliverClientFrame(t, carrier, Close{Physical: 1})
			requireEngineSettled(t, pump, func(e *StreamEngine) bool {
				status, ok := e.Status(1)
				return ok && status.State == StreamClosing
			})
		}, errStreamSettled},
		{"physical failure", func(t *testing.T, _ *Pump, carrier *carrierFixture, _ chan struct{}) {
			carrier.deliver(t, fakeFrame{err: errors.New("carriage lost")})
		}, ErrPhysicalClosed},
		{"credit beyond window resets the stream", func(t *testing.T, pump *Pump, carrier *carrierFixture, _ chan struct{}) {
			deliverClientFrame(t, carrier, WindowUpdate{Physical: 1, Credit: pump.Ceilings().StreamWindow()})
		}, errStreamSettled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			inBubble(t, func(t *testing.T) {
				pump, carrier := newTestPump(t, DirectionClient)
				pump.Start(context.Background())
				deliverClientFrame(t, carrier, openFor(1))
				requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })
				require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
				chunk, full := floodWindow(pump.Ceilings())
				for i := 0; i < full; i++ {
					require.NoError(t, pump.SendData(1, chunk, nil))
				}

				cancel := make(chan struct{})
				sent := make(chan error, 1)
				go func() { sent <- pump.SendData(1, chunk, cancel) }()
				tc.settle(t, pump, carrier, cancel)
				err := <-sent
				require.ErrorIs(t, err, tc.want)
			})
		})
	}
}

// TestPumpReturnsCreditInBatches proves the receiver grants consumed credit
// back with one WindowUpdate per batch, not one per chunk.
func TestPumpReturnsCreditInBatches(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		pump.Start(context.Background())
		deliverClientFrame(t, carrier, openFor(1))
		requireEngineSettled(t, pump, func(e *StreamEngine) bool { return e.Live() == 1 })
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))

		chunk, full := floodWindow(pump.Ceilings())
		for i := 0; i < full; i++ {
			deliverClientFrame(t, carrier, Data{Physical: 1, Data: chunk})
		}
		var granted uint64
		updates := 0
		for i := 0; i < full; i++ {
			_, ok := takeSettled(t, pump, 1)
			require.True(t, ok)
		}
		for granted < pump.Ceilings().creditReturnThreshold() {
			message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
			require.NoError(t, err)
			update, ok := message.(WindowUpdate)
			require.True(t, ok, "got %T", message)
			granted += update.Credit
			updates++
		}
		require.Less(t, updates, full, "credit is batched")
	})
}

// TestPumpInboundTerminalRetiresQueuedOutbound proves an inbound Close or Reset
// discards the stream's queued outbound frames and retires the scheduler
// record, and that 3000 stream cycles stay bounded with no post-terminal
// dequeue.
func TestPumpInboundTerminalRetiresQueuedOutbound(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionClient)
		const churn = 3000
		for i := 1; i <= churn; i++ {
			id := PhysicalStreamID(i)
			require.NoError(t, pump.Engine().Open(Open{Ref: testRef(id)}))
			require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
			require.NoError(t, pump.SendData(id, []byte("out"), nil))
			require.Equal(t, 1, pump.scheduler.QueuedFrames(id))

			if i%2 == 0 {
				require.NoError(t, pump.applyClient(Close{Physical: id}))
			} else {
				require.NoError(t, pump.applyClient(Reset{Physical: id}))
			}
			require.Zero(t, pump.scheduler.QueuedFrames(id), "queued outbound discarded")
			require.Zero(t, pump.scheduler.AggregateBytes())
			require.False(t, pump.Engine().Closed())
		}
		require.Zero(t, pump.Engine().Live())
		require.Zero(t, pump.Engine().AggregateBytes())
		require.LessOrEqual(t, len(pump.scheduler.streams), MaxRetiredSchedulerRecords)
		_, ok := pump.scheduler.Dequeue()
		require.False(t, ok, "no post-terminal dequeue")
	})
}

// watchEntries reports how many inbound watch entries the pump retains.
func watchEntries(pump *Pump) int {
	pump.watchMu.Lock()
	defer pump.watchMu.Unlock()
	return len(pump.watch)
}

// TestPumpWatchUnknownAndTerminalClosed proves Watch hands back an
// already-closed channel for zero, unknown, and terminal identities without
// retaining a watch entry, and returns one stable open channel for a live
// stream that rotates on every applied frame.
func TestPumpWatchUnknownAndTerminalClosed(t *testing.T) {
	inBubble(t, func(t *testing.T) {
		pump, _ := newTestPump(t, DirectionClient)

		require.True(t, channelClosed(pump.Watch(0)), "zero identity is closed")
		require.True(t, channelClosed(pump.Watch(9)), "unknown identity is closed")
		require.Zero(t, watchEntries(pump), "unknown identities retain no entry")

		require.NoError(t, pump.Engine().Open(openFor(1)))
		live := pump.Watch(1)
		require.False(t, channelClosed(live))
		require.Equal(t, 1, watchEntries(pump))
		require.Equal(t, live, pump.Watch(1), "a live watch channel is stable")

		// An applied frame rotates the watch channel: the old one closes and a
		// fresh open one is installed.
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
		require.NoError(t, pump.applyClient(Data{Physical: 1, Data: []byte("x")}))
		rotated := pump.Watch(1)
		require.NotEqual(t, live, rotated, "an applied frame rotates the channel")
		require.True(t, channelClosed(live))

		// A closing stream keeps its queue readable: the applied Close rotates the
		// watch channel, but the stream is not terminal until its preserved chunk is
		// taken.
		require.NoError(t, pump.applyClient(Close{Physical: 1}))
		require.True(t, channelClosed(rotated), "the close frame rotated the current channel")
		require.Equal(t, StreamClosing, mustStatus(t, pump.Engine(), 1).State)
		closingWatch := pump.Watch(1)
		require.False(t, channelClosed(closingWatch), "a closing stream is still readable")
		require.Equal(t, closingWatch, pump.Watch(1), "the closing watch channel is stable")

		chunk, ok := pump.Take(1)
		require.True(t, ok)
		require.Equal(t, []byte("x"), chunk)
		require.True(t, channelClosed(closingWatch), "draining the last chunk rotated the watch channel")
		require.True(t, channelClosed(pump.Watch(1)), "terminal identity is closed")
		require.Zero(t, watchEntries(pump), "terminal Watch retains no entry")

		// An evicted identity is closed and retains no entry either.
		for i := 2; i <= MaxRetiredStreamRecords+2; i++ {
			id := PhysicalStreamID(i)
			require.NoError(t, pump.Engine().Open(openFor(id)))
			_, _ = pump.Engine().Close(Close{Physical: id})
		}
		_, tracked := pump.Engine().Status(1)
		require.False(t, tracked, "stream 1 record was evicted")
		require.True(t, channelClosed(pump.Watch(1)), "evicted identity is closed")
		require.Zero(t, watchEntries(pump), "evicted Watch retains no entry")
	})
}
