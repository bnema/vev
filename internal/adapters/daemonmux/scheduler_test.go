package daemonmux

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
)

// testScheduler returns a scheduler with an open admission gate and one open
// stream per requested physical identity, in ascending order.
func testScheduler(t *testing.T, streams int) (*StreamEngine, *Scheduler, []PhysicalStreamID) {
	t.Helper()
	engine := testEngine()
	scheduler := NewScheduler(engine)
	ids := make([]PhysicalStreamID, streams)
	for i := range ids {
		ids[i] = PhysicalStreamID(i + 1)
		openTestStream(t, engine, ids[i])
	}
	return engine, scheduler, ids
}

func countActive(scheduler *Scheduler, ids []PhysicalStreamID) int {
	active := 0
	for _, id := range ids {
		if scheduler.QueuedFrames(id) > 0 {
			active++
		}
	}
	return active
}

// overflowOne queues one small frame for id and then overflows the aggregate
// bound with a frame the budget cannot hold, so id's record is overflowed and
// empty. The aggregate is restored for the next caller.
func overflowOne(t *testing.T, scheduler *Scheduler, id PhysicalStreamID) {
	t.Helper()
	saved := scheduler.maxAggregateBytes
	defer func() { scheduler.maxAggregateBytes = saved }()
	require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("a")}))
	scheduler.maxAggregateBytes = scheduler.AggregateBytes()
	require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("b")}), ErrSchedulerFull)
}

// TestSchedulerConstants pins the negotiated bounds the scheduler is specified
// to enforce.
func TestSchedulerConstants(t *testing.T) {
	require.Equal(t, 64<<20, int(MaxMuxAggregateBytes))
	require.Equal(t, uint64(512<<10), DefaultMuxCeilings().StreamWindow())
	require.Equal(t, int64(64<<10), SchedulerQuantum)
	require.Equal(t, 4, MaxMuxSchedulerControlBurst)
}

func TestSchedulerStrings(t *testing.T) {
	require.Equal(t, "data", FrameData.String())
	require.Equal(t, "control", FrameControl.String())
	require.Equal(t, "unknown", FrameClass(0).String())
	require.Equal(t, "client", DirectionClient.String())
	require.Equal(t, "server", DirectionServer.String())
	require.Equal(t, "unknown", EnvelopeDirection(0).String())
}

func TestSchedulerNilSafety(t *testing.T) {
	var scheduler *Scheduler
	_, ok := scheduler.Dequeue()
	require.False(t, ok)
	require.False(t, scheduler.Pending())
	require.Zero(t, scheduler.Active())
	require.Zero(t, scheduler.AggregateBytes())
	require.Zero(t, scheduler.QueuedFrames(1))
	require.Zero(t, scheduler.QueuedBytes(1))
	require.False(t, scheduler.Reset(1))
	require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 1, Data: []byte("x")}), ErrInvalidMessage)
	require.ErrorIs(t, scheduler.EnqueueServer(Opened{Ref: testRef(1)}), ErrInvalidMessage)
}

// TestSchedulerAdmissionGate proves a frame is schedulable only for a stream the
// gate admitted and has not terminalized, and that a nil gate trusts the
// caller.
func TestSchedulerAdmissionGate(t *testing.T) {
	t.Run("engine gate", func(t *testing.T) {
		engine := testEngine()
		scheduler := NewScheduler(engine)
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 9, Data: []byte("x")}), ErrSchedulerUnadmitted)

		require.NoError(t, engine.Open(Open{Ref: testRef(9)}))
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: 9, Data: []byte("x")}))

		require.NoError(t, engine.Opened(Opened{Ref: testRef(9)}))
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: 9, Data: []byte("y")}))

		_, err := engine.Close(Close{Physical: 9})
		require.NoError(t, err)
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 9, Data: []byte("z")}), ErrSchedulerUnadmitted)
	})

	t.Run("nil gate", func(t *testing.T) {
		scheduler := NewScheduler(nil)
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: 7, Data: []byte("x")}))
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 0, Data: []byte("x")}), ErrInvalidMessage)
	})

	t.Run("unknown variant fails closed", func(t *testing.T) {
		scheduler := NewScheduler(nil)
		require.ErrorIs(t, scheduler.EnqueueClient(unknownClientMessage{}), ErrWrongDirection)
		require.ErrorIs(t, scheduler.EnqueueServer(unknownServerMessage{}), ErrWrongDirection)
	})
}

// TestSchedulerEnvelopeRoundTrip proves a dequeued envelope carries the
// complete serialized directional envelope, decoded by the matching codec, and
// that neither the bytes nor the committed message aliases caller memory.
func TestSchedulerEnvelopeRoundTrip(t *testing.T) {
	ceiling := DefaultMuxCeilings()
	_, scheduler, ids := testScheduler(t, 1)

	payload := []byte("payload")
	require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: payload}))
	payload[0] = 'X' // a caller mutation must never reach the queued frame

	envelope, ok := scheduler.Dequeue()
	require.True(t, ok)
	require.Equal(t, ids[0], envelope.Physical)
	require.Equal(t, DirectionClient, envelope.Direction)
	require.Equal(t, FrameData, envelope.Class)
	require.Nil(t, envelope.Server)
	data, ok := envelope.Client.(Data)
	require.True(t, ok)
	require.Equal(t, []byte("payload"), data.Data)

	decoded, err := DecodeClient(envelope.Bytes, ceiling.MaxReceiveEnvelopeBytes, ceiling.StreamChunkLimit)
	require.NoError(t, err)
	require.Equal(t, Data{Physical: ids[0], Data: []byte("payload")}, decoded)

	require.NoError(t, scheduler.EnqueueServer(Opened{Ref: testRef(ids[0])}))
	serverEnvelope, ok := scheduler.Dequeue()
	require.True(t, ok)
	require.Equal(t, DirectionServer, serverEnvelope.Direction)
	require.Equal(t, FrameControl, serverEnvelope.Class)
	require.Nil(t, serverEnvelope.Client)
	decodedServer, err := DecodeServer(serverEnvelope.Bytes, ceiling.MaxReceiveEnvelopeBytes, ceiling.StreamChunkLimit)
	require.NoError(t, err)
	require.Equal(t, Opened{Ref: testRef(ids[0])}, decodedServer)
}

// TestSchedulerTwoAndHundredStreamsFairness proves deterministic round-robin
// over a small and a large concurrent set: every stream is served once per
// round, in admission order, with its own frames in order.
func TestSchedulerTwoAndHundredStreamsFairness(t *testing.T) {
	for _, count := range []int{2, 100} {
		t.Run(fmt.Sprintf("%d streams", count), func(t *testing.T) {
			_, scheduler, ids := testScheduler(t, count)
			const rounds = 2
			for round := 0; round < rounds; round++ {
				for _, id := range ids {
					require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte{byte(id), byte(round)}}))
				}
			}
			require.Equal(t, count, scheduler.Active())

			seen := make(map[PhysicalStreamID]int, count)
			for served := 0; served < rounds*count; served++ {
				envelope, ok := scheduler.Dequeue()
				require.True(t, ok, "dequeue %d", served)
				want := ids[served%count]
				require.Equal(t, want, envelope.Physical, "dequeue %d", served)
				require.Equal(t, FrameData, envelope.Class)
				require.Equal(t, DirectionClient, envelope.Direction)
				data, ok := envelope.Client.(Data)
				require.True(t, ok)
				require.Equal(t, []byte{byte(want), byte(seen[want])}, data.Data, "per-stream order")
				seen[want]++
			}
			require.False(t, scheduler.Pending())
			require.Zero(t, scheduler.AggregateBytes())
		})
	}
}

// TestSchedulerStalledAndHotStream proves a continuously backlogged hot stream
// never starves a sibling, and a full stalled backlog never blocks another
// stream: every stream is served once per round while all queues stay full.
func TestSchedulerStalledAndHotStream(t *testing.T) {
	_, scheduler, ids := testScheduler(t, 4)

	// The hot stream keeps a maximal backlog; the siblings are refilled to full
	// whenever the round drains a slot.
	const backlog = 8
	fill := func(id PhysicalStreamID, seed byte) {
		for scheduler.QueuedFrames(id) < backlog {
			require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte{seed}}))
		}
	}

	const iterations = 400
	served := make(map[PhysicalStreamID]int, len(ids))
	for i := 0; i < iterations; i++ {
		fill(ids[0], byte(i))
		for _, id := range ids[1:] {
			fill(id, byte(i))
		}
		envelope, ok := scheduler.Dequeue()
		require.True(t, ok, "iteration %d", i)
		served[envelope.Physical]++
	}

	for _, id := range ids {
		require.Greater(t, served[id], 0, "stream %d starved", id)
	}
	fair := iterations / (len(ids) + 1)
	for _, id := range ids {
		require.GreaterOrEqual(t, served[id], fair, "stream %d fell behind the hot stream", id)
	}
}

// TestSchedulerCloseOrdering proves a Close never overtakes earlier accepted
// data of its own stream while control metadata still gets bounded priority
// over another stream's data.
func TestSchedulerCloseOrdering(t *testing.T) {
	_, scheduler, ids := testScheduler(t, 2)

	require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: []byte("a")}))
	require.NoError(t, scheduler.EnqueueClient(Close{Physical: ids[1]}))

	first, ok := scheduler.Dequeue()
	require.True(t, ok)
	require.Equal(t, ids[1], first.Physical, "control metadata takes bounded priority")
	require.Equal(t, FrameControl, first.Class)
	_, isClose := first.Client.(Close)
	require.True(t, isClose)

	require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: []byte("b")}))
	require.NoError(t, scheduler.EnqueueClient(Close{Physical: ids[0]}))

	var order []string
	for scheduler.Pending() {
		envelope, ok := scheduler.Dequeue()
		require.True(t, ok)
		if envelope.Physical != ids[0] {
			continue
		}
		switch message := envelope.Client.(type) {
		case Data:
			order = append(order, string(message.Data))
		case Close:
			order = append(order, "close")
		default:
			t.Fatalf("unexpected message %T", message)
		}
	}
	require.Equal(t, []string{"a", "b", "close"}, order)

	require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: []byte("c")}), ErrSchedulerSettled)
	require.ErrorIs(t, scheduler.EnqueueClient(Close{Physical: ids[0]}), ErrSchedulerSettled)
}

// TestSchedulerControlPriorityBounded proves control frames are prioritized
// over data but only for a bounded burst, so control metadata can never starve
// data.
func TestSchedulerControlPriorityBounded(t *testing.T) {
	_, scheduler, ids := testScheduler(t, 5)
	const backlog = 8
	for round := 0; round < backlog; round++ {
		for _, id := range ids[:4] {
			require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte{byte(id), byte(round)}}))
		}
		require.NoError(t, scheduler.EnqueueServer(Opened{Ref: testRef(ids[4])}))
	}

	dataServed := make(map[PhysicalStreamID]int, 4)
	controls := 0
	run := 0
	for scheduler.Pending() {
		envelope, ok := scheduler.Dequeue()
		require.True(t, ok)
		if envelope.Class == FrameControl {
			require.Equal(t, ids[4], envelope.Physical)
			controls++
			run++
			require.LessOrEqual(t, run, MaxMuxSchedulerControlBurst, "control burst exceeded")
			continue
		}
		run = 0
		dataServed[envelope.Physical]++
	}
	require.Equal(t, backlog, controls)
	for _, id := range ids[:4] {
		require.Equal(t, backlog, dataServed[id], "stream %d starved by control", id)
	}
}

// TestSchedulerResetDiscard proves a reset discards exactly its own queue,
// frees its bytes immediately, is idempotent, and never affects a sibling.
func TestSchedulerResetDiscard(t *testing.T) {
	engine, scheduler, ids := testScheduler(t, 2)
	require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: []byte("one")}))
	require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: []byte("two")}))
	require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[1], Data: []byte("sibling")}))

	siblingBytes := scheduler.QueuedBytes(ids[1])
	require.Greater(t, scheduler.QueuedBytes(ids[0]), 0)

	require.True(t, scheduler.Reset(ids[0]))
	require.Zero(t, scheduler.QueuedFrames(ids[0]))
	require.Zero(t, scheduler.QueuedBytes(ids[0]))
	require.Equal(t, siblingBytes, scheduler.AggregateBytes(), "the sibling keeps its budget")
	require.Equal(t, 1, scheduler.Active())

	require.False(t, scheduler.Reset(ids[0]), "cancel is idempotent")
	require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: []byte("later")}), ErrSchedulerSettled)

	envelope, ok := scheduler.Dequeue()
	require.True(t, ok)
	require.Equal(t, ids[1], envelope.Physical)
	require.Zero(t, scheduler.AggregateBytes())

	require.False(t, scheduler.Reset(99), "an unadmitted stream is a no-op")

	openTestStream(t, engine, 3)
	require.True(t, scheduler.Reset(3), "an admitted but idle stream settles")
	require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 3, Data: []byte("x")}), ErrSchedulerSettled)
}

// TestSchedulerAggregateCap proves the aggregate bound discards exactly the
// offending stream and leaves its siblings' budget intact.
func TestSchedulerAggregateCap(t *testing.T) {
	t.Run("aggregate byte bound", func(t *testing.T) {
		_, scheduler, ids := testScheduler(t, 3)
		payload := make([]byte, 300)
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: payload}))
		unit := scheduler.QueuedBytes(ids[0])
		require.Positive(t, unit)

		scheduler.maxAggregateBytes = unit * 3

		require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: payload}))
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[1], Data: payload}))
		require.Equal(t, unit*3, scheduler.AggregateBytes())

		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: ids[2], Data: payload}), ErrSchedulerFull)
		require.Zero(t, scheduler.QueuedBytes(ids[2]))
		require.Equal(t, unit*3, scheduler.AggregateBytes(), "siblings keep their budget")
		require.Equal(t, 2, scheduler.Active())
	})
}

// TestSchedulerConcurrentEnqueueDequeueReset exercises every scheduler method
// from many goroutines so the race detector can prove the shared state is
// serialized and the accounting stays consistent.
func TestSchedulerConcurrentEnqueueDequeueReset(t *testing.T) {
	const streams = 8
	_, scheduler, ids := testScheduler(t, streams)

	const (
		producers   = 4
		perProducer = 300
		consumers   = 2
		perConsumer = 600
		resets      = 50
	)

	var wg sync.WaitGroup
	wg.Add(producers + consumers + 1)
	for producer := 0; producer < producers; producer++ {
		go func(producer int) {
			defer wg.Done()
			for i := 0; i < perProducer; i++ {
				id := ids[(producer+i)%streams]
				_ = scheduler.EnqueueClient(Data{Physical: id, Data: []byte{byte(producer), byte(i)}})
			}
		}(producer)
	}
	for consumer := 0; consumer < consumers; consumer++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perConsumer; i++ {
				_, _ = scheduler.Dequeue()
			}
		}()
	}
	go func() {
		defer wg.Done()
		for i := 0; i < resets; i++ {
			_ = scheduler.Reset(ids[i%streams])
		}
	}()
	wg.Wait()

	require.GreaterOrEqual(t, scheduler.AggregateBytes(), 0)
	require.LessOrEqual(t, scheduler.AggregateBytes(), int(MaxMuxAggregateBytes))

	total := 0
	for _, id := range ids {
		total += scheduler.QueuedBytes(id)
	}
	require.Equal(t, total, scheduler.AggregateBytes(), "aggregate accounting is exact")
	require.Equal(t, countActive(scheduler, ids), scheduler.Active())
}

// BenchmarkSchedulerEnqueueDequeue measures the steady-state cost of encoding,
// enqueuing, and dequeuing a single-stream data frame.
func BenchmarkSchedulerEnqueueDequeue(b *testing.B) {
	engine := NewStreamEngine()
	if err := engine.Open(Open{Ref: testRef(1)}); err != nil {
		b.Fatal(err)
	}
	if err := engine.Opened(Opened{Ref: testRef(1)}); err != nil {
		b.Fatal(err)
	}
	scheduler := NewScheduler(engine)
	payload := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := scheduler.EnqueueClient(Data{Physical: 1, Data: payload}); err != nil {
			b.Fatal(err)
		}
		if _, ok := scheduler.Dequeue(); !ok {
			b.Fatal("scheduler empty")
		}
	}
}

// BenchmarkSchedulerEnqueueDequeueManyStreams measures the same path across a
// full active ring.
func BenchmarkSchedulerEnqueueDequeueManyStreams(b *testing.B) {
	const streams = 32
	engine := NewStreamEngine()
	for i := 1; i <= streams; i++ {
		ref := testRef(PhysicalStreamID(i))
		if err := engine.Open(Open{Ref: ref}); err != nil {
			b.Fatal(err)
		}
		if err := engine.Opened(Opened{Ref: ref}); err != nil {
			b.Fatal(err)
		}
	}
	scheduler := NewScheduler(engine)
	payload := make([]byte, 256)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := PhysicalStreamID(i%streams + 1)
		if err := scheduler.EnqueueClient(Data{Physical: id, Data: payload}); err != nil {
			b.Fatal(err)
		}
		if _, ok := scheduler.Dequeue(); !ok {
			b.Fatal("scheduler empty")
		}
	}
}

// dequeueOrTimeout dequeues one frame, failing the test instead of hanging if
// the DRR ring stalls on a frame larger than the quantum.
func dequeueOrTimeout(t *testing.T, scheduler *Scheduler) (OutboundEnvelope, bool) {
	t.Helper()
	type result struct {
		envelope OutboundEnvelope
		ok       bool
	}
	done := make(chan result, 1)
	go func() {
		envelope, ok := scheduler.Dequeue()
		done <- result{envelope: envelope, ok: ok}
	}()
	select {
	case r := <-done:
		return r.envelope, r.ok
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue made no progress: the DRR ring stalled")
		return OutboundEnvelope{}, false
	}
}

// maxEnvOpen builds one Open whose environment fills the per-request bound, so
// its serialized envelope is far larger than the 64 KiB quantum.
func maxEnvOpen(t *testing.T, id PhysicalStreamID) Open {
	t.Helper()
	env := make([]string, ports.BrokerMaxEnvEntries)
	for i := range env {
		prefix := fmt.Sprintf("K%d=", i)
		env[i] = prefix + strings.Repeat("v", ports.BrokerMaxEnvEntryBytes-len(prefix))
	}
	open := testOpen()
	open.Ref = testRef(id)
	open.Env = env
	return open
}

// TestNewSchedulerWithCeilings proves the constructor validates the negotiated
// advertisement, enforces the negotiated chunk and aggregate ceilings, and
// copies the value so a caller cannot mutate live enforcement.
func TestNewSchedulerWithCeilings(t *testing.T) {
	t.Run("invalid advertisement refused", func(t *testing.T) {
		bad := DefaultMuxCeilings()
		bad.StreamChunkLimit = 0
		scheduler, err := NewSchedulerWithCeilings(nil, bad)
		require.ErrorIs(t, err, ErrInvalidCeilings)
		require.Nil(t, scheduler)
		_, err = NewSchedulerWithCeilings(nil, MuxCeilings{})
		require.ErrorIs(t, err, ErrInvalidCeilings)
	})

	t.Run("negotiated chunk and aggregate limits enforced", func(t *testing.T) {
		probeCeiling := MuxCeilings{
			MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
			StreamChunkLimit:        8,
			MaxStreams:              1,
			MaxAggregateBytes:       MinMuxAggregateBytes,
		}
		probe, err := NewSchedulerWithCeilings(nil, probeCeiling)
		require.NoError(t, err)
		require.NoError(t, probe.EnqueueClient(Data{Physical: 1, Data: make([]byte, 8)}))
		unit := probe.QueuedBytes(1)
		require.Positive(t, unit)

		// The aggregate floor admits exactly one maximum chunk plus envelope
		// overhead.
		floorCeiling := probeCeiling
		floorCeiling.StreamChunkLimit = MaxMuxChunkBytes
		floor, err := NewSchedulerWithCeilings(nil, floorCeiling)
		require.NoError(t, err)
		require.NoError(t, floor.EnqueueClient(Data{Physical: 1, Data: make([]byte, MaxMuxChunkBytes)}))
		require.Equal(t, 1, floor.Active())

		// A scheduler tightened below the negotiated aggregate discards exactly
		// the offending stream and frees its budget.
		scheduler, err := NewSchedulerWithCeilings(nil, probeCeiling)
		require.NoError(t, err)
		scheduler.maxAggregateBytes = unit * 2
		// The negotiated chunk ceiling is refused statelessly.
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 1, Data: make([]byte, 9)}), ErrTooLarge)
		require.Zero(t, scheduler.AggregateBytes())
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: 1, Data: make([]byte, 8)}))
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: 1, Data: make([]byte, 8)}))
		require.Equal(t, unit*2, scheduler.AggregateBytes())
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 1, Data: make([]byte, 8)}), ErrSchedulerFull)
		require.Zero(t, scheduler.AggregateBytes())
	})
}

// TestSchedulerTerminalBypass proves exactly one terminal Close or Reset is
// accepted for a known identity the engine has already settled, that the
// terminal frame bypasses normal admission, that a duplicate terminal is
// refused, and that data stays refused.
func TestSchedulerTerminalBypass(t *testing.T) {
	t.Run("settled identity signals once", func(t *testing.T) {
		engine := testEngine()
		scheduler := NewScheduler(engine)
		openTestStream(t, engine, 1)
		_, err := engine.Reset(Reset{Physical: 1})
		require.NoError(t, err)

		// Data for the settled, unadmitted identity stays refused.
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 1, Data: []byte("x")}), ErrSchedulerUnadmitted)
		// Exactly one terminal frame is accepted so the peer can be signaled.
		require.NoError(t, scheduler.EnqueueClient(Reset{Physical: 1}))
		envelope, ok := dequeueOrTimeout(t, scheduler)
		require.True(t, ok)
		require.Equal(t, PhysicalStreamID(1), envelope.Physical)
		require.Equal(t, FrameControl, envelope.Class)
		_, isReset := envelope.Client.(Reset)
		require.True(t, isReset)

		// The duplicate terminal, and every later data frame, is refused.
		require.ErrorIs(t, scheduler.EnqueueClient(Reset{Physical: 1}), ErrSchedulerSettled)
		require.ErrorIs(t, scheduler.EnqueueClient(Close{Physical: 1}), ErrSchedulerSettled)
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 1, Data: []byte("y")}), ErrSchedulerSettled)
		// An identity the engine never admitted is still refused.
		require.ErrorIs(t, scheduler.EnqueueClient(Close{Physical: 9}), ErrSchedulerUnadmitted)
	})

	t.Run("queue overflow signals the peer", func(t *testing.T) {
		engine := testEngine()
		scheduler := NewScheduler(engine)
		openTestStream(t, engine, 1)
		chunk, full := floodWindow(DefaultMuxCeilings())
		for i := 0; i < full; i++ {
			_, err := engine.Data(Data{Physical: 1, Data: chunk})
			require.NoError(t, err)
		}
		_, err := engine.Data(Data{Physical: 1, Data: chunk})
		require.ErrorIs(t, err, ErrStreamQueueFull)
		require.Equal(t, StreamTerminal, mustStatus(t, engine, 1).State)

		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 1, Data: []byte("x")}), ErrSchedulerUnadmitted)
		require.NoError(t, scheduler.EnqueueClient(Reset{Physical: 1}))
		_, ok := dequeueOrTimeout(t, scheduler)
		require.True(t, ok)
		require.ErrorIs(t, scheduler.EnqueueClient(Reset{Physical: 1}), ErrSchedulerSettled)
	})

	t.Run("nil gate trusts one terminal", func(t *testing.T) {
		scheduler := NewScheduler(nil)
		require.NoError(t, scheduler.EnqueueClient(Close{Physical: 7}))
		require.ErrorIs(t, scheduler.EnqueueClient(Close{Physical: 7}), ErrSchedulerSettled)
		require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: 7, Data: []byte("x")}), ErrSchedulerSettled)
	})
}

// TestSchedulerLargeFrameProgress is the regression for the stalled DRR ring:
// the maximum 64 KiB chunk and a maximal-environment Open both make progress
// without spinning.
func TestSchedulerLargeFrameProgress(t *testing.T) {
	t.Run("maximum chunk fits the quantum", func(t *testing.T) {
		_, scheduler, ids := testScheduler(t, 1)
		chunk := make([]byte, MaxMuxChunkBytes)
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: ids[0], Data: chunk}))
		envelope, ok := dequeueOrTimeout(t, scheduler)
		require.True(t, ok)
		require.Equal(t, ids[0], envelope.Physical)
		require.False(t, scheduler.Pending())
	})

	t.Run("maximum environment Open exceeds the quantum", func(t *testing.T) {
		scheduler := NewScheduler(nil)
		const streams = 5
		ids := make([]PhysicalStreamID, streams)
		for i := range ids {
			ids[i] = PhysicalStreamID(i + 1)
			require.NoError(t, scheduler.EnqueueClient(maxEnvOpen(t, ids[i])))
		}
		seen := make(map[PhysicalStreamID]bool, streams)
		for i := 0; i < streams; i++ {
			envelope, ok := dequeueOrTimeout(t, scheduler)
			require.True(t, ok, "dequeue %d", i)
			open, isOpen := envelope.Client.(Open)
			require.True(t, isOpen, "dequeue %d carries %T", i, envelope.Client)
			require.Len(t, open.Env, ports.BrokerMaxEnvEntries)
			seen[envelope.Physical] = true
		}
		require.Len(t, seen, streams)
		require.False(t, scheduler.Pending())
	})
}

// TestSchedulerBoundedRetiredRecords proves a long-lived scheduler keeps a
// bounded record table: the oldest settled records are evicted, while the
// newest identity still refuses a duplicate terminal.
func TestSchedulerBoundedRetiredRecords(t *testing.T) {
	scheduler := NewScheduler(nil)
	const churn = 4000
	for i := 1; i <= churn; i++ {
		id := PhysicalStreamID(i)
		require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte{byte(i)}}))
		require.True(t, scheduler.Reset(id))
	}
	require.LessOrEqual(t, len(scheduler.streams), MaxRetiredSchedulerRecords)
	require.Len(t, scheduler.retired, MaxRetiredSchedulerRecords)
	require.Zero(t, scheduler.Active())
	require.Zero(t, scheduler.AggregateBytes())

	newest := PhysicalStreamID(churn)
	require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: newest, Data: []byte("x")}), ErrSchedulerSettled)
}

// FuzzSchedulerEngineSmoke drives a scheduler over an engine gate with
// arbitrary operation bytes and proves the pair never panics, keeps its
// accounting exact, and stays bounded.
func FuzzSchedulerEngineSmoke(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	f.Add([]byte{1, 1, 1, 1, 1, 1, 1, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		engine := NewStreamEngine()
		scheduler := NewScheduler(engine)
		limit := len(ops)
		if limit > 256 {
			limit = 256
		}
		for i := 0; i+1 < limit; i += 2 {
			id := PhysicalStreamID(ops[i]%8 + 1)
			switch ops[i+1] % 6 {
			case 0:
				_ = engine.Open(Open{Ref: testRef(id)})
			case 1:
				_ = engine.Opened(Opened{Ref: testRef(id)})
			case 2:
				_ = scheduler.EnqueueClient(Data{Physical: id, Data: []byte{ops[i]}})
			case 3:
				_ = scheduler.EnqueueClient(Close{Physical: id})
			case 4:
				_, _ = engine.Data(Data{Physical: id, Data: []byte{ops[i]}})
			case 5:
				_ = scheduler.Reset(id)
			}
		}
		_, _ = scheduler.Dequeue()

		total := 0
		for id := PhysicalStreamID(1); id <= 8; id++ {
			total += scheduler.QueuedBytes(id)
		}
		require.Equal(t, total, scheduler.AggregateBytes())
		require.GreaterOrEqual(t, scheduler.AggregateBytes(), 0)
		require.LessOrEqual(t, len(scheduler.streams), int(MaxMuxStreams+MaxRetiredSchedulerRecords))
	})
}

// TestSchedulerOverflowAllowsOneTerminal proves an overflowed stream discards
// its queue and refuses data, yet still accepts exactly one terminal Close or
// Reset, sealing only after that terminal.
func TestSchedulerOverflowAllowsOneTerminal(t *testing.T) {
	terminals := []struct {
		name    string
		enqueue func(*Scheduler, PhysicalStreamID) error
	}{
		{"reset", func(s *Scheduler, id PhysicalStreamID) error { return s.EnqueueClient(Reset{Physical: id}) }},
		{"close", func(s *Scheduler, id PhysicalStreamID) error { return s.EnqueueClient(Close{Physical: id}) }},
	}
	for _, terminal := range terminals {
		t.Run(terminal.name, func(t *testing.T) {
			_, scheduler, ids := testScheduler(t, 1)
			id := ids[0]
			require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("a")}))
			scheduler.maxAggregateBytes = 2 * scheduler.AggregateBytes()
			require.NoError(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("b")}))
			require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("c")}), ErrSchedulerFull)
			require.Zero(t, scheduler.QueuedFrames(id))
			require.Zero(t, scheduler.AggregateBytes())
			require.False(t, scheduler.Pending())

			// The record is overflowed but not sealed: data stays refused with
			// the overflow sentinel.
			require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("d")}), ErrSchedulerFull)

			require.NoError(t, terminal.enqueue(scheduler, id))
			require.Equal(t, 1, scheduler.QueuedFrames(id))
			envelope, ok := scheduler.Dequeue()
			require.True(t, ok)
			require.Equal(t, id, envelope.Physical)
			require.Equal(t, FrameControl, envelope.Class)

			// The one terminal sealed the record: everything afterwards,
			// including a duplicate terminal, is refused.
			require.ErrorIs(t, scheduler.EnqueueClient(Data{Physical: id, Data: []byte("e")}), ErrSchedulerSettled)
			require.ErrorIs(t, terminal.enqueue(scheduler, id), ErrSchedulerSettled)
			require.False(t, scheduler.Reset(id))
		})
	}
}

// TestSchedulerOverflowRecordsBounded proves overflow records that never take
// their terminal stay bounded: they are evicted oldest-first past
// MaxRetiredSchedulerRecords.
func TestSchedulerOverflowRecordsBounded(t *testing.T) {
	scheduler := NewScheduler(nil)
	const churn = MaxRetiredSchedulerRecords * 4
	for i := 1; i <= churn; i++ {
		id := PhysicalStreamID(i)
		overflowOne(t, scheduler, id)
	}
	require.LessOrEqual(t, len(scheduler.streams), MaxRetiredSchedulerRecords)
	require.Zero(t, scheduler.AggregateBytes())
	require.False(t, scheduler.Pending())
}

// TestSchedulerOverflowTerminalReclaimsRetiredRecords is the regression for a
// record that is listed retired while empty, later accepts its one terminal
// Reset while that terminal stays queued, and is then pushed past the
// retention bound by later streams: eviction must not permanently orphan the
// record. Every terminal stays deliverable, and draining them returns the
// stream table to the configured bound.
func TestSchedulerOverflowTerminalReclaimsRetiredRecords(t *testing.T) {
	scheduler := NewScheduler(nil)
	const churn = MaxRetiredSchedulerRecords + 64

	for i := 1; i <= churn; i++ {
		id := PhysicalStreamID(i)
		// Overflow the queue so the record is retired while still empty.
		overflowOne(t, scheduler, id)
		// The one terminal the overflow leaves open lands while the record sits
		// in the retired list; later streams force it past the bound.
		require.NoError(t, scheduler.EnqueueClient(Reset{Physical: id}))
	}

	delivered := make(map[PhysicalStreamID]int, churn)
	for scheduler.Pending() {
		envelope, ok := scheduler.Dequeue()
		require.True(t, ok)
		_, isReset := envelope.Client.(Reset)
		require.True(t, isReset, "terminal frame must stay deliverable, got %T", envelope.Client)
		delivered[envelope.Physical]++
	}
	require.Len(t, delivered, churn, "every overflowed stream signals its peer exactly once")
	for id, count := range delivered {
		require.Equal(t, 1, count, "stream %d delivered %d terminals", id, count)
	}

	require.Zero(t, scheduler.AggregateBytes())
	require.Zero(t, scheduler.Active())
	require.LessOrEqual(t, len(scheduler.streams), MaxRetiredSchedulerRecords,
		"drained overflow records must not be orphaned past the retention bound")
	require.LessOrEqual(t, len(scheduler.retired), MaxRetiredSchedulerRecords)

	// The newest identity still refuses a duplicate terminal, so the retained
	// record set is live, not merely truncated.
	require.ErrorIs(t, scheduler.EnqueueClient(Reset{Physical: PhysicalStreamID(churn)}), ErrSchedulerSettled)
}

// TestSchedulerRefuseAdmitsFreshRefusalOnce proves the fresh-refusal exception:
// a Refused for an identity the engine never admitted is scheduled once and
// seals the record, so a duplicate and every later frame are refused; an
// unencodable refusal creates no record.
func TestSchedulerRefuseAdmitsFreshRefusalOnce(t *testing.T) {
	s := NewScheduler(nil)
	detail := testErrorDetail()
	detail.AdmissionCode = 1
	refused := Refused{Ref: testRef(1), Error: detail}

	require.NoError(t, s.Refuse(refused))
	envelope, ok := s.Dequeue()
	require.True(t, ok)
	require.Equal(t, PhysicalStreamID(1), envelope.Physical)
	require.Equal(t, DirectionServer, envelope.Direction)
	require.Equal(t, FrameControl, envelope.Class)
	require.Equal(t, refused, envelope.Server)

	// The fresh refusal seals the identity even though the engine never
	// admitted the stream: a duplicate and every later frame are refused.
	require.ErrorIs(t, s.Refuse(refused), ErrSchedulerSettled)
	require.ErrorIs(t, s.EnqueueServer(Refused{Ref: testRef(1), Error: detail}), ErrSchedulerSettled)
	require.ErrorIs(t, s.EnqueueServer(Data{Physical: 1, Data: []byte("x")}), ErrSchedulerSettled)
	_, ok = s.Dequeue()
	require.False(t, ok)

	// A refusal whose reference cannot be encoded is rejected without creating
	// a record.
	require.ErrorIs(t, s.Refuse(Refused{}), ErrInvalidMessage)
	require.Zero(t, s.QueuedFrames(0))

	var nilScheduler *Scheduler
	require.ErrorIs(t, nilScheduler.Refuse(refused), ErrInvalidMessage)
}
