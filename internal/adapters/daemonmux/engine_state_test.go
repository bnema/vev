package daemonmux

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

func testEngine() *StreamEngine { return NewStreamEngine() }

// admitTestStream admits one stream in opening state.
func admitTestStream(t *testing.T, e *StreamEngine, id PhysicalStreamID) StreamRef {
	t.Helper()
	ref := testRef(id)
	require.NoError(t, e.Open(Open{Ref: ref}))
	return ref
}

// openTestStream admits and confirms one stream.
func openTestStream(t *testing.T, e *StreamEngine, id PhysicalStreamID) StreamRef {
	t.Helper()
	ref := admitTestStream(t, e, id)
	require.NoError(t, e.Opened(Opened{Ref: ref}))
	return ref
}

func mustStatus(t *testing.T, e *StreamEngine, id PhysicalStreamID) StreamStatus {
	t.Helper()
	status, ok := e.Status(id)
	require.True(t, ok, "stream %d is not tracked", id)
	return status
}

func channelClosed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}

func TestNewStreamEngine(t *testing.T) {
	e := NewStreamEngine()
	require.Zero(t, e.Live())
	require.Zero(t, e.AggregateBytes())
	require.False(t, e.Closed())
	require.NoError(t, e.Err())
	require.Equal(t, domain.RemoteFailureNone, e.FailureKind())
	require.NotNil(t, e.Done())
}

func TestStreamStateStrings(t *testing.T) {
	require.Equal(t, "opening", StreamOpening.String())
	require.Equal(t, "open", StreamOpen.String())
	require.Equal(t, "closing", StreamClosing.String())
	require.Equal(t, "terminal", StreamTerminal.String())
	require.Equal(t, "unknown", StreamState(0).String())
	require.Equal(t, "unknown", StreamState(200).String())
}

// TestStreamEngineOpenAdmission is the admission table: complete references,
// strictly increasing identities that are never reused, and one shared
// pending-plus-live ceiling.
func TestStreamEngineOpenAdmission(t *testing.T) {
	t.Run("incomplete identities refused", func(t *testing.T) {
		e := testEngine()
		cases := []struct {
			name string
			ref  StreamRef
		}{
			{"zero ref", StreamRef{}},
			{"zero physical", StreamRef{Physical: 0, Epoch: 7, Connection: testConnectionID(0x11), Client: 3}},
			{"zero epoch", StreamRef{Physical: 1, Epoch: 0, Connection: testConnectionID(0x11), Client: 3}},
			{"zero connection", StreamRef{Physical: 1, Epoch: 7, Client: 3}},
			{"zero client", StreamRef{Physical: 1, Epoch: 7, Connection: testConnectionID(0x11), Client: 0}},
		}
		for _, tc := range cases {
			require.ErrorIs(t, e.Open(Open{Ref: tc.ref}), ErrInvalidStream, tc.name)
		}
		require.Zero(t, e.Live())
		_, ok := e.Status(1)
		require.False(t, ok)
	})

	t.Run("strictly increasing and never reused", func(t *testing.T) {
		e := testEngine()
		admitTestStream(t, e, 5)
		require.ErrorIs(t, e.Open(Open{Ref: testRef(5)}), ErrStreamIDReused)
		require.ErrorIs(t, e.Open(Open{Ref: testRef(1)}), ErrStreamIDReused)
		require.NoError(t, e.Opened(Opened{Ref: testRef(5)}))
		admitTestStream(t, e, 6)

		disposition, err := e.Close(Close{Physical: 5})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.ErrorIs(t, e.Open(Open{Ref: testRef(5)}), ErrStreamIDReused, "a settled ID is never reused")
		require.ErrorIs(t, e.Open(Open{Ref: testRef(6)}), ErrStreamIDReused)
		require.NoError(t, e.Open(Open{Ref: testRef(7)}))
		require.Equal(t, 2, e.Live())
	})

	t.Run("pending and live share the ceiling", func(t *testing.T) {
		e := testEngine()
		for i := 1; i <= int(MaxMuxStreams); i++ {
			require.NoError(t, e.Open(Open{Ref: testRef(PhysicalStreamID(i))}))
		}
		require.Equal(t, int(MaxMuxStreams), e.Live())
		require.ErrorIs(t, e.Open(Open{Ref: testRef(PhysicalStreamID(MaxMuxStreams + 1))}), ErrTooManyStreams)

		// Confirming a pending stream does not free a slot: pending and live
		// share the one ceiling.
		require.NoError(t, e.Opened(Opened{Ref: testRef(1)}))
		require.ErrorIs(t, e.Open(Open{Ref: testRef(PhysicalStreamID(MaxMuxStreams + 2))}), ErrTooManyStreams)

		// Settling one stream frees exactly one slot for a fresh higher ID.
		disposition, err := e.Reset(Reset{Physical: 1})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.Equal(t, int(MaxMuxStreams)-1, e.Live())
		require.NoError(t, e.Open(Open{Ref: testRef(PhysicalStreamID(MaxMuxStreams + 2))}))
		require.ErrorIs(t, e.Open(Open{Ref: testRef(PhysicalStreamID(MaxMuxStreams + 3))}), ErrTooManyStreams)
	})
}

// TestStreamEngineFutureAndRetired proves the high-water fence: an identity
// above it was never opened and is refused, while traffic at or below it that
// is no longer live belongs to a retired stream and is ignored.
func TestStreamEngineFutureAndRetired(t *testing.T) {
	t.Run("future identity refused", func(t *testing.T) {
		e := testEngine()
		openTestStream(t, e, 2)
		future := PhysicalStreamID(3)
		require.ErrorIs(t, e.Opened(Opened{Ref: testRef(future)}), ErrFutureStream)
		require.ErrorIs(t, e.Refused(Refused{Ref: testRef(future)}), ErrFutureStream)
		disposition, err := e.Data(Data{Physical: future, Data: []byte{'x'}})
		require.ErrorIs(t, err, ErrFutureStream)
		require.Zero(t, disposition)
		disposition, err = e.Close(Close{Physical: future})
		require.ErrorIs(t, err, ErrFutureStream)
		require.Zero(t, disposition)
		disposition, err = e.Reset(Reset{Physical: future})
		require.ErrorIs(t, err, ErrFutureStream)
		require.Zero(t, disposition)
		require.Equal(t, 1, e.Live())
	})

	t.Run("retired traffic ignored including gaps", func(t *testing.T) {
		e := testEngine()
		openTestStream(t, e, 5)
		disposition, err := e.Reset(Reset{Physical: 5})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		openTestStream(t, e, 9)

		// 6 and 8 were never admitted but sit below the high-water mark: they
		// are retired traffic, not future streams.
		for _, id := range []PhysicalStreamID{5, 6, 8} {
			disposition, err := e.Data(Data{Physical: id, Data: []byte{'x'}})
			require.NoError(t, err)
			require.Equal(t, StreamDiscarded, disposition)
			disposition, err = e.Close(Close{Physical: id})
			require.NoError(t, err)
			require.Equal(t, StreamDiscarded, disposition)
			disposition, err = e.Reset(Reset{Physical: id})
			require.NoError(t, err)
			require.Equal(t, StreamDiscarded, disposition)
		}

		// A retired identity can never be confirmed or refused again.
		require.ErrorIs(t, e.Opened(Opened{Ref: testRef(5)}), ErrStreamState)
		require.ErrorIs(t, e.Refused(Refused{Ref: testRef(5)}), ErrStreamState)
		require.ErrorIs(t, e.Opened(Opened{Ref: testRef(6)}), ErrStreamState)
		require.Equal(t, 1, e.Live())
	})
}

type engineStep struct {
	name    string
	run     func(*StreamEngine, PhysicalStreamID) (StreamDisposition, error)
	want    StreamDisposition
	wantErr error
}

func stepAdmit(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	if err := e.Open(Open{Ref: testRef(id)}); err != nil {
		return 0, err
	}
	return StreamAccepted, nil
}

func stepOpened(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	if err := e.Opened(Opened{Ref: testRef(id)}); err != nil {
		return 0, err
	}
	return StreamAccepted, nil
}

func stepRefused(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	if err := e.Refused(Refused{Ref: testRef(id)}); err != nil {
		return 0, err
	}
	return StreamAccepted, nil
}

func stepData(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	return e.Data(Data{Physical: id, Data: []byte{'d'}})
}

func stepClose(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	return e.Close(Close{Physical: id})
}

func stepReset(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	return e.Reset(Reset{Physical: id})
}

// stepTake drains one queued chunk, reporting StreamAccepted when a chunk was
// taken and StreamDiscarded when the queue was already empty. It is the drain
// half of the orderly-close lifecycle: taking the last chunk of a closing
// stream terminalizes it.
func stepTake(e *StreamEngine, id PhysicalStreamID) (StreamDisposition, error) {
	if _, ok := e.Take(id); ok {
		return StreamAccepted, nil
	}
	return StreamDiscarded, nil
}

// TestStreamEngineLifecycle is the stream state table: opening -> open ->
// terminal, with legal Opened/Refused only from opening, data only while open,
// and late traffic discarded.
func TestStreamEngineLifecycle(t *testing.T) {
	cases := []struct {
		name  string
		steps []engineStep
	}{
		{
			name: "confirmed then closed",
			steps: []engineStep{
				{"admit", stepAdmit, StreamAccepted, nil},
				{"data while opening", stepData, 0, ErrStreamState},
				{"opened", stepOpened, StreamAccepted, nil},
				{"opened twice", stepOpened, 0, ErrStreamState},
				{"refused when open", stepRefused, 0, ErrStreamState},
				{"close", stepClose, StreamAccepted, nil},
				{"late data", stepData, StreamDiscarded, nil},
				{"late close", stepClose, StreamDiscarded, nil},
				{"late reset", stepReset, StreamDiscarded, nil},
				{"opened after close", stepOpened, 0, ErrStreamState},
				{"refused after close", stepRefused, 0, ErrStreamState},
			},
		},
		{
			name: "orderly close preserves queued data until drained",
			steps: []engineStep{
				{"admit", stepAdmit, StreamAccepted, nil},
				{"opened", stepOpened, StreamAccepted, nil},
				{"data when open", stepData, StreamAccepted, nil},
				{"close with unread data", stepClose, StreamAccepted, nil},
				{"close twice", stepClose, StreamDiscarded, nil},
				{"data after close refused", stepData, 0, ErrStreamState},
				{"drain the preserved chunk", stepTake, StreamAccepted, nil},
				{"late close", stepClose, StreamDiscarded, nil},
				{"late data", stepData, StreamDiscarded, nil},
				{"late reset", stepReset, StreamDiscarded, nil},
				{"opened after close", stepOpened, 0, ErrStreamState},
			},
		},
		{
			name: "refused from opening",
			steps: []engineStep{
				{"admit", stepAdmit, StreamAccepted, nil},
				{"refused", stepRefused, StreamAccepted, nil},
				{"refused twice", stepRefused, 0, ErrStreamState},
				{"opened after refusal", stepOpened, 0, ErrStreamState},
				{"data after refusal", stepData, StreamDiscarded, nil},
			},
		},
		{
			name: "close while opening is terminal",
			steps: []engineStep{
				{"admit", stepAdmit, StreamAccepted, nil},
				{"close", stepClose, StreamAccepted, nil},
				{"data after close", stepData, StreamDiscarded, nil},
				{"opened after close", stepOpened, 0, ErrStreamState},
			},
		},
		{
			name: "reset while opening is terminal and idempotent",
			steps: []engineStep{
				{"admit", stepAdmit, StreamAccepted, nil},
				{"reset", stepReset, StreamAccepted, nil},
				{"reset idempotent", stepReset, StreamDiscarded, nil},
				{"close idempotent", stepClose, StreamDiscarded, nil},
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := testEngine()
			id := PhysicalStreamID(4)
			for _, s := range tc.steps {
				disposition, err := s.run(e, id)
				if s.wantErr != nil {
					require.ErrorIs(t, err, s.wantErr, s.name)
					require.Zero(t, disposition, s.name)
					continue
				}
				require.NoError(t, err, s.name)
				require.Equal(t, s.want, disposition, s.name)
			}
			require.Zero(t, e.Live())
			require.Equal(t, StreamTerminal, mustStatus(t, e, id).State)
		})
	}
}

// TestStreamEngineRejectedAdmissionLeavesNothing proves a refused admission
// records nothing and cannot resurrect a settled identity.
func TestStreamEngineRejectedAdmissionLeavesNothing(t *testing.T) {
	e := testEngine()
	require.ErrorIs(t, e.Open(Open{}), ErrInvalidStream)
	require.Zero(t, e.Live())
	_, ok := e.Status(1)
	require.False(t, ok)

	admitTestStream(t, e, 1)
	require.ErrorIs(t, e.Open(Open{Ref: testRef(1)}), ErrStreamIDReused)
	require.Equal(t, 1, e.Live())
	require.Equal(t, StreamOpening, mustStatus(t, e, 1).State)
	_, ok = e.Status(2)
	require.False(t, ok)
}

// TestStreamEngineRefFence proves Opened and Refused are legal only for the
// opening stream whose full reference was recorded at admission.
func TestStreamEngineRefFence(t *testing.T) {
	e := testEngine()
	ref := admitTestStream(t, e, 4)

	mismatched := ref
	mismatched.Epoch++
	require.ErrorIs(t, e.Opened(Opened{Ref: mismatched}), ErrStreamRefMismatch)
	require.Equal(t, StreamOpening, mustStatus(t, e, 4).State)
	require.ErrorIs(t, e.Refused(Refused{Ref: mismatched}), ErrStreamRefMismatch)
	require.Equal(t, StreamOpening, mustStatus(t, e, 4).State)

	require.NoError(t, e.Opened(Opened{Ref: ref}))
	require.Equal(t, StreamOpen, mustStatus(t, e, 4).State)

	// Legality is checked before reference identity: a settled stream refuses
	// both a matching and a mismatched confirmation.
	require.ErrorIs(t, e.Opened(Opened{Ref: ref}), ErrStreamState)
	require.ErrorIs(t, e.Opened(Opened{Ref: mismatched}), ErrStreamState)
}

// TestStreamEngineRefusalRetained proves a refusal settles the stream
// orderly and retains the typed refusal for the caller.
func TestStreamEngineRefusalRetained(t *testing.T) {
	e := testEngine()
	admitTestStream(t, e, 2)
	detail := testErrorDetail()
	require.NoError(t, e.Refused(Refused{Ref: testRef(2), Error: detail}))

	status := mustStatus(t, e, 2)
	require.Equal(t, StreamTerminal, status.State)
	require.True(t, status.Refused)
	require.Equal(t, detail, status.Refusal)
	require.True(t, channelClosed(status.Done))
	require.NoError(t, status.Err)
	require.Equal(t, domain.RemoteFailureNone, status.FailureKind)
	require.Zero(t, e.Live())
}

// TestStreamEngineTwoAndHundredStreams proves admission, confirmation, data,
// and orderly close for a small and a large concurrent set. A stream whose
// already-queued chunk was drained before the Close terminalizes immediately;
// every other stream preserves its queued chunk while closing until Take
// drains it.
func TestStreamEngineTwoAndHundredStreams(t *testing.T) {
	for _, count := range []int{2, 100} {
		t.Run(fmt.Sprintf("%d streams", count), func(t *testing.T) {
			e := testEngine()
			for i := 1; i <= count; i++ {
				ref := openTestStream(t, e, PhysicalStreamID(i))
				require.Equal(t, ref, mustStatus(t, e, PhysicalStreamID(i)).Ref)
			}
			require.Equal(t, count, e.Live())

			for i := 1; i <= count; i++ {
				disposition, err := e.Data(Data{Physical: PhysicalStreamID(i), Data: []byte{byte(i)}})
				require.NoError(t, err)
				require.Equal(t, StreamAccepted, disposition)
			}
			require.Equal(t, count, e.AggregateBytes())

			taken, ok := e.Take(PhysicalStreamID(1))
			require.True(t, ok)
			require.Equal(t, []byte{1}, taken)
			require.Equal(t, count-1, e.AggregateBytes())
			require.Zero(t, mustStatus(t, e, 1).QueuedChunks)

			for i := 1; i <= count; i++ {
				disposition, err := e.Close(Close{Physical: PhysicalStreamID(i)})
				require.NoError(t, err)
				require.Equal(t, StreamAccepted, disposition)
			}
			// The drained stream is terminal; every other stream is closing and
			// still holds exactly its one unread, charged chunk.
			require.Equal(t, count-1, e.Live())
			require.Equal(t, count-1, e.AggregateBytes())
			require.False(t, e.Closed(), "closing every stream never terminalizes the physical connection")
			terminal := mustStatus(t, e, 1)
			require.Equal(t, StreamTerminal, terminal.State)
			require.True(t, channelClosed(terminal.Done))
			for i := 2; i <= count; i++ {
				status := mustStatus(t, e, PhysicalStreamID(i))
				require.Equal(t, StreamClosing, status.State, "stream %d", i)
				require.False(t, channelClosed(status.Done), "stream %d", i)
				require.Equal(t, 1, status.QueuedChunks, "stream %d", i)
			}

			// Draining every preserved chunk releases its bytes and terminalizes
			// the stream.
			for i := 2; i <= count; i++ {
				chunk, ok := e.Take(PhysicalStreamID(i))
				require.True(t, ok, "stream %d", i)
				require.Equal(t, []byte{byte(i)}, chunk, "stream %d", i)
			}
			require.Zero(t, e.Live())
			require.Zero(t, e.AggregateBytes())
			require.False(t, e.Closed(), "closing every stream never terminalizes the physical connection")
			for i := 1; i <= count; i++ {
				status := mustStatus(t, e, PhysicalStreamID(i))
				require.Equal(t, StreamTerminal, status.State)
				require.True(t, channelClosed(status.Done))
				require.NoError(t, status.Err)
			}
		})
	}
}

// TestStreamEngineIsolation proves one stream's close or reset terminalizes
// neither the physical connection nor a sibling.
func TestStreamEngineIsolation(t *testing.T) {
	e := testEngine()
	openTestStream(t, e, 1)
	openTestStream(t, e, 2)
	openTestStream(t, e, 3)
	_, err := e.Data(Data{Physical: 1, Data: []byte("a")})
	require.NoError(t, err)

	disposition, err := e.Close(Close{Physical: 2})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	require.False(t, e.Closed())
	require.NoError(t, e.Err())
	require.Equal(t, StreamOpen, mustStatus(t, e, 1).State)
	require.Equal(t, StreamOpen, mustStatus(t, e, 3).State)
	require.Equal(t, 2, e.Live())
	require.Equal(t, 1, e.AggregateBytes())

	disposition, err = e.Reset(Reset{Physical: 3})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	require.False(t, e.Closed())
	require.Equal(t, StreamOpen, mustStatus(t, e, 1).State)
	require.Equal(t, 1, e.Live())

	disposition, err = e.Data(Data{Physical: 1, Data: []byte("b")})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	require.Equal(t, 2, e.AggregateBytes())
}

// TestStreamEngineQueueOverflow proves a stalled queue resets exactly its own
// stream, frees that stream's budget, and leaves a sibling progressing.
func TestStreamEngineQueueOverflow(t *testing.T) {
	e := testEngine()
	openTestStream(t, e, 1)
	openTestStream(t, e, 2)
	_, err := e.Data(Data{Physical: 2, Data: []byte("sibling")})
	require.NoError(t, err)

	for i := 0; i < MaxMuxStreamQueueChunks; i++ {
		disposition, err := e.Data(Data{Physical: 1, Data: []byte{byte(i)}})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
	}
	require.Equal(t, MaxMuxStreamQueueChunks, mustStatus(t, e, 1).QueuedChunks)

	disposition, err := e.Data(Data{Physical: 1, Data: []byte{'x'}})
	require.ErrorIs(t, err, ErrStreamQueueFull)
	require.Zero(t, disposition)

	status := mustStatus(t, e, 1)
	require.Equal(t, StreamTerminal, status.State)
	require.True(t, channelClosed(status.Done))
	require.ErrorIs(t, status.Err, ErrStreamQueueFull)
	require.Equal(t, domain.RemoteFailureTransport, status.FailureKind)
	require.Zero(t, status.QueuedChunks)
	require.Zero(t, status.QueuedBytes)
	require.Equal(t, len("sibling"), e.AggregateBytes(), "the overflowed stream released its budget")
	require.Equal(t, 1, e.Live())
	require.False(t, e.Closed())

	sibling := mustStatus(t, e, 2)
	require.Equal(t, StreamOpen, sibling.State)
	require.False(t, channelClosed(sibling.Done))
	disposition, err = e.Data(Data{Physical: 2, Data: []byte("more")})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	require.Equal(t, len("sibling")+len("more"), e.AggregateBytes())

	// Cancellation of an already terminal stream stays idempotent and never
	// rewrites its cause.
	disposition, err = e.Reset(Reset{Physical: 1})
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)
	require.ErrorIs(t, mustStatus(t, e, 1).Err, ErrStreamQueueFull)
}

// TestStreamEngineQueueByteBounds proves the per-stream and aggregate byte
// ceilings reset only the offending stream.
func TestStreamEngineQueueByteBounds(t *testing.T) {
	t.Run("per-stream byte bound", func(t *testing.T) {
		e := testEngine()
		// The codec never admits a chunk above the negotiated chunk ceiling;
		// raise the engine's private ceiling so this test can reach the
		// defensive per-stream byte bound directly.
		e.maxChunkBytes = MaxMuxStreamQueueBytes
		openTestStream(t, e, 1)
		chunk := make([]byte, MaxMuxStreamQueueBytes/2+1)
		disposition, err := e.Data(Data{Physical: 1, Data: chunk})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.Equal(t, len(chunk), e.AggregateBytes())

		// The second chunk fits the chunk-count and aggregate bounds but
		// exceeds the per-stream byte bound.
		disposition, err = e.Data(Data{Physical: 1, Data: chunk})
		require.ErrorIs(t, err, ErrStreamQueueFull)
		require.Zero(t, disposition)
		require.Zero(t, e.AggregateBytes(), "the reset stream released its bytes")
		require.Equal(t, StreamTerminal, mustStatus(t, e, 1).State)
	})

	t.Run("aggregate byte bound", func(t *testing.T) {
		e := testEngine()
		// As above: bypass the codec-level chunk ceiling to reach the
		// defensive aggregate byte bound with oversized chunks.
		e.maxChunkBytes = 16 << 20
		chunk := make([]byte, 16<<20)
		for i := 1; i <= 4; i++ {
			openTestStream(t, e, PhysicalStreamID(i))
			disposition, err := e.Data(Data{Physical: PhysicalStreamID(i), Data: chunk})
			require.NoError(t, err)
			require.Equal(t, StreamAccepted, disposition)
		}
		require.Equal(t, int(MaxMuxAggregateBytes), e.AggregateBytes())

		openTestStream(t, e, 5)
		disposition, err := e.Data(Data{Physical: 5, Data: chunk})
		require.ErrorIs(t, err, ErrStreamQueueFull)
		require.Zero(t, disposition)
		require.Equal(t, StreamTerminal, mustStatus(t, e, 5).State)
		require.Equal(t, int(MaxMuxAggregateBytes), e.AggregateBytes(), "siblings keep their budget")
		for i := 1; i <= 4; i++ {
			require.Equal(t, StreamOpen, mustStatus(t, e, PhysicalStreamID(i)).State)
		}
		require.Equal(t, 4, e.Live())

		// Taking one sibling chunk releases exactly its bytes, so a fresh
		// stream takes the freed capacity.
		_, ok := e.Take(PhysicalStreamID(1))
		require.True(t, ok)
		require.Equal(t, int(MaxMuxAggregateBytes)-len(chunk), e.AggregateBytes())
		openTestStream(t, e, 6)
		disposition, err = e.Data(Data{Physical: 6, Data: chunk})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.Equal(t, int(MaxMuxAggregateBytes), e.AggregateBytes())
	})
}

// TestStreamEngineTake proves the FIFO pop returns an engine-owned copy and
// releases the chunk's bytes.
func TestStreamEngineTake(t *testing.T) {
	e := testEngine()
	openTestStream(t, e, 1)
	chunk := []byte("payload")
	_, err := e.Data(Data{Physical: 1, Data: chunk})
	require.NoError(t, err)

	chunk[0] = 'P' // mutating the caller's slice must not change the queue
	taken, ok := e.Take(PhysicalStreamID(1))
	require.True(t, ok)
	require.Equal(t, []byte("payload"), taken)
	require.Zero(t, e.AggregateBytes())

	_, ok = e.Take(PhysicalStreamID(1))
	require.False(t, ok)
	_, ok = e.Take(PhysicalStreamID(9))
	require.False(t, ok)
}

// TestStreamEngineResetCause proves a reset is abnormal: it carries the typed
// cause when present and the terminal sentinel otherwise.
func TestStreamEngineResetCause(t *testing.T) {
	t.Run("typed detail", func(t *testing.T) {
		e := testEngine()
		openTestStream(t, e, 1)
		detail := ErrorDetail{
			Code: ports.BrokerErrorUnavailable, Text: "reset by peer",
			AdmissionCode: 3, FailureKind: domain.RemoteFailureTimeout,
		}
		disposition, err := e.Reset(Reset{Physical: 1, HasError: true, Error: detail})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)

		status := mustStatus(t, e, 1)
		require.Error(t, status.Err)
		require.Equal(t, domain.RemoteFailureTimeout, status.FailureKind)
		var brokerErr ports.BrokerError
		require.ErrorAs(t, status.Err, &brokerErr)
		require.Equal(t, ports.BrokerErrorUnavailable, brokerErr.Code)
		require.Equal(t, "reset by peer", brokerErr.Text)
	})

	t.Run("no detail", func(t *testing.T) {
		e := testEngine()
		openTestStream(t, e, 1)
		disposition, err := e.Reset(Reset{Physical: 1})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)

		status := mustStatus(t, e, 1)
		require.ErrorIs(t, status.Err, ErrTerminalFailure)
		require.Equal(t, domain.RemoteFailureTransport, status.FailureKind)
	})
}

// TestStreamEnginePhysicalFailureOrdering proves a physical failure closes
// Done before terminalizing every live stream, that every stream observes the
// physical outcome, and that a stream settled beforehand keeps its own cause.
func TestStreamEnginePhysicalFailureOrdering(t *testing.T) {
	e := testEngine()
	const live = 5
	for i := 1; i <= live; i++ {
		openTestStream(t, e, PhysicalStreamID(i))
	}
	_, err := e.Reset(Reset{Physical: 5, HasError: true, Error: testErrorDetail()})
	require.NoError(t, err)
	siblingCause := mustStatus(t, e, 5).Err
	require.Error(t, siblingCause)

	cause := fmt.Errorf("physical loss: %w", errTerminalSentinel)
	observed := make([]bool, live-1)
	var wg sync.WaitGroup
	for i := 1; i < live; i++ {
		status := mustStatus(t, e, PhysicalStreamID(i))
		wg.Add(1)
		go func(i int, done <-chan struct{}) {
			defer wg.Done()
			<-done
			// A reader woken by a stream must observe the physical
			// connection already terminal.
			observed[i-1] = e.Closed()
		}(i, status.Done)
	}
	e.Fail(domain.RemoteFailureTimeout, cause)
	wg.Wait()

	require.True(t, e.Closed())
	require.Equal(t, cause, e.Err())
	require.ErrorIs(t, e.Err(), errTerminalSentinel)
	require.Equal(t, domain.RemoteFailureTimeout, e.FailureKind())
	require.Zero(t, e.Live())
	require.Zero(t, e.AggregateBytes())

	for i := 1; i < live; i++ {
		status := mustStatus(t, e, PhysicalStreamID(i))
		require.Equal(t, StreamTerminal, status.State)
		require.True(t, channelClosed(status.Done))
		require.Equal(t, cause, status.Err)
		require.Equal(t, domain.RemoteFailureTimeout, status.FailureKind)
		require.True(t, observed[i-1], "stream %d was terminalized before the physical Done", i)
	}

	// The stream settled before the loss keeps its own, independent cause.
	settled := mustStatus(t, e, 5)
	require.Equal(t, StreamTerminal, settled.State)
	require.Equal(t, siblingCause, settled.Err)
}

// TestStreamEngineTerminate proves an orderly physical close terminalizes
// every stream with a nil error and refuses further admission.
func TestStreamEngineTerminate(t *testing.T) {
	e := testEngine()
	for i := 1; i <= 3; i++ {
		openTestStream(t, e, PhysicalStreamID(i))
	}
	_, err := e.Data(Data{Physical: 1, Data: []byte("queued")})
	require.NoError(t, err)

	e.Terminate()
	require.True(t, e.Closed())
	require.NoError(t, e.Err())
	require.Equal(t, domain.RemoteFailureNone, e.FailureKind())
	require.Zero(t, e.Live())
	require.Zero(t, e.AggregateBytes())
	for i := 1; i <= 3; i++ {
		status := mustStatus(t, e, PhysicalStreamID(i))
		require.Equal(t, StreamTerminal, status.State)
		require.True(t, channelClosed(status.Done))
		require.NoError(t, status.Err)
	}

	require.ErrorIs(t, e.Open(Open{Ref: testRef(9)}), ErrPhysicalClosed)
	e.Terminate()
	require.True(t, e.Closed())
}

// TestStreamEnginePhysicalClosedFrames proves frames after a physical terminal
// outcome stay classified: settled streams discard traffic, admission is
// closed, and a future identity is still refused.
func TestStreamEnginePhysicalClosedFrames(t *testing.T) {
	e := testEngine()
	openTestStream(t, e, 1)
	e.Fail(domain.RemoteFailureTransport, errors.New("gone"))

	for _, run := range []struct {
		name string
		run  func() (StreamDisposition, error)
	}{
		{"data", func() (StreamDisposition, error) { return e.Data(Data{Physical: 1, Data: []byte{'x'}}) }},
		{"close", func() (StreamDisposition, error) { return e.Close(Close{Physical: 1}) }},
		{"reset", func() (StreamDisposition, error) { return e.Reset(Reset{Physical: 1}) }},
	} {
		disposition, err := run.run()
		require.NoError(t, err, run.name)
		require.Equal(t, StreamDiscarded, disposition, run.name)
	}

	require.ErrorIs(t, e.Open(Open{Ref: testRef(2)}), ErrPhysicalClosed)
	_, err := e.Data(Data{Physical: 3, Data: []byte{'x'}})
	require.ErrorIs(t, err, ErrFutureStream)
}

// TestStreamEngineConcurrentStreams exercises concurrent confirmation, data,
// orderly close, abnormal reset, and admission over one engine.
func TestStreamEngineConcurrentStreams(t *testing.T) {
	const streams = 64
	const admitted = 32
	e := testEngine()
	for i := 1; i <= streams; i++ {
		admitTestStream(t, e, PhysicalStreamID(i))
	}

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 1; i <= streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := PhysicalStreamID(i)
			assert.NoError(t, e.Opened(Opened{Ref: testRef(id)}))
			disposition, err := e.Data(Data{Physical: id, Data: []byte{byte(i)}})
			assert.NoError(t, err)
			assert.Equal(t, StreamAccepted, disposition)
			if i%2 == 0 {
				disposition, err = e.Close(Close{Physical: id})
			} else {
				disposition, err = e.Reset(Reset{Physical: id})
			}
			assert.NoError(t, err)
			assert.Equal(t, StreamAccepted, disposition)
		}(i)
	}
	// One goroutine keeps admitting fresh, strictly increasing identities
	// while the others settle theirs.
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		for i := 1; i <= admitted; i++ {
			id := PhysicalStreamID(streams + i)
			assert.NoError(t, e.Open(Open{Ref: testRef(id)}))
			assert.NoError(t, e.Opened(Opened{Ref: testRef(id)}))
			disposition, err := e.Close(Close{Physical: id})
			assert.NoError(t, err)
			assert.Equal(t, StreamAccepted, disposition)
		}
	}()
	close(start)
	wg.Wait()

	// The even streams were closed with one chunk still unread, so they are
	// closing and still charged; draining each queue terminalizes it. Odd
	// streams and the admitted-only goroutine are already terminal.
	for i := 1; i <= streams+admitted; i++ {
		for {
			if _, ok := e.Take(PhysicalStreamID(i)); !ok {
				break
			}
		}
	}

	require.Zero(t, e.Live())
	require.Zero(t, e.AggregateBytes())
	require.False(t, e.Closed())
	for i := 1; i <= streams+admitted; i++ {
		require.Equal(t, StreamTerminal, mustStatus(t, e, PhysicalStreamID(i)).State)
	}
}

// TestStreamEngineConcurrentPhysicalFailure races stream work against a
// physical failure and proves the engine settles consistently.
func TestStreamEngineConcurrentPhysicalFailure(t *testing.T) {
	const streams = 64
	e := testEngine()
	for i := 1; i <= streams; i++ {
		openTestStream(t, e, PhysicalStreamID(i))
	}
	cause := errors.New("physical loss")

	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 1; i <= streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			id := PhysicalStreamID(i)
			switch i % 3 {
			case 0:
				_, _ = e.Data(Data{Physical: id, Data: []byte{byte(i)}})
			case 1:
				_, _ = e.Close(Close{Physical: id})
			default:
				_, _ = e.Reset(Reset{Physical: id})
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		<-start
		e.Fail(domain.RemoteFailureTimeout, cause)
	}()
	close(start)
	wg.Wait()

	require.True(t, e.Closed())
	require.Equal(t, cause, e.Err())
	require.Zero(t, e.Live())
	require.Zero(t, e.AggregateBytes())
	for i := 1; i <= streams; i++ {
		status := mustStatus(t, e, PhysicalStreamID(i))
		require.Equal(t, StreamTerminal, status.State)
		require.True(t, channelClosed(status.Done))
	}
}

// TestStreamEngineNilSafety proves the nil-receiver paths.
func TestStreamEngineNilSafety(t *testing.T) {
	var e *StreamEngine
	require.ErrorIs(t, e.Open(Open{Ref: testRef(1)}), ErrPhysicalClosed)
	require.ErrorIs(t, e.Opened(Opened{Ref: testRef(1)}), ErrPhysicalClosed)
	require.ErrorIs(t, e.Refused(Refused{Ref: testRef(1)}), ErrPhysicalClosed)
	_, err := e.Data(Data{Physical: 1, Data: []byte{'x'}})
	require.ErrorIs(t, err, ErrPhysicalClosed)
	_, err = e.Close(Close{Physical: 1})
	require.ErrorIs(t, err, ErrPhysicalClosed)
	_, err = e.Reset(Reset{Physical: 1})
	require.ErrorIs(t, err, ErrPhysicalClosed)
	require.Zero(t, e.Live())
	require.Zero(t, e.AggregateBytes())
	require.True(t, e.Closed())
	require.Nil(t, e.Done())
	require.NoError(t, e.Err())
	require.Equal(t, domain.RemoteFailureNone, e.FailureKind())
	_, ok := e.Status(1)
	require.False(t, ok)
	_, ok = e.Take(1)
	require.False(t, ok)
	e.Fail(domain.RemoteFailureTransport, errors.New("ignored"))
	e.Terminate()
}

// TestNewStreamEngineWithCeilings proves the constructor validates the
// negotiated advertisement, enforces the negotiated maxima, and copies the
// value so a caller cannot mutate live enforcement.
func TestNewStreamEngineWithCeilings(t *testing.T) {
	t.Run("invalid advertisement refused", func(t *testing.T) {
		bad := DefaultMuxCeilings()
		bad.MaxStreams = 0
		engine, err := NewStreamEngineWithCeilings(bad)
		require.ErrorIs(t, err, ErrInvalidCeilings)
		require.Nil(t, engine)

		bad = DefaultMuxCeilings()
		bad.StreamChunkLimit = MaxMuxChunkBytes + 1
		_, err = NewStreamEngineWithCeilings(bad)
		require.ErrorIs(t, err, ErrInvalidCeilings)

		_, err = NewStreamEngineWithCeilings(MuxCeilings{})
		require.ErrorIs(t, err, ErrInvalidCeilings)
	})

	t.Run("negotiated small limits enforced", func(t *testing.T) {
		ceilings := MuxCeilings{
			MaxReceiveEnvelopeBytes: MinMuxEnvelopeBytes,
			StreamChunkLimit:        32768,
			MaxStreams:              2,
			MaxAggregateBytes:       MinMuxAggregateBytes,
		}
		e, err := NewStreamEngineWithCeilings(ceilings)
		require.NoError(t, err)
		openTestStream(t, e, 1)
		openTestStream(t, e, 2)
		require.ErrorIs(t, e.Open(Open{Ref: testRef(3)}), ErrTooManyStreams)

		// The negotiated chunk ceiling is refused statelessly, mutating
		// nothing.
		_, err = e.Data(Data{Physical: 1, Data: make([]byte, 32769)})
		require.ErrorIs(t, err, ErrTooLarge)
		require.Equal(t, StreamOpen, mustStatus(t, e, 1).State)
		require.Zero(t, e.AggregateBytes())

		// Two full chunks fit the aggregate floor; a third would exceed it and
		// resets exactly the offending stream.
		_, err = e.Data(Data{Physical: 1, Data: make([]byte, 32768)})
		require.NoError(t, err)
		require.Equal(t, 32768, e.AggregateBytes())
		_, err = e.Data(Data{Physical: 2, Data: make([]byte, 32768)})
		require.NoError(t, err)
		require.Equal(t, 65536, e.AggregateBytes())
		_, err = e.Data(Data{Physical: 1, Data: make([]byte, 32768)})
		require.ErrorIs(t, err, ErrStreamQueueFull)
		require.Equal(t, StreamTerminal, mustStatus(t, e, 1).State)
		require.Equal(t, 32768, e.AggregateBytes(), "the reset stream released its bytes")
		require.Equal(t, StreamOpen, mustStatus(t, e, 2).State)
	})

	t.Run("ceilings are copied", func(t *testing.T) {
		ceilings := DefaultMuxCeilings()
		ceilings.MaxStreams = 1
		e, err := NewStreamEngineWithCeilings(ceilings)
		require.NoError(t, err)
		// A caller mutating its own copy must not raise the engine's ceiling.
		ceilings.MaxStreams = MaxMuxStreams
		openTestStream(t, e, 1)
		require.ErrorIs(t, e.Open(Open{Ref: testRef(2)}), ErrTooManyStreams)
	})
}

// TestStreamEngineBoundedRetiredRecords proves a churned long-lived engine
// keeps a bounded table: the oldest retired records are evicted, while the
// high-water mark still classifies them as retired rather than future.
func TestStreamEngineBoundedRetiredRecords(t *testing.T) {
	e := testEngine()
	const churn = 4000
	for i := 1; i <= churn; i++ {
		id := PhysicalStreamID(i)
		openTestStream(t, e, id)
		_, err := e.Close(Close{Physical: id})
		require.NoError(t, err)
	}
	require.LessOrEqual(t, len(e.streams), MaxRetiredStreamRecords)
	require.Len(t, e.retired, MaxRetiredStreamRecords)

	// The newest identity is retained; the oldest record was evicted but its
	// high-water mark still classifies late traffic as retired.
	require.Equal(t, StreamTerminal, mustStatus(t, e, PhysicalStreamID(churn)).State)
	_, ok := e.Status(PhysicalStreamID(1))
	require.False(t, ok)
	disposition, err := e.Data(Data{Physical: PhysicalStreamID(1), Data: []byte{'x'}})
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)

	// An identity above the high-water mark is still refused, not discarded.
	_, err = e.Data(Data{Physical: PhysicalStreamID(churn + 1), Data: []byte{'x'}})
	require.ErrorIs(t, err, ErrFutureStream)
}

// TestStreamEnginePhysicalFirstWins proves the physical terminal publication
// is first-wins: whichever of Fail or Terminate publishes first fixes the
// connection and every live stream's outcome.
func TestStreamEnginePhysicalFirstWins(t *testing.T) {
	t.Run("failure wins an orderly close", func(t *testing.T) {
		e := testEngine()
		openTestStream(t, e, 1)
		cause := errors.New("physical loss")
		e.Fail(domain.RemoteFailureTimeout, cause)
		e.Terminate()
		require.True(t, e.Closed())
		require.Equal(t, cause, e.Err())
		require.Equal(t, domain.RemoteFailureTimeout, e.FailureKind())
		status := mustStatus(t, e, 1)
		require.Equal(t, cause, status.Err)
		require.Equal(t, domain.RemoteFailureTimeout, status.FailureKind)
	})

	t.Run("orderly close wins a failure", func(t *testing.T) {
		e := testEngine()
		openTestStream(t, e, 1)
		e.Terminate()
		e.Fail(domain.RemoteFailureTimeout, errors.New("late"))
		require.True(t, e.Closed())
		require.NoError(t, e.Err())
		require.Equal(t, domain.RemoteFailureNone, e.FailureKind())
		status := mustStatus(t, e, 1)
		require.NoError(t, status.Err)
		require.Equal(t, domain.RemoteFailureNone, status.FailureKind)
	})
}

// TestStreamEngineClosingPreservesQueuedData proves the orderly-close
// semantics: a Close that finds unread data marks the stream closing instead of
// discarding it, no new data is accepted, already-accepted chunks stay readable
// in order, and the first Take that empties the queue terminalizes the stream
// and releases its slot and bytes. A sibling is untouched throughout.
func TestStreamEngineClosingPreservesQueuedData(t *testing.T) {
	e := testEngine()
	closingRef := openTestStream(t, e, 1)
	siblingRef := openTestStream(t, e, 2)

	for _, chunk := range []struct {
		physical PhysicalStreamID
		data     string
	}{{1, "first"}, {1, "second"}, {2, "sibling"}} {
		disposition, err := e.Data(Data{Physical: chunk.physical, Data: []byte(chunk.data)})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
	}
	require.Equal(t, len("first")+len("second")+len("sibling"), e.AggregateBytes())

	// The orderly Close preserves both accepted chunks.
	disposition, err := e.Close(Close{Physical: 1})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	status := mustStatus(t, e, 1)
	require.Equal(t, StreamClosing, status.State)
	require.Equal(t, closingRef, status.Ref)
	require.False(t, channelClosed(status.Done), "a closing stream is not terminal yet")
	require.Equal(t, 2, status.QueuedChunks)
	require.Equal(t, len("first")+len("second"), status.QueuedBytes)
	require.Equal(t, 2, e.Live(), "the closing stream still holds its admission slot")
	require.Equal(t, len("first")+len("second")+len("sibling"), e.AggregateBytes())

	// No new data is accepted while closing, and a second Close changes nothing.
	_, err = e.Data(Data{Physical: 1, Data: []byte("late")})
	require.ErrorIs(t, err, ErrStreamState)
	disposition, err = e.Close(Close{Physical: 1})
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)
	require.Equal(t, 2, mustStatus(t, e, 1).QueuedChunks, "a duplicate close preserved the queue")

	// Draining preserves order and releases bytes chunk by chunk; only the last
	// take terminalizes the stream.
	chunk, ok := e.Take(1)
	require.True(t, ok)
	require.Equal(t, []byte("first"), chunk)
	require.Equal(t, StreamClosing, mustStatus(t, e, 1).State, "still closing with one chunk left")
	require.Equal(t, 2, e.Live())
	require.Equal(t, len("second")+len("sibling"), e.AggregateBytes())

	chunk, ok = e.Take(1)
	require.True(t, ok)
	require.Equal(t, []byte("second"), chunk)
	status = mustStatus(t, e, 1)
	require.Equal(t, StreamTerminal, status.State)
	require.True(t, channelClosed(status.Done))
	require.Zero(t, status.QueuedChunks)
	require.NoError(t, status.Err)
	require.Equal(t, 1, e.Live(), "the drained stream released its slot")
	require.Equal(t, len("sibling"), e.AggregateBytes(), "only the sibling's bytes remain")

	// A drained closing stream accepts no second drain and no new frame.
	_, ok = e.Take(1)
	require.False(t, ok)
	disposition, err = e.Close(Close{Physical: 1})
	require.NoError(t, err)
	require.Equal(t, StreamDiscarded, disposition)

	// The sibling is untouched and still holds its own queued chunk.
	siblingStatus := mustStatus(t, e, 2)
	require.Equal(t, StreamOpen, siblingStatus.State)
	require.Equal(t, siblingRef, siblingStatus.Ref)
	require.Equal(t, 1, siblingStatus.QueuedChunks)
	chunk, ok = e.Take(2)
	require.True(t, ok)
	require.Equal(t, []byte("sibling"), chunk)
}

// TestStreamEngineClosingResetDiscardsImmediately proves a Reset abandons a
// closing stream's preserved queue at once: the bytes, the admission slot, and
// the terminal outcome are released without waiting for a consumer.
func TestStreamEngineClosingResetDiscardsImmediately(t *testing.T) {
	e := testEngine()
	openTestStream(t, e, 1)
	_, err := e.Data(Data{Physical: 1, Data: []byte("queued")})
	require.NoError(t, err)
	disposition, err := e.Close(Close{Physical: 1})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	require.Equal(t, StreamClosing, mustStatus(t, e, 1).State)

	disposition, err = e.Reset(Reset{Physical: 1})
	require.NoError(t, err)
	require.Equal(t, StreamAccepted, disposition)
	status := mustStatus(t, e, 1)
	require.Equal(t, StreamTerminal, status.State)
	require.True(t, channelClosed(status.Done))
	require.Zero(t, status.QueuedChunks)
	require.Zero(t, status.QueuedBytes)
	require.Zero(t, e.AggregateBytes())
	require.Zero(t, e.Live())
	_, ok := e.Take(1)
	require.False(t, ok, "a reset discarded the preserved queue")
}

// TestStreamEngineClosingAccountingBounded proves the closing accounting is
// balanced across many cycles and stays bounded: a closing stream keeps its
// slot and bytes until the draining Take releases them, and the retired record
// set never grows past its retention bound.
func TestStreamEngineClosingAccountingBounded(t *testing.T) {
	e := testEngine()
	const churn = MaxRetiredStreamRecords * 3
	for i := 1; i <= churn; i++ {
		id := PhysicalStreamID(i)
		openTestStream(t, e, id)
		_, err := e.Data(Data{Physical: id, Data: []byte{byte(i)}})
		require.NoError(t, err)
		disposition, err := e.Close(Close{Physical: id})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.Equal(t, StreamClosing, mustStatus(t, e, id).State)
		require.Equal(t, 1, e.Live())
		require.Equal(t, 1, e.AggregateBytes())

		chunk, ok := e.Take(id)
		require.True(t, ok)
		require.Equal(t, []byte{byte(i)}, chunk)
		require.Equal(t, StreamTerminal, mustStatus(t, e, id).State)
		require.Zero(t, e.Live())
		require.Zero(t, e.AggregateBytes())
	}
	require.Len(t, e.retired, MaxRetiredStreamRecords)
	require.LessOrEqual(t, len(e.streams), MaxRetiredStreamRecords)
}
