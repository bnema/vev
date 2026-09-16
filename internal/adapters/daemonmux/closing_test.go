package daemonmux

import (
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPumpInboundDataClosePipelineDrainsThenEOF proves a pipelined inbound
// Data followed by an orderly Close stays readable: the consumer drains the
// accepted bytes and only then observes io.EOF, at which point the stream is
// terminal and its bytes are released.
func TestPumpInboundDataClosePipelineDrainsThenEOF(t *testing.T) {
	pump, carrier := newTestPump(t, DirectionClient)
	pump.Start(context.Background())
	require.NoError(t, pump.Engine().Open(openFor(1)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
	status := mustStatus(t, pump.Engine(), 1)
	pipe := newMuxStreamPipe(pump, 1, pump.Ceilings().StreamChunkLimit, status.Done)

	// The peer pipelines one Data frame and its Close before the consumer reads.
	deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("hello")})
	deliverClientFrame(t, carrier, Close{Physical: 1})

	buffer := make([]byte, len("hello"))
	_, err := io.ReadFull(pipe, buffer)
	require.NoError(t, err)
	require.Equal(t, "hello", string(buffer))
	// The stream ends only after the preserved bytes are drained.
	_, err = pipe.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.EOF)

	requireEngineEventually(t, pump, func(e *StreamEngine) bool { return e.Live() == 0 })
	status = mustStatus(t, pump.Engine(), 1)
	require.Equal(t, StreamTerminal, status.State)
	require.True(t, channelClosed(status.Done))
	require.Zero(t, status.QueuedChunks)
	require.Zero(t, pump.Engine().AggregateBytes())
	require.NoError(t, status.Err)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}

// TestPumpInboundCloseSiblingIsolation proves an orderly Close with unread data
// settles only its own stream once drained: a sibling keeps its state, its
// queued chunk, and its admission slot while the closing stream's data is
// preserved. Draining the sibling never closes it.
func TestPumpInboundCloseSiblingIsolation(t *testing.T) {
	pump, carrier := newTestPump(t, DirectionClient)
	pump.Start(context.Background())
	require.NoError(t, pump.Engine().Open(openFor(1)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
	require.NoError(t, pump.Engine().Open(openFor(2)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(2)}))

	deliverClientFrame(t, carrier, Data{Physical: 1, Data: []byte("one")})
	deliverClientFrame(t, carrier, Data{Physical: 2, Data: []byte("two")})
	deliverClientFrame(t, carrier, Close{Physical: 1})

	requireEngineEventually(t, pump, func(e *StreamEngine) bool {
		status, ok := e.Status(1)
		return ok && status.State == StreamClosing
	})
	sibling := mustStatus(t, pump.Engine(), 2)
	require.Equal(t, StreamOpen, sibling.State, "the sibling is untouched by the close")
	require.Equal(t, 1, sibling.QueuedChunks)

	chunk, ok := takeEventually(t, pump, 2)
	require.True(t, ok)
	require.Equal(t, []byte("two"), chunk)
	require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State, "draining a sibling never closes it")

	chunk, ok = takeEventually(t, pump, 1)
	require.True(t, ok)
	require.Equal(t, []byte("one"), chunk)
	require.Equal(t, StreamTerminal, mustStatus(t, pump.Engine(), 1).State)
	require.Equal(t, StreamOpen, mustStatus(t, pump.Engine(), 2).State)
	require.Equal(t, 1, pump.Engine().Live())
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}

// TestPumpInboundCloseChurnAccountingBounded proves the closing accounting is
// balanced and bounded across many inbound Data-then-Close cycles: each closing
// stream holds exactly its unread bytes until the draining Take releases them,
// and the engine's retained record set never grows past its retention bound.
func TestPumpInboundCloseChurnAccountingBounded(t *testing.T) {
	pump, _ := newTestPump(t, DirectionClient)
	pump.Start(context.Background())
	const churn = int(MaxMuxStreams) * 8
	for i := 1; i <= churn; i++ {
		id := PhysicalStreamID(i)
		require.NoError(t, pump.Engine().Open(openFor(id)))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(id)}))
		require.NoError(t, pump.applyClient(Data{Physical: id, Data: []byte("payload")}))
		require.NoError(t, pump.applyClient(Close{Physical: id}))

		require.Equal(t, StreamClosing, mustStatus(t, pump.Engine(), id).State)
		require.Equal(t, 1, pump.Engine().Live(), "the closing stream still holds its slot")
		require.Equal(t, len("payload"), pump.Engine().AggregateBytes())

		chunk, ok := pump.Take(id)
		require.True(t, ok)
		require.Equal(t, []byte("payload"), chunk)
		require.Equal(t, StreamTerminal, mustStatus(t, pump.Engine(), id).State)
		require.Zero(t, pump.Engine().Live())
		require.Zero(t, pump.Engine().AggregateBytes())
	}
	require.Len(t, pump.Engine().retired, MaxRetiredStreamRecords)
	require.LessOrEqual(t, len(pump.Engine().streams), MaxRetiredStreamRecords)
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
}

// TestPumpInboundResetDiscardsClosingQueue proves a Reset still abandons a
// closing stream's preserved queue immediately, releasing the bytes and the
// slot without waiting for a consumer.
func TestPumpInboundResetDiscardsClosingQueue(t *testing.T) {
	pump, _ := newTestPump(t, DirectionClient)
	require.NoError(t, pump.Engine().Open(openFor(1)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
	require.NoError(t, pump.applyClient(Data{Physical: 1, Data: []byte("queued")}))
	require.NoError(t, pump.applyClient(Close{Physical: 1}))
	require.Equal(t, StreamClosing, mustStatus(t, pump.Engine(), 1).State)

	require.NoError(t, pump.applyClient(Reset{Physical: 1}))

	status := mustStatus(t, pump.Engine(), 1)
	require.Equal(t, StreamTerminal, status.State)
	require.True(t, channelClosed(status.Done))
	require.Zero(t, status.QueuedChunks)
	require.Zero(t, pump.Engine().AggregateBytes())
	require.Zero(t, pump.Engine().Live())
	_, ok := pump.Take(1)
	require.False(t, ok, "the reset discarded the preserved queue")
}
