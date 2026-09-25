package daemonmux

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// TestPumpApplyOpenDuplicateIsolation proves a duplicate or replayed inbound
// Open never disturbs the stream it names and never produces an outbound frame:
// a live stream keeps its state, its watch, and its queued outbound work, a
// retired stream stays terminal, and a replayed identity below the high-water
// mark stays unrecorded.
func TestPumpApplyOpenDuplicateIsolation(t *testing.T) {
	t.Run("live stream", func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		require.NoError(t, pump.Engine().Open(openFor(1)))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
		// A queued outbound frame proves the duplicate Open neither resets the
		// engine record nor discards the scheduler's queued work.
		require.NoError(t, pump.SendData(1, []byte("out"), nil))
		before := mustStatus(t, pump.Engine(), 1)
		watch := pump.Watch(1)

		require.NoError(t, pump.applyOpen(openFor(1)))

		after := mustStatus(t, pump.Engine(), 1)
		require.Equal(t, before, after, "a duplicate open left the live stream untouched")
		require.Equal(t, StreamOpen, after.State)
		require.Equal(t, 1, pump.Engine().Live())
		require.False(t, channelClosed(after.Done))
		require.Equal(t, watch, pump.Watch(1), "a duplicate open never rotated the watch channel")
		require.Equal(t, 1, pump.scheduler.QueuedFrames(1), "the queued outbound frame survived")
		carrier.requireNoSent(t, 50*time.Millisecond)

		pump.Start(context.Background())
		message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
		require.NoError(t, err)
		require.Equal(t, Data{Physical: 1, Data: []byte("out")}, message)
		carrier.requireNoSent(t, 50*time.Millisecond)
	})

	t.Run("retired stream", func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		require.NoError(t, pump.Engine().Open(openFor(1)))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
		disposition, err := pump.Engine().Close(Close{Physical: 1})
		require.NoError(t, err)
		require.Equal(t, StreamAccepted, disposition)
		require.Equal(t, StreamTerminal, mustStatus(t, pump.Engine(), 1).State)

		require.NoError(t, pump.applyOpen(openFor(1)))

		status := mustStatus(t, pump.Engine(), 1)
		require.Equal(t, StreamTerminal, status.State, "a replayed open never revives a retired stream")
		require.NoError(t, status.Err)
		require.Zero(t, pump.Engine().Live())
		carrier.requireNoSent(t, 50*time.Millisecond)
	})

	t.Run("replayed identity below the high-water mark", func(t *testing.T) {
		pump, carrier := newTestPump(t, DirectionClient)
		require.NoError(t, pump.Engine().Open(openFor(3)))
		require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(3)}))

		// Identity 1 is at or below the high-water mark but was never admitted:
		// it can only be a replay, so it is ignored instead of refused.
		require.NoError(t, pump.applyOpen(openFor(1)))
		_, tracked := pump.Engine().Status(1)
		require.False(t, tracked, "a replayed identity is never recorded")
		require.Equal(t, 1, pump.Engine().Live())
		carrier.requireNoSent(t, 50*time.Millisecond)
	})
}

// TestPumpFreshOpenRefusedSendsTypedRefused proves a fresh Open the engine
// refused for capacity is signalled with exactly one typed Mux Refused carrying
// its admission code, records no stream, and leaves the live sibling and the
// physical connection untouched.
func TestPumpFreshOpenRefusedSendsTypedRefused(t *testing.T) {
	ceilings := DefaultMuxCeilings()
	ceilings.MaxStreams = 1
	pump, carrier := newTestPumpWithCeilings(t, DirectionClient, ceilings)
	pump.Start(context.Background())
	require.NoError(t, pump.Engine().Open(openFor(1)))
	require.NoError(t, pump.Engine().Opened(Opened{Ref: testRef(1)}))
	require.Equal(t, 1, pump.Engine().Live())

	// The fresh identity is refused by the stream ceiling before it is recorded.
	deliverClientFrame(t, carrier, openFor(2))

	message, err := DecodeServer(carrier.nextSent(t), testEnvelopeCeiling, testChunkCeiling)
	require.NoError(t, err)
	refused, ok := message.(Refused)
	require.True(t, ok, "a fresh refusal is a Mux Refused")
	require.Equal(t, testRef(2), refused.Ref)
	require.Equal(t, ports.BrokerErrorUnavailable, refused.Error.Code)
	require.Equal(t, uint32(1), refused.Error.AdmissionCode, "capacity refusal carries the limit code")
	require.Equal(t, domain.RemoteFailureNone, refused.Error.FailureKind)
	require.NotEmpty(t, refused.Error.Text)
	require.LessOrEqual(t, len(refused.Error.Text), ports.BrokerMaxErrorBytes)
	require.NoError(t, refused.Error.validate())

	// No stream was recorded for the refused identity, and the sibling and the
	// physical connection are untouched.
	_, tracked := pump.Engine().Status(2)
	require.False(t, tracked, "a fresh refusal records no stream")
	require.Equal(t, 1, pump.Engine().Live())
	require.False(t, channelClosed(mustStatus(t, pump.Engine(), 1).Done))
	require.False(t, pump.Engine().Closed())
	require.False(t, channelClosed(pump.Done()))
	carrier.requireNoSent(t, 50*time.Millisecond)
}

// TestOpenRefusalDetail proves the closed admission taxonomy the broker
// classifies without matching text: capacity is a limit, a closed physical
// connection is closed, and every other refusal is an invalid request.
func TestOpenRefusalDetail(t *testing.T) {
	tests := []struct {
		name  string
		cause error
		code  uint32
		text  string
	}{
		{"capacity", ErrTooManyStreams, 1, "stream limit reached"},
		{"closed", ErrPhysicalClosed, 2, "physical connection closed"},
		{"invalid authority", ErrInvalidStream, 4, "invalid open request"},
		{"unknown refusal", errors.New("boom"), 4, "invalid open request"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail := openRefusalDetail(tt.cause)
			require.Equal(t, tt.code, detail.AdmissionCode)
			require.Equal(t, tt.text, detail.Text)
			require.Equal(t, ports.BrokerErrorUnavailable, detail.Code)
			require.Equal(t, domain.RemoteFailureNone, detail.FailureKind)
			require.NoError(t, detail.validate())
		})
	}
}
