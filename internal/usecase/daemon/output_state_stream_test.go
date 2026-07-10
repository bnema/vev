package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func TestOutputStateStreamBuildsPipelinedDependencyChain(t *testing.T) {
	stream := newOutputStateStream()
	firstFrame := renderer.NewFrame(3, 1)
	fillOutputStateRows(firstFrame, []string{"abc"})
	first, ok, err := drawOutputState(t, stream, firstFrame, nil, true, 7)
	require.NoError(t, err)
	require.True(t, ok)
	firstOut, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstOut.BaseStateNum)
	require.Equal(t, uint64(1), firstOut.NewStateNum)
	require.Equal(t, uint64(7), firstOut.EchoAck)

	secondFrame := firstFrame.Clone()
	secondFrame.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	second, ok, err := drawOutputState(t, stream, secondFrame, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false, 8)
	require.NoError(t, err)
	require.True(t, ok)
	secondOut, err := ports.UnmarshalOutput(second.Payload)
	require.NoError(t, err)
	require.Equal(t, firstOut.NewStateNum, secondOut.BaseStateNum)
	require.Equal(t, uint64(2), secondOut.NewStateNum)
	require.Equal(t, uint64(2), stream.outstanding())
}

func TestOutputStateStreamRepeatedUnackedScrollRemainsClientCorrect(t *testing.T) {
	stream := newOutputStateStream()
	client := vt.NewScreen(4, 3)
	initial := renderer.NewFrame(4, 3)
	fillOutputStateRows(initial, []string{"AAAA", "BBBB", "CCCC"})
	first, ok, err := drawOutputState(t, stream, initial, nil, true, 0)
	require.NoError(t, err)
	require.True(t, ok)
	mustApplyOutput(t, client, first)
	stream.ack(1)

	scrolled := renderer.NewFrame(4, 3)
	fillOutputStateRows(scrolled, []string{"BBBB", "CCCC", "N   "})
	damage := []renderer.Damage{
		{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 4, Height: 3, Count: 1},
		{Kind: renderer.DamageText, X: 0, Y: 2, Width: 4, Height: 1},
	}
	second, ok, err := drawOutputState(t, stream, scrolled, damage, false, 0)
	require.NoError(t, err)
	require.True(t, ok)
	mustApplyOutput(t, client, second)

	// The renderer baseline is the preceding emitted frame, so repeating stale
	// scroll damage falls back to an overwrite instead of applying a second
	// incompatible scroll program.
	third, ok, err := drawOutputState(t, stream, scrolled, damage, false, 0)
	require.NoError(t, err)
	require.True(t, ok)
	mustApplyOutput(t, client, third)
	for y, want := range []string{"BBBB", "CCCC", "N   "} {
		require.Equal(t, want, outputStateRow(client.Frame.Row(y)))
	}
}

func TestOutputStateStreamAckProgressionReleasesDeferredPaint(t *testing.T) {
	stream := newOutputStateStream()
	for range maxUnackedOutputStates {
		stream.frame([]byte("output"), false, 0)
	}
	require.True(t, stream.deferIfAtCapacity(false))
	_, ok := stream.takeDeferred()
	require.False(t, ok)
	stream.ack(maxUnackedOutputStates - 1)
	reset, ok := stream.takeDeferred()
	require.True(t, ok)
	require.False(t, reset)
	require.Equal(t, uint64(1), stream.outstanding())

	// Stale and future ACKs do not move the cumulative acknowledgement.
	stream.ack(maxUnackedOutputStates - 2)
	stream.ack(maxUnackedOutputStates + 1)
	require.Equal(t, uint64(maxUnackedOutputStates-1), stream.acked)
}

func TestOutputStateStreamResetIsDependencyFreeAndClearsDeferral(t *testing.T) {
	stream := newOutputStateStream()
	for range maxUnackedOutputStates {
		stream.frame([]byte("output"), false, 0)
	}
	require.True(t, stream.deferIfAtCapacity(false))
	for range 32 {
		require.True(t, stream.deferIfAtCapacity(true), "reset paints must coalesce while the stream is at capacity")
	}
	require.Equal(t, uint64(maxUnackedOutputStates), stream.outstanding())
	stream.ack(maxUnackedOutputStates)
	reset, ok := stream.takeDeferred()
	require.True(t, ok)
	require.True(t, reset, "a coalesced reset must flush as a dependency-free repaint")

	frame := renderer.NewFrame(2, 1)
	fillOutputStateRows(frame, []string{"ok"})
	outFrame, ok, err := drawOutputState(t, stream, frame, nil, true, 0)
	require.NoError(t, err)
	require.True(t, ok)
	out, err := ports.UnmarshalOutput(outFrame.Payload)
	require.NoError(t, err)
	require.Zero(t, out.BaseStateNum)
	_, ok = stream.takeDeferred()
	require.False(t, ok)
}

func drawOutputState(t *testing.T, stream *outputStateStream, frame renderer.Frame, damage []renderer.Damage, reset bool, echoAck uint64) (ports.Frame, bool, error) {
	t.Helper()
	data, err := stream.render(frame, damage, reset)
	if err != nil || len(data) == 0 {
		return ports.Frame{}, false, err
	}
	return stream.frame(data, reset, echoAck), true, nil
}

func TestOutputStateStreamWindowOneCoalescesUntilCumulativeAck(t *testing.T) {
	stream := newOutputStateStream(1)
	stream.frame([]byte("first"), true, 0)
	for range 100 {
		require.True(t, stream.deferIfAtCapacity(false))
	}
	require.Equal(t, uint64(1), stream.outstanding())

	// A stale ACK cannot release the coalesced paint; the cumulative ACK for
	// the actually sent state does.
	stream.ack(0)
	_, ok := stream.takeDeferred()
	require.False(t, ok)
	stream.ack(1)
	reset, ok := stream.takeDeferred()
	require.True(t, ok)
	require.False(t, reset)
	stream.frame([]byte("latest"), reset, 0)
	require.Equal(t, uint64(1), stream.outstanding())

	sideEffect := stream.sideEffect([]byte("reliable"), 0)
	output, err := ports.UnmarshalOutput(sideEffect.Payload)
	require.NoError(t, err)
	require.Zero(t, output.BaseStateNum)
	require.Zero(t, output.NewStateNum)
	require.Equal(t, uint64(2), stream.next)
}

func TestOutputStateStreamDefaultsAndNormalizesWindow(t *testing.T) {
	require.Equal(t, uint64(8), newOutputStateStream().maxOutstanding)
	require.Equal(t, uint64(8), newOutputStateStream(0).maxOutstanding)
	require.Equal(t, uint64(8), newOutputStateStream(9).maxOutstanding)
	require.Equal(t, uint64(1), newOutputStateStream(1).maxOutstanding)
}

func TestNormalizeOutputWindow(t *testing.T) {
	for _, tt := range []struct {
		input, want uint8
	}{{0, 8}, {1, 1}, {8, 8}, {9, 8}, {255, 8}} {
		require.Equal(t, tt.want, normalizeOutputWindow(tt.input))
	}
}
