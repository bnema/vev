package daemon

import (
	"errors"
	"runtime"
	"testing"

	"github.com/bnema/vev/internal/domain"
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

func TestOutputStateStreamCapacityProbeDoesNotRaceWithSend(t *testing.T) {
	stream := newOutputStateStream(1)
	frame := renderer.NewFrame(3, 1)
	fillOutputStateRows(frame, []string{"abc"})
	prepared, err := stream.prepare(frame, nil, true)
	require.NoError(t, err)

	entered := make(chan struct{})
	release := make(chan struct{})
	sendDone := make(chan error, 1)
	go func() {
		sendDone <- prepared.send(prepared.data, 0, func(ports.Frame) error {
			close(entered)
			<-release
			return nil
		})
	}()
	<-entered

	probed := make(chan struct{})
	go func() {
		for range 100_000 {
			_ = stream.atCapacity()
			runtime.Gosched()
		}
		close(probed)
	}()
	close(release)
	require.NoError(t, <-sendDone)
	<-probed
	require.True(t, stream.atCapacity())

	stream.ack(1)
	require.False(t, stream.atCapacity())
	stream.rebase()
	require.False(t, stream.atCapacity())
}

func TestOutputStateStreamEpochsAreAttachmentLocalAndPreparedFramesAreFenced(t *testing.T) {
	frame := renderer.NewFrame(3, 1)
	fillOutputStateRows(frame, []string{"abc"})
	newAttachment := func() *outputStateStream {
		stream := newOutputStateStream()
		ac := &attachedClient{output: stream, size: domain.Size{Cols: 3, Rows: 1}}
		stream.attachment = ac
		return stream
	}

	first := newAttachment()
	second := newAttachment()
	firstPrepared, err := first.prepare(frame, nil, true)
	require.NoError(t, err)
	var firstFrame ports.Frame
	require.NoError(t, firstPrepared.send(firstPrepared.data, 0, func(frame ports.Frame) error {
		firstFrame = frame
		return nil
	}))
	firstOutput, err := ports.UnmarshalOutput(firstFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstOutput.Epoch)
	require.Equal(t, uint64(1), firstOutput.New)
	require.True(t, firstOutput.Full)
	first.ack(firstOutput.Epoch, firstOutput.New)

	secondPrepared, err := second.prepare(frame, nil, true)
	require.NoError(t, err)
	require.NoError(t, secondPrepared.send(secondPrepared.data, 0, func(ports.Frame) error { return nil }))
	require.Equal(t, uint64(1), second.epoch)
	require.Equal(t, uint64(1), second.next)
	require.Equal(t, uint64(1), first.next)

	changed := frame.Clone()
	changed.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	pending, err := first.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false)
	require.NoError(t, err)
	first.rebase()
	require.Equal(t, uint64(2), first.epoch)
	require.Zero(t, first.next)
	require.Zero(t, first.acked)
	var staleSent bool
	require.NoError(t, pending.send(pending.data, 0, func(ports.Frame) error {
		staleSent = true
		return nil
	}))
	require.False(t, staleSent)
	require.Zero(t, first.next)
	first.ack(1, 1)
	require.Zero(t, first.acked, "an ACK from the retired epoch must not advance the new epoch")

	reset, err := first.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false)
	require.NoError(t, err)
	var resetFrame ports.Frame
	require.NoError(t, reset.send(reset.data, 0, func(frame ports.Frame) error {
		resetFrame = frame
		return nil
	}))
	resetOutput, err := ports.UnmarshalOutput(resetFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(2), resetOutput.Epoch)
	require.Zero(t, resetOutput.Base)
	require.Equal(t, uint64(1), resetOutput.New)
	require.True(t, resetOutput.Full)
}

func TestPreparedOutputDropsReplacedConnectionAndView(t *testing.T) {
	for _, tt := range []struct {
		name   string
		change func(*attachedClient)
	}{
		{name: "connection generation", change: func(ac *attachedClient) { ac.connectionGeneration.Add(1) }},
		{name: "view revision", change: func(ac *attachedClient) {
			ac.viewMu.Lock()
			ac.view.revision++
			ac.viewMu.Unlock()
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stream := newOutputStateStream()
			ac := &attachedClient{output: stream, size: domain.Size{Cols: 3, Rows: 1}}
			stream.attachment = ac
			frame := renderer.NewFrame(3, 1)
			fillOutputStateRows(frame, []string{"abc"})
			initial, err := stream.prepare(frame, nil, true)
			require.NoError(t, err)
			require.NoError(t, initial.send(initial.data, 0, func(ports.Frame) error { return nil }))
			changed := frame.Clone()
			changed.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
			pending, err := stream.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false)
			require.NoError(t, err)
			tt.change(ac)
			var sent bool
			require.NoError(t, pending.send(pending.data, 0, func(ports.Frame) error {
				sent = true
				return nil
			}))
			require.False(t, sent)
			require.Equal(t, uint64(1), stream.next)

			retry, err := stream.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false)
			require.NoError(t, err)
			require.NoError(t, retry.send(retry.data, 0, func(ports.Frame) error { return nil }))
			require.Equal(t, uint64(2), stream.next)
		})
	}
}

func TestOutputStateSideEffectsDoNotAdvanceEpochStateOrACK(t *testing.T) {
	stream := newOutputStateStream()
	stream.ack(1, 0)
	beforeEpoch, beforeNext, beforeAck := stream.epoch, stream.next, stream.acked
	out, err := ports.UnmarshalOutput(stream.sideEffect([]byte("pty"), 9).Payload)
	require.NoError(t, err)
	require.Equal(t, beforeEpoch, out.Epoch)
	require.Zero(t, out.Base)
	require.Zero(t, out.New)
	require.False(t, out.Full)
	require.Equal(t, uint64(9), out.Echo)
	require.Equal(t, beforeNext, stream.next)
	require.Equal(t, beforeAck, stream.acked)
}

func TestOutputStateStreamFailedSendRetriesSnapshotWithoutAdvancing(t *testing.T) {
	stream := newOutputStateStream()
	initial := renderer.NewFrame(3, 1)
	fillOutputStateRows(initial, []string{"abc"})
	first, err := stream.prepare(initial, nil, true)
	require.NoError(t, err)
	require.NoError(t, first.send(first.data, 0, func(ports.Frame) error { return nil }))
	require.Equal(t, uint64(1), stream.next)

	changed := initial.Clone()
	changed.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	pending, err := stream.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false)
	require.NoError(t, err)
	probe, err := stream.renderer.Prepare(initial, nil, false)
	require.NoError(t, err)
	require.Empty(t, probe.Bytes(), "preparation must not advance the renderer shadow")

	sendErr := errors.New("send failed")
	require.ErrorIs(t, pending.send(pending.data, 0, func(ports.Frame) error { return sendErr }), sendErr)
	require.Equal(t, uint64(1), stream.next, "failed send must not advance the state chain")
	pending.commitNoSend()
	probe, err = stream.renderer.Prepare(initial, nil, false)
	require.NoError(t, err)
	require.Empty(t, probe.Bytes(), "failed send must retain the committed renderer shadow")

	retry, err := stream.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false)
	require.NoError(t, err)
	var sent ports.Frame
	require.NoError(t, retry.send(retry.data, 0, func(frame ports.Frame) error {
		sent = frame
		return nil
	}))
	out, err := ports.UnmarshalOutput(sent.Payload)
	require.NoError(t, err)
	require.Zero(t, out.BaseStateNum, "retry after ambiguous send failure must be dependency-free")
	require.Equal(t, uint64(2), out.NewStateNum, "state numbers remain monotonic across rebases")
	require.Equal(t, uint64(2), stream.next, "successful retry advances the chain exactly once")
	require.NoError(t, retry.send(retry.data, 0, func(ports.Frame) error {
		t.Fatal("completed output sent twice")
		return nil
	}))
	require.Equal(t, uint64(2), stream.next)
	probe, err = stream.renderer.Prepare(changed, nil, false)
	require.NoError(t, err)
	require.Empty(t, probe.Bytes(), "successful retry commits the candidate shadow")
}

func TestOutputStateStreamNoByteCommitAdvancesShadowWithoutState(t *testing.T) {
	stream := newOutputStateStream()
	frame := renderer.NewFrame(3, 1)
	fillOutputStateRows(frame, []string{"abc"})
	initial, err := stream.prepare(frame, nil, true)
	require.NoError(t, err)
	require.NoError(t, initial.send(initial.data, 0, func(ports.Frame) error { return nil }))

	noOp, err := stream.prepare(frame, nil, false)
	require.NoError(t, err)
	require.Empty(t, noOp.data)
	noOp.commitNoSend()
	noOp.commitNoSend()
	require.Equal(t, uint64(1), stream.next, "no-byte commit must not create a state frame")

	probe, err := stream.renderer.Prepare(frame, nil, false)
	require.NoError(t, err)
	require.Empty(t, probe.Bytes(), "no-byte commit retains the prepared visual candidate")
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

func (s *outputStateStream) render(frame renderer.Frame, damage []renderer.Damage, reset bool) ([]byte, error) {
	prepared, err := s.prepare(frame, damage, reset)
	if err != nil {
		return nil, err
	}
	if len(prepared.data) == 0 {
		prepared.commitNoSend()
		return nil, nil
	}
	// This helper tests renderer replay bytes without publishing a wire frame.
	// Production state advancement happens only through preparedOutput.send.
	prepared.draw.Commit()
	s.forceSnapshot = false
	return prepared.data, nil
}

func outputStateFrame(stream *outputStateStream, data []byte, reset bool, echoAck uint64) ports.Frame {
	stream.next++
	base := stream.next - 1
	if reset {
		base = 0
	}
	return frameOutputState(data, base, stream.next, echoAck)
}

func drawOutputState(t *testing.T, stream *outputStateStream, frame renderer.Frame, damage []renderer.Damage, reset bool, echoAck uint64) (ports.Frame, bool, error) {
	t.Helper()
	prepared, err := stream.prepare(frame, damage, reset)
	if err != nil || len(prepared.data) == 0 {
		if prepared != nil {
			prepared.commitNoSend()
		}
		return ports.Frame{}, false, err
	}
	var output ports.Frame
	err = prepared.send(prepared.data, echoAck, func(frame ports.Frame) error {
		output = frame
		return nil
	})
	if err != nil {
		return ports.Frame{}, false, err
	}
	return output, true, nil
}

// A resize is a state reset, but it must not turn subsequent idle or damaged
// renders into full frames.
func TestOutputStateStreamResizeFrameThenNoopAndDamageAreDifferential(t *testing.T) {
	screen := vt.NewScreen(4, 2)
	screen.Write([]byte("abcd"))
	screen.ClearDamage()
	screen.Resize(6, 2)

	stream := newOutputStateStream()
	resized, ok, err := drawOutputState(t, stream, screen.Frame, screen.Damage(), true, 0)
	require.NoError(t, err)
	require.True(t, ok)
	resizeOutput, err := ports.UnmarshalOutput(resized.Payload)
	require.NoError(t, err)
	require.Zero(t, resizeOutput.BaseStateNum, "resize must emit the one reset frame")
	screen.ClearDamage()

	_, ok, err = drawOutputState(t, stream, screen.Frame, screen.Damage(), false, 0)
	require.NoError(t, err)
	require.False(t, ok, "an unchanged render after resize must emit nothing")

	screen.Frame.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	damaged, ok, err := drawOutputState(t, stream, screen.Frame, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false, 0)
	require.NoError(t, err)
	require.True(t, ok)
	damageOutput, err := ports.UnmarshalOutput(damaged.Payload)
	require.NoError(t, err)
	require.Equal(t, resizeOutput.NewStateNum, damageOutput.BaseStateNum, "later damage must remain incremental")
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
