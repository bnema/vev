package daemon

import (
	"errors"
	"runtime"
	"testing"

	vt "github.com/bnema/vev-vt"
	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev-vt/graphics"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/stretchr/testify/require"
)

func TestAttachmentOutputBuildsPipelinedDependencyChain(t *testing.T) {
	stream := newOutputStateStream()
	firstFrame := renderer.NewFrame(3, 1)
	fillOutputStateRows(firstFrame, []string{"abc"})
	first, ok, err := drawOutputState(t, stream, firstFrame, nil, true, 7)
	require.NoError(t, err)
	require.True(t, ok)
	firstOut, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(0), firstOut.Base)
	require.Equal(t, uint64(1), firstOut.New)
	require.Equal(t, uint64(7), firstOut.Echo)

	secondFrame := firstFrame.Clone()
	secondFrame.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	second, ok, err := drawOutputState(t, stream, secondFrame, []renderer.Damage{{Kind: renderer.DamageText, Width: 1, Height: 1}}, false, 8)
	require.NoError(t, err)
	require.True(t, ok)
	secondOut, err := ports.UnmarshalOutput(second.Payload)
	require.NoError(t, err)
	require.Equal(t, firstOut.New, secondOut.Base)
	require.Equal(t, uint64(2), secondOut.New)
	require.Equal(t, uint64(2), stream.outstanding())
}

func TestAttachmentOutputUsesTerminalColorProfile(t *testing.T) {
	frame := renderer.NewFrame(1, 1)
	frame.Set(0, 0, renderer.Cell{Rune: 'X', Style: renderer.Style{
		Foreground:           -1,
		Background:           -1,
		HasForegroundRGB:     true,
		ForegroundRGB:        renderer.RGB{R: 255, G: 0, B: 0},
		HasBackgroundRGB:     true,
		BackgroundRGB:        renderer.RGB{R: 0, G: 0, B: 255},
		HasUnderlineColorRGB: true,
		UnderlineColorRGB:    renderer.RGB{R: 0, G: 255, B: 0},
	}})

	cases := []struct {
		name         string
		capabilities ports.TerminalCapabilities
		want         string
	}{
		{
			name:         "truecolor",
			capabilities: ports.TerminalCapabilities{ColorMode: ports.TerminalColorTrueColor},
			want:         "\x1b[1;1H\x1b[0;38;2;255;0;0;48;2;0;0;255;58;2;0;255;0mX\x1b[0m",
		},
		{
			name:         "indexed 256",
			capabilities: ports.TerminalCapabilities{ColorMode: ports.TerminalColorIndexed256},
			want:         "\x1b[1;1H\x1b[0;38;5;196;48;5;21;58;5;46mX\x1b[0m",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := newOutputStateStreamForCapabilities(tc.capabilities)
			prepared, err := stream.prepare(frame, nil, true)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(prepared.data))
		})
	}
}

func TestAttachmentOutputCapacityProbeDoesNotRaceWithSend(t *testing.T) {
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

	stream.ack(1, 1)
	require.False(t, stream.atCapacity())
	stream.rebase()
	require.False(t, stream.atCapacity())
}

func TestAttachmentOutputEpochsAreAttachmentLocalAndPreparedFramesAreFenced(t *testing.T) {
	frame := renderer.NewFrame(3, 1)
	fillOutputStateRows(frame, []string{"abc"})
	newAttachment := func() *attachmentOutput {
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

func TestOutputStateAckRequiresCurrentEpoch(t *testing.T) {
	stream := newOutputStateStream()
	stream.next = 2
	stream.ack(2, 1)
	require.Zero(t, stream.acked, "a stale epoch must not retire output state")
	stream.ack(1, 1)
	require.Equal(t, uint64(1), stream.acked)
	stream.ack(1, 3)
	require.Equal(t, uint64(1), stream.acked, "a future state must not advance the ACK window")
}

func TestOutputStateSideEffectsDoNotAdvanceEpochStateOrACK(t *testing.T) {
	stream := newOutputStateStream()
	stream.ack(1, 0)
	beforeEpoch, beforeNext, beforeAck := stream.epoch, stream.next, stream.acked
	frame, err := stream.sideEffect([]byte("pty"), 9)
	require.NoError(t, err)
	out, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, beforeEpoch, out.Epoch)
	require.Zero(t, out.Base)
	require.Zero(t, out.New)
	require.False(t, out.Full)
	require.Equal(t, uint64(9), out.Echo)
	require.Equal(t, beforeNext, stream.next)
	require.Equal(t, beforeAck, stream.acked)
}

func TestAttachmentOutputFailedSendRetriesSnapshotWithoutAdvancing(t *testing.T) {
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
	require.Zero(t, out.Base, "retry after ambiguous send failure must be dependency-free")
	require.Equal(t, uint64(2), out.New, "state numbers remain monotonic across rebases")
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

func TestAttachmentOutputNoByteCommitAdvancesShadowWithoutState(t *testing.T) {
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

func TestAttachmentOutputRepeatedUnackedScrollRemainsClientCorrect(t *testing.T) {
	stream := newOutputStateStream()
	client := vt.NewScreen(4, 3)
	initial := renderer.NewFrame(4, 3)
	fillOutputStateRows(initial, []string{"AAAA", "BBBB", "CCCC"})
	first, ok, err := drawOutputState(t, stream, initial, nil, true, 0)
	require.NoError(t, err)
	require.True(t, ok)
	mustApplyOutput(t, client, first)
	stream.ack(1, 1)

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

func (s *attachmentOutput) render(frame renderer.Frame, damage []renderer.Damage, reset bool) ([]byte, error) {
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

func outputStateFrame(stream *attachmentOutput, data []byte, reset bool, echoAck uint64) ports.Frame {
	stream.next++
	base := stream.next - 1
	if reset {
		base = 0
	}
	frame, err := frameOutputState(data, base, stream.next, echoAck)
	if err != nil {
		panic(err)
	}
	return frame
}

func drawOutputState(t *testing.T, stream *attachmentOutput, frame renderer.Frame, damage []renderer.Damage, reset bool, echoAck uint64) (ports.Frame, bool, error) {
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
func TestAttachmentOutputResizeFrameThenNoopAndDamageAreDifferential(t *testing.T) {
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
	require.Zero(t, resizeOutput.Base, "resize must emit the one reset frame")
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
	require.Equal(t, resizeOutput.New, damageOutput.Base, "later damage must remain incremental")
}

func TestAttachmentOutputFailedSendKeepsTextCursorAndGraphicsSpeculative(t *testing.T) {
	scene := graphics.NewScene(graphics.Limits{})
	asset, err := scene.AddAsset(graphics.AssetBlob{Encoded: []byte("asset"), Width: 1, Height: 1})
	require.NoError(t, err)
	_, err = scene.PlaceAsset(asset, graphics.PixelRect{Width: 1, Height: 1})
	require.NoError(t, err)
	state := &capturedRenderState{panes: []capturedPaneRenderState{{
		graphics:         scene.Snapshot(),
		graphicsGeometry: domain.Geometry{Size: domain.Size{Cols: 1, Rows: 1}, PixelWidth: 1, PixelHeight: 1},
		placement:        layout.Placement{Content: domain.Rect{Width: 1, Height: 1}},
	}}}
	graphicsState := newGraphicsOutputState()
	output := attachmentOutputWithGraphics(graphicsState)
	output.lastCursor = cursorOut{valid: true, row: 1, col: 1}
	ac := &attachedClient{output: output, terminalCapabilities: ports.TerminalCapabilities{KittyGraphics: true}}
	output.attachment = ac
	frame := renderer.NewFrame(1, 1)
	prepared, err := output.prepareFrame(nil, state, frame, []renderer.Damage{renderer.FullRedraw()}, true, cursorOut{row: 2, col: 3})
	require.NoError(t, err)

	sendErr := errors.New("send failed")
	require.ErrorIs(t, prepared.send(0, func(ports.Frame) error { return sendErr }), sendErr)

	require.Equal(t, cursorOut{valid: true, row: 1, col: 1}, output.lastCursor)
	require.Empty(t, graphicsState.assets)
	require.Empty(t, graphicsState.placements)
	require.NotEmpty(t, graphicsState.pendingImages)
	require.NotEmpty(t, graphicsState.pendingPlaces)
}

func TestAttachmentOutputRebaseRetiresAttachmentState(t *testing.T) {
	output := newOutputStateStream()
	output.next = 3
	output.acked = 2
	output.lastCursor = cursorOut{valid: true, row: 2, col: 3}
	output.lastRoutePosition = ports.RoutePosition{ActiveTabID: "tab"}

	output.rebaseAttachment()

	require.Equal(t, uint64(2), output.epoch)
	require.Zero(t, output.next)
	require.Zero(t, output.acked)
	require.True(t, output.forceSnapshot)
	require.Equal(t, cursorOut{valid: true, row: 2, col: 3}, output.lastCursor, "the forced frame will reassert cursor state")
	require.Equal(t, ports.RoutePosition{}, output.lastRoutePosition)
}

func TestAttachmentOutputDefaultsAndNormalizesWindow(t *testing.T) {
	for _, tt := range []struct {
		name   string
		window []uint8
		want   uint64
	}{
		{name: "omitted", want: 8},
		{name: "zero", window: []uint8{0}, want: 8},
		{name: "oversized", window: []uint8{9}, want: 8},
		{name: "exact window", window: []uint8{1}, want: 1},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, newOutputStateStream(tt.window...).maxOutstanding)
		})
	}

	output := newOutputStateStream()
	output.setWindow(2)
	require.Equal(t, uint64(2), output.maxOutstanding)
	require.Equal(t, uint64(2), output.maxOutstandingAtomic.Load())
}

func TestNormalizeOutputWindow(t *testing.T) {
	for _, tt := range []struct {
		input, want uint8
	}{{0, 8}, {1, 1}, {8, 8}, {9, 8}, {255, 8}} {
		require.Equal(t, tt.want, normalizeOutputWindow(tt.input))
	}
}
