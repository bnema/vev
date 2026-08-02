package daemon

import (
	"errors"
	"math"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/require"
)

func TestStructuredOutputStreamSnapshotsThenDeltas(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	frame := structuredTestFrame("abc", "def")
	cursor := cursorOut{valid: true}

	first, err := stream.prepare(frame, nil, cursor, true, 7)
	require.NoError(t, err)
	firstUpdate := decodeStructured(t, first.data)
	require.Equal(t, ports.ScreenUpdateSnapshot, firstUpdate.Kind)
	require.Zero(t, firstUpdate.BaseStateNum)
	require.Equal(t, uint64(1), firstUpdate.NewStateNum)
	require.Len(t, firstUpdate.Spans, 2)
	for y, span := range firstUpdate.Spans {
		require.Equal(t, uint16(y), span.Y)
		require.Zero(t, span.X)
		require.Len(t, span.Cells, frame.Width)
		for x, cell := range span.Cells {
			require.Equal(t, frame.At(x, y).Rune, cell.Rune)
			require.Equal(t, frame.At(x, y).Continuation, cell.Continuation)
		}
	}
	require.NoError(t, first.send(func(got ports.Frame) error {
		require.Equal(t, ports.MsgScreenUpdate, got.Type)
		gotUpdate, err := ports.UnmarshalScreenUpdate(got.Payload)
		require.NoError(t, err)
		require.Equal(t, uint64(7), gotUpdate.EchoAck)
		return nil
	}))
	require.Equal(t, uint64(1), state.next)

	changed := frame.Clone()
	changed.Set(1, 0, renderer.Cell{Rune: 'X', Style: renderer.DefaultStyle()})
	second, err := stream.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, X: 1, Y: 0, Width: 1, Height: 1}}, cursor, false, 0)
	require.NoError(t, err)
	secondUpdate := decodeStructured(t, second.data)
	require.Equal(t, ports.ScreenUpdateDelta, secondUpdate.Kind)
	require.Equal(t, uint64(1), secondUpdate.BaseStateNum)
	require.Equal(t, uint64(2), secondUpdate.NewStateNum)
	require.Len(t, secondUpdate.Spans, 1)
	require.Equal(t, uint16(1), secondUpdate.Spans[0].X)
	require.NoError(t, second.send(func(ports.Frame) error { return nil }))
	require.Equal(t, uint64(2), state.next)
}

func TestStructuredOutputStreamCursorOnlyAndNoopOwnership(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	frame := structuredTestFrame("abc")
	cursor := cursorOut{valid: true}

	first, err := stream.prepare(frame, nil, cursor, true, 0)
	require.NoError(t, err)
	require.NoError(t, first.send(func(ports.Frame) error { return nil }))

	moved := cursor
	moved.col = 2
	cursorOnly, err := stream.prepare(frame, nil, moved, false, 0)
	require.NoError(t, err)
	cursorUpdate := decodeStructured(t, cursorOnly.data)
	require.Equal(t, ports.ScreenUpdateDelta, cursorUpdate.Kind)
	require.Empty(t, cursorUpdate.Spans)
	require.Nil(t, cursorUpdate.Scroll)
	require.NoError(t, cursorOnly.send(func(ports.Frame) error { return nil }))
	require.Equal(t, uint64(2), state.next)

	noop, err := stream.prepare(frame, nil, moved, false, 0)
	require.NoError(t, err)
	require.Empty(t, noop.data)
	require.NoError(t, noop.send(func(ports.Frame) error {
		t.Fatal("noop must not send a frame")
		return nil
	}))
	require.Equal(t, uint64(2), state.next)
	require.True(t, stream.cursorOut.valid)
	require.Equal(t, moved.row, stream.cursorOut.row)
	require.Equal(t, moved.col, stream.cursorOut.col)
	require.Equal(t, !moved.hidden, !stream.cursorOut.hidden)
}

func TestStructuredOutputStreamDeltaCommitReusesShadowStorage(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	cursor := cursorOut{valid: true}
	frame := structuredTestFrame("abc", "def")

	initial, err := stream.prepare(frame, nil, cursor, true, 0)
	require.NoError(t, err)
	require.NoError(t, initial.send(func(ports.Frame) error { return nil }))
	shadowCells := stream.shadow.Cells
	require.NotEmpty(t, shadowCells)
	shadowStart := &shadowCells[0]
	shadowCap := cap(shadowCells)

	changed := frame.Clone()
	changed.Set(1, 0, renderer.Cell{Rune: 'X', Style: renderer.DefaultStyle()})
	delta, err := stream.prepare(changed, []renderer.Damage{{Kind: renderer.DamageText, X: 1, Y: 0, Width: 1, Height: 1}}, cursor, false, 0)
	require.NoError(t, err)
	require.NoError(t, delta.send(func(ports.Frame) error { return nil }))

	require.Same(t, shadowStart, &stream.shadow.Cells[0])
	require.Equal(t, shadowCap, cap(stream.shadow.Cells))
	require.Equal(t, changed.At(1, 0), stream.shadow.At(1, 0))
	require.Equal(t, uint64(2), state.next)
}

func TestStructuredOutputStreamResetAndResizeUseBaseZero(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	cursor := cursorOut{valid: true}
	frame := structuredTestFrame("abc")

	first, err := stream.prepare(frame, nil, cursor, true, 0)
	require.NoError(t, err)
	require.NoError(t, first.send(func(ports.Frame) error { return nil }))
	changed := frame.Clone()
	changed.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	delta, err := stream.prepare(changed, nil, cursor, false, 0)
	require.NoError(t, err)
	require.NoError(t, delta.send(func(ports.Frame) error { return nil }))

	resized := structuredTestFrame("abcd", "efgh")
	reset, err := stream.prepare(resized, nil, cursor, true, 0)
	require.NoError(t, err)
	resetUpdate := decodeStructured(t, reset.data)
	require.Equal(t, ports.ScreenUpdateSnapshot, resetUpdate.Kind)
	require.Zero(t, resetUpdate.BaseStateNum)
	require.Equal(t, uint64(3), resetUpdate.NewStateNum)
	require.Len(t, resetUpdate.Spans, resized.Height)
	for _, span := range resetUpdate.Spans {
		require.Equal(t, uint16(resized.Width), uint16(len(span.Cells)))
	}
}

func TestStructuredOutputStreamFailedSendRetainsTransactionAndRetriesSnapshot(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	cursor := cursorOut{valid: true}
	frame := structuredTestFrame("abc")

	first, err := stream.prepare(frame, nil, cursor, true, 0)
	require.NoError(t, err)
	require.NoError(t, first.send(func(ports.Frame) error { return nil }))
	before := stream.shadow.Clone()
	beforeCursor := stream.cursorOut

	changed := frame.Clone()
	changed.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	changedCursor := cursorOut{valid: true, row: 0, col: 1, hidden: true, hasStyle: true}
	pending, err := stream.prepare(changed, nil, changedCursor, false, 0)
	require.NoError(t, err)
	sendErr := errors.New("send failed")
	require.ErrorIs(t, pending.send(func(ports.Frame) error { return sendErr }), sendErr)
	require.Equal(t, uint64(1), state.next)
	require.Equal(t, before, stream.shadow)
	require.Equal(t, beforeCursor, stream.cursorOut)
	require.True(t, stream.forceSnapshot)

	retry, err := stream.prepare(changed, nil, changedCursor, false, 0)
	require.NoError(t, err)
	retryUpdate := decodeStructured(t, retry.data)
	require.Equal(t, ports.ScreenUpdateSnapshot, retryUpdate.Kind)
	require.Zero(t, retryUpdate.BaseStateNum)
	require.Equal(t, uint64(2), retryUpdate.NewStateNum)
	require.Len(t, retryUpdate.Spans, changed.Height)
	require.NoError(t, retry.send(func(ports.Frame) error { return nil }))
	require.Equal(t, uint64(2), state.next)
	require.False(t, stream.forceSnapshot)
	require.NoError(t, retry.send(func(ports.Frame) error {
		t.Fatal("completed send must not send twice")
		return nil
	}))
}

func TestStructuredOutputStreamNonNoopCommitForcesSnapshotRetry(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	cursor := cursorOut{valid: true}
	frame := structuredTestFrame("abc")

	first, err := stream.prepare(frame, nil, cursor, true, 0)
	require.NoError(t, err)
	require.NoError(t, first.send(func(ports.Frame) error { return nil }))
	before := stream.shadow.Clone()

	changed := frame.Clone()
	changed.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})
	pending, err := stream.prepare(changed, nil, cursor, false, 0)
	require.NoError(t, err)
	require.NotEmpty(t, pending.data)
	pending.commitNoSend()
	require.Equal(t, before, stream.shadow, "non-noop commit must not publish its candidate")
	require.Equal(t, uint64(1), state.next)
	require.True(t, stream.forceSnapshot)

	retry, err := stream.prepare(changed, nil, cursor, false, 0)
	require.NoError(t, err)
	retryUpdate := decodeStructured(t, retry.data)
	require.Equal(t, ports.ScreenUpdateSnapshot, retryUpdate.Kind)
	require.Zero(t, retryUpdate.BaseStateNum)
	require.NoError(t, retry.send(func(ports.Frame) error { return nil }))
	require.Equal(t, uint64(2), state.next)
}

func TestStructuredOutputStreamComparesCursorValidityAndHidden(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	frame := structuredTestFrame("a")
	visible := cursorOut{valid: true, row: 0, col: 0}
	first, err := stream.prepare(frame, nil, visible, true, 0)
	require.NoError(t, err)
	require.NoError(t, first.send(func(ports.Frame) error { return nil }))

	hidden := visible
	hidden.hidden = true
	second, err := stream.prepare(frame, nil, hidden, false, 0)
	require.NoError(t, err)
	secondUpdate := decodeStructured(t, second.data)
	require.Empty(t, secondUpdate.Spans)
	require.False(t, secondUpdate.Cursor.Visible)
	require.NoError(t, second.send(func(ports.Frame) error { return nil }))

	invalid := hidden
	invalid.valid = false
	third, err := stream.prepare(frame, nil, invalid, false, 0)
	require.NoError(t, err)
	thirdUpdate := decodeStructured(t, third.data)
	require.False(t, thirdUpdate.Cursor.Visible)
	require.NoError(t, third.send(func(ports.Frame) error { return nil }))

	noop, err := stream.prepare(frame, nil, invalid, false, 0)
	require.NoError(t, err)
	require.Empty(t, noop.data)
}

func TestStructuredOutputStreamRejectsInvalidCursorAndGeometry(t *testing.T) {
	cases := []struct {
		name   string
		frame  renderer.Frame
		cursor cursorOut
	}{
		{name: "cursor row outside frame", frame: structuredTestFrame("a"), cursor: cursorOut{valid: true, row: 1}},
		{name: "cursor col outside frame", frame: structuredTestFrame("a"), cursor: cursorOut{valid: true, col: 1}},
		{name: "cursor style out of range", frame: structuredTestFrame("a"), cursor: cursorOut{valid: true, hasStyle: true, style: 7}},
		{name: "screen cell cap", frame: renderer.NewFrame(513, 512), cursor: cursorOut{valid: true}},
		{name: "uint16 dimension cap", frame: renderer.NewFrame(math.MaxUint16+1, 1), cursor: cursorOut{valid: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stream := newStructuredOutputStream(newOutputStateStream())
			_, err := stream.prepare(tc.frame, nil, tc.cursor, true, 0)
			require.Error(t, err)
			require.Zero(t, stream.state.next, "rejected geometry must not advance state")
		})
	}
}

func TestStructuredOutputStreamPreservesExplicitZeroCursorStyle(t *testing.T) {
	state := newOutputStateStream()
	stream := newStructuredOutputStream(state)
	frame := structuredTestFrame("a")
	unset := cursorOut{valid: true}
	first, err := stream.prepare(frame, nil, unset, true, 0)
	require.NoError(t, err)
	require.NoError(t, first.send(func(ports.Frame) error { return nil }))

	explicit := unset
	explicit.hasStyle = true
	second, err := stream.prepare(frame, nil, explicit, false, 0)
	require.NoError(t, err)
	got := decodeStructured(t, second.data)
	require.True(t, got.Cursor.StyleSet)
	require.Zero(t, got.Cursor.Style)
	require.NoError(t, second.send(func(ports.Frame) error { return nil }))
}

func TestStructuredOutputStreamSharesACKWindow(t *testing.T) {
	state := newOutputStateStream(1)
	stream := newStructuredOutputStream(state)
	prepared, err := stream.prepare(structuredTestFrame("a"), nil, cursorOut{valid: true}, true, 0)
	require.NoError(t, err)
	require.NoError(t, prepared.send(func(ports.Frame) error { return nil }))
	require.True(t, stream.atCapacity())
	stream.ack(1)
	require.False(t, stream.atCapacity())
	require.Equal(t, uint64(0), stream.outstanding())
}

// BenchmarkStructuredOutputPrepare is a component diagnostic for structured
// planning, encoding, and commit without decode/apply; it is not a pipeline
// comparison with ANSI apply.
func BenchmarkStructuredOutputPrepare(b *testing.B) {
	fixtures := []struct {
		name   string
		frame  renderer.Frame
		damage []renderer.Damage
		mutate func(renderer.Frame, int)
	}{
		{
			name:   "one-cell",
			frame:  structuredRows(120, 40),
			damage: []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 1, Height: 1}},
			mutate: func(frame renderer.Frame, i int) {
				cell := frame.At(0, 0)
				cell.Rune = rune('a' + i%26)
				frame.Set(0, 0, cell)
			},
		},
		{
			name:   "full-line",
			frame:  structuredRows(120, 40),
			damage: []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 120, Height: 1}},
			mutate: func(frame renderer.Frame, i int) {
				for x := range frame.Width {
					cell := frame.At(x, 0)
					cell.Rune = rune('a' + (x+i)%26)
					frame.Set(x, 0, cell)
				}
			},
		},
		{
			name:   "styled-fragment",
			frame:  structuredStyledFrame(120, 40),
			damage: []renderer.Damage{{Kind: renderer.DamageText, X: 0, Y: 0, Width: 120, Height: 1}},
			mutate: func(frame renderer.Frame, i int) {
				for x := range frame.Width {
					cell := frame.At(x, 0)
					cell.Rune = rune('a' + (x+i)%26)
					frame.Set(x, 0, cell)
				}
			},
		},
		{
			name:   "full-screen-scroll",
			frame:  structuredRows(120, 40),
			damage: []renderer.Damage{{Kind: renderer.DamageScrollUp, X: 0, Y: 0, Width: 120, Height: 40, Count: 1}, {Kind: renderer.DamageText, X: 0, Y: 39, Width: 120, Height: 1}},
			mutate: func(frame renderer.Frame, i int) {
				frame.ScrollUp(0, 39, 1)
				for x := range frame.Width {
					frame.Set(x, 39, renderer.Cell{Rune: rune('a' + i%26), Style: renderer.DefaultStyle()})
				}
			},
		},
	}
	for _, fixture := range fixtures {
		b.Run(fixture.name, func(b *testing.B) {
			stream := newStructuredOutputStream(newOutputStateStream())
			cursor := cursorOut{valid: true}
			initial, err := stream.prepare(fixture.frame, nil, cursor, true, 0)
			if err != nil {
				b.Fatal(err)
			}
			if err := initial.send(func(ports.Frame) error { return nil }); err != nil {
				b.Fatal(err)
			}
			var wireBytes, spans, snapshots, mutation int
			b.ResetTimer()
			for b.Loop() {
				fixture.mutate(fixture.frame, mutation)
				mutation++
				prepared, err := stream.prepare(fixture.frame, fixture.damage, cursor, false, 0)
				if err != nil {
					b.Fatal(err)
				}
				wireBytes += len(prepared.data)
				spans += len(prepared.update.Spans)
				if prepared.update.Kind == ports.ScreenUpdateSnapshot {
					snapshots++
				}
				if err := prepared.send(func(ports.Frame) error { return nil }); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			if snapshots != 0 {
				b.Fatalf("unexpected snapshots/op: %d", snapshots)
			}
			b.ReportMetric(float64(wireBytes)/float64(b.N), "wirebytes/op")
			b.ReportMetric(float64(spans)/float64(b.N), "spans/op")
			b.ReportMetric(float64(snapshots)/float64(b.N), "snapshots/op")
		})
	}
}

func decodeStructured(t *testing.T, data []byte) ports.ScreenUpdate {
	t.Helper()
	require.NotEmpty(t, data)
	update, err := ports.UnmarshalScreenUpdate(data)
	require.NoError(t, err)
	return update
}

func structuredTestFrame(lines ...string) renderer.Frame {
	width := len([]rune(lines[0]))
	frame := renderer.NewFrame(width, len(lines))
	for y, line := range lines {
		for x, r := range []rune(line) {
			frame.Set(x, y, renderer.Cell{Rune: r, Style: renderer.DefaultStyle()})
		}
	}
	return frame
}

func structuredRows(width, height int) renderer.Frame {
	frame := renderer.NewFrame(width, height)
	for y := range height {
		for x := range width {
			frame.Set(x, y, renderer.Cell{Rune: rune('0' + y%10), Style: renderer.DefaultStyle()})
		}
	}
	return frame
}

func structuredStyledFrame(width, height int) renderer.Frame {
	frame := structuredRows(width, height)
	for x := range width {
		frame.Set(x, 0, renderer.Cell{Rune: 'x', Style: renderer.Style{Bold: x%2 == 0, Foreground: x % 8, Background: (x + 1) % 8}})
	}
	return frame
}
