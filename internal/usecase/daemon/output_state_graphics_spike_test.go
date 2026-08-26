package daemon

import (
	"testing"

	renderer "github.com/bnema/vev-vt/ansi"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/dgram"
	"github.com/stretchr/testify/require"
)

// TestOutputStateStreamReplaysInterruptedKittyUpload proves the output-state
// epoch is a sufficient ordering boundary for a multi-record terminal upload:
// once rebased, stale continuation bytes cannot be emitted and the replacement
// epoch starts with an owned-ID delete followed by a complete upload.
func TestOutputStateStreamReplaysInterruptedKittyUpload(t *testing.T) {
	stream := newOutputStateStream()
	frame := renderer.NewFrame(1, 1)
	frame.Set(0, 0, renderer.Cell{Rune: 'x', Style: renderer.DefaultStyle()})

	send := func(data []byte, reset bool) ports.Output {
		t.Helper()
		prepared, err := stream.prepare(frame, []renderer.Damage{renderer.FullRedraw()}, reset)
		require.NoError(t, err)
		var sent ports.Frame
		require.NoError(t, prepared.send(data, 0, func(out ports.Frame) error {
			sent = out
			return nil
		}))
		out, err := ports.UnmarshalOutput(sent.Payload)
		require.NoError(t, err)
		return out
	}

	first := send([]byte("\x1b_Ga=t,i=41,m=1;first\x1b\\"), true)
	require.Equal(t, uint64(1), first.Epoch)
	require.Zero(t, first.Base)
	require.Equal(t, uint64(1), first.New)

	continuation := send([]byte("\x1b_Gm=1;second\x1b\\"), false)
	require.Equal(t, first.New, continuation.Base)
	require.Equal(t, uint64(2), continuation.New)

	stale, err := stream.prepare(frame, []renderer.Damage{renderer.FullRedraw()}, false)
	require.NoError(t, err)
	stream.rebase()
	require.NoError(t, stale.send([]byte("\x1b_Gm=0;stale\x1b\\"), 0, func(ports.Frame) error {
		t.Fatal("stale output must not be sent")
		return nil
	}))
	require.False(t, stale.sent, "a pre-rebase continuation must not cross epochs")

	deleteOwned := send([]byte("\x1b_Ga=d,d=i,i=41;\x1b\\"), false)
	require.Equal(t, uint64(2), deleteOwned.Epoch)
	require.Zero(t, deleteOwned.Base)
	require.Equal(t, uint64(1), deleteOwned.New)
	require.True(t, deleteOwned.Full)

	replayedFirst := send([]byte("\x1b_Ga=t,i=41,m=1;first\x1b\\"), false)
	replayedLast := send([]byte("\x1b_Gm=0;second\x1b\\"), false)
	require.Equal(t, deleteOwned.New, replayedFirst.Base)
	require.Equal(t, replayedFirst.New, replayedLast.Base)
	require.Equal(t, uint64(3), replayedLast.New)
}

func TestKittyOutputRecordReassemblesAfterDatagramReordering(t *testing.T) {
	// 128 KiB matches the current Kitty upload chunk and forces fragmentation
	// while remaining far below the 1,024-fragment reassembly ceiling.
	data := make([]byte, 128<<10)
	state := uint32(1)
	for i := range data {
		state = state*1664525 + 1013904223
		data[i] = byte(state >> 24)
	}
	payload, err := ports.MarshalOutput(ports.Output{
		Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: data,
	})
	require.NoError(t, err)
	record := append([]byte{byte(ports.MsgOutput)}, payload...)
	fragments, err := dgram.FragmentPayload(1, record, dgram.DefaultMTU-dgram.HeaderSize-16)
	require.NoError(t, err)
	require.Greater(t, len(fragments), 1)
	require.Less(t, len(fragments), 1024)

	reassembler := dgram.NewReassembler()
	lost := len(fragments) / 2
	for i := len(fragments) - 1; i >= 0; i-- {
		if i == lost {
			continue // Simulate one lost datagram while every other fragment reorders.
		}
		_, ok, err := reassembler.Add(fragments[i])
		require.NoError(t, err)
		require.False(t, ok)
	}
	complete, ok, err := reassembler.Add(fragments[lost]) // Retransmitted loss.
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, record, complete)
}
