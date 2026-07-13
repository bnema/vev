package main_test

import (
	"net"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/testutil/replaytest"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

// TestTransportReplayIntegration stays at the legal application boundary: it
// observes only typed output frames and terminal bytes, never daemon internals.
func TestTransportReplayIntegration(t *testing.T) {
	frames := replaytest.Transcript()
	left, right := net.Pipe()
	t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
	sender, receiver := ipc.NewTransport(left), ipc.NewTransport(right)
	var wg sync.WaitGroup
	wg.Go(func() {
		for _, frame := range frames {
			if err := sender.Send(frame); err != nil {
				t.Errorf("Send: %v", err)
				return
			}
		}
	})

	terminal := vt.NewScreen(8, 3)
	var bytes []byte
	for _, want := range frames {
		got, err := receiver.Recv()
		require.NoError(t, err)
		require.Equal(t, want, got)
		output, err := ports.UnmarshalOutput(got.Payload)
		require.NoError(t, err)
		bytes = append(bytes, output.Data...)
		terminal.Write(output.Data)
	}
	wg.Wait()
	require.Equal(t, "\x1b[2J\x1b[Hone\r\ntwo\x1b[2;1HTWO", string(bytes))
	require.Equal(t, []string{"one     ", "TWO     ", "        "}, integrationFrameRows(terminal.Frame))
}

func integrationFrameRows(frame renderer.Frame) []string {
	rows := make([]string, frame.Height)
	for y := range rows {
		cells := frame.Row(y)
		for _, cell := range cells {
			rows[y] += string(cell.Rune)
		}
	}
	return rows
}
