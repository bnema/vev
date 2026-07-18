package main_test

import (
	"io"
	"net"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/adapters/ipc"
	"github.com/bnema/vev/internal/adapters/sshstdio"
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

// TestThemeGenerationTransportSequences proves the generation's cleared then
// definitive wire sequence is unchanged across the local socket and remote
// SSH-stdio transports. The transports only carry frames; neither can infer
// or alter the palette lifecycle.
func TestThemeGenerationTransportSequences(t *testing.T) {
	cleared := ports.Theme{
		HasForeground: true, Foreground: renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true, Background: renderer.RGB{R: 4, G: 5, B: 6},
		TrueColor: true, SchemeKnown: true,
	}
	palette := [16]renderer.RGB{2: {R: 125, G: 181, B: 181}, 10: {R: 125, G: 181, B: 181}}
	definitive := cleared
	definitive.Palette = palette
	definitive.PaletteKnown = 1<<2 | 1<<10
	sequence := []ports.Theme{cleared, definitive}

	tests := []struct {
		name string
		pair func(*testing.T) (ports.Transport, ports.Transport)
	}{
		{
			name: "local ipc socket",
			pair: func(t *testing.T) (ports.Transport, ports.Transport) {
				left, right := net.Pipe()
				t.Cleanup(func() { _ = left.Close(); _ = right.Close() })
				return ipc.NewTransport(left), ipc.NewTransport(right)
			},
		},
		{
			name: "remote ssh stdio",
			pair: func(t *testing.T) (ports.Transport, ports.Transport) {
				clientRead, serverWrite := io.Pipe()
				serverRead, clientWrite := io.Pipe()
				closeClient := func() error {
					_ = clientRead.Close()
					return clientWrite.Close()
				}
				closeServer := func() error {
					_ = serverRead.Close()
					return serverWrite.Close()
				}
				t.Cleanup(func() { _ = closeClient(); _ = closeServer() })
				return sshstdio.NewTransport(clientRead, clientWrite, closeClient), sshstdio.NewTransport(serverRead, serverWrite, closeServer)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sender, receiver := tt.pair(t)
			done := make(chan error, 1)
			go func() {
				for _, snapshot := range sequence {
					if err := sender.Send(ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(snapshot)}); err != nil {
						done <- err
						return
					}
				}
				done <- nil
			}()

			for _, want := range sequence {
				frame, err := receiver.Recv()
				require.NoError(t, err)
				require.Equal(t, ports.MsgTheme, frame.Type)
				got, err := ports.UnmarshalTheme(frame.Payload)
				require.NoError(t, err)
				require.Equal(t, want, got)
			}
			require.NoError(t, <-done)
		})
	}
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
