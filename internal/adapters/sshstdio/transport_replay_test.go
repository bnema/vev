package sshstdio

import (
	"io"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

// This must stay byte-for-byte equal to the carriage transcript in the other
// transport adapters. It is composed before it reaches stdio.
func replayTranscript() []ports.Frame {
	return []ports.Frame{
		{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("\x1b[2J\x1b[Hone\r\ntwo")})},
		{Type: ports.MsgOutput, Payload: ports.MarshalOutput(ports.Output{BaseStateNum: 1, NewStateNum: 2, EchoAck: 7, Data: []byte("\x1b[2;1HTWO")})},
	}
}

func TestTransportReplay(t *testing.T) {
	clientRead, serverWrite := io.Pipe()
	serverRead, clientWrite := io.Pipe()
	sender := NewTransport(clientRead, clientWrite, func() error { return clientWrite.Close() })
	receiver := NewTransport(serverRead, serverWrite, func() error { return serverRead.Close() })
	t.Cleanup(func() { _ = sender.Close(); _ = receiver.Close() })
	frames := replayTranscript()

	var wg sync.WaitGroup
	wg.Go(func() {
		for _, frame := range frames {
			if err := sender.Send(frame); err != nil {
				t.Errorf("Send: %v", err)
				return
			}
		}
	})
	for _, want := range frames {
		got, err := receiver.Recv()
		require.NoError(t, err)
		require.Equal(t, want.Type, got.Type)
		require.Equal(t, want.Payload, got.Payload)
		out, err := ports.UnmarshalOutput(got.Payload)
		require.NoError(t, err)
		wantOut, err := ports.UnmarshalOutput(want.Payload)
		require.NoError(t, err)
		require.Equal(t, wantOut, out)
	}
	wg.Wait()
}
