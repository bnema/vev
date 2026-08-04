package client

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/bnema/vev/internal/ports"
	"github.com/stretchr/testify/require"
)

type ackRecordingTransport struct{ sent chan ports.Frame }

func (t *ackRecordingTransport) Send(f ports.Frame) error   { t.sent <- f; return nil }
func (t *ackRecordingTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (t *ackRecordingTransport) Close() error               { return nil }

func TestCumulativeAckBypassesFullNormalSendQueue(t *testing.T) {
	normal := make(chan ports.Frame, sendQueueDepth)
	for range cap(normal) {
		normal <- ports.Frame{Type: ports.MsgInput}
	}
	const epoch = 7
	acks := newCumulativeAckQueue()
	for state := uint64(1); state <= maxUnackedOutputStatesForTest; state++ {
		acks.offer(epoch, state)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := &ackRecordingTransport{sent: make(chan ports.Frame, sendQueueDepth+1)}
	errCh := make(chan error, 1)
	go runSender(ctx, cancel, tr, normal, acks, errCh, slog.Default())

	first := <-tr.sent
	require.Equal(t, ports.MsgAck, first.Type)
	ack, err := ports.UnmarshalAck(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(epoch), ack.Epoch)
	require.Equal(t, uint64(maxUnackedOutputStatesForTest), ack.State)
}

const maxUnackedOutputStatesForTest = 8
