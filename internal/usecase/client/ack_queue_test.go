package client

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

type ackRecordingTransport struct{ sent chan wire.Frame }

func (t *ackRecordingTransport) Send(f wire.Frame) error   { t.sent <- f; return nil }
func (t *ackRecordingTransport) Recv() (wire.Frame, error) { return wire.Frame{}, io.EOF }
func (t *ackRecordingTransport) Close() error              { return nil }

func TestCumulativeAckBypassesFullNormalSendQueue(t *testing.T) {
	normal := make(chan protocol.ClientMessage, sendQueueDepth)
	for range cap(normal) {
		normal <- protocol.Input{}
	}
	const epoch = 7
	acks := newCumulativeAckQueue()
	for state := uint64(1); state <= maxUnackedOutputStatesForTest; state++ {
		acks.offer(epoch, state)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := &ackRecordingTransport{sent: make(chan wire.Frame, sendQueueDepth+1)}
	errCh := make(chan error, 1)
	go runSender(ctx, cancel, tr, nil, nil, normal, nil, acks, errCh, slog.Default())

	first := <-tr.sent
	require.Equal(t, wire.MsgAck, first.Type)
	ack, err := wire.UnmarshalAck(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(epoch), ack.Epoch)
	require.Equal(t, uint64(maxUnackedOutputStatesForTest), ack.State)
}

func TestSamePeerSwitchControlBypassesHeldInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	normal := make(chan protocol.ClientMessage, 1)
	control := make(chan protocol.ClientMessage, 1)
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	held := make(chan struct{})
	gate.afterInputHeld = func() { close(held) }
	transport := &ackRecordingTransport{sent: make(chan wire.Frame, 2)}
	acks := newCumulativeAckQueue()
	errs := make(chan error, 1)
	go runSender(ctx, cancel, transport, control, nil, normal, gate, acks, errs, slog.Default())

	normal <- protocol.Input{Data: []byte("held")}
	awaitSenderSignal(t, held)
	control <- protocol.SamePeerSwitchRequest{RequestID: 1, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}}
	if got := awaitSenderFrame(t, transport.sent); got.Type != wire.MsgSamePeerSwitchRequest {
		t.Fatalf("first frame = %d, want same-peer switch request", got.Type)
	}
	select {
	case got := <-transport.sent:
		t.Fatalf("held input sent before switch resolution: %d", got.Type)
	default:
	}
	gate.setPaused(false)
	if got := awaitSenderFrame(t, transport.sent); got.Type != wire.MsgInput {
		t.Fatalf("released frame = %d, want input", got.Type)
	}
}

func TestSenderBarrierLeavesPausedInputQueued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	normal := make(chan protocol.ClientMessage, 1)
	barriers := make(chan chan struct{})
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	transport := &ackRecordingTransport{sent: make(chan wire.Frame, 1)}
	go runSender(ctx, cancel, transport, nil, barriers, normal, gate, newCumulativeAckQueue(), make(chan error, 1), slog.Default())

	normal <- protocol.Input{Data: []byte("held")}
	barrierDone := make(chan struct{})
	barriers <- barrierDone
	awaitSenderSignal(t, barrierDone)
	select {
	case frame := <-transport.sent:
		t.Fatalf("barrier sent paused input frame %d", frame.Type)
	default:
	}

	gate.setPaused(false)
	if got := awaitSenderFrame(t, transport.sent); got.Type != wire.MsgInput {
		t.Fatalf("released frame = %d, want input", got.Type)
	}
}

func TestSenderBarrierFlushesPendingAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acks := newCumulativeAckQueue()
	acks.offer(3, 9)
	transport := &ackRecordingTransport{sent: make(chan wire.Frame, 1)}
	barriers := make(chan chan struct{})
	go runSender(ctx, cancel, transport, nil, barriers, make(chan protocol.ClientMessage, 1), newSamePeerInputGate(), acks, make(chan error, 1), slog.Default())

	done := make(chan struct{})
	barriers <- done
	frame := awaitSenderFrame(t, transport.sent)
	require.Equal(t, wire.MsgAck, frame.Type)
	awaitSenderSignal(t, done)
}

func awaitSenderSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatal("sender did not hold input")
	}
}

func awaitSenderFrame(t *testing.T, frames <-chan wire.Frame) wire.Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatal("sender did not emit frame")
		return wire.Frame{}
	}
}

const maxUnackedOutputStatesForTest = 8
