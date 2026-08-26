package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

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
	go runSender(ctx, cancel, tr, nil, nil, normal, nil, acks, errCh, slog.Default())

	first := <-tr.sent
	require.Equal(t, ports.MsgAck, first.Type)
	ack, err := ports.UnmarshalAck(first.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(epoch), ack.Epoch)
	require.Equal(t, uint64(maxUnackedOutputStatesForTest), ack.State)
}

func TestSamePeerSwitchControlBypassesHeldInput(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	normal := make(chan ports.Frame, 1)
	control := make(chan ports.Frame, 1)
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	held := make(chan struct{})
	gate.afterInputHeld = func() { close(held) }
	transport := &ackRecordingTransport{sent: make(chan ports.Frame, 2)}
	acks := newCumulativeAckQueue()
	errs := make(chan error, 1)
	go runSender(ctx, cancel, transport, control, nil, normal, gate, acks, errs, slog.Default())

	normal <- ports.Frame{Type: ports.MsgInput, Payload: []byte("held")}
	awaitSenderSignal(t, held)
	control <- ports.Frame{Type: ports.MsgSamePeerSwitchRequest, Payload: []byte("switch")}
	if got := awaitSenderFrame(t, transport.sent); got.Type != ports.MsgSamePeerSwitchRequest {
		t.Fatalf("first frame = %d, want same-peer switch request", got.Type)
	}
	select {
	case got := <-transport.sent:
		t.Fatalf("held input sent before switch resolution: %d", got.Type)
	default:
	}
	gate.setPaused(false)
	if got := awaitSenderFrame(t, transport.sent); got.Type != ports.MsgInput {
		t.Fatalf("released frame = %d, want input", got.Type)
	}
}

func TestSenderBarrierLeavesPausedInputQueued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	normal := make(chan ports.Frame, 1)
	barriers := make(chan chan struct{})
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	transport := &ackRecordingTransport{sent: make(chan ports.Frame, 1)}
	go runSender(ctx, cancel, transport, nil, barriers, normal, gate, newCumulativeAckQueue(), make(chan error, 1), slog.Default())

	normal <- ports.Frame{Type: ports.MsgInput, Payload: []byte("held")}
	barrierDone := make(chan struct{})
	barriers <- barrierDone
	awaitSenderSignal(t, barrierDone)
	select {
	case frame := <-transport.sent:
		t.Fatalf("barrier sent paused input frame %d", frame.Type)
	default:
	}

	gate.setPaused(false)
	if got := awaitSenderFrame(t, transport.sent); got.Type != ports.MsgInput {
		t.Fatalf("released frame = %d, want input", got.Type)
	}
}

func TestSenderBarrierFlushesPendingAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acks := newCumulativeAckQueue()
	acks.offer(3, 9)
	transport := &ackRecordingTransport{sent: make(chan ports.Frame, 1)}
	barriers := make(chan chan struct{})
	go runSender(ctx, cancel, transport, nil, barriers, make(chan ports.Frame, 1), newSamePeerInputGate(), acks, make(chan error, 1), slog.Default())

	done := make(chan struct{})
	barriers <- done
	frame := awaitSenderFrame(t, transport.sent)
	require.Equal(t, ports.MsgAck, frame.Type)
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

func awaitSenderFrame(t *testing.T, frames <-chan ports.Frame) ports.Frame {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatal("sender did not emit frame")
		return ports.Frame{}
	}
}

const maxUnackedOutputStatesForTest = 8

type failAfterOneInputTransport struct{ sends int }

func (t *failAfterOneInputTransport) Send(ports.Frame) error {
	t.sends++
	if t.sends == 2 {
		return errors.New("sender link failed")
	}
	return nil
}
func (*failAfterOneInputTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (*failAfterOneInputTransport) Close() error               { return nil }

func TestInputReplayLedgerRetainsFramesAcceptedBeforeSenderFailure(t *testing.T) {
	ledger := newInputReplayLedger()
	ledger.register(1, []byte("first"))
	ledger.register(2, []byte("second"))

	frames := make(chan ports.Frame, 2)
	frames <- ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{InputSeq: 1, Data: []byte("first")})}
	frames <- ports.Frame{Type: ports.MsgInput, Payload: ports.MarshalInput(ports.Input{InputSeq: 2, Data: []byte("second")})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errs := make(chan error, 1)
	go runSender(ctx, cancel, &failAfterOneInputTransport{}, nil, nil, frames, newSamePeerInputGate(), newCumulativeAckQueue(), errs, slog.Default(), ledger)

	require.Error(t, <-errs)
	require.Equal(t, []byte("second"), ledger.takeUnsent(), "queued input must become residual after the transport failure")
	require.Empty(t, ledger.takeUnsent())
}
