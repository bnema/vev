package client

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/stretchr/testify/require"
)

type ackRecordingTransport struct{ sent chan wire.Envelope }

func (t *ackRecordingTransport) Send(f wire.Envelope) error   { t.sent <- f; return nil }
func (t *ackRecordingTransport) Recv() (wire.Envelope, error) { return wire.Envelope{}, io.EOF }
func (t *ackRecordingTransport) Close() error                 { return nil }

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
	tr := &ackRecordingTransport{sent: make(chan wire.Envelope, sendQueueDepth+1)}
	errCh := make(chan error, 1)
	go runSender(ctx, cancel, tr, nil, nil, normal, nil, acks, errCh, slog.Default())

	first := <-tr.sent
	ack, err := sessionwire.DecodeClientEnvelope(first.Payload)
	require.NoError(t, err)
	require.Equal(t, protocol.Ack{Epoch: uint64(epoch), State: uint64(maxUnackedOutputStatesForTest)}, ack)
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
	transport := &ackRecordingTransport{sent: make(chan wire.Envelope, 2)}
	acks := newCumulativeAckQueue()
	errs := make(chan error, 1)
	go runSender(ctx, cancel, transport, control, nil, normal, gate, acks, errs, slog.Default())

	normal <- protocol.Input{Data: []byte("held")}
	awaitSenderSignal(t, held)
	control <- protocol.SamePeerSwitchRequest{RequestID: 1, Target: protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "work"}}
	if got := awaitSenderFrame(t, transport.sent); clientMessageName(t, got) != "SamePeerSwitchRequest" {
		t.Fatalf("first frame = %s, want same-peer switch request", clientMessageName(t, got))
	}
	select {
	case got := <-transport.sent:
		t.Fatalf("held input sent before switch resolution: %s", clientMessageName(t, got))
	default:
	}
	gate.setPaused(false)
	if got := awaitSenderFrame(t, transport.sent); clientMessageName(t, got) != "Input" {
		t.Fatalf("released frame = %s, want input", clientMessageName(t, got))
	}
}

func TestSenderBarrierLeavesPausedInputQueued(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	normal := make(chan protocol.ClientMessage, 1)
	barriers := make(chan chan struct{})
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	transport := &ackRecordingTransport{sent: make(chan wire.Envelope, 1)}
	go runSender(ctx, cancel, transport, nil, barriers, normal, gate, newCumulativeAckQueue(), make(chan error, 1), slog.Default())

	normal <- protocol.Input{Data: []byte("held")}
	barrierDone := make(chan struct{})
	barriers <- barrierDone
	awaitSenderSignal(t, barrierDone)
	select {
	case frame := <-transport.sent:
		t.Fatalf("barrier sent paused input frame %s", clientMessageName(t, frame))
	default:
	}

	gate.setPaused(false)
	if got := awaitSenderFrame(t, transport.sent); clientMessageName(t, got) != "Input" {
		t.Fatalf("released frame = %s, want input", clientMessageName(t, got))
	}
}

func TestSenderBarrierFlushesPendingAck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	acks := newCumulativeAckQueue()
	acks.offer(3, 9)
	transport := &ackRecordingTransport{sent: make(chan wire.Envelope, 1)}
	barriers := make(chan chan struct{})
	go runSender(ctx, cancel, transport, nil, barriers, make(chan protocol.ClientMessage, 1), newSamePeerInputGate(), acks, make(chan error, 1), slog.Default())

	done := make(chan struct{})
	barriers <- done
	frame := awaitSenderFrame(t, transport.sent)
	require.Equal(t, "Ack", clientMessageName(t, frame))
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

func awaitSenderFrame(t *testing.T, frames <-chan wire.Envelope) wire.Envelope {
	t.Helper()
	select {
	case frame := <-frames:
		return frame
	case <-time.After(time.Second):
		t.Fatal("sender did not emit frame")
		return wire.Envelope{}
	}
}

func clientMessageName(t *testing.T, envelope wire.Envelope) string {
	if t != nil {
		t.Helper()
	}
	message, err := sessionwire.DecodeClientEnvelope(envelope.Payload)
	if err != nil {
		return ""
	}
	switch message.(type) {
	case protocol.Hello:
		return "Hello"
	case protocol.Input:
		return "Input"
	case protocol.Resize:
		return "Resize"
	case protocol.Detach:
		return "Detach"
	case protocol.Ping:
		return "Ping"
	case protocol.List:
		return "List"
	case protocol.Kill:
		return "Kill"
	case protocol.Theme:
		return "Theme"
	case protocol.Ack:
		return "Ack"
	case protocol.ImagePush:
		return "ImagePush"
	case protocol.ClientNotice:
		return "ClientNotice"
	case protocol.CommandRequest:
		return "CommandRequest"
	case protocol.OutputResetRequest:
		return "OutputResetRequest"
	case protocol.UIFence:
		return "UIFence"
	case protocol.RemotePreviewRequest:
		return "RemotePreviewRequest"
	case protocol.RouteAttentionSubscription:
		return "RouteAttentionSubscription"
	case protocol.SamePeerSwitchRequest:
		return "SamePeerSwitchRequest"
	case protocol.RecentRouteSnapshot:
		return "RecentRouteSnapshot"
	case protocol.RouteNavigationFailure:
		return "RouteNavigationFailure"
	case protocol.SessionCreationFailure:
		return "SessionCreationFailure"
	case protocol.NavigationInventoryRequest:
		return "NavigationInventoryRequest"
	case protocol.NavigationInventoryPublication:
		return "NavigationInventoryPublication"
	case protocol.NavigationInventoryFailure:
		return "NavigationInventoryFailure"
	case protocol.PickerBegin:
		return "PickerBegin"
	case protocol.PickerClose:
		return "PickerClose"
	case protocol.PickerSelection:
		return "PickerSelection"
	case protocol.PickerPreviewRequest:
		return "PickerPreviewRequest"
	case protocol.PickerControlRequest:
		return "PickerControlRequest"
	default:
		return ""
	}
}

const maxUnackedOutputStatesForTest = 8
