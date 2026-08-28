package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

func TestCloseAndDialAttachTargetSkipsSamePeerSwitch(t *testing.T) {
	term := newReconnectToastTerminalHarness(t)
	defer term.closeInput()
	transport := newReconnectToastLinkTransport()
	transport.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempt := newReconnectAttachAttempt(
		term.term,
		transport,
		newReconnectHandshakeClock(t),
		AttachRequest{Intent: protocol.IntentAttach, SessionName: "source"},
		0,
		&terminalThemeState{},
		nil,
		&milestones{},
	)
	attempt.runner.ledger = newRouteLedger()
	resultCh := make(chan attachResult, 1)
	go func() { resultCh <- attempt.run(ctx) }()

	exact := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{7}, SessionName: "stopped"}
	target := protocol.AttachTarget{
		Session: "stopped", Intent: protocol.IntentAttach, ExactTarget: &exact,
		EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned,
	}
	transport.recvCh <- reconnectToastRecv{frame: ports.Frame{
		Type: ports.MsgAttachTarget, Payload: ports.MarshalAttachTarget(target),
	}}

	select {
	case result := <-resultCh:
		require.NoError(t, result.err)
		require.Equal(t, &target, result.target)
	case <-time.After(time.Second):
		t.Fatal("close-and-dial target did not return a handoff")
	}
	_, sentSamePeer := transport.sends.find(func(frame ports.Frame) bool {
		return frame.Type == ports.MsgSamePeerSwitchRequest
	})
	require.False(t, sentSamePeer)
}

func TestSamePeerInputGateBlocksUntilReleased(t *testing.T) {
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ready := make(chan bool, 1)
	go func() { ready <- gate.wait(ctx) }()
	gate.setPaused(false)
	if ok := <-ready; !ok {
		t.Fatal("input gate did not release")
	}
}

func TestSamePeerInputGateCancels(t *testing.T) {
	gate := newSamePeerInputGate()
	gate.setPaused(true)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if gate.wait(ctx) {
		t.Fatal("cancelled input gate wait succeeded")
	}
}
