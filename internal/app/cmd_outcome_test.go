package app

// CommandResult caller contract (explicit command-result slice). runCmd sends
// exactly one CommandRequest with a unique nonzero RequestID and consumes
// exactly one correlated CommandResult: a lost, wrong-typed, uncorrelated, or
// malformed reply, or a deadline/cancellation after the send, is a typed
// outcome-unknown that is never replayed and whose send count stays one. A
// caller canceled before the send is a definite not-sent outcome, and a
// definite daemon failure keeps its bounded error and exit code. Every case is
// bounded and uses fake clocks or gates, never sleeps.

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/daemon"
)

// countCommandRequests reports how many CommandRequests the transport placed on
// the wire: exactly one is the no-replay invariant.
func countCommandRequests(t *testing.T, transport *cmdTestTransport) int {
	t.Helper()
	count := 0
	for _, frame := range transport.sent {
		message, err := sessionwire.DecodeClientEnvelope(frame.Payload)
		if err != nil {
			continue
		}
		if _, ok := message.(protocol.CommandRequest); ok {
			count++
		}
	}
	return count
}

// runCmdReply drives one runCmdWithDeps invocation against a scripted transport.
func runCmdReply(ctx context.Context, transport *cmdTestTransport, clock ports.Clock) error {
	return runCmdWithDeps(ctx, cmdInvocation{slug: "split-right"}, cmdDeps{
		stdout: io.Discard,
		getenv: func(string) string { return "" },
		dial:   func(context.Context, string) (wire.Transport, error) { return transport, nil },
		clock:  clock,
	})
}

// rawServerEnvelope wraps already-serialized server envelope bytes without
// re-encoding them. Fixtures that model hostile or corrupt frames use it so the
// now-correct strict semantic encoder can never reject the fixture first.
func rawServerEnvelope(raw []byte) wire.Envelope { return wire.Envelope{Payload: raw} }

// malformedCommandOutcomeEnvelope marshals a server CommandResult whose
// outcome/code combination the semantic contract forbids (a succeeded outcome
// with a nonzero code). proto.Marshal deliberately bypasses
// sessionwire.EncodeServerMessage, which refuses the semantic value outright.
func malformedCommandOutcomeEnvelope(t *testing.T) wire.Envelope {
	t.Helper()
	raw, err := proto.Marshal(&wire.ServerEnvelope{Payload: &wire.ServerEnvelope_CommandResult{CommandResult: &wire.CommandResult{
		RequestId: 1,
		Outcome:   wire.CommandOutcome_COMMAND_OUTCOME_SUCCEEDED,
		Code:      uint32(protocol.ErrInternal),
	}}})
	require.NoError(t, err)
	// The semantic encoder must refuse the same value, which proves this fixture
	// really is a hand-built frame rather than a re-encoded semantic message.
	_, encodeErr := sessionwire.EncodeServerMessage(protocol.CommandResult{
		RequestID: 1, Outcome: protocol.CommandSucceeded, Code: protocol.ErrInternal,
	})
	require.Error(t, encodeErr, "a forbidden outcome must not be encodable")
	return rawServerEnvelope(raw)
}

// TestRunCmdLostUncorrelatedOrWrongTypedReplyIsOutcomeUnknown proves a reply
// that is lost, wrong-typed, uncorrelated, or malformed leaves a typed
// outcome-unknown: the request was sent, so it is never resent, and the
// one-shot connection is retired exactly once.
func TestRunCmdLostUncorrelatedOrWrongTypedReplyIsOutcomeUnknown(t *testing.T) {
	tests := []struct {
		name        string
		transport   *cmdTestTransport
		wantContain string
	}{
		{
			name:        "reply lost with EOF",
			transport:   &cmdTestTransport{recvErr: io.EOF},
			wantContain: "EOF",
		},
		{
			name:        "reply lost with a closed pipe",
			transport:   &cmdTestTransport{recvErr: io.ErrClosedPipe},
			wantContain: "closed pipe",
		},
		{
			name:        "wrong reply type",
			transport:   &cmdTestTransport{recv: mustServerEnvelope(protocol.Pong{})},
			wantContain: "unexpected command reply protocol.Pong",
		},
		{
			name: "wrong reply request ID",
			transport: &cmdTestTransport{preserveReplyID: true, recv: mustServerEnvelope(protocol.CommandResult{
				RequestID: 99, Outcome: protocol.CommandSucceeded,
			})},
			wantContain: "unexpected command reply request ID 99",
		},
		{
			name:        "malformed outcome bytes are refused",
			transport:   &cmdTestTransport{recv: malformedCommandOutcomeEnvelope(t)},
			wantContain: "wire value out of semantic range",
		},
		{
			name: "truncated envelope bytes are refused",
			transport: &cmdTestTransport{recv: rawServerEnvelope(
				mustServerEnvelope(protocol.CommandResult{RequestID: 1, Outcome: protocol.CommandSucceeded}).Payload[:3])},
			wantContain: "wire: envelope is truncated",
		},
		{
			name: "definite daemon unknown outcome",
			transport: &cmdTestTransport{recv: mustServerEnvelope(protocol.CommandResult{
				Outcome: protocol.CommandOutcomeUnknown, Text: "daemon is shutting down",
			})},
			wantContain: "daemon is shutting down",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := runCmdReply(context.Background(), tt.transport, nil)

			require.ErrorIs(t, err, errCommandOutcomeUnknown, "a lost or uncorrelated reply is outcome-unknown")
			require.NotErrorIs(t, err, errCommandNotSent, "the request was sent, so the outcome is not a definite not-sent")
			require.ErrorContains(t, err, tt.wantContain)
			require.Equal(t, 3, ExitCode(err))
			require.Equal(t, 1, countCommandRequests(t, tt.transport), "an indeterminate command must never be resent")
			require.Equal(t, 1, tt.transport.closeCalls, "the one-shot connection is retired exactly once")
		})
	}
}

// TestRunCmdCancelAfterSendIsOutcomeUnknownWithoutResend proves a caller
// canceled while awaiting the reply reports outcome-unknown, sends exactly one
// request, and retires the connection.
func TestRunCmdCancelAfterSendIsOutcomeUnknownWithoutResend(t *testing.T) {
	replyStarted := make(chan struct{})
	recvGate := make(chan struct{})
	transport := &cmdTestTransport{replyStarted: replyStarted, recvGate: recvGate}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runCmdReply(ctx, transport, nil) }()

	// Wait until the reply read is actually parked, then cancel. The gate makes
	// entry deterministic without sleeping.
	<-replyStarted
	cancel()
	err := <-done
	close(recvGate)

	require.ErrorIs(t, err, errCommandOutcomeUnknown)
	require.Equal(t, 3, ExitCode(err))
	require.Equal(t, 1, countCommandRequests(t, transport), "a canceled command must never be resent")
	require.Equal(t, 1, transport.closeCalls, "cancellation after the send must retire the connection")
}

// TestRunCmdTimeoutAfterSendIsOutcomeUnknownWithoutResend proves a request
// whose result deadline expires while awaiting the reply reports
// outcome-unknown, sends exactly one request, and retires the connection. The
// fake clock keeps the deadline deterministic.
func TestRunCmdTimeoutAfterSendIsOutcomeUnknownWithoutResend(t *testing.T) {
	timerCh := make(chan time.Time, 1)
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return(timerCh).Once()
	timer.EXPECT().Stop().Return(true).Once()
	delayCh := make(chan time.Duration, 1)
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().NewTimer(mock.Anything).Run(func(delay time.Duration) {
		delayCh <- delay
	}).Return(timer).Once()

	replyStarted := make(chan struct{})
	recvGate := make(chan struct{})
	transport := &cmdTestTransport{replyStarted: replyStarted, recvGate: recvGate}
	done := make(chan error, 1)
	go func() { done <- runCmdReply(context.Background(), transport, clock) }()

	<-replyStarted
	require.Equal(t, daemon.CommandRequestTimeout, <-delayCh)
	timerCh <- time.Time{}
	err := <-done
	close(recvGate)

	require.ErrorIs(t, err, errCommandOutcomeUnknown)
	require.ErrorContains(t, err, daemon.ErrCommandRequestTimeout.Error())
	require.Equal(t, 3, ExitCode(err))
	require.Equal(t, 1, countCommandRequests(t, transport), "a timed-out command must never be resent")
	require.Equal(t, 1, transport.closeCalls, "a result timeout must retire the connection")
}

// TestRunCmdCancelBeforeSendIsDefiniteNotSent proves a caller already canceled
// before the request is sent reports a definite not-sent outcome, puts no
// CommandRequest on the wire, and is not misclassified as unknown.
func TestRunCmdCancelBeforeSendIsDefiniteNotSent(t *testing.T) {
	transport := &cmdTestTransport{recv: mustServerEnvelope(protocol.CommandResult{Outcome: protocol.CommandSucceeded})}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := runCmdReply(ctx, transport, nil)

	require.ErrorIs(t, err, errCommandNotSent)
	require.NotErrorIs(t, err, errCommandOutcomeUnknown, "a not-sent command has a definite outcome")
	require.Equal(t, 0, countCommandRequests(t, transport), "a canceled-before-send command must not reach the wire")
}

// TestRunCmdSuccessAndFailurePreserveCorrelationAndBoundedError proves a
// definite daemon outcome is reported with its exact text and exit code, and
// still sends exactly one request.
func TestRunCmdSuccessAndFailurePreserveCorrelationAndBoundedError(t *testing.T) {
	tests := []struct {
		name     string
		result   protocol.CommandResult
		wantCode int
	}{
		{
			name:     "success",
			result:   protocol.CommandResult{Outcome: protocol.CommandSucceeded, Output: "done"},
			wantCode: 0,
		},
		{
			name:     "invalid arguments are a usage error",
			result:   protocol.CommandResult{Outcome: protocol.CommandFailed, Code: protocol.ErrInvalidCommandArgs, Text: "usage: split-right"},
			wantCode: 2,
		},
		{
			name:     "runtime failure is an exit one failure",
			result:   protocol.CommandResult{Outcome: protocol.CommandFailed, Code: protocol.ErrNoSuchTarget, Text: "no live sessions"},
			wantCode: 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := new(strings.Builder)
			transport := &cmdTestTransport{recv: mustServerEnvelope(tt.result)}
			err := runCmdWithDeps(context.Background(), cmdInvocation{slug: "split-right"}, cmdDeps{
				stdout: out,
				getenv: func(string) string { return "" },
				dial:   func(context.Context, string) (wire.Transport, error) { return transport, nil },
			})

			require.Equal(t, tt.wantCode, ExitCode(err))
			require.Equal(t, 1, countCommandRequests(t, transport), "a definite outcome sends exactly one request")
			require.NotErrorIs(t, err, errCommandOutcomeUnknown, "a definite outcome is never reported as unknown")
			if tt.result.Outcome == protocol.CommandSucceeded {
				require.NoError(t, err)
				require.Equal(t, tt.result.Output+"\n", out.String())
			} else {
				require.ErrorContains(t, err, tt.result.Text)
				require.NotErrorIs(t, err, errCommandNotSent)
			}
		})
	}
}
