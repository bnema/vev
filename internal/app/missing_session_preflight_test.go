package app

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
	"github.com/bnema/vev/internal/usecase/client"
)

func fakeSessionListDialer(t *testing.T, sessions []protocol.SessionInfo) func() wire.Dialer {
	t.Helper()
	transport := wiremocks.NewMockTransport(t)
	transport.EXPECT().Send(mock.Anything).RunAndReturn(func(frame wire.Frame) error {
		require.Equal(t, wire.MsgList, frame.Type)
		return nil
	}).Once()
	transport.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		return wire.Frame{Type: wire.MsgSessions, Payload: wire.MarshalSessions(protocol.Sessions{Sessions: sessions})}, nil
	}).Once()
	transport.EXPECT().Close().Return(nil)
	return func() wire.Dialer { return stubPreflightDialer{transport: transport} }
}

type stubPreflightDialer struct{ transport wire.Transport }

func (d stubPreflightDialer) Dial(context.Context) (wire.Transport, error) {
	return d.transport, nil
}

type stubPreflightErrorDialer struct{ err error }

func (d stubPreflightErrorDialer) Dial(context.Context) (wire.Transport, error) {
	return nil, d.err
}

func TestRunAttachWithDepsMissingSessionCreatePrompt(t *testing.T) {
	tests := []struct {
		name           string
		sessions       []protocol.SessionInfo
		answer         string
		terminal       bool
		wantIntent     uint8
		wantPromptPart string
		wantNoPrompt   bool
	}{
		{name: "missing confirm creates", sessions: nil, answer: "y\n", terminal: true, wantIntent: protocol.IntentNew, wantPromptPart: `vev: session "codejack" doesn't exist, want to create it? [y/N]`},
		{name: "missing decline attaches", sessions: nil, answer: "n\n", terminal: true, wantIntent: protocol.IntentAttach, wantPromptPart: "codejack"},
		{name: "missing empty answer attaches", sessions: nil, answer: "\n", terminal: true, wantIntent: protocol.IntentAttach},
		{name: "missing unknown answer attaches", sessions: nil, answer: "later\n", terminal: true, wantIntent: protocol.IntentAttach, wantPromptPart: "[y/N]"},
		{name: "missing non-terminal attaches", sessions: nil, answer: "y\n", terminal: false, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
		{name: "present attaches without prompt", sessions: []protocol.SessionInfo{{Name: "codejack"}}, answer: "y\n", terminal: true, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var intents []uint8
			var promptOut strings.Builder
			err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "codejack", "", "", nil, runAttachDeps{
				localDialer:          fakeSessionListDialer(t, tt.sessions),
				attachPromptIn:       strings.NewReader(tt.answer),
				attachPromptOut:      &promptOut,
				attachPromptTerminal: func() bool { return tt.terminal },
				runClient: func(_ context.Context, _ client.Dependencies, request client.AttachRequest) error {
					intents = append(intents, request.Intent)
					return nil
				},
			})
			require.NoError(t, err)
			require.Equal(t, []uint8{tt.wantIntent}, intents, "preflight must select the attach intent before any client attempt")
			if tt.wantPromptPart != "" {
				require.Contains(t, promptOut.String(), tt.wantPromptPart)
			}
			if tt.wantNoPrompt {
				require.Empty(t, promptOut.String())
			}
		})
	}
}

func TestRunAttachWithDepsMissingSessionPreflightSkipsNonAttach(t *testing.T) {
	for _, tt := range []struct {
		name   string
		intent uint8
		remote string
	}{
		{name: "create", intent: protocol.IntentNew},
		{name: "remote", intent: protocol.IntentAttach, remote: "remote.example"},
		{name: "ephemeral", intent: protocol.IntentEphemeral},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var intents []uint8
			var promptOut strings.Builder
			session := ""
			if tt.intent != protocol.IntentEphemeral {
				session = "codejack"
			}
			err := runAttachWithDeps(context.Background(), tt.intent, session, tt.remote, "", nil, runAttachDeps{
				attachPromptIn:       strings.NewReader("y\n"),
				attachPromptOut:      &promptOut,
				attachPromptTerminal: func() bool { return true },
				runClient: func(_ context.Context, deps client.Dependencies, request client.AttachRequest) error {
					intents = append(intents, request.Intent)
					if tt.remote != "" {
						require.True(t, deps.Remote)
					}
					return nil
				},
			})
			require.NoError(t, err)
			require.Equal(t, []uint8{tt.intent}, intents)
			require.Empty(t, promptOut.String(), "preflight must not run outside direct local attach")
		})
	}
}

func TestRunAttachWithDepsMissingSessionPreflightUnavailableAttaches(t *testing.T) {
	var promptOut strings.Builder
	dialErr := errors.New("socket unreachable")
	var intents []uint8
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "codejack", "", "", nil, runAttachDeps{
		localDialer:          func() wire.Dialer { return stubPreflightErrorDialer{err: dialErr} },
		attachPromptIn:       strings.NewReader("y\n"),
		attachPromptOut:      &promptOut,
		attachPromptTerminal: func() bool { return true },
		runClient: func(_ context.Context, _ client.Dependencies, request client.AttachRequest) error {
			intents = append(intents, request.Intent)
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, []uint8{protocol.IntentAttach}, intents)
	require.Empty(t, promptOut.String(), "an undeterminable preflight must not prompt; the daemon rejection decides")
}

func TestRunAttachWithDepsMissingSessionCancelledPreflightSkipsPrompt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var promptOut strings.Builder
	var intents []uint8
	err := runAttachWithDeps(ctx, protocol.IntentAttach, "codejack", "", "", nil, runAttachDeps{
		localDialer:          func() wire.Dialer { return stubPreflightErrorDialer{err: context.Canceled} },
		attachPromptIn:       strings.NewReader("y\n"),
		attachPromptOut:      &promptOut,
		attachPromptTerminal: func() bool { return true },
		runClient: func(_ context.Context, _ client.Dependencies, request client.AttachRequest) error {
			intents = append(intents, request.Intent)
			return nil
		},
	})
	require.NoError(t, err)
	require.Equal(t, []uint8{protocol.IntentAttach}, intents)
	require.Empty(t, promptOut.String())
}
