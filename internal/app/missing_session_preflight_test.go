package app

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

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
	dialer := wiremocks.NewMockDialer(t)
	dialer.EXPECT().Dial(mock.Anything).RunAndReturn(func(context.Context) (wire.Transport, error) {
		return transport, nil
	}).Once()
	return func() wire.Dialer { return dialer }
}

func fakeFailingListDialer(t *testing.T, dialErr error) func() wire.Dialer {
	t.Helper()
	dialer := wiremocks.NewMockDialer(t)
	dialer.EXPECT().Dial(mock.Anything).RunAndReturn(func(context.Context) (wire.Transport, error) {
		return nil, dialErr
	}).Maybe()
	return func() wire.Dialer { return dialer }
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
		{name: "missing confirm creates", sessions: nil, answer: "y\n", terminal: true, wantIntent: protocol.IntentNew, wantPromptPart: `vev: session "scratch" doesn't exist, want to create and attach to it? [y/N]`},
		{name: "missing decline attaches", sessions: nil, answer: "n\n", terminal: true, wantIntent: protocol.IntentAttach, wantPromptPart: "scratch"},
		{name: "missing empty answer attaches", sessions: nil, answer: "\n", terminal: true, wantIntent: protocol.IntentAttach},
		{name: "missing unknown answer attaches", sessions: nil, answer: "later\n", terminal: true, wantIntent: protocol.IntentAttach, wantPromptPart: "[y/N]"},
		{name: "missing non-terminal attaches", sessions: nil, answer: "y\n", terminal: false, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
		{name: "present attaches without prompt", sessions: []protocol.SessionInfo{{Name: "scratch"}}, answer: "y\n", terminal: true, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var intents []uint8
			var promptOut strings.Builder
			err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
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
				session = "scratch"
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
	originalProbe := daemonLifecycleProbe
	t.Cleanup(func() { daemonLifecycleProbe = originalProbe })
	// Fail lifecycle acquisition so the preflight cannot fall back to the
	// on-disk catalogue: its error path must attach without prompting,
	// independent of the machine's real daemon state.
	lifecycleErr := errors.New("lifecycle unavailable")
	daemonLifecycleProbe = fakeLifecycleProbe{err: lifecycleErr}
	var promptOut strings.Builder
	dialErr := errors.New("socket unreachable")
	var intents []uint8
	err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
		localDialer:          fakeFailingListDialer(t, dialErr),
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
	err := runAttachWithDeps(ctx, protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
		localDialer:          fakeFailingListDialer(t, context.Canceled),
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

type blockingPreflightTransport struct {
	blockOnSend bool
	entered     chan struct{}
	enterOnce   sync.Once
	closed      chan struct{}
	closeOnce   sync.Once
}

func newBlockingPreflightTransport() *blockingPreflightTransport {
	return &blockingPreflightTransport{entered: make(chan struct{}), closed: make(chan struct{})}
}

func (t *blockingPreflightTransport) markEntered() {
	t.enterOnce.Do(func() { close(t.entered) })
}

func (t *blockingPreflightTransport) Send(wire.Frame) error {
	if t.blockOnSend {
		t.markEntered()
		<-t.closed
		return errors.New("transport closed")
	}
	return nil
}

func (t *blockingPreflightTransport) Recv() (wire.Frame, error) {
	if !t.blockOnSend {
		t.markEntered()
	}
	<-t.closed
	return wire.Frame{}, errors.New("transport closed")
}

func (t *blockingPreflightTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func TestListLocalSessionsOnTimesOutOnSilentDaemon(t *testing.T) {
	oldTimeout := preflightListTimeout
	preflightListTimeout = 50 * time.Millisecond
	defer func() { preflightListTimeout = oldTimeout }()

	transport := newBlockingPreflightTransport()
	started := time.Now()
	_, err := listLocalSessionsOn(context.Background(), func(context.Context) (wire.Transport, error) {
		return transport, nil
	})
	elapsed := time.Since(started)
	require.Error(t, err)
	require.Less(t, elapsed, 5*time.Second, "a silent daemon must not stall the preflight past its bound")
	select {
	case <-transport.closed:
	default:
		t.Fatal("preflight timeout must close the exchange transport")
	}
}

func TestListLocalSessionsOnHonorsCancellation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		blockOnSend bool
	}{
		{name: "blocked send", blockOnSend: true},
		{name: "blocked recv", blockOnSend: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transport := newBlockingPreflightTransport()
			transport.blockOnSend = tt.blockOnSend
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := listLocalSessionsOn(ctx, func(context.Context) (wire.Transport, error) {
					return transport, nil
				})
				done <- err
			}()
			// Wait until the exchange is blocked inside the transport
			// before cancelling, so the test exercises in-flight
			// cancellation rather than the earlier dial-path check.
			select {
			case <-transport.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("exchange did not reach the blocked transport")
			}
			started := time.Now()
			cancel()
			select {
			case err := <-done:
				require.ErrorIs(t, err, context.Canceled)
				require.Less(t, time.Since(started), preflightListTimeout, "in-flight cancellation must beat the list bound")
			case <-time.After(5 * time.Second):
				t.Fatal("preflight did not release on context cancellation")
			}
			select {
			case <-transport.closed:
			default:
				t.Fatal("cancellation must close the exchange transport")
			}
		})
	}
}
