package app

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
	"github.com/bnema/vev/internal/usecase/client"
)

// sessionListDialer serves a canned session listing over generated mocks.
func sessionListDialer(t *testing.T, sessions []protocol.SessionInfo) func() wire.Dialer {
	t.Helper()
	transport := wiremocks.NewMockTransport(t)
	transport.EXPECT().Send(mock.Anything).RunAndReturn(func(frame wire.Frame) error {
		require.Equal(t, wire.MsgList, frame.Type)
		return nil
	}).Once()
	transport.EXPECT().Recv().Return(wire.Frame{Type: wire.MsgSessions, Payload: wire.MarshalSessions(protocol.Sessions{Sessions: sessions})}, nil).Once()
	transport.EXPECT().Close().Return(nil)
	dialer := wiremocks.NewMockDialer(t)
	dialer.EXPECT().Dial(mock.Anything).Return(transport, nil).Once()
	return func() wire.Dialer { return dialer }
}

// failingSessionListDialer fails the listing dial so the preflight falls
// back to a plain attach.
func failingSessionListDialer(t *testing.T, dialErr error) func() wire.Dialer {
	t.Helper()
	dialer := wiremocks.NewMockDialer(t)
	dialer.EXPECT().Dial(mock.Anything).Return(nil, dialErr).Maybe()
	return func() wire.Dialer { return dialer }
}

// terminalStub serves canned prompt I/O through a generated terminal mock.
func terminalStub(t *testing.T, in io.Reader, out io.Writer) func() ports.Terminal {
	t.Helper()
	terminal := portsmocks.NewMockTerminal(t)
	terminal.EXPECT().In().Return(in).Maybe()
	terminal.EXPECT().Out().Return(out).Maybe()
	return func() ports.Terminal { return terminal }
}

// interactiveProbe injects the console decision, standing in for the
// production probe so the prompt matrix runs without owning a real TTY.
func interactiveProbe(interactive bool) func(ports.Terminal) bool {
	return func(ports.Terminal) bool { return interactive }
}

func TestRunAttachWithDepsMissingSessionCreatePrompt(t *testing.T) {
	tests := []struct {
		name           string
		sessions       []protocol.SessionInfo
		answer         string
		notInteractive bool
		wantIntent     uint8
		wantPromptPart string
		wantNoPrompt   bool
	}{
		{name: "missing confirm creates", sessions: nil, answer: "y\n", wantIntent: protocol.IntentNew, wantPromptPart: `vev: session "scratch" doesn't exist, want to create and attach to it? [y/N]`},
		{name: "missing decline attaches", sessions: nil, answer: "n\n", wantIntent: protocol.IntentAttach, wantPromptPart: "scratch"},
		{name: "missing empty answer attaches", sessions: nil, answer: "\n", wantIntent: protocol.IntentAttach},
		{name: "missing unknown answer attaches", sessions: nil, answer: "later\n", wantIntent: protocol.IntentAttach, wantPromptPart: "[y/N]"},
		{name: "present attaches without prompt", sessions: []protocol.SessionInfo{{Name: "scratch"}}, answer: "y\n", wantIntent: protocol.IntentAttach, wantNoPrompt: true},
		{name: "non-interactive console attaches without prompt", sessions: nil, answer: "y\n", notInteractive: true, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var intents []uint8
			var promptOut strings.Builder
			err := runAttachWithDeps(context.Background(), protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
				localDialer:        sessionListDialer(t, tt.sessions),
				terminal:           terminalStub(t, strings.NewReader(tt.answer), &promptOut),
				interactiveConsole: interactiveProbe(!tt.notInteractive),
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

// TestTerminalIsInteractive covers the production probe's non-console
// paths. Reaching the true case needs a real PTY, which this suite does not
// own; production always hands the probe an *os.File stdin.
func TestTerminalIsInteractive(t *testing.T) {
	require.False(t, terminalIsInteractive(nil), "a missing terminal cannot prompt")

	console, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = console.Close()
		_ = writer.Close()
	})

	for _, tt := range []struct {
		name string
		in   io.Reader
	}{
		{name: "detached reader", in: strings.NewReader("y\n")},
		{name: "non-terminal file", in: console},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.False(t, terminalIsInteractive(terminalStub(t, tt.in, io.Discard)()))
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
				terminal:           terminalStub(t, strings.NewReader("y\n"), &promptOut),
				interactiveConsole: interactiveProbe(true),
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
	for _, tt := range []struct {
		name    string
		dialErr error
		cancel  bool
	}{
		{name: "dial fails", dialErr: errors.New("socket unreachable")},
		{name: "context cancelled", dialErr: context.Canceled, cancel: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			stubLifecycleUnavailable(t)
			ctx := context.Background()
			if tt.cancel {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			var promptOut strings.Builder
			var intents []uint8
			err := runAttachWithDeps(ctx, protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
				localDialer:        failingSessionListDialer(t, tt.dialErr),
				terminal:           terminalStub(t, strings.NewReader("y\n"), &promptOut),
				interactiveConsole: interactiveProbe(true),
				runClient: func(_ context.Context, _ client.Dependencies, request client.AttachRequest) error {
					intents = append(intents, request.Intent)
					return nil
				},
			})
			require.NoError(t, err)
			require.Equal(t, []uint8{protocol.IntentAttach}, intents)
			require.Empty(t, promptOut.String(), "an undeterminable preflight must not prompt; the daemon rejection decides")
		})
	}
}

// stubLifecycleUnavailable fails lifecycle acquisition so the preflight
// cannot fall back to the on-disk catalogue, independent of the machine's
// real daemon state.
func stubLifecycleUnavailable(t *testing.T) {
	t.Helper()
	originalProbe := daemonLifecycleProbe
	t.Cleanup(func() { daemonLifecycleProbe = originalProbe })
	daemonLifecycleProbe = fakeLifecycleProbe{err: errors.New("lifecycle unavailable")}
}

// stalledListTransport parks the list exchange inside one operation until
// closed, letting tests exercise the preflight bound mid-flight. entered
// fires when the exchange reaches the parked operation.
func stalledListTransport(t *testing.T, blockOnSend bool) (*wiremocks.MockTransport, chan struct{}, chan struct{}) {
	t.Helper()
	entered := make(chan struct{})
	closed := make(chan struct{})
	transport := wiremocks.NewMockTransport(t)
	block := func() {
		close(entered)
		<-closed
	}
	if blockOnSend {
		transport.EXPECT().Send(mock.Anything).Run(func(wire.Frame) { block() }).Return(errors.New("transport closed")).Once()
	} else {
		transport.EXPECT().Send(mock.Anything).Return(nil).Once()
		transport.EXPECT().Recv().Run(func() { block() }).Return(wire.Frame{}, errors.New("transport closed")).Once()
	}
	transport.EXPECT().Close().Run(func() {
		select {
		case <-closed:
		default:
			close(closed)
		}
	}).Return(nil)
	return transport, entered, closed
}

func TestListSessionsTimesOutOnSilentDaemon(t *testing.T) {
	oldTimeout := preflightListTimeout
	preflightListTimeout = 50 * time.Millisecond
	defer func() { preflightListTimeout = oldTimeout }()

	transport, _, closed := stalledListTransport(t, false)
	started := time.Now()
	_, err := listSessionsWithDialer(context.Background(), func(context.Context) (wire.Transport, error) {
		return transport, nil
	})
	elapsed := time.Since(started)
	require.Error(t, err)
	require.Less(t, elapsed, 5*time.Second, "a silent daemon must not stall the preflight past its bound")
	select {
	case <-closed:
	default:
		t.Fatal("preflight timeout must close the exchange transport")
	}
}

func TestListSessionsHonorsCancellation(t *testing.T) {
	for _, tt := range []struct {
		name        string
		blockOnSend bool
	}{
		{name: "blocked send", blockOnSend: true},
		{name: "blocked recv", blockOnSend: false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			transport, entered, closed := stalledListTransport(t, tt.blockOnSend)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := listSessionsWithDialer(ctx, func(context.Context) (wire.Transport, error) {
					return transport, nil
				})
				done <- err
			}()
			// Wait until the exchange is blocked inside the transport
			// before cancelling, so the test exercises in-flight
			// cancellation rather than the earlier dial-path check.
			select {
			case <-entered:
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
			case <-closed:
			default:
				t.Fatal("cancellation must close the exchange transport")
			}
		})
	}
}
