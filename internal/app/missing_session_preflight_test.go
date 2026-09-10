package app

import (
	"context"
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/term"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
	"github.com/bnema/vev/internal/usecase/client"
	"github.com/bnema/vev/pkg/rawterm"
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
	terminal.EXPECT().Flush().Return(nil).Maybe()
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

// TestTerminalIsInteractive covers the production probe on both sides of the
// console check: a real terminal file and the streams that are not consoles.
func TestTerminalIsInteractive(t *testing.T) {
	require.False(t, terminalIsInteractive(nil), "a missing terminal cannot prompt")
	pty := openPtySlave(t)
	require.True(t, terminalIsInteractive(term.NewWithFiles(pty, pty)), "a real terminal file is a console")

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

// openPtySlave returns the slave side of a real PTY pair: the only way to
// hand the probe an *os.File the kernel reports as a terminal.
func openPtySlave(t *testing.T) *os.File {
	t.Helper()
	master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY|syscall.O_CLOEXEC, 0)
	if err != nil {
		t.Skipf("open /dev/ptmx: %v", err)
	}
	t.Cleanup(func() { _ = master.Close() })
	slave, err := rawterm.PreparePty(int(master.Fd()))
	require.NoError(t, err, "prepare pty")
	t.Cleanup(func() { _ = slave.Close() })
	return slave
}

// TestRunAttachWithDepsMissingSessionPromptFlushesQuestion drives the real
// terminal adapter, whose output is buffered: the preflight has to flush the
// question before it blocks on the answer, or the user waits in front of a
// blank console and the question only shows up after the daemon replies. The
// observed wrapper used by `--ui-observe` embeds the port, so it has to keep
// flushing end to end.
func TestRunAttachWithDepsMissingSessionPromptFlushesQuestion(t *testing.T) {
	for _, tt := range []struct {
		name string
		wrap func(ports.Terminal) ports.Terminal
	}{
		{name: "direct terminal", wrap: func(terminal ports.Terminal) ports.Terminal { return terminal }},
		{name: "observed terminal", wrap: func(terminal ports.Terminal) ports.Terminal {
			return observedTerminal{Terminal: terminal}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			consoleIn, answers, err := os.Pipe()
			require.NoError(t, err)
			questions, consoleOut, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = consoleIn.Close()
				_ = answers.Close()
				_ = questions.Close()
				_ = consoleOut.Close()
			})

			terminal := tt.wrap(term.NewWithFiles(consoleIn, consoleOut))
			var intents []uint8
			done := make(chan error, 1)
			go func() {
				done <- runAttachWithDeps(context.Background(), protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
					localDialer:        sessionListDialer(t, nil),
					terminal:           func() ports.Terminal { return terminal },
					interactiveConsole: interactiveProbe(true),
					runClient: func(_ context.Context, _ client.Dependencies, request client.AttachRequest) error {
						intents = append(intents, request.Intent)
						return nil
					},
				})
			}()

			question := readPrompt(t, questions)
			require.Contains(t, question, "want to create and attach to it? [y/N] ", "the question must reach the console before the preflight reads an answer")

			_, err = answers.WriteString("y\n")
			require.NoError(t, err)
			select {
			case err := <-done:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("the preflight did not release after the answer")
			}
			require.Equal(t, []uint8{protocol.IntentNew}, intents)
		})
	}
}

// readPrompt returns the console output that ends the create question,
// failing the test instead of hanging when nothing arrives.
func readPrompt(t *testing.T, console *os.File) string {
	t.Helper()
	require.NoError(t, console.SetReadDeadline(time.Now().Add(5*time.Second)))
	var output []byte
	frame := make([]byte, 64)
	for {
		read, err := console.Read(frame)
		output = append(output, frame[:read]...)
		if strings.Contains(string(output), "[y/N] ") {
			return string(output)
		}
		if err != nil {
			t.Fatalf("preflight prompt never reached the console (read %q: %v)", output, err)
		}
	}
}

// TestRunAttachWithDepsMissingSessionPromptFailureAttaches covers a console
// that cannot show the question: the preflight must keep the plain attach
// path instead of consuming an answer nobody was asked for. The input blocks
// forever, so a prompt that reads it would only return on context expiry.
func TestRunAttachWithDepsMissingSessionPromptFailureAttaches(t *testing.T) {
	closedOut, err := os.CreateTemp(t.TempDir(), "vev-console")
	require.NoError(t, err)
	require.NoError(t, closedOut.Close())

	for _, tt := range []struct {
		name     string
		out      io.Writer
		flushErr error
	}{
		{name: "write fails", out: closedOut},
		{name: "flush fails", out: io.Discard, flushErr: errors.New("console gone")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			consoleIn, consoleOut, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() {
				_ = consoleIn.Close()
				_ = consoleOut.Close()
			})
			terminal := portsmocks.NewMockTerminal(t)
			terminal.EXPECT().In().Return(consoleIn).Maybe()
			terminal.EXPECT().Out().Return(tt.out).Maybe()
			terminal.EXPECT().Flush().Return(tt.flushErr).Maybe()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			var intents []uint8
			err = runAttachWithDeps(ctx, protocol.IntentAttach, "scratch", "", "", nil, runAttachDeps{
				localDialer:        sessionListDialer(t, nil),
				terminal:           func() ports.Terminal { return terminal },
				interactiveConsole: interactiveProbe(true),
				runClient: func(_ context.Context, _ client.Dependencies, request client.AttachRequest) error {
					intents = append(intents, request.Intent)
					return nil
				},
			})
			require.NoError(t, err, "an unusable console must not fail the attach")
			require.Equal(t, []uint8{protocol.IntentAttach}, intents)
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
