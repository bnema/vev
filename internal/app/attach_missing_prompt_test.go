package app

// Attach-missing create prompt (Plan 003 E4).
//
// resolveAttachCreationIntent restores the `vev attach <missing>` create
// prompt main once ran through the direct-dial preflight, but through the
// broker: it peeks the first committed publication over the same
// connectProductionClientBroker seam the ordinary terminal composition uses,
// decides existence with the exact lookup the later resolver also runs
// (attachTargetSessionExists / brokerObservationExactTarget), and on a
// confirmed create hands its already-open connection back so the run still
// opens exactly one broker connection.

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
	"github.com/bnema/vev/pkg/rawterm"
)

// interactiveProbe injects the console decision, standing in for the
// production probe so the prompt matrix runs without owning a real TTY.
func interactiveProbe(interactive bool) func(ports.Terminal) bool {
	return func(ports.Terminal) bool { return interactive }
}

// withAttachInteractiveConsole installs a scripted console decision for the
// duration of a test.
func withAttachInteractiveConsole(t *testing.T, interactive bool) {
	t.Helper()
	previous := attachInteractiveConsole
	attachInteractiveConsole = interactiveProbe(interactive)
	t.Cleanup(func() { attachInteractiveConsole = previous })
}

// withPreflightBroker installs a scripted connectProductionClientBroker seam
// for the duration of a test, exactly like the terminal composition tests in
// broker_client_test.go, so the preflight peek never reaches a production
// broker.
func withPreflightBroker(t *testing.T, connect func(context.Context) (ports.BrokerService, error)) {
	t.Helper()
	previous := connectProductionClientBroker
	connectProductionClientBroker = connect
	t.Cleanup(func() { connectProductionClientBroker = previous })
}

func TestAttachTargetSessionExists(t *testing.T) {
	tests := []struct {
		name         string
		snapshot     ports.BrokerSnapshot
		remote       string
		target       string
		wantExists   bool
		wantHostKnow bool
	}{
		{
			name:         "local session present",
			snapshot:     terminalCompositionSnapshot([]string{"work"}, nil),
			target:       "work",
			wantExists:   true,
			wantHostKnow: true,
		},
		{
			name:         "local session absent",
			snapshot:     terminalCompositionSnapshot([]string{"work"}, nil),
			target:       "gone",
			wantExists:   false,
			wantHostKnow: true,
		},
		{
			name:         "local daemon absent from snapshot",
			snapshot:     ports.BrokerSnapshot{Epoch: 1, Revision: 1},
			target:       "gone",
			wantExists:   false,
			wantHostKnow: false,
		},
		{
			name:         "remote session present",
			snapshot:     terminalCompositionSnapshot(nil, map[string][]string{"user@example.test": {"work"}}),
			remote:       "user@example.test",
			target:       "work",
			wantExists:   true,
			wantHostKnow: true,
		},
		{
			name:         "remote session absent on known host",
			snapshot:     terminalCompositionSnapshot(nil, map[string][]string{"user@example.test": {"work"}}),
			remote:       "user@example.test",
			target:       "gone",
			wantExists:   false,
			wantHostKnow: true,
		},
		{
			name:         "remote host not configured",
			snapshot:     terminalCompositionSnapshot([]string{"work"}, nil),
			remote:       "user@elsewhere.test",
			target:       "work",
			wantExists:   false,
			wantHostKnow: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			exists, hostKnown := attachTargetSessionExists(tc.snapshot, tc.remote, tc.target)
			require.Equal(t, tc.wantExists, exists)
			require.Equal(t, tc.wantHostKnow, hostKnown)
		})
	}
}

func TestResolveAttachCreationIntentSkipsNonAttach(t *testing.T) {
	for _, tc := range []struct {
		name   string
		intent uint8
		target string
	}{
		{name: "create", intent: protocol.IntentNew, target: "scratch"},
		{name: "ephemeral", intent: protocol.IntentEphemeral, target: ""},
		{name: "attach with no name", intent: protocol.IntentAttach, target: ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			connected := false
			withPreflightBroker(t, func(context.Context) (ports.BrokerService, error) {
				connected = true
				return nil, errors.New("must not connect")
			})
			intent, service, err := resolveAttachCreationIntent(context.Background(), tc.intent, tc.target, "", nil)
			require.NoError(t, err)
			require.Equal(t, tc.intent, intent)
			require.Nil(t, service)
			require.False(t, connected, "a non-attach or nameless intent never peeks the broker")
		})
	}
}

func TestResolveAttachCreationIntentBrokerUnavailableKeepsAttach(t *testing.T) {
	withPreflightBroker(t, func(context.Context) (ports.BrokerService, error) {
		return nil, errors.New("broker unreachable")
	})
	intent, service, err := resolveAttachCreationIntent(context.Background(), protocol.IntentAttach, "scratch", "", nil)
	require.NoError(t, err)
	require.Equal(t, protocol.IntentAttach, intent)
	require.Nil(t, service, "an undeterminable preflight must not hand back a connection")
}

func TestResolveAttachCreationIntentExistingSessionAttachesWithoutPrompt(t *testing.T) {
	service := portsmocks.NewMockBrokerService(t)
	service.EXPECT().Snapshot().Return(terminalCompositionSnapshot([]string{"work"}, nil)).Once()
	withPreflightBroker(t, func(context.Context) (ports.BrokerService, error) { return service, nil })
	withAttachInteractiveConsole(t, true) // must not even be consulted

	intent, got, err := resolveAttachCreationIntent(context.Background(), protocol.IntentAttach, "work", "", nil)
	require.NoError(t, err)
	require.Equal(t, protocol.IntentAttach, intent)
	require.Same(t, service, got, "the peeked connection is handed back for reuse")
}

func TestResolveAttachCreationIntentUnknownHostAttachesWithoutPrompt(t *testing.T) {
	service := portsmocks.NewMockBrokerService(t)
	service.EXPECT().Snapshot().Return(terminalCompositionSnapshot([]string{"work"}, nil)).Once()
	withPreflightBroker(t, func(context.Context) (ports.BrokerService, error) { return service, nil })
	withAttachInteractiveConsole(t, true)

	intent, got, err := resolveAttachCreationIntent(context.Background(), protocol.IntentAttach, "work", "user@elsewhere.test", nil)
	require.NoError(t, err)
	require.Equal(t, protocol.IntentAttach, intent)
	require.Same(t, service, got)
}

func TestResolveAttachCreationIntentMissingSessionPrompt(t *testing.T) {
	tests := []struct {
		name           string
		remote         string
		answer         string
		interactive    bool
		wantIntent     uint8
		wantPromptPart string
		wantNoPrompt   bool
	}{
		{name: "local confirm creates", answer: "y\n", interactive: true, wantIntent: protocol.IntentNew, wantPromptPart: `vev: session "gone" doesn't exist, want to create and attach to it? [y/N]`},
		{name: "local decline attaches", answer: "n\n", interactive: true, wantIntent: protocol.IntentAttach, wantPromptPart: "gone"},
		{name: "local empty answer attaches", answer: "\n", interactive: true, wantIntent: protocol.IntentAttach},
		{name: "local unknown answer attaches", answer: "later\n", interactive: true, wantIntent: protocol.IntentAttach, wantPromptPart: "[y/N]"},
		{name: "non-interactive console attaches without prompt", answer: "y\n", interactive: false, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
		{name: "remote confirm creates", remote: "user@example.test", answer: "y\n", interactive: true, wantIntent: protocol.IntentNew, wantPromptPart: `session "gone" doesn't exist`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var snapshot ports.BrokerSnapshot
			if tc.remote == "" {
				snapshot = terminalCompositionSnapshot([]string{"work"}, nil)
			} else {
				snapshot = terminalCompositionSnapshot(nil, map[string][]string{tc.remote: {"work"}})
			}
			service := portsmocks.NewMockBrokerService(t)
			service.EXPECT().Snapshot().Return(snapshot).Once()
			withPreflightBroker(t, func(context.Context) (ports.BrokerService, error) { return service, nil })
			withAttachInteractiveConsole(t, tc.interactive)

			terminal := portsmocks.NewMockTerminal(t)
			var promptOut strings.Builder
			terminal.EXPECT().In().Return(strings.NewReader(tc.answer)).Maybe()
			terminal.EXPECT().Out().Return(&promptOut).Maybe()
			terminal.EXPECT().Flush().Return(nil).Maybe()

			intent, got, err := resolveAttachCreationIntent(context.Background(), protocol.IntentAttach, "gone", tc.remote, terminal)
			require.NoError(t, err)
			require.Equal(t, tc.wantIntent, intent)
			require.Same(t, service, got, "the peeked connection is always handed back on a definite answer")
			if tc.wantPromptPart != "" {
				require.Contains(t, promptOut.String(), tc.wantPromptPart)
			}
			if tc.wantNoPrompt {
				require.Empty(t, promptOut.String())
			}
		})
	}
}

func TestResolveAttachCreationIntentCancelledPromptClosesThePeek(t *testing.T) {
	service := portsmocks.NewMockBrokerService(t)
	service.EXPECT().Snapshot().Return(terminalCompositionSnapshot([]string{"work"}, nil)).Once()
	service.EXPECT().Close().Return(nil).Once()
	withPreflightBroker(t, func(context.Context) (ports.BrokerService, error) { return service, nil })
	withAttachInteractiveConsole(t, true)

	terminal := portsmocks.NewMockTerminal(t)
	reader, writer := io.Pipe()
	t.Cleanup(func() { _ = reader.Close(); _ = writer.Close() })
	terminal.EXPECT().In().Return(reader).Maybe()
	terminal.EXPECT().Out().Return(io.Discard).Maybe()
	terminal.EXPECT().Flush().Return(nil).Maybe()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct {
		intent  uint8
		service ports.BrokerService
		err     error
	}, 1)
	go func() {
		intent, got, err := resolveAttachCreationIntent(ctx, protocol.IntentAttach, "gone", "", terminal)
		done <- struct {
			intent  uint8
			service ports.BrokerService
			err     error
		}{intent, got, err}
	}()
	cancel()
	select {
	case result := <-done:
		require.ErrorIs(t, result.err, context.Canceled)
		require.Nil(t, result.service, "a cancelled prompt closes and drops the peeked connection")
	case <-time.After(5 * time.Second):
		t.Fatal("resolveAttachCreationIntent did not release on cancellation")
	}
}

func TestConfirmMissingSessionCreate(t *testing.T) {
	tests := []struct {
		name           string
		answer         string
		interactive    bool
		wantIntent     uint8
		wantPromptPart string
		wantNoPrompt   bool
	}{
		{name: "accepts yes", answer: "y\n", interactive: true, wantIntent: protocol.IntentNew, wantPromptPart: `session "scratch" doesn't exist, want to create and attach to it? [y/N]`},
		{name: "accepts yes word", answer: "yes\n", interactive: true, wantIntent: protocol.IntentNew},
		{name: "declines no", answer: "n\n", interactive: true, wantIntent: protocol.IntentAttach},
		{name: "defaults no on empty answer", answer: "\n", interactive: true, wantIntent: protocol.IntentAttach},
		{name: "defaults no on unknown answer", answer: "later\n", interactive: true, wantIntent: protocol.IntentAttach},
		{name: "non-interactive console skips the prompt", answer: "y\n", interactive: false, wantIntent: protocol.IntentAttach, wantNoPrompt: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			withAttachInteractiveConsole(t, tc.interactive)
			terminal := portsmocks.NewMockTerminal(t)
			var out strings.Builder
			terminal.EXPECT().In().Return(strings.NewReader(tc.answer)).Maybe()
			terminal.EXPECT().Out().Return(&out).Maybe()
			terminal.EXPECT().Flush().Return(nil).Maybe()

			intent, err := confirmMissingSessionCreate(context.Background(), "scratch", terminal)
			require.NoError(t, err)
			require.Equal(t, tc.wantIntent, intent)
			if tc.wantPromptPart != "" {
				require.Contains(t, out.String(), tc.wantPromptPart)
			}
			if tc.wantNoPrompt {
				require.Empty(t, out.String())
			}
		})
	}
}

// TestConfirmMissingSessionCreateFailureAttaches covers a console that cannot
// show the question: the prompt must keep the plain attach path instead of
// consuming an answer nobody was asked for. The input blocks forever, so a
// prompt that reads it would only return on context expiry.
func TestConfirmMissingSessionCreateFailureAttaches(t *testing.T) {
	closedOut, err := os.CreateTemp(t.TempDir(), "vev-console")
	require.NoError(t, err)
	require.NoError(t, closedOut.Close())

	for _, tc := range []struct {
		name     string
		out      io.Writer
		flushErr error
	}{
		{name: "write fails", out: closedOut},
		{name: "flush fails", out: io.Discard, flushErr: errors.New("console gone")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withAttachInteractiveConsole(t, true)
			consoleIn, consoleOut, err := os.Pipe()
			require.NoError(t, err)
			t.Cleanup(func() { _ = consoleIn.Close(); _ = consoleOut.Close() })

			terminal := portsmocks.NewMockTerminal(t)
			terminal.EXPECT().In().Return(consoleIn).Maybe()
			terminal.EXPECT().Out().Return(tc.out).Maybe()
			terminal.EXPECT().Flush().Return(tc.flushErr).Maybe()

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			intent, err := confirmMissingSessionCreate(ctx, "scratch", terminal)
			require.NoError(t, err, "an unusable console must not fail the attach")
			require.Equal(t, protocol.IntentAttach, intent)
		})
	}
}

// TestConfirmMissingSessionCreateFlushesQuestion drives the real terminal
// adapter, whose output is buffered: the prompt has to flush the question
// before it blocks on the answer, or the user waits in front of a blank
// console and the question only shows up after the client repaints.
func TestConfirmMissingSessionCreateFlushesQuestion(t *testing.T) {
	withAttachInteractiveConsole(t, true)
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

	terminal := term.NewWithFiles(consoleIn, consoleOut)
	done := make(chan struct {
		intent uint8
		err    error
	}, 1)
	go func() {
		intent, err := confirmMissingSessionCreate(context.Background(), "scratch", terminal)
		done <- struct {
			intent uint8
			err    error
		}{intent, err}
	}()

	question := readAttachPrompt(t, questions)
	require.Contains(t, question, "want to create and attach to it? [y/N] ", "the question must reach the console before the prompt reads an answer")

	_, err = answers.WriteString("y\n")
	require.NoError(t, err)
	select {
	case result := <-done:
		require.NoError(t, result.err)
		require.Equal(t, protocol.IntentNew, result.intent)
	case <-time.After(5 * time.Second):
		t.Fatal("the prompt did not release after the answer")
	}
}

// readAttachPrompt returns the console output that ends the create question,
// failing the test instead of hanging when nothing arrives.
func readAttachPrompt(t *testing.T, console *os.File) string {
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
			t.Fatalf("prompt never reached the console (read %q: %v)", output, err)
		}
	}
}

// TestTerminalIsInteractive covers the production probe on both sides of the
// console check: a real terminal file and the streams that are not consoles.
func TestTerminalIsInteractive(t *testing.T) {
	require.False(t, terminalIsInteractive(nil), "a missing terminal cannot prompt")
	pty := openAttachPtySlave(t)
	require.True(t, terminalIsInteractive(term.NewWithFiles(pty, pty)), "a real terminal file is a console")

	console, writer, err := os.Pipe()
	require.NoError(t, err)
	t.Cleanup(func() { _ = console.Close(); _ = writer.Close() })

	for _, tc := range []struct {
		name string
		in   io.Reader
	}{
		{name: "detached reader", in: strings.NewReader("y\n")},
		{name: "non-terminal file", in: console},
	} {
		t.Run(tc.name, func(t *testing.T) {
			terminal := portsmocks.NewMockTerminal(t)
			terminal.EXPECT().In().Return(tc.in).Maybe()
			require.False(t, terminalIsInteractive(terminal))
		})
	}
}

// openAttachPtySlave returns the slave side of a real PTY pair: the only way
// to hand the probe an *os.File the kernel reports as a terminal.
func openAttachPtySlave(t *testing.T) *os.File {
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

func TestPreconnectedBrokerConnectorReusesThenFallsBack(t *testing.T) {
	first := portsmocks.NewMockBrokerService(t)
	second := portsmocks.NewMockBrokerService(t)
	fallback := portsmocks.NewMockBrokerConnector(t)
	fallback.EXPECT().Connect(mock.Anything).Return(second, nil).Once()
	connector := withPreconnected(fallback, first)

	got, err := connector.Connect(context.Background())
	require.NoError(t, err)
	require.Same(t, first, got, "the first Connect reuses the preflight's own connection")

	got, err = connector.Connect(context.Background())
	require.NoError(t, err)
	require.Same(t, second, got, "a later Connect reconnects through the fallback")
}

func TestWithPreconnectedNilPassesThroughUnchanged(t *testing.T) {
	fallback := portsmocks.NewMockBrokerConnector(t)
	fallback.EXPECT().Connect(mock.Anything).Return(nil, nil).Once()
	got := withPreconnected(fallback, nil)
	service, err := got.Connect(context.Background())
	require.NoError(t, err)
	require.Nil(t, service)
}
