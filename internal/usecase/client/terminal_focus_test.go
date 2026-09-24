package client

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/protocol"
)

func TestStripTerminalFocusReports(t *testing.T) {
	tests := []struct {
		name      string
		start     domain.TerminalFocus
		input     string
		want      string
		wantFocus domain.TerminalFocus
		changed   bool
	}{
		{name: "plain input passes through", input: "ls\r", want: "ls\r"},
		{name: "focus in only", input: "\x1b[I", want: "", wantFocus: domain.TerminalFocusFocused, changed: true},
		{name: "focus out only", start: domain.TerminalFocusFocused, input: "\x1b[O", want: "", wantFocus: domain.TerminalFocusUnfocused, changed: true},
		{name: "last report wins", input: "\x1b[I\x1b[O", want: "", wantFocus: domain.TerminalFocusUnfocused, changed: true},
		{name: "keys around a report keep their order", input: "a\x1b[Ib", want: "ab", wantFocus: domain.TerminalFocusFocused, changed: true},
		{name: "arrow keys and csi are untouched", input: "\x1b[A\x1b[1;5C\x1b[<0;1;1M", want: "\x1b[A\x1b[1;5C\x1b[<0;1;1M"},
		{name: "a lone escape is never withheld", input: "\x1b", want: "\x1b"},
		{name: "a split report is forwarded as typed", input: "\x1b[", want: "\x1b["},
		{name: "a repeated state changes nothing", start: domain.TerminalFocusFocused, input: "\x1b[I", want: "", wantFocus: domain.TerminalFocusFocused},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			state := newTerminalFocusState()
			state.set(tt.start)
			_, changed := state.Watch()

			got := stripTerminalFocusReports(state, []byte(tt.input))

			require.Equal(t, tt.want, string(got))
			want := tt.wantFocus
			if want == domain.TerminalFocusUnknown {
				want = tt.start
			}
			require.Equal(t, want, state.Focus())
			select {
			case <-changed:
				require.True(t, tt.changed, "no change was expected")
			default:
				require.False(t, tt.changed, "a change was expected")
			}
		})
	}
}

func TestFocusReporterSendsKnownChangesOnce(t *testing.T) {
	state := newTerminalFocusState()
	reporter := &focusReporter{state: state}

	_, ok := reporter.next()
	require.False(t, ok, "unknown focus is never sent")

	steps := []struct {
		focus  domain.TerminalFocus
		want   protocol.TerminalFocus
		wantOK bool
	}{
		{focus: domain.TerminalFocusFocused, want: protocol.TerminalFocus{Focus: domain.TerminalFocusFocused}, wantOK: true},
		{focus: domain.TerminalFocusUnfocused, want: protocol.TerminalFocus{Focus: domain.TerminalFocusUnfocused}, wantOK: true},
		{focus: domain.TerminalFocusUnfocused},
	}
	for _, step := range steps {
		wake := reporter.changed
		state.set(step.focus)
		if step.wantOK {
			select {
			case <-wake:
			default:
				t.Fatalf("a change to %s must wake the reporter", step.focus)
			}
		}
		got, ok := reporter.next()
		require.Equal(t, step.wantOK, ok)
		require.Equal(t, step.want, got)
	}
}

// TestTerminalInputPumpStripsFocusReports proves focus reports never reach an
// attachment: a read that only carried a report delivers nothing, the focus
// the terminal reported is readable on the foreground, and later keys arrive
// intact.
func TestTerminalInputPumpStripsFocusReports(t *testing.T) {
	pr, pw := io.Pipe()
	pump := newTerminalInputPump(pr)
	pump.start()
	defer pump.stop()
	defer pw.Close()
	host := newWorkerTestHost(newWorkerTestTerminal(), pump, nil)

	observed := make(chan []byte, 1)
	focus := make(chan domain.TerminalFocus, 1)
	worker := &fakeWorker{run: func(ctx context.Context, fg AttachmentForeground) AttachmentEvent {
		event, ok := fg.Input(ctx)
		if !ok {
			return AttachmentEvent{Kind: AttachmentEventFailed, Err: errors.New("no input")}
		}
		fg.AckInput()
		observed <- append([]byte(nil), event.Data...)
		focus <- attachmentTerminalFocus(fg).Focus()
		return AttachmentEvent{Kind: AttachmentEventEnded}
	}}

	go func() {
		_, _ = pw.Write([]byte("\x1b[O"))
		_, _ = pw.Write([]byte("\x1b[Iab"))
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, adopted := host.Run(ctx, AttachmentToken{Generation: 1, Attempt: 1}, worker, newWorkerTestStream())
	require.True(t, adopted)

	require.Equal(t, []byte("ab"), <-observed, "the focus-only read is never delivered")
	require.Equal(t, domain.TerminalFocusFocused, <-focus)
}

func TestNilTerminalFocusStateIsUnknown(t *testing.T) {
	var state *TerminalFocusState
	focus, changed := state.Watch()
	require.Equal(t, domain.TerminalFocusUnknown, focus)
	require.Nil(t, changed)
	require.Equal(t, domain.TerminalFocusUnknown, state.Focus())
}

// TestAttachmentDeclaresFocusInHelloThenReportsChanges proves the daemon
// learns a terminal's focus before its first paint: Hello carries the focus
// already known, and each later change follows as TerminalFocus.
func TestAttachmentDeclaresFocusInHelloThenReportsChanges(t *testing.T) {
	pump := newTerminalInputPump(blockingReader{})
	pump.focus.set(domain.TerminalFocusUnfocused)
	stream := newSessionTestStream()
	host := newWorkerTestHost(newWorkerTestTerminal(), pump, nil)
	worker, err := newSessionAttachmentWorker(sessionTestWorkerConfig(sessionTestRequest(true)))
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _, _ = host.Run(ctx, AttachmentToken{Generation: 4, Attempt: 1}, worker, stream) }()

	hello := awaitHello(t, stream)
	require.Equal(t, domain.TerminalFocusUnfocused, hello.TerminalFocus)
	require.NoError(t, protocol.ValidateHello(hello))

	stream.deliver(protocol.Welcome{SessionName: "alpha"})
	stream.deliver(sessionTestOutput(1, "\x1b[Hready"))
	pump.focus.set(domain.TerminalFocusFocused)
	require.Eventually(t, func() bool {
		for _, message := range stream.messages() {
			if message == (protocol.TerminalFocus{Focus: domain.TerminalFocusFocused}) {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond, "a focus change after attach is reported")
}

// blockingReader is a terminal that never sends input.
type blockingReader struct{}

func (blockingReader) Read([]byte) (int, error) { select {} }
