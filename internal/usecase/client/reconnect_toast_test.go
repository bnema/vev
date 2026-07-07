package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/renderer"
)

type reconnectToastTerminalHarness struct {
	term     *portsmocks.MockTerminal
	in       *io.PipeReader
	inWriter *io.PipeWriter
	out      bytes.Buffer
	size     domain.Size
	resizeCh chan domain.Size
}

func newReconnectToastTerminalHarness(t *testing.T) *reconnectToastTerminalHarness {
	t.Helper()
	in, inWriter := io.Pipe()
	h := &reconnectToastTerminalHarness{
		term:     portsmocks.NewMockTerminal(t),
		in:       in,
		inWriter: inWriter,
		size:     domain.Size{Cols: 80, Rows: 24},
		resizeCh: make(chan domain.Size),
	}
	h.term.EXPECT().EnterRaw().Return(func() error { return nil }, nil).Maybe()
	h.term.EXPECT().Size().Return(h.size, nil).Maybe()
	h.term.EXPECT().ResizeEvents().Return((<-chan domain.Size)(h.resizeCh)).Maybe()
	h.term.EXPECT().QueryColors().Return(nil).Maybe()
	h.term.EXPECT().In().Return(h.in).Maybe()
	h.term.EXPECT().Out().Return(&h.out).Maybe()
	h.term.EXPECT().Flush().Return(nil).Maybe()
	return h
}

func (h *reconnectToastTerminalHarness) closeInput() { _ = h.inWriter.Close() }

func reconnectToastWelcome(token uint64) ports.Frame {
	return reconnectToastWelcomeNamed("", token)
}

func reconnectToastWelcomeNamed(name string, token uint64) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s1", SessionName: name, ResumeToken: token, Capabilities: ports.CapabilityResume})}
}

func reconnectToastDetach(reason uint8) ports.Frame {
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: reason})}
}

func mockReconnectTransport(t *testing.T, recvs ...reconnectToastRecv) *portsmocks.MockTransport {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(nil).Maybe()
	for _, recv := range recvs {
		tr.EXPECT().Recv().Return(recv.frame, recv.err).Once()
	}
	tr.EXPECT().Recv().Return(ports.Frame{}, io.EOF).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	return tr
}

type reconnectToastRecv struct {
	frame ports.Frame
	err   error
}

type reconnectToastSequenceDialer struct {
	transports []ports.Transport
	calls      int
}

func (d *reconnectToastSequenceDialer) Dial(context.Context) (ports.Transport, error) {
	if d.calls >= len(d.transports) {
		return nil, io.EOF
	}
	tr := d.transports[d.calls]
	d.calls++
	return tr, nil
}

type reconnectToastRecordingTransport struct {
	recvs  []reconnectToastRecv
	sends  []ports.Frame
	closed bool
}

func (t *reconnectToastRecordingTransport) Send(f ports.Frame) error {
	t.sends = append(t.sends, f)
	return nil
}

func (t *reconnectToastRecordingTransport) Recv() (ports.Frame, error) {
	if len(t.recvs) == 0 {
		return ports.Frame{}, io.EOF
	}
	recv := t.recvs[0]
	t.recvs = t.recvs[1:]
	return recv.frame, recv.err
}

func (t *reconnectToastRecordingTransport) Close() error {
	t.closed = true
	return nil
}

func reconnectToastHelloFromSend(t *testing.T, tr *reconnectToastRecordingTransport) ports.Hello {
	t.Helper()
	require.NotEmpty(t, tr.sends)
	require.Equal(t, ports.MsgHello, tr.sends[0].Type)
	hello, err := ports.UnmarshalHello(tr.sends[0].Payload)
	require.NoError(t, err)
	return hello
}

func TestReconnectToastDrawAndClearHelpers(t *testing.T) {
	var out bytes.Buffer
	size := domain.Size{Cols: 80, Rows: 24}

	require.NoError(t, drawReconnectToast(&out, size))
	require.Contains(t, out.String(), "┌")
	require.Contains(t, out.String(), reconnectToastMessage)
	require.Contains(t, out.String(), "\x1b[0m")

	require.NoError(t, clearReconnectToast(&out, size))
	bounds := reconnectToastBounds(size)
	require.Contains(t, out.String(), strings.Repeat(" ", bounds.Width))
}

func TestReconnectToastLinesClampToBounds(t *testing.T) {
	bounds := reconnectToastBounds(domain.Size{Cols: 8, Rows: 3})
	lines := reconnectToastLines(bounds)
	require.Len(t, lines, bounds.Height)
	for _, line := range lines {
		require.LessOrEqual(t, displayWidth(line), bounds.Width)
	}
	require.Contains(t, strings.Join(lines, "\n"), "…")
}

func displayWidth(s string) int {
	width := 0
	for _, r := range s {
		width += renderer.RuneWidth(r)
	}
	return width
}

func TestReconnectSleepWithResizeEventsRedrawsUntilTimerFires(t *testing.T) {
	clk := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	timerCh := make(chan time.Time, 1)
	clk.EXPECT().NewTimer(time.Hour).Return(timer).Once()
	timer.EXPECT().C().Return((<-chan time.Time)(timerCh)).Maybe()
	timer.EXPECT().Stop().Return(true).Once()
	resizeCh := make(chan domain.Size, 2)
	resizeCh <- domain.Size{Cols: 100, Rows: 30}
	resizeCh <- domain.Size{Cols: 120, Rows: 40}
	got := make(chan domain.Size, 2)
	done := make(chan bool, 1)

	go func() {
		done <- sleepReconnectWithResizeEvents(context.Background(), clk, time.Hour, resizeCh, func(size domain.Size) {
			got <- size
		})
	}()

	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, <-got)
	require.Equal(t, domain.Size{Cols: 120, Rows: 40}, <-got)
	timerCh <- time.Time{}
	require.True(t, <-done)
}

func TestRemoteReconnectToastLifecycleWithWrappedTransportError(t *testing.T) {
	linkDead := errors.New("remote link dead")
	wrappedLinkDead := errors.Join(
		fmt.Errorf("remote transport receive failed: %w", io.EOF),
		linkDead,
	)
	oldSleep := reconnectSleep
	oldSleepWithResize := reconnectSleepWithResize
	reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return true }
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Size, func(domain.Size)) bool {
		return true
	}
	defer func() {
		reconnectSleep = oldSleep
		reconnectSleepWithResize = oldSleepWithResize
	}()

	term := newReconnectToastTerminalHarness(t)
	defer term.closeInput()
	tr1 := &reconnectToastRecordingTransport{recvs: []reconnectToastRecv{
		{frame: reconnectToastWelcome(44)},
		{err: wrappedLinkDead},
	}}
	tr2 := &reconnectToastRecordingTransport{recvs: []reconnectToastRecv{
		{frame: reconnectToastWelcome(55)},
		{frame: reconnectToastDetach(ports.ReasonDetach)},
	}}
	dialer := &reconnectToastSequenceDialer{transports: []ports.Transport{tr1, tr2}}

	err := Run(context.Background(), dialer, term.term, portsmocks.NewMockClock(t), ports.IntentAttach, "main", true, nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.True(t, tr1.closed)
	require.True(t, tr2.closed)
	require.Equal(t, 2, dialer.calls)

	firstHello := reconnectToastHelloFromSend(t, tr1)
	resumeHello := reconnectToastHelloFromSend(t, tr2)
	require.Equal(t, ports.IntentAttach, firstHello.Intent)
	require.Zero(t, firstHello.ResumeToken)
	require.Equal(t, ports.IntentResume, resumeHello.Intent)
	require.Equal(t, uint64(44), resumeHello.ResumeToken)
	require.Equal(t, firstHello.ClientID, resumeHello.ClientID)

	out := term.out.String()
	require.Contains(t, out, reconnectToastMessage)
	require.Contains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
}

func TestRemoteEphemeralReconnectUsesAssignedSessionName(t *testing.T) {
	linkDead := errors.New("remote link dead")
	oldSleep := reconnectSleep
	oldSleepWithResize := reconnectSleepWithResize
	reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return true }
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Size, func(domain.Size)) bool {
		return true
	}
	defer func() {
		reconnectSleep = oldSleep
		reconnectSleepWithResize = oldSleepWithResize
	}()

	term := newReconnectToastTerminalHarness(t)
	defer term.closeInput()
	tr1 := &reconnectToastRecordingTransport{recvs: []reconnectToastRecv{
		{frame: reconnectToastWelcomeNamed("0", 44)},
		{err: linkDead},
	}}
	tr2 := &reconnectToastRecordingTransport{recvs: []reconnectToastRecv{
		{frame: reconnectToastWelcomeNamed("0", 55)},
		{frame: reconnectToastDetach(ports.ReasonDetach)},
	}}
	dialer := &reconnectToastSequenceDialer{transports: []ports.Transport{tr1, tr2}}

	err := Run(context.Background(), dialer, term.term, portsmocks.NewMockClock(t), ports.IntentEphemeral, "", true, nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	require.Equal(t, 2, dialer.calls)

	firstHello := reconnectToastHelloFromSend(t, tr1)
	resumeHello := reconnectToastHelloFromSend(t, tr2)
	require.Equal(t, ports.IntentEphemeral, firstHello.Intent)
	require.Empty(t, firstHello.Name)
	require.Zero(t, firstHello.ResumeToken)
	require.Equal(t, ports.IntentResume, resumeHello.Intent)
	require.Equal(t, "0", resumeHello.Name)
	require.Equal(t, uint64(44), resumeHello.ResumeToken)
	require.Equal(t, firstHello.ClientID, resumeHello.ClientID)
	require.Contains(t, term.out.String(), reconnectToastMessage)
}

func TestRemoteReconnectToastLifecycle(t *testing.T) {
	tests := []struct {
		name      string
		configure func(t *testing.T, dialer *portsmocks.MockDialer)
		sleep     func(cancel context.CancelFunc) bool
		wantErr   func(t *testing.T, err error)
		wantBlank bool
	}{
		{
			name: "clears on successful reconnect",
			configure: func(t *testing.T, dialer *portsmocks.MockDialer) {
				tr1 := mockReconnectTransport(t, reconnectToastRecv{frame: reconnectToastWelcome(11)}, reconnectToastRecv{err: io.EOF})
				tr2 := mockReconnectTransport(t, reconnectToastRecv{frame: reconnectToastWelcome(22)}, reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)})
				dialer.EXPECT().Dial(mock.Anything).Return(tr1, nil).Once()
				dialer.EXPECT().Dial(mock.Anything).Return(tr2, nil).Once()
			},
			sleep:     func(context.CancelFunc) bool { return true },
			wantErr:   func(t *testing.T, err error) { require.NoError(t, err) },
			wantBlank: true,
		},
		{
			name: "clears on cancellation",
			configure: func(t *testing.T, dialer *portsmocks.MockDialer) {
				tr := mockReconnectTransport(t, reconnectToastRecv{frame: reconnectToastWelcome(11)}, reconnectToastRecv{err: io.EOF})
				dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()
			},
			sleep: func(cancel context.CancelFunc) bool {
				cancel()
				return false
			},
			wantErr:   func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
			wantBlank: true,
		},
		{
			name: "clears on final exit",
			configure: func(t *testing.T, dialer *portsmocks.MockDialer) {
				tr1 := mockReconnectTransport(t, reconnectToastRecv{frame: reconnectToastWelcome(11)}, reconnectToastRecv{err: io.EOF})
				tr2 := mockReconnectTransport(t, reconnectToastRecv{frame: reconnectToastWelcome(22)}, reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonSessionKilled)})
				dialer.EXPECT().Dial(mock.Anything).Return(tr1, nil).Once()
				dialer.EXPECT().Dial(mock.Anything).Return(tr2, nil).Once()
			},
			sleep: func(context.CancelFunc) bool { return true },
			wantErr: func(t *testing.T, err error) {
				var detached *DetachedError
				require.True(t, errors.As(err, &detached))
			},
			wantBlank: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldSleep := reconnectSleep
			oldSleepWithResize := reconnectSleepWithResize
			ctx, cancel := context.WithCancel(context.Background())
			reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return tt.sleep(cancel) }
			reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Size, func(domain.Size)) bool {
				return tt.sleep(cancel)
			}
			defer func() {
				reconnectSleep = oldSleep
				reconnectSleepWithResize = oldSleepWithResize
			}()

			term := newReconnectToastTerminalHarness(t)
			defer term.closeInput()
			dialer := portsmocks.NewMockDialer(t)
			tt.configure(t, dialer)

			err := Run(ctx, dialer, term.term, portsmocks.NewMockClock(t), ports.IntentAttach, "main", true, nil, slog.New(slog.DiscardHandler))
			tt.wantErr(t, err)
			out := term.out.String()
			require.Contains(t, out, reconnectToastMessage)
			if tt.wantBlank {
				require.Contains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
			}
		})
	}
}
