package client

import (
	"bytes"
	"context"
	"errors"
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
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s1", ResumeToken: token, Capabilities: ports.CapabilityResume})}
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
