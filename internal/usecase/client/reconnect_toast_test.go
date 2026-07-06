package client

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
)

type reconnectToastClock struct {
	timer   *reconnectToastTimer
	created chan struct{}
}

func newReconnectToastClock() *reconnectToastClock {
	return &reconnectToastClock{created: make(chan struct{})}
}

func (*reconnectToastClock) Now() time.Time { return time.Time{} }
func (c *reconnectToastClock) NewTimer(time.Duration) ports.Timer {
	c.timer = &reconnectToastTimer{ch: make(chan time.Time, 1)}
	close(c.created)
	return c.timer
}

type reconnectToastTimer struct{ ch chan time.Time }

func (t *reconnectToastTimer) C() <-chan time.Time      { return t.ch }
func (t *reconnectToastTimer) Reset(time.Duration) bool { return true }
func (t *reconnectToastTimer) Stop() bool               { return true }
func (t *reconnectToastTimer) fire()                    { t.ch <- time.Time{} }

type reconnectToastDialer struct {
	trs   []ports.Transport
	calls atomic.Int32
}

func (d *reconnectToastDialer) Dial(context.Context) (ports.Transport, error) {
	i := int(d.calls.Add(1)) - 1
	if i >= len(d.trs) {
		return nil, io.EOF
	}
	return d.trs[i], nil
}

type reconnectToastTransport struct {
	recvs []reconnectToastRecv
	sends []ports.Frame
}

type reconnectToastRecv struct {
	frame ports.Frame
	err   error
}

func (t *reconnectToastTransport) Send(f ports.Frame) error {
	t.sends = append(t.sends, f)
	return nil
}

func (t *reconnectToastTransport) Recv() (ports.Frame, error) {
	if len(t.recvs) == 0 {
		return ports.Frame{}, io.EOF
	}
	it := t.recvs[0]
	t.recvs = t.recvs[1:]
	return it.frame, it.err
}

func (t *reconnectToastTransport) Close() error { return nil }

type reconnectToastTerminal struct {
	in       *blockingReconnectToastReader
	out      bytes.Buffer
	size     domain.Size
	rawCount atomic.Int32
}

func newReconnectToastTerminal() *reconnectToastTerminal {
	return &reconnectToastTerminal{in: &blockingReconnectToastReader{done: make(chan struct{})}, size: domain.Size{Cols: 80, Rows: 24}}
}

func (t *reconnectToastTerminal) EnterRaw() (func() error, error) {
	t.rawCount.Add(1)
	return func() error { return nil }, nil
}
func (t *reconnectToastTerminal) Size() (domain.Size, error)       { return t.size, nil }
func (t *reconnectToastTerminal) ResizeEvents() <-chan domain.Size { return make(chan domain.Size) }
func (t *reconnectToastTerminal) QueryColors() error               { return nil }
func (t *reconnectToastTerminal) In() io.Reader                    { return t.in }
func (t *reconnectToastTerminal) Out() io.Writer                   { return &t.out }
func (t *reconnectToastTerminal) Flush() error                     { return nil }

type blockingReconnectToastReader struct{ done chan struct{} }

func (r *blockingReconnectToastReader) Read([]byte) (int, error) {
	<-r.done
	return 0, io.EOF
}
func (r *blockingReconnectToastReader) unblock() { close(r.done) }

func reconnectToastWelcome(token uint64) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s1", ResumeToken: token, Capabilities: ports.CapabilityResume})}
}

func reconnectToastDetach(reason uint8) ports.Frame {
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: reason})}
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
	clk := newReconnectToastClock()
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

	<-clk.created
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, <-got)
	require.Equal(t, domain.Size{Cols: 120, Rows: 40}, <-got)
	clk.timer.fire()
	require.True(t, <-done)
}

func TestRemoteReconnectToastClearsOnSuccessfulReconnect(t *testing.T) {
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

	term := newReconnectToastTerminal()
	defer term.in.unblock()
	tr1 := &reconnectToastTransport{recvs: []reconnectToastRecv{{frame: reconnectToastWelcome(11)}, {err: io.EOF}}}
	tr2 := &reconnectToastTransport{recvs: []reconnectToastRecv{{frame: reconnectToastWelcome(22)}, {frame: reconnectToastDetach(ports.ReasonDetach)}}}
	dialer := &reconnectToastDialer{trs: []ports.Transport{tr1, tr2}}

	err := Run(context.Background(), dialer, term, newReconnectToastClock(), ports.IntentAttach, "main", true, nil, slog.New(slog.DiscardHandler))
	require.NoError(t, err)
	out := term.out.String()
	require.Contains(t, out, reconnectToastMessage)
	require.Contains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
}

func TestRemoteReconnectToastClearsOnCancellation(t *testing.T) {
	oldSleep := reconnectSleep
	oldSleepWithResize := reconnectSleepWithResize
	ctx, cancel := context.WithCancel(context.Background())
	reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool {
		cancel()
		return false
	}
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Size, func(domain.Size)) bool {
		cancel()
		return false
	}
	defer func() {
		reconnectSleep = oldSleep
		reconnectSleepWithResize = oldSleepWithResize
	}()

	term := newReconnectToastTerminal()
	defer term.in.unblock()
	tr := &reconnectToastTransport{recvs: []reconnectToastRecv{{frame: reconnectToastWelcome(11)}, {err: io.EOF}}}
	dialer := &reconnectToastDialer{trs: []ports.Transport{tr}}

	err := Run(ctx, dialer, term, newReconnectToastClock(), ports.IntentAttach, "main", true, nil, slog.New(slog.DiscardHandler))
	require.ErrorIs(t, err, context.Canceled)
	out := term.out.String()
	require.Contains(t, out, reconnectToastMessage)
	require.Contains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
}

func TestRemoteReconnectToastClearsOnFinalExit(t *testing.T) {
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

	term := newReconnectToastTerminal()
	defer term.in.unblock()
	tr1 := &reconnectToastTransport{recvs: []reconnectToastRecv{{frame: reconnectToastWelcome(11)}, {err: io.EOF}}}
	tr2 := &reconnectToastTransport{recvs: []reconnectToastRecv{{frame: reconnectToastWelcome(22)}, {frame: reconnectToastDetach(ports.ReasonSessionKilled)}}}
	dialer := &reconnectToastDialer{trs: []ports.Transport{tr1, tr2}}

	err := Run(context.Background(), dialer, term, newReconnectToastClock(), ports.IntentAttach, "main", true, nil, slog.New(slog.DiscardHandler))
	var detached *DetachedError
	require.True(t, errors.As(err, &detached))
	out := term.out.String()
	require.Contains(t, out, reconnectToastMessage)
	require.Contains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
}
