package client

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
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
	h.term.EXPECT().In().Return(h.in).Maybe()
	h.term.EXPECT().Out().Return(&h.out).Maybe()
	h.term.EXPECT().Flush().Return(nil).Maybe()
	return h
}

func newReconnectToastTerminalHarnessWithOutput(t *testing.T, out io.Writer) *reconnectToastTerminalHarness {
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
	h.term.EXPECT().In().Return(h.in).Maybe()
	h.term.EXPECT().Out().Return(out).Maybe()
	h.term.EXPECT().Flush().Return(nil).Maybe()
	return h
}

func (h *reconnectToastTerminalHarness) closeInput() { _ = h.inWriter.Close() }

type reconnectToastOutputRecorder struct {
	mu        sync.Mutex
	pending   strings.Builder
	completed chan string
}

func newReconnectToastOutputRecorder() *reconnectToastOutputRecorder {
	return &reconnectToastOutputRecorder{completed: make(chan string, 3)}
}

func (r *reconnectToastOutputRecorder) Write(p []byte) (int, error) {
	const restoreCursor = "\x1b[0m\x1b[u"

	r.mu.Lock()
	r.pending.Write(p)
	var output string
	if string(p) == restoreCursor {
		output = r.pending.String()
		r.pending.Reset()
	}
	r.mu.Unlock()

	if output != "" {
		r.completed <- output
	}
	return len(p), nil
}

func reconnectToastWelcome(token uint64) ports.Frame {
	return reconnectToastWelcomeNamed("", token)
}

func reconnectToastWelcomeNamed(name string, token uint64) ports.Frame {
	return ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{SessionID: "s1", SessionName: name, ResumeToken: token, Capabilities: ports.CapabilityResume})}
}

func reconnectToastDetach(reason uint8) ports.Frame {
	return ports.Frame{Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: reason})}
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
	mu     sync.Mutex
	recvs  []reconnectToastRecv
	sends  []ports.Frame
	closed bool
}

func newReconnectToastRecordingTransport(recvs ...reconnectToastRecv) *reconnectToastRecordingTransport {
	return &reconnectToastRecordingTransport{recvs: append([]reconnectToastRecv(nil), recvs...)}
}

type reconnectToastLinkTransport struct {
	recvCh chan reconnectToastRecv
	sends  []ports.Frame
	events chan ports.LinkEvent
	state  ports.LinkState
}

func newReconnectToastLinkTransport() *reconnectToastLinkTransport {
	return &reconnectToastLinkTransport{
		recvCh: make(chan reconnectToastRecv, 4),
		events: make(chan ports.LinkEvent, 4),
		state:  ports.LinkStateConnected,
	}
}

func (t *reconnectToastLinkTransport) Send(f ports.Frame) error {
	t.sends = append(t.sends, f)
	return nil
}

func (t *reconnectToastLinkTransport) Recv() (ports.Frame, error) {
	recv := <-t.recvCh
	return recv.frame, recv.err
}

func (t *reconnectToastLinkTransport) Close() error { return nil }

func (t *reconnectToastLinkTransport) LinkEvents() <-chan ports.LinkEvent { return t.events }

func (t *reconnectToastLinkTransport) LinkState() ports.LinkState { return t.state }

func (t *reconnectToastRecordingTransport) Send(f ports.Frame) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sends = append(t.sends, f)
	return nil
}

func (t *reconnectToastRecordingTransport) Recv() (ports.Frame, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recvs) == 0 {
		return ports.Frame{}, io.EOF
	}
	recv := t.recvs[0]
	t.recvs = t.recvs[1:]
	return recv.frame, recv.err
}

func (t *reconnectToastRecordingTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	return nil
}

func (t *reconnectToastRecordingTransport) sentFrames() []ports.Frame {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]ports.Frame(nil), t.sends...)
}

func (t *reconnectToastRecordingTransport) wasClosed() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.closed
}

// assertReconnectToastAttemptPublishesOnlyClearedTheme verifies the precise
// handshake publication for these no-OSC-response reconnect paths. A final
// Theme would be wrong here: no terminal palette response was supplied.
func assertReconnectToastAttemptPublishesOnlyClearedTheme(t *testing.T, tr *reconnectToastRecordingTransport) {
	t.Helper()
	frames := tr.sentFrames()
	require.Len(t, frames, 2)
	require.Equal(t, ports.MsgHello, frames[0].Type)
	require.Equal(t, ports.MsgTheme, frames[1].Type)
	theme, err := ports.UnmarshalTheme(frames[1].Payload)
	require.NoError(t, err)
	require.False(t, theme.HasForeground)
	require.False(t, theme.HasBackground)
	require.Zero(t, theme.PaletteKnown)
	require.True(t, tr.wasClosed())
}

func reconnectToastHelloFromSend(t *testing.T, tr *reconnectToastRecordingTransport) ports.Hello {
	t.Helper()
	frames := tr.sentFrames()
	require.NotEmpty(t, frames)
	require.Equal(t, ports.MsgHello, frames[0].Type)
	hello, err := ports.UnmarshalHello(frames[0].Payload)
	require.NoError(t, err)
	return hello
}

// newReconnectHandshakeClock supplies the non-firing timer needed by the
// bounded pre-Welcome handshake; reconnect sleeps are stubbed by these tests.
func newReconnectHandshakeClock(t *testing.T) *portsmocks.MockClock {
	t.Helper()
	clk := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return((<-chan time.Time)(make(chan time.Time))).Maybe()
	timer.EXPECT().Stop().Return(true).Maybe()
	clk.EXPECT().NewTimer(preWelcomeTimeout).Return(timer).Maybe()
	// Every successful attach starts an initial palette-generation deadline.
	clk.EXPECT().NewTimer(paletteGenerationDeadline).Return(timer).Maybe()
	return clk
}

func newReconnectAttachAttempt(term ports.Terminal, transport ports.Transport, clock ports.Clock, request AttachRequest, resumeToken uint64, state *terminalThemeState, linkEvents <-chan ports.LinkEvent, ms *milestones) *attachAttempt {
	rawEntered := false
	enterRaw := func() error {
		if rawEntered {
			return nil
		}
		_, err := term.EnterRaw()
		if err == nil {
			rawEntered = true
			ms.rawEntered = true
		}
		return err
	}
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler)}
	return &attachAttempt{
		runner:      runner,
		transport:   transport,
		request:     request,
		resumeToken: resumeToken,
		clientID:    [16]byte{1},
		milestones:  ms,
		themeState:  state,
		enterRaw:    enterRaw,
		reconnect: &reconnectUI{
			term:       term,
			remote:     request.Remote,
			rawEntered: &rawEntered,
			stage:      reconnectStageOfflineRetrying,
		},
		linkEvents: linkEvents,
	}
}

func TestReconnectToastDegradedClearsModalAndProbingIsVisible(t *testing.T) {
	out := newReconnectToastOutputRecorder()
	term := newReconnectToastTerminalHarnessWithOutput(t, out)
	defer term.closeInput()
	tr := newReconnectToastLinkTransport()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
	probed := <-out.completed
	require.Contains(t, probed, reconnectStageMessage(reconnectStageProbingUDP))

	tr.events <- ports.LinkEvent{State: ports.LinkStateDegraded}
	cleared := <-out.completed
	assertReconnectToastClearCoversBounds(t, cleared, reconnectToastBoundsFor(term.size, reconnectStageMessage(reconnectStageProbingUDP)))

	tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
	probed = <-out.completed
	require.Contains(t, probed, reconnectStageMessage(reconnectStageProbingUDP))

	tr.recvCh <- reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)}
	result := <-resultCh
	require.NoError(t, result.err)
	require.True(t, result.welcomed)
}

func TestAttachAttemptOfflineLinkEventReturnsReconnectableError(t *testing.T) {
	term := newReconnectToastTerminalHarness(t)
	defer term.closeInput()
	tr := newReconnectToastLinkTransport()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.events <- ports.LinkEvent{State: ports.LinkStateOffline}
	result := <-resultCh
	require.ErrorIs(t, result.err, errLinkOffline)
	require.True(t, result.welcomed)
	require.True(t, shouldReconnect(result.err), "offline exit must be reconnectable")
}

func TestReconnectToastBoundsUseCenterAnchor(t *testing.T) {
	bounds := reconnectToastBounds(domain.Size{Cols: 80, Rows: 24})
	require.Equal(t, domain.Rect{X: 29, Y: 10, Width: 22, Height: 3}, bounds)
}

func TestReconnectToastDrawAndClearHelpers(t *testing.T) {
	var out bytes.Buffer
	size := domain.Size{Cols: 80, Rows: 24}

	require.NoError(t, drawReconnectToast(&out, size))
	require.Contains(t, out.String(), "┌")
	require.Contains(t, out.String(), reconnectToastMessage)
	require.Contains(t, out.String(), "\x1b[0m")

	bounds := reconnectToastBounds(size)
	require.NoError(t, clearReconnectToast(&out, bounds))
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

func TestReconnectToastStageTransitionsClearDrawnBounds(t *testing.T) {
	size := domain.Size{Cols: 80, Rows: 24}
	tests := []struct {
		name   string
		stages []reconnectStage
	}{
		{
			name:   "degraded to probing to ssh to offline",
			stages: []reconnectStage{reconnectStageDegraded, reconnectStageProbingUDP, reconnectStageSSH, reconnectStageOfflineRetrying},
		},
		{
			name:   "widest ssh to offline",
			stages: []reconnectStage{reconnectStageSSH, reconnectStageOfflineRetrying},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var out bytes.Buffer
			previousBounds, err := drawReconnectToastStage(&out, size, tt.stages[0])
			require.NoError(t, err)

			for _, stage := range tt.stages[1:] {
				out.Reset()
				require.NoError(t, clearReconnectToast(&out, previousBounds))
				assertReconnectToastClearCoversBounds(t, out.String(), previousBounds)

				out.Reset()
				previousBounds, err = drawReconnectToastStage(&out, size, stage)
				require.NoError(t, err)
			}
		})
	}
}

func TestReconnectToastClearAfterWidestStageErasesFullBorder(t *testing.T) {
	size := domain.Size{Cols: 80, Rows: 24}
	var out bytes.Buffer
	bounds, err := drawReconnectToastStage(&out, size, reconnectStageSSH)
	require.NoError(t, err)
	require.Greater(t, bounds.Width, reconnectToastBounds(size).Width)

	out.Reset()
	require.NoError(t, clearReconnectToast(&out, bounds))
	assertReconnectToastClearCoversBounds(t, out.String(), bounds)
}

func assertReconnectToastClearCoversBounds(t *testing.T, output string, bounds domain.Rect) {
	t.Helper()
	blank := strings.Repeat(" ", bounds.Width)
	for row := range bounds.Height {
		require.Contains(t, output, fmt.Sprintf("\x1b[%d;%dH\x1b[0m%s", bounds.Y+row+1, bounds.X+1, blank))
	}
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

type reconnectToastFailOnceWriter struct {
	bytes.Buffer
	needle string
	failed bool
}

func (w *reconnectToastFailOnceWriter) Write(p []byte) (int, error) {
	n, _ := w.Buffer.Write(p)
	if !w.failed && strings.Contains(string(p), w.needle) {
		w.failed = true
		return n, errors.New("reconnect toast write failed")
	}
	return n, nil
}

func TestRemoteReconnectToastClearsFailedDrawBounds(t *testing.T) {
	oldSleep := reconnectSleep
	oldSleepWithResize := reconnectSleepWithResize
	ctx, cancel := context.WithCancel(context.Background())
	reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return false }
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Size, func(domain.Size)) bool {
		cancel()
		return false
	}
	defer func() {
		reconnectSleep = oldSleep
		reconnectSleepWithResize = oldSleepWithResize
	}()

	out := &reconnectToastFailOnceWriter{needle: "reconnecting through SSH"}
	term := newReconnectToastTerminalHarnessWithOutput(t, out)
	defer term.closeInput()
	tr := &reconnectToastRecordingTransport{recvs: []reconnectToastRecv{
		{frame: reconnectToastWelcome(44)},
		{err: io.EOF},
	}}
	dialer := &reconnectToastSequenceDialer{transports: []ports.Transport{tr}}

	err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(ctx, AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, out.failed)
	assertReconnectToastClearCoversBounds(t, out.String(), reconnectToastBoundsFor(term.size, reconnectStageMessage(reconnectStageSSH)))
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

	err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(context.Background(), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true})
	require.NoError(t, err)
	require.True(t, tr1.wasClosed())
	require.True(t, tr2.wasClosed())
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

	err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(context.Background(), AttachRequest{Intent: ports.IntentEphemeral, SessionName: "", Remote: true})
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
		configure func(t *testing.T, dialer *portsmocks.MockDialer) []*reconnectToastRecordingTransport
		sleep     func(cancel context.CancelFunc) bool
		wantErr   func(t *testing.T, err error)
		wantBlank bool
	}{
		{
			name: "clears on successful reconnect",
			configure: func(t *testing.T, dialer *portsmocks.MockDialer) []*reconnectToastRecordingTransport {
				tr1 := newReconnectToastRecordingTransport(reconnectToastRecv{frame: reconnectToastWelcome(11)}, reconnectToastRecv{err: io.EOF})
				tr2 := newReconnectToastRecordingTransport(reconnectToastRecv{frame: reconnectToastWelcome(22)}, reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)})
				dialer.EXPECT().Dial(mock.Anything).Return(tr1, nil).Once()
				dialer.EXPECT().Dial(mock.Anything).Return(tr2, nil).Once()
				return []*reconnectToastRecordingTransport{tr1, tr2}
			},
			sleep:     func(context.CancelFunc) bool { return true },
			wantErr:   func(t *testing.T, err error) { require.NoError(t, err) },
			wantBlank: true,
		},
		{
			name: "clears on cancellation",
			configure: func(t *testing.T, dialer *portsmocks.MockDialer) []*reconnectToastRecordingTransport {
				tr := newReconnectToastRecordingTransport(reconnectToastRecv{frame: reconnectToastWelcome(11)}, reconnectToastRecv{err: io.EOF})
				dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()
				return []*reconnectToastRecordingTransport{tr}
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
			configure: func(t *testing.T, dialer *portsmocks.MockDialer) []*reconnectToastRecordingTransport {
				tr1 := newReconnectToastRecordingTransport(reconnectToastRecv{frame: reconnectToastWelcome(11)}, reconnectToastRecv{err: io.EOF})
				tr2 := newReconnectToastRecordingTransport(reconnectToastRecv{frame: reconnectToastWelcome(22)}, reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonSessionKilled)})
				dialer.EXPECT().Dial(mock.Anything).Return(tr1, nil).Once()
				dialer.EXPECT().Dial(mock.Anything).Return(tr2, nil).Once()
				return []*reconnectToastRecordingTransport{tr1, tr2}
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
			transports := tt.configure(t, dialer)

			err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(ctx, AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true})
			tt.wantErr(t, err)
			for _, transport := range transports {
				assertReconnectToastAttemptPublishesOnlyClearedTheme(t, transport)
			}
			out := term.out.String()
			require.Contains(t, out, reconnectToastMessage)
			if tt.wantBlank {
				require.Contains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
			}
		})
	}
}
