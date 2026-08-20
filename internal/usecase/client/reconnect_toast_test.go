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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

type reconnectToastTerminalHarness struct {
	term       *portsmocks.MockTerminal
	in         *io.PipeReader
	inWriter   *io.PipeWriter
	out        bytes.Buffer
	size       domain.Size
	resizeCh   chan domain.Geometry
	sizeCalls  atomic.Int32
	sizeCalled chan int32
}

func newReconnectToastTerminalHarness(t *testing.T) *reconnectToastTerminalHarness {
	t.Helper()
	in, inWriter := io.Pipe()
	h := &reconnectToastTerminalHarness{
		term:       portsmocks.NewMockTerminal(t),
		in:         in,
		inWriter:   inWriter,
		size:       domain.Size{Cols: 80, Rows: 24},
		resizeCh:   make(chan domain.Geometry, sendQueueDepth),
		sizeCalled: make(chan int32, 16),
	}
	h.term.EXPECT().EnterRaw().Return(func() error { return nil }, nil).Maybe()
	h.term.EXPECT().Geometry().Run(func() {
		call := h.sizeCalls.Add(1)
		select {
		case h.sizeCalled <- call:
		default:
		}
	}).Return(domain.Geometry{Size: h.size}, nil).Maybe()
	h.term.EXPECT().ResizeEvents().Return((<-chan domain.Geometry)(h.resizeCh)).Maybe()
	h.term.EXPECT().In().Return(h.in).Maybe()
	h.term.EXPECT().Out().Return(&h.out).Maybe()
	h.term.EXPECT().Flush().Return(nil).Maybe()
	return h
}

func newReconnectToastTerminalHarnessWithOutput(t *testing.T, out io.Writer) *reconnectToastTerminalHarness {
	t.Helper()
	return newReconnectToastTerminalHarnessWithOutputAndFlush(t, out, func() error { return nil })
}

func newReconnectToastTerminalHarnessWithOutputAndFlush(t *testing.T, out io.Writer, flush func() error) *reconnectToastTerminalHarness {
	t.Helper()
	in, inWriter := io.Pipe()
	h := &reconnectToastTerminalHarness{
		term:       portsmocks.NewMockTerminal(t),
		in:         in,
		inWriter:   inWriter,
		size:       domain.Size{Cols: 80, Rows: 24},
		resizeCh:   make(chan domain.Geometry, sendQueueDepth),
		sizeCalled: make(chan int32, 16),
	}
	h.term.EXPECT().EnterRaw().Return(func() error { return nil }, nil).Maybe()
	h.term.EXPECT().Geometry().Run(func() {
		call := h.sizeCalls.Add(1)
		select {
		case h.sizeCalled <- call:
		default:
		}
	}).Return(domain.Geometry{Size: h.size}, nil).Maybe()
	h.term.EXPECT().ResizeEvents().Return((<-chan domain.Geometry)(h.resizeCh)).Maybe()
	h.term.EXPECT().In().Return(h.in).Maybe()
	h.term.EXPECT().Out().Return(out).Maybe()
	h.term.EXPECT().Flush().RunAndReturn(flush).Maybe()
	return h
}

func (h *reconnectToastTerminalHarness) closeInput() { _ = h.inWriter.Close() }

type reconnectToastOutputRecorder struct {
	mu        sync.Mutex
	pending   strings.Builder
	all       strings.Builder
	completed chan string
}

func newReconnectToastOutputRecorder() *reconnectToastOutputRecorder {
	return &reconnectToastOutputRecorder{completed: make(chan string, 3)}
}

func (r *reconnectToastOutputRecorder) Write(p []byte) (int, error) {
	const restoreCursor = "\x1b[0m\x1b[u"

	r.mu.Lock()
	r.pending.Write(p)
	r.all.Write(p)
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

func (r *reconnectToastOutputRecorder) String() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.all.String()
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

type reconnectToastSentFrames struct {
	mu      sync.Mutex
	frames  []ports.Frame
	changed chan struct{}
}

func newReconnectToastSentFrames() *reconnectToastSentFrames {
	return &reconnectToastSentFrames{changed: make(chan struct{}, 1)}
}

func (s *reconnectToastSentFrames) record(frame ports.Frame) {
	s.mu.Lock()
	s.frames = append(s.frames, frame)
	s.mu.Unlock()
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func (s *reconnectToastSentFrames) find(match func(ports.Frame) bool) (ports.Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, frame := range s.frames {
		if match(frame) {
			return frame, true
		}
	}
	return ports.Frame{}, false
}

type reconnectToastLinkTransport struct {
	recvCh        chan reconnectToastRecv
	sends         *reconnectToastSentFrames
	clientNotices chan ports.ClientNotice
	events        chan ports.LinkEvent
	state         ports.LinkState
}

func newReconnectToastLinkTransport() *reconnectToastLinkTransport {
	return &reconnectToastLinkTransport{
		recvCh:        make(chan reconnectToastRecv, 4),
		sends:         newReconnectToastSentFrames(),
		clientNotices: make(chan ports.ClientNotice, 2),
		events:        make(chan ports.LinkEvent, 4),
		state:         ports.LinkStateConnected,
	}
}

func (t *reconnectToastLinkTransport) Send(f ports.Frame) error {
	t.sends.record(f)
	if f.Type != ports.MsgClientNotice {
		return nil
	}
	notice, err := ports.UnmarshalClientNotice(f.Payload)
	if err != nil {
		return err
	}
	t.clientNotices <- notice
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
	require.GreaterOrEqual(t, len(frames), 2)
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

func TestResumeNeedsExactAttach(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		want bool
	}{
		{name: "moved resume token", err: errRouteTargetChanged, want: true},
		{name: "missing target", err: &ProtocolError{Code: ports.ErrNoSuchTarget}, want: true},
		{name: "missing session", err: &ProtocolError{Code: ports.ErrNoSuchSession}, want: true},
		{name: "internal", err: &ProtocolError{Code: ports.ErrInternal}},
		{name: "transport", err: io.EOF},
	} {
		t.Run(test.name, func(t *testing.T) {
			require.Equal(t, test.want, resumeNeedsExactAttach(test.err))
		})
	}
}

func TestProbingToastReconcilesDaemonOutputAndDismissal(t *testing.T) {
	out := newReconnectToastOutputRecorder()
	var flushes atomic.Int32
	term := newReconnectToastTerminalHarnessWithOutputAndFlush(t, out, func() error {
		flushes.Add(1)
		return nil
	})
	defer term.closeInput()
	tr := newReconnectToastLinkTransport()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
	firstToast := requireReconnectToastOutput(t, out.completed)
	require.Contains(t, firstToast, "┌")
	require.Contains(t, firstToast, "probing UDP path")

	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 1,
		Base:  0,
		New:   2,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Full:  true,
		Data:  []byte("daemon incremental"),
	})}}
	redrawn := requireReconnectToastOutput(t, out.completed)
	require.Contains(t, redrawn, "daemon incremental")
	require.Contains(t, redrawn, "┌")
	require.Contains(t, redrawn, "probing UDP path")

	tr.events <- ports.LinkEvent{State: ports.LinkStateConnected}
	require.Equal(t, ports.ClientNoticeLinkConnected, (<-tr.clientNotices).Action)
	require.Equal(t, term.size, requireResize(t, tr.sends).Size)

	beforeAwaitingReset := out.String()
	beforeStatelessFlushes := flushes.Load()
	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 1,
		Base:  0,
		New:   0,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Data:  []byte("stateless side effect"),
	})}}
	require.Eventually(t, func() bool {
		return strings.Contains(out.String(), "stateless side effect") && flushes.Load() > beforeStatelessFlushes
	}, time.Second, time.Millisecond)

	afterStateless := out.String()
	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 1,
		Base:  2,
		New:   3,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Data:  []byte("intervening incremental"),
	})}}
	requireAckedState(t, tr.sends, 1, 3)
	require.Contains(t, out.String(), "intervening incremental")
	require.Contains(t, afterStateless[len(beforeAwaitingReset):], "stateless side effect")

	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 2,
		Base:  0,
		New:   4,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Full:  true,
		Data:  []byte("authoritative reset"),
	})}}
	requireAckedState(t, tr.sends, 2, 4)
	require.Eventually(t, func() bool { return strings.Contains(out.String(), "authoritative reset") }, time.Second, time.Millisecond)
	require.NotContains(t, out.String()[len(beforeAwaitingReset):], strings.Repeat(" ", reconnectToastBoundsFor(term.size, "probing UDP path").Width))

	beforeIncrementFlushes := flushes.Load()
	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 2,
		Base:  4,
		New:   5,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Data:  []byte("increment after reset"),
	})}}
	require.Eventually(t, func() bool {
		return strings.Contains(out.String(), "increment after reset") && flushes.Load() > beforeIncrementFlushes
	}, time.Second, time.Millisecond)
	requireAckedState(t, tr.sends, 2, 5)

	tr.recvCh <- reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)}
	result := requireAttachResult(t, resultCh)
	require.NoError(t, result.err)
}

func TestActiveReconnectToastStageTransitionReconcilesBeforeRedraw(t *testing.T) {
	out := newReconnectToastOutputRecorder()
	var flushes atomic.Int32
	term := newReconnectToastTerminalHarnessWithOutputAndFlush(t, out, func() error {
		flushes.Add(1)
		return nil
	})
	defer term.closeInput()
	tr := newReconnectToastLinkTransport()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
	probingToast := requireReconnectToastOutput(t, out.completed)
	require.Contains(t, probingToast, "probing UDP path")

	tr.events <- ports.LinkEvent{State: ports.LinkStateDead}
	require.Equal(t, term.size, requireResize(t, tr.sends).Size)
	offlineMessage := reconnectStageMessage(reconnectStageOfflineRetrying)
	require.NotContains(t, out.String(), offlineMessage)

	beforeReset := out.String()
	for _, output := range []ports.Output{
		{Epoch: 1, Base: 1, New: 2, Size: domain.Size{Cols: 1, Rows: 1}, Data: []byte("first skipped increment")},
		{Epoch: 1, Base: 2, New: 3, Size: domain.Size{Cols: 1, Rows: 1}, Data: []byte("second skipped increment")},
	} {
		tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(output)}}
		require.Equal(t, beforeReset, out.String())
	}
	requireSentFrame(t, tr.sends, "output reset request", func(frame ports.Frame) bool {
		return frame.Type == ports.MsgOutputResetRequest
	})
	_, sentAck := tr.sends.find(func(frame ports.Frame) bool { return frame.Type == ports.MsgAck })
	require.False(t, sentAck, "discarded output must not be ACKed")

	// Handoff cleanup is an ordered terminal side effect, not a replay state.
	// It must cross the outstanding reset gate, flush before the control handoff,
	// and must not manufacture an independent ACK.
	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 2,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Data:  []byte("handoff graphics cleanup"),
	})}}
	cleanup := requireReconnectToastOutput(t, out.completed)
	require.Contains(t, cleanup, "handoff graphics cleanup")
	_, sentAck = tr.sends.find(func(frame ports.Frame) bool { return frame.Type == ports.MsgAck })
	require.False(t, sentAck, "state-independent cleanup must not be ACKed")

	beforeResetFlushes := flushes.Load()
	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 2,
		Base:  0,
		New:   4,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Full:  true,
		Data:  []byte("stage transition reset"),
	})}}
	redrawn := requireReconnectToastOutput(t, out.completed)
	require.Contains(t, redrawn, "stage transition reset")
	require.Contains(t, redrawn, offlineMessage)
	require.Eventually(t, func() bool { return flushes.Load() > beforeResetFlushes }, time.Second, time.Millisecond)
	requireAckedState(t, tr.sends, 2, 4)

	beforeIncrementFlushes := flushes.Load()
	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 2,
		Base:  4,
		New:   5,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Data:  []byte("stage increment after reset"),
	})}}
	incremental := requireReconnectToastOutput(t, out.completed)
	require.Contains(t, incremental, "stage increment after reset")
	require.Contains(t, incremental, offlineMessage)
	require.Eventually(t, func() bool { return flushes.Load() > beforeIncrementFlushes }, time.Second, time.Millisecond)
	requireAckedState(t, tr.sends, 2, 5)

	tr.recvCh <- reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)}
	result := requireAttachResult(t, resultCh)
	require.NoError(t, result.err)
}

func requireReconnectToastOutput(t *testing.T, completed <-chan string) string {
	t.Helper()
	select {
	case output := <-completed:
		return output
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for reconnect toast terminal output")
		return ""
	}
}

func requireSentFrame(t *testing.T, sends *reconnectToastSentFrames, description string, match func(ports.Frame) bool) ports.Frame {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for {
		if frame, ok := sends.find(match); ok {
			return frame
		}
		select {
		case <-sends.changed:
		case <-timer.C:
			t.Fatalf("timed out waiting for %s", description)
			return ports.Frame{}
		}
	}
}

func requireResize(t *testing.T, sends *reconnectToastSentFrames) ports.Resize {
	t.Helper()
	frame := requireSentFrame(t, sends, "Resize frame", func(frame ports.Frame) bool {
		return frame.Type == ports.MsgResize
	})
	resize, err := ports.UnmarshalResize(frame.Payload)
	require.NoError(t, err)
	return resize
}

func requireAckedState(t *testing.T, sends *reconnectToastSentFrames, epoch, state uint64) {
	t.Helper()
	frame := requireSentFrame(t, sends, fmt.Sprintf("ACK for epoch %d state %d", epoch, state), func(frame ports.Frame) bool {
		if frame.Type != ports.MsgAck {
			return false
		}
		ack, err := ports.UnmarshalAck(frame.Payload)
		return err == nil && ack.Epoch == epoch && ack.State >= state
	})
	ack, err := ports.UnmarshalAck(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, epoch, ack.Epoch)
	require.GreaterOrEqual(t, ack.State, state)
}

func requireAttachResult(t *testing.T, results <-chan attachResult) attachResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for attach attempt result")
		return attachResult{}
	}
}

type armedReconnectToastWriter struct {
	next    io.Writer
	needle  string
	armed   atomic.Bool
	failure error
}

func (w *armedReconnectToastWriter) Write(p []byte) (int, error) {
	if w.armed.Load() && strings.Contains(string(p), w.needle) {
		return 0, w.failure
	}
	return w.next.Write(p)
}

type reconnectRuntimeObserver struct {
	mu    sync.Mutex
	marks []ports.RuntimeMark
}

func (o *reconnectRuntimeObserver) ObserveRuntime(mark ports.RuntimeMark) {
	o.mu.Lock()
	o.marks = append(o.marks, mark)
	o.mu.Unlock()
}

func (o *reconnectRuntimeObserver) Flush() {}
func (o *reconnectRuntimeObserver) Close() {}

func (o *reconnectRuntimeObserver) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.marks)
}

type reconnectFlushObserver struct {
	flushes *atomic.Int32
	seen    chan int32
}

func (o *reconnectFlushObserver) ObserveRuntime(mark ports.RuntimeMark) {
	if mark.Kind != ports.RuntimeTerminalFlushed {
		return
	}
	select {
	case o.seen <- o.flushes.Load():
	default:
	}
}
func (o *reconnectFlushObserver) Flush() {}
func (o *reconnectFlushObserver) Close() {}

type reconnectStatusSignalWriter struct {
	written chan struct{}
	once    sync.Once
}

func (w *reconnectStatusSignalWriter) Write(p []byte) (int, error) {
	if strings.Contains(string(p), statusReconnect) {
		w.once.Do(func() { close(w.written) })
	}
	return len(p), nil
}

func TestLocalReconnectStatusFlushesDaemonOutputBeforeObservationAndAck(t *testing.T) {
	var flushes atomic.Int32
	writer := &reconnectStatusSignalWriter{written: make(chan struct{})}
	term := newReconnectToastTerminalHarnessWithOutputAndFlush(t, writer, func() error {
		flushes.Add(1)
		return nil
	})
	defer term.closeInput()
	tr := newReconnectToastLinkTransport()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}
	observer := &reconnectFlushObserver{flushes: &flushes, seen: make(chan int32, 1)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main"}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	attempt.runner.runtimeObserver = observer
	go func() { resultCh <- attempt.run(context.Background()) }()

	beforeStatus := flushes.Load()
	tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
	select {
	case <-writer.written:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for local reconnect status write")
	}
	require.Eventually(t, func() bool { return flushes.Load() > beforeStatus }, time.Second, time.Millisecond)
	beforeOutput := flushes.Load()

	tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
		Epoch: 1,
		Base:  0,
		New:   2,
		Size:  domain.Size{Cols: 1, Rows: 1},
		Full:  true,
		Data:  []byte("daemon output behind local reconnect status"),
	})}}

	select {
	case observedAtFlush := <-observer.seen:
		require.Greater(t, observedAtFlush, beforeOutput, "terminal output must be flushed before publishing RuntimeTerminalFlushed")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for terminal-flushed observation")
	}
	requireAckedState(t, tr.sends, 1, 2)
	require.Greater(t, flushes.Load(), beforeOutput, "daemon output must be flushed before ACK")

	tr.recvCh <- reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)}
	require.NoError(t, requireAttachResult(t, resultCh).err)
}

func TestLocalReconnectStatusTransitionsDoNotRequestAuthoritativeReset(t *testing.T) {
	tests := []struct {
		name       string
		transition ports.LinkState
	}{
		{name: "dismissal", transition: ports.LinkStateConnected},
		{name: "stage change", transition: ports.LinkStateDead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := newReconnectToastOutputRecorder()
			term := newReconnectToastTerminalHarnessWithOutput(t, out)
			defer term.closeInput()
			tr := newReconnectToastLinkTransport()
			tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

			resultCh := make(chan attachResult, 1)
			ms := milestones{}
			attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main"}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
			go func() { resultCh <- attempt.run(context.Background()) }()

			tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
			require.Eventually(t, func() bool { return strings.Contains(out.String(), statusReconnect) }, time.Second, time.Millisecond)
			tr.events <- ports.LinkEvent{State: tt.transition}
			if tt.transition == ports.LinkStateConnected {
				require.Equal(t, ports.ClientNoticeLinkConnected, (<-tr.clientNotices).Action)
			}

			const output = "local status output remains visible"
			tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
				Epoch: 1,
				Base:  0,
				New:   2,
				Size:  domain.Size{Cols: 1, Rows: 1},
				Full:  true,
				Data:  []byte(output),
			})}}
			require.Eventually(t, func() bool { return strings.Contains(out.String(), output) }, time.Second, time.Millisecond)
			requireAckedState(t, tr.sends, 1, 2)
			_, sentResize := tr.sends.find(func(frame ports.Frame) bool { return frame.Type == ports.MsgResize })
			require.False(t, sentResize, "local reconnect status must not request a daemon reset")

			tr.recvCh <- reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)}
			require.NoError(t, requireAttachResult(t, resultCh).err)
		})
	}
}

func TestReconnectOverlayRedrawFailureDoesNotObserveOrAckOutput(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*testing.T, *reconnectToastOutputRecorder) (*reconnectToastTerminalHarness, func())
		wantError string
	}{
		{
			name: "write",
			configure: func(t *testing.T, out *reconnectToastOutputRecorder) (*reconnectToastTerminalHarness, func()) {
				failure := errors.New("redraw write failed")
				writer := &armedReconnectToastWriter{next: out, needle: "probing UDP path", failure: failure}
				return newReconnectToastTerminalHarnessWithOutput(t, writer), func() { writer.armed.Store(true) }
			},
			wantError: "redraw write failed",
		},
		{
			name: "flush",
			configure: func(t *testing.T, out *reconnectToastOutputRecorder) (*reconnectToastTerminalHarness, func()) {
				var armed atomic.Bool
				term := newReconnectToastTerminalHarnessWithOutputAndFlush(t, out, func() error {
					if armed.Load() {
						return errors.New("redraw flush failed")
					}
					return nil
				})
				return term, func() { armed.Store(true) }
			},
			wantError: "redraw flush failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			out := newReconnectToastOutputRecorder()
			term, arm := tt.configure(t, out)
			defer term.closeInput()
			tr := newReconnectToastLinkTransport()
			tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}
			observer := &reconnectRuntimeObserver{}

			resultCh := make(chan attachResult, 1)
			ms := milestones{}
			attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
			attempt.runner.runtimeObserver = observer
			go func() { resultCh <- attempt.run(context.Background()) }()

			tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
			requireReconnectToastOutput(t, out.completed)
			arm()
			tr.recvCh <- reconnectToastRecv{frame: ports.Frame{Type: ports.MsgOutput, Payload: mustMarshalOutput(ports.Output{
				Epoch: 1,
				Base:  0,
				New:   2,
				Size:  domain.Size{Cols: 1, Rows: 1},
				Full:  true,
				Data:  []byte("daemon output before failed redraw"),
			})}}

			result := requireAttachResult(t, resultCh)
			require.ErrorContains(t, result.err, tt.wantError)
			require.Zero(t, observer.count(), "failed terminal redraw must not publish a terminal-flushed mark")
		})
	}
}

func TestReconnectLinkEventsNotifyDaemonWithoutLocalTerminalWrites(t *testing.T) {
	out := newReconnectToastOutputRecorder()
	term := newReconnectToastTerminalHarnessWithOutput(t, out)
	defer term.closeInput()
	tr := newReconnectToastLinkTransport()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.events <- ports.LinkEvent{State: ports.LinkStateDegraded}
	tr.events <- ports.LinkEvent{State: ports.LinkStateConnected}
	require.Equal(t, ports.ClientNoticeLinkDegraded, (<-tr.clientNotices).Action)
	require.Equal(t, ports.ClientNoticeLinkConnected, (<-tr.clientNotices).Action)
	select {
	case bytes := <-out.completed:
		t.Fatalf("link notices wrote client-local terminal bytes: %q", bytes)
	default:
	}

	tr.recvCh <- reconnectToastRecv{frame: reconnectToastDetach(ports.ReasonDetach)}
	result := requireAttachResult(t, resultCh)
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
	result := requireAttachResult(t, resultCh)
	require.ErrorIs(t, result.err, errLinkOffline)
	require.True(t, result.welcomed)
	require.True(t, shouldReconnect(result.err), "offline exit must be reconnectable")
}

type reconnectResetTimer struct {
	ch chan time.Time
}

func (t *reconnectResetTimer) C() <-chan time.Time      { return t.ch }
func (t *reconnectResetTimer) Reset(time.Duration) bool { return true }
func (t *reconnectResetTimer) Stop() bool               { return true }

type reconnectResetClock struct {
	preWelcome chan *reconnectResetTimer
}

func newReconnectResetClock() *reconnectResetClock {
	return &reconnectResetClock{preWelcome: make(chan *reconnectResetTimer, 4)}
}

func (c *reconnectResetClock) Now() time.Time { return time.Time{} }

func (c *reconnectResetClock) NewTimer(d time.Duration) ports.Timer {
	timer := &reconnectResetTimer{ch: make(chan time.Time, 1)}
	if d == preWelcomeTimeout {
		c.preWelcome <- timer
	}
	return timer
}

type reconnectResetBlockingTransport struct {
	mu        sync.Mutex
	recvs     []reconnectToastRecv
	entered   chan struct{}
	closed    chan struct{}
	enterOnce sync.Once
	closeOnce sync.Once
}

func newReconnectResetBlockingTransport() *reconnectResetBlockingTransport {
	return &reconnectResetBlockingTransport{
		recvs:   []reconnectToastRecv{{frame: reconnectToastWelcomeNamed("assigned", 44)}},
		entered: make(chan struct{}),
		closed:  make(chan struct{}),
	}
}

func (t *reconnectResetBlockingTransport) Send(frame ports.Frame) error {
	if frame.Type != ports.MsgResize {
		return nil
	}
	t.enterOnce.Do(func() { close(t.entered) })
	<-t.closed
	return io.ErrClosedPipe
}

func (t *reconnectResetBlockingTransport) Recv() (ports.Frame, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.recvs) == 0 {
		return ports.Frame{}, io.EOF
	}
	recv := t.recvs[0]
	t.recvs = t.recvs[1:]
	return recv.frame, recv.err
}

func (t *reconnectResetBlockingTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

func TestPreSenderReconnectResetIsBounded(t *testing.T) {
	tests := []struct {
		name    string
		trigger func(context.CancelFunc, *reconnectResetTimer)
		wantErr error
	}{
		{
			name: "timeout",
			trigger: func(_ context.CancelFunc, timer *reconnectResetTimer) {
				timer.ch <- time.Time{}
			},
			wantErr: errHandshakeTimeout,
		},
		{
			name: "cancellation",
			trigger: func(cancel context.CancelFunc, _ *reconnectResetTimer) {
				cancel()
			},
			wantErr: context.Canceled,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term := newReconnectToastTerminalHarness(t)
			defer term.closeInput()
			tr := newReconnectResetBlockingTransport()
			clock := newReconnectResetClock()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			ms := milestones{}
			attempt := newReconnectAttachAttempt(term.term, tr, clock, AttachRequest{Intent: ports.IntentResume, SessionName: "requested", Remote: true}, 12, &terminalThemeState{}, nil, &ms)
			attempt.reconnect.showing = true
			resultCh := make(chan attachResult, 1)
			go func() { resultCh <- attempt.run(ctx) }()

			handshakeTimer := <-clock.preWelcome
			select {
			case <-tr.entered:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for synchronous reconnect reset Send")
			}
			tt.trigger(cancel, handshakeTimer)

			result := requireAttachResult(t, resultCh)
			require.ErrorIs(t, result.err, tt.wantErr)
			require.True(t, result.transportClosed)
			require.True(t, result.welcomed)
			require.Equal(t, uint64(44), result.resumeToken)
			require.Equal(t, "assigned", result.sessionName)
		})
	}
}

type reconnectToastBlockingSendTransport struct {
	*reconnectToastLinkTransport
	armed       atomic.Bool
	blockType   ports.MsgType
	sendErr     error
	entered     chan struct{}
	release     chan struct{}
	enteredOnce sync.Once
	releaseOnce sync.Once
}

func newReconnectToastBlockingSendTransport() *reconnectToastBlockingSendTransport {
	return &reconnectToastBlockingSendTransport{
		reconnectToastLinkTransport: newReconnectToastLinkTransport(),
		blockType:                   ports.MsgClientNotice,
		entered:                     make(chan struct{}),
		release:                     make(chan struct{}),
	}
}

func (t *reconnectToastBlockingSendTransport) Send(frame ports.Frame) error {
	if err := t.reconnectToastLinkTransport.Send(frame); err != nil {
		return err
	}
	if t.armed.Load() && frame.Type == t.blockType {
		t.enteredOnce.Do(func() { close(t.entered) })
		<-t.release
		return t.sendErr
	}
	return nil
}

func requireTerminalSizeCalls(t *testing.T, term *reconnectToastTerminalHarness, want int32) {
	t.Helper()
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	for term.sizeCalls.Load() < want {
		select {
		case <-term.sizeCalled:
		case <-timer.C:
			t.Fatalf("timed out waiting for terminal Size call %d; got %d", want, term.sizeCalls.Load())
		}
	}
}

func TestReconnectResetEnqueueCancellationExitsCleanly(t *testing.T) {
	tests := []struct {
		name       string
		transition ports.LinkState
	}{
		{name: "dismissal", transition: ports.LinkStateConnected},
		{name: "stage change", transition: ports.LinkStateDead},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			term := newReconnectToastTerminalHarness(t)
			defer term.closeInput()
			tr := newReconnectToastBlockingSendTransport()
			releaseSender := func() { tr.releaseOnce.Do(func() { close(tr.release) }) }
			defer releaseSender()
			tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			resultCh := make(chan attachResult, 1)
			ms := milestones{}
			attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
			go func() { resultCh <- attempt.run(ctx) }()

			tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
			requireTerminalSizeCalls(t, term, 2)
			tr.blockType = ports.MsgResize
			tr.armed.Store(true)
			term.resizeCh <- domain.Geometry{Size: term.size}
			select {
			case <-tr.entered:
			case <-time.After(time.Second):
				t.Fatal("timed out waiting for sender to block")
			}
			for range sendQueueDepth {
				term.resizeCh <- domain.Geometry{Size: term.size}
			}
			require.Eventually(t, func() bool { return len(term.resizeCh) == 0 }, time.Second, time.Millisecond)

			tr.events <- ports.LinkEvent{State: tt.transition}
			requireTerminalSizeCalls(t, term, 3)
			cancel()

			result := requireAttachResult(t, resultCh)
			require.NoError(t, result.err)
			require.True(t, result.welcomed)
			releaseSender()
		})
	}
}

func TestReconnectResetCancellationPreservesQueuedSenderError(t *testing.T) {
	term := newReconnectToastTerminalHarness(t)
	defer term.closeInput()
	tr := newReconnectToastBlockingSendTransport()
	releaseSender := func() { tr.releaseOnce.Do(func() { close(tr.release) }) }
	defer releaseSender()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.events <- ports.LinkEvent{State: ports.LinkStateProbing}
	requireTerminalSizeCalls(t, term, 2)
	tr.blockType = ports.MsgResize
	tr.armed.Store(true)
	term.resizeCh <- domain.Geometry{Size: term.size}
	select {
	case <-tr.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sender to block")
	}
	for range sendQueueDepth {
		term.resizeCh <- domain.Geometry{Size: term.size}
	}
	require.Eventually(t, func() bool { return len(term.resizeCh) == 0 }, time.Second, time.Millisecond)

	tr.events <- ports.LinkEvent{State: ports.LinkStateConnected}
	requireTerminalSizeCalls(t, term, 3)
	sendFailure := errors.New("queued sender failure")
	tr.sendErr = sendFailure
	releaseSender()

	result := requireAttachResult(t, resultCh)
	require.ErrorIs(t, result.err, sendFailure)
	require.ErrorContains(t, result.err, "sending to daemon")
}

func TestAttachAttemptReturnsWhileSenderSendIsBlockedAfterCancellation(t *testing.T) {
	term := newReconnectToastTerminalHarness(t)
	defer term.closeInput()
	tr := newReconnectToastBlockingSendTransport()
	releaseSender := func() { tr.releaseOnce.Do(func() { close(tr.release) }) }
	defer releaseSender()
	tr.recvCh <- reconnectToastRecv{frame: reconnectToastWelcome(44)}

	resultCh := make(chan attachResult, 1)
	ms := milestones{}
	attempt := newReconnectAttachAttempt(term.term, tr, newReconnectHandshakeClock(t), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true}, 0, &terminalThemeState{}, tr.LinkEvents(), &ms)
	go func() { resultCh <- attempt.run(context.Background()) }()

	tr.armed.Store(true)
	tr.events <- ports.LinkEvent{State: ports.LinkStateDegraded}
	select {
	case <-tr.entered:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for sender to block in transport Send")
	}
	tr.events <- ports.LinkEvent{State: ports.LinkStateOffline}

	select {
	case result := <-resultCh:
		require.ErrorIs(t, result.err, errLinkOffline)
	case <-time.After(time.Second):
		releaseSender()
		requireAttachResult(t, resultCh)
		t.Fatal("attach attempt waited for the blocked sender before returning control to Runner")
	}
	releaseSender()
}

func TestDetachedResultLegacyReplacementCompatibilityAndCleanDetach(t *testing.T) {
	// Current daemons keep healthy displaced clients connected. Continue decoding
	// ReasonReplaced from older peers so mixed-version detach handling stays clear.
	for _, tt := range []struct {
		name     string
		reason   uint8
		wantText string
	}{
		{name: "clean detach", reason: ports.ReasonDetach},
		{name: "legacy replacement", reason: ports.ReasonReplaced, wantText: "session taken over by another client"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			err := detachedResult(tt.reason)
			if tt.wantText == "" {
				require.NoError(t, err)
				return
			}
			var detached *DetachedError
			require.ErrorAs(t, err, &detached)
			require.Equal(t, tt.wantText, detached.Text)
		})
	}
}

func TestReconnectToastBoundsUseCenterAnchor(t *testing.T) {
	bounds := reconnectToastBounds(domain.Size{Cols: 80, Rows: 24})
	require.Equal(t, domain.Rect{X: 29, Y: 10, Width: 22, Height: 3}, bounds)
}

func TestReconnectToastDrawHelper(t *testing.T) {
	var out bytes.Buffer
	require.NoError(t, drawReconnectToast(&out, domain.Size{Cols: 80, Rows: 24}))
	require.Contains(t, out.String(), "┌")
	require.Contains(t, out.String(), reconnectToastMessage)
	require.Contains(t, out.String(), "\x1b[0m")
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

func TestReconnectToastStageBytesUnchangedAfterClientHelperExtraction(t *testing.T) {
	for _, stage := range []reconnectStage{reconnectStageDegraded, reconnectStageProbingUDP, reconnectStageSSH, reconnectStageOfflineRetrying} {
		t.Run(reconnectStageMessage(stage), func(t *testing.T) {
			var got, want bytes.Buffer
			gotBounds, gotErr := drawReconnectToastStage(&got, domain.Size{Cols: 80, Rows: 24}, stage)
			wantBounds, wantErr := legacyReconnectToastStage(&want, domain.Size{Cols: 80, Rows: 24}, stage)
			require.Equal(t, wantErr, gotErr)
			require.Equal(t, wantBounds, gotBounds)
			require.Equal(t, want.String(), got.String())
		})
	}
}

func legacyReconnectToastStage(out io.Writer, size domain.Size, stage reconnectStage) (domain.Rect, error) {
	message := reconnectStageMessage(stage)
	bounds := reconnectToastBoundsFor(size, message)
	if bounds.Width <= 0 || bounds.Height <= 0 {
		return domain.Rect{}, nil
	}
	return bounds, writeReconnectToast(out, bounds, reconnectToastLinesFor(bounds, message))
}

func TestReconnectToastStagesDrawTheirCurrentMessage(t *testing.T) {
	for _, stage := range []reconnectStage{reconnectStageDegraded, reconnectStageProbingUDP, reconnectStageSSH, reconnectStageOfflineRetrying} {
		t.Run(reconnectStageMessage(stage), func(t *testing.T) {
			var out bytes.Buffer
			_, err := drawReconnectToastStage(&out, domain.Size{Cols: 80, Rows: 24}, stage)
			require.NoError(t, err)
			require.Contains(t, out.String(), reconnectStageMessage(stage))
		})
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
	resizeCh := make(chan domain.Geometry, 2)
	resizeCh <- domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}}
	resizeCh <- domain.Geometry{Size: domain.Size{Cols: 120, Rows: 40}}
	got := make(chan domain.Size, 2)
	type sleepResult struct {
		slept bool
		err   error
	}
	done := make(chan sleepResult, 1)

	go func() {
		slept, err := sleepReconnectWithResizeEvents(context.Background(), clk, time.Hour, resizeCh, func(size domain.Size) error {
			got <- size
			return nil
		})
		done <- sleepResult{slept: slept, err: err}
	}()

	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, <-got)
	require.Equal(t, domain.Size{Cols: 120, Rows: 40}, <-got)
	timerCh <- time.Time{}
	result := <-done
	require.NoError(t, result.err)
	require.True(t, result.slept)
}

func TestReconnectSleepWithResizeEventsReturnsRedrawError(t *testing.T) {
	clk := portsmocks.NewMockClock(t)
	timer := portsmocks.NewMockTimer(t)
	clk.EXPECT().NewTimer(time.Hour).Return(timer).Once()
	timer.EXPECT().C().Return((<-chan time.Time)(make(chan time.Time))).Maybe()
	timer.EXPECT().Stop().Return(true).Once()
	resizeCh := make(chan domain.Geometry, 1)
	resizeCh <- domain.Geometry{Size: domain.Size{Cols: 100, Rows: 30}}
	redrawErr := errors.New("redraw failed")

	slept, err := sleepReconnectWithResizeEvents(context.Background(), clk, time.Hour, resizeCh, func(domain.Size) error {
		return redrawErr
	})

	require.False(t, slept)
	require.ErrorIs(t, err, redrawErr)
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

func TestReconnectRetryFailuresPreserveAttachError(t *testing.T) {
	attachErr := errors.New("original attach failure")
	retryErr := errors.New("retry presentation failure")
	tests := []struct {
		name       string
		output     func() io.Writer
		retrySleep func(func(domain.Size) error) (bool, error)
	}{
		{
			name: "draw stage",
			output: func() io.Writer {
				writer := &armedReconnectToastWriter{next: io.Discard, needle: reconnectStageMessage(reconnectStageSSH), failure: retryErr}
				writer.armed.Store(true)
				return writer
			},
			retrySleep: func(func(domain.Size) error) (bool, error) { return true, nil },
		},
		{
			name:   "sleep",
			output: func() io.Writer { return io.Discard },
			retrySleep: func(func(domain.Size) error) (bool, error) {
				return false, retryErr
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldSleepWithResize := reconnectSleepWithResize
			reconnectSleepWithResize = func(_ context.Context, _ ports.Clock, _ time.Duration, _ <-chan domain.Geometry, redraw func(domain.Size) error) (bool, error) {
				return tt.retrySleep(redraw)
			}
			defer func() { reconnectSleepWithResize = oldSleepWithResize }()

			term := newReconnectToastTerminalHarnessWithOutput(t, tt.output())
			defer term.closeInput()
			tr := newReconnectToastRecordingTransport(
				reconnectToastRecv{frame: reconnectToastWelcome(44)},
				reconnectToastRecv{err: attachErr},
			)
			dialer := &reconnectToastSequenceDialer{transports: []ports.Transport{tr}}

			err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(context.Background(), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true})
			require.ErrorIs(t, err, attachErr)
			require.ErrorIs(t, err, retryErr)
		})
	}
}

func TestReconnectRetrySleepFailuresPreserveDialError(t *testing.T) {
	dialErr := errors.New("original reconnect dial failure")
	retryErr := errors.New("retry presentation failure")
	tests := []struct {
		name       string
		failSecond func(func(domain.Size) error) (bool, error)
	}{
		{
			name: "resize redraw",
			failSecond: func(redraw func(domain.Size) error) (bool, error) {
				return false, redraw(domain.Size{Cols: 100, Rows: 30})
			},
		},
		{
			name: "sleep",
			failSecond: func(func(domain.Size) error) (bool, error) {
				return false, retryErr
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldSleepWithResize := reconnectSleepWithResize
			calls := 0
			reconnectSleepWithResize = func(_ context.Context, _ ports.Clock, _ time.Duration, _ <-chan domain.Geometry, redraw func(domain.Size) error) (bool, error) {
				calls++
				if calls == 1 {
					return true, nil
				}
				return tt.failSecond(redraw)
			}
			defer func() { reconnectSleepWithResize = oldSleepWithResize }()

			var armed atomic.Bool
			term := newReconnectToastTerminalHarnessWithOutputAndFlush(t, io.Discard, func() error {
				if armed.Load() {
					return retryErr
				}
				return nil
			})
			defer term.closeInput()
			tr := newReconnectToastRecordingTransport(
				reconnectToastRecv{frame: reconnectToastWelcome(44)},
				reconnectToastRecv{err: io.EOF},
			)
			dialer := portsmocks.NewMockDialer(t)
			dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()
			dialer.EXPECT().Dial(mock.Anything).Run(func(context.Context) { armed.Store(true) }).Return(nil, dialErr).Once()

			err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(context.Background(), AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: true})
			require.ErrorIs(t, err, dialErr)
			require.ErrorIs(t, err, retryErr)
		})
	}
}

func TestRemoteReconnectToastFailedDrawDoesNotBlankBounds(t *testing.T) {
	oldSleep := reconnectSleep
	oldSleepWithResize := reconnectSleepWithResize
	ctx, cancel := context.WithCancel(context.Background())
	reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return false }
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Geometry, func(domain.Size) error) (bool, error) {
		cancel()
		return false, nil
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
	require.ErrorContains(t, err, "reconnect toast write failed")
	require.True(t, out.failed)
	require.NotContains(t, out.String(), strings.Repeat(" ", reconnectToastBoundsFor(term.size, reconnectStageMessage(reconnectStageSSH)).Width))
}

type reconnectStatusClearFailWriter struct {
	clearErr error
}

func (w *reconnectStatusClearFailWriter) Write(p []byte) (int, error) {
	if string(p) == statusClear {
		return 0, w.clearErr
	}
	return len(p), nil
}

func TestReconnectCancellationPreservesContextWhenStatusClearFails(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*portsmocks.MockDialer, ports.Transport)
		sleep     func(context.CancelFunc) bool
	}{
		{
			name: "after attach failure",
			configure: func(dialer *portsmocks.MockDialer, tr ports.Transport) {
				dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()
			},
			sleep: func(cancel context.CancelFunc) bool {
				cancel()
				return false
			},
		},
		{
			name: "after reconnect dial failure",
			configure: func(dialer *portsmocks.MockDialer, tr ports.Transport) {
				dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()
				dialer.EXPECT().Dial(mock.Anything).Return(nil, errors.New("redial failed")).Once()
			},
			sleep: func() func(context.CancelFunc) bool {
				calls := 0
				return func(cancel context.CancelFunc) bool {
					calls++
					if calls == 1 {
						return true
					}
					cancel()
					return false
				}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldSleep := reconnectSleep
			oldSleepWithResize := reconnectSleepWithResize
			ctx, cancel := context.WithCancel(context.Background())
			reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return tt.sleep(cancel) }
			reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Geometry, func(domain.Size) error) (bool, error) {
				return tt.sleep(cancel), nil
			}
			defer func() {
				reconnectSleep = oldSleep
				reconnectSleepWithResize = oldSleepWithResize
			}()

			clearErr := errors.New("status clear failed")
			term := newReconnectToastTerminalHarnessWithOutput(t, &reconnectStatusClearFailWriter{clearErr: clearErr})
			defer term.closeInput()
			tr := newReconnectToastRecordingTransport(
				reconnectToastRecv{frame: reconnectToastWelcome(11)},
				reconnectToastRecv{err: io.EOF},
			)
			dialer := portsmocks.NewMockDialer(t)
			tt.configure(dialer, tr)

			err := NewRunner(Dependencies{Dialer: dialer, Terminal: term.term, Clock: newReconnectHandshakeClock(t), Logger: slog.New(slog.DiscardHandler)}).Run(ctx, AttachRequest{Intent: ports.IntentAttach, SessionName: "main"})
			require.ErrorIs(t, err, context.Canceled)
			require.ErrorIs(t, err, clearErr)
		})
	}
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
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Geometry, func(domain.Size) error) (bool, error) {
		return true, nil
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
	require.NotContains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
}

func TestRemoteEphemeralReconnectUsesAssignedSessionName(t *testing.T) {
	linkDead := errors.New("remote link dead")
	oldSleep := reconnectSleep
	oldSleepWithResize := reconnectSleepWithResize
	reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return true }
	reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Geometry, func(domain.Size) error) (bool, error) {
		return true, nil
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
			sleep:   func(context.CancelFunc) bool { return true },
			wantErr: func(t *testing.T, err error) { require.NoError(t, err) },
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
			wantErr: func(t *testing.T, err error) { require.ErrorIs(t, err, context.Canceled) },
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
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldSleep := reconnectSleep
			oldSleepWithResize := reconnectSleepWithResize
			ctx, cancel := context.WithCancel(context.Background())
			reconnectSleep = func(context.Context, ports.Clock, time.Duration) bool { return tt.sleep(cancel) }
			reconnectSleepWithResize = func(context.Context, ports.Clock, time.Duration, <-chan domain.Geometry, func(domain.Size) error) (bool, error) {
				return tt.sleep(cancel), nil
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
			require.NotContains(t, out, strings.Repeat(" ", reconnectToastBounds(term.size).Width))
		})
	}
}
