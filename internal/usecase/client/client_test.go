package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/client"
)

func init() {
	// Keep client diagnostics out of the test runner's stderr.
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
}

// realClock is a stdlib-backed ports.Clock for tests that exercise the stdin
// pump end to end; the paste coalescer's flush timer runs on wall time here.
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }
func (realClock) NewTimer(d time.Duration) ports.Timer {
	return &realTimer{t: time.NewTimer(d)}
}

type realTimer struct{ t *time.Timer }

func (r *realTimer) C() <-chan time.Time        { return r.t.C }
func (r *realTimer) Reset(d time.Duration) bool { return r.t.Reset(d) }
func (r *realTimer) Stop() bool                 { return r.t.Stop() }

// recvItem is one scripted Recv result.
type recvItem struct {
	f   ports.Frame
	err error
}

type markedDatagramTransport struct{ ports.Transport }

func (markedDatagramTransport) DatagramTransport() {}

// scriptRecv wires a MockTransport's Recv to yield the given items in order,
// then block forever (simulating a live but idle connection). It returns a
// cleanup that unblocks the parked reader.
func scriptRecv(tr *portsmocks.MockTransport, items ...recvItem) func() {
	ch := make(chan recvItem, len(items))
	for _, it := range items {
		ch <- it
	}
	done := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case it := <-ch:
			return it.f, it.err
		case <-done:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	return func() { close(done) }
}

func frameOf(t ports.MsgType, payload []byte) ports.Frame {
	return ports.Frame{Type: t, Payload: payload}
}

// blockingReader blocks on Read until closed, then returns EOF. Stands in
// for a terminal's stdin that produces nothing during the test.
type blockingReader struct{ ch chan struct{} }

func newBlockingReader() *blockingReader { return &blockingReader{ch: make(chan struct{})} }

func (b *blockingReader) Read(p []byte) (int, error) {
	<-b.ch
	return 0, io.EOF
}
func (b *blockingReader) unblock() { close(b.ch) }

type oneShotBlockingReader struct {
	data []byte
	done chan struct{}
	once sync.Once
}

func newOneShotBlockingReader(data []byte) *oneShotBlockingReader {
	return &oneShotBlockingReader{data: data, done: make(chan struct{})}
}

func (r *oneShotBlockingReader) Read(p []byte) (int, error) {
	read := false
	r.once.Do(func() {
		copy(p, r.data)
		read = true
	})
	if read {
		return len(r.data), nil
	}
	<-r.done
	return 0, io.EOF
}

func (r *oneShotBlockingReader) unblock() { close(r.done) }

type chunkedBlockingReader struct {
	chunks [][]byte
	done   chan struct{}
	mu     sync.Mutex
	next   int
}

func newChunkedBlockingReader(chunks ...[]byte) *chunkedBlockingReader {
	return &chunkedBlockingReader{chunks: chunks, done: make(chan struct{})}
}

func (r *chunkedBlockingReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.next < len(r.chunks) {
		chunk := r.chunks[r.next]
		r.next++
		return copy(p, chunk), nil
	}
	<-r.done
	return 0, io.EOF
}

func (r *chunkedBlockingReader) unblock() { close(r.done) }

// queryBoundaryReader reports an attempt to read the second chunk before
// returning it, allowing a test to prove that a scheme re-query is a real
// stdin generation boundary rather than a lossy notification.
type queryBoundaryReader struct {
	first, second     []byte
	secondRead        chan struct{}
	allowSecond, done <-chan struct{}
	reads             int
}

func (r *queryBoundaryReader) Read(p []byte) (int, error) {
	switch r.reads {
	case 0:
		r.reads++
		return copy(p, r.first), nil
	case 1:
		select {
		case r.secondRead <- struct{}{}:
		case <-r.done:
			return 0, io.EOF
		}
		select {
		case <-r.allowSecond:
			r.reads++
			return copy(p, r.second), nil
		case <-r.done:
			return 0, io.EOF
		}
	default:
		<-r.done
		return 0, io.EOF
	}
}

func newHappyTerminal(t *testing.T, out *bytes.Buffer, restoreCount *atomic.Int32, resizeCh chan domain.Size) (*portsmocks.MockTerminal, *blockingReader) {
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error {
		restoreCount.Add(1)
		return nil
	}, nil).Once()
	in := newBlockingReader()
	tm.EXPECT().In().Return(in).Maybe()
	tm.EXPECT().Out().Return(out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	return tm, in
}

func isType(typ ports.MsgType) any {
	return mock.MatchedBy(func(f ports.Frame) bool { return f.Type == typ })
}

// transportDialer adapts one already-open test transport to the Runner API.
type transportDialer struct{ transport ports.Transport }

func (d transportDialer) Dial(context.Context) (ports.Transport, error) {
	return d.transport, nil
}

func runTestClient(ctx context.Context, deps client.Dependencies, request client.AttachRequest) error {
	return client.NewRunner(deps).Run(ctx, request)
}

func testDependencies(dialer ports.Dialer, terminal ports.Terminal, clock ports.Clock, clipboard ports.ClipboardReader, observer ports.SerializedRuntimeObserver) client.Dependencies {
	return client.Dependencies{
		Dialer:          dialer,
		Terminal:        terminal,
		Clock:           clock,
		Clipboard:       clipboard,
		Logger:          slog.New(slog.DiscardHandler),
		RuntimeObserver: observer,
	}
}

func attachTestDependencies(transport ports.Transport, terminal ports.Terminal, clock ports.Clock) client.Dependencies {
	return testDependencies(transportDialer{transport: transport}, terminal, clock, nil, nil)
}

const testPreWelcomeTimeout = 15 * time.Second

func newHandshakeClock(t *testing.T, timerCount int) (*portsmocks.MockClock, <-chan chan time.Time) {
	t.Helper()
	created := make(chan chan time.Time, timerCount)
	clk := portsmocks.NewMockClock(t)
	clk.EXPECT().NewTimer(testPreWelcomeTimeout).RunAndReturn(func(time.Duration) ports.Timer {
		timerC := make(chan time.Time, 1)
		timer := portsmocks.NewMockTimer(t)
		timer.EXPECT().C().Return((<-chan time.Time)(timerC)).Maybe()
		timer.EXPECT().Stop().Return(true).Once()
		created <- timerC
		return timer
	}).Times(timerCount)
	return clk, created
}

func TestRunBoundsPreWelcomeOperations(t *testing.T) {
	tests := []struct {
		name  string
		phase string
		end   string
		want  error
	}{
		{name: "cancel while sending hello", phase: "send", end: "cancel", want: context.Canceled},
		{name: "timeout while sending hello", phase: "send", end: "timeout", want: context.DeadlineExceeded},
		{name: "cancel while receiving welcome", phase: "recv", end: "cancel", want: context.Canceled},
		{name: "timeout while receiving welcome", phase: "recv", end: "timeout", want: context.DeadlineExceeded},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			timerCount := 1
			if tt.phase == "recv" {
				timerCount = 2
			}
			clk, createdTimers := newHandshakeClock(t, timerCount)

			term := portsmocks.NewMockTerminal(t)
			term.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
			// No EnterRaw expectation: it must not run before Welcome.

			started := make(chan struct{})
			closed := make(chan struct{})
			operationExited := make(chan struct{})
			tr := portsmocks.NewMockTransport(t)
			tr.EXPECT().Close().Run(func() { close(closed) }).Return(nil).Once()

			block := func() error {
				close(started)
				<-closed
				close(operationExited)
				return io.ErrClosedPipe
			}
			if tt.phase == "send" {
				tr.EXPECT().Send(isType(ports.MsgHello)).RunAndReturn(func(ports.Frame) error {
					return block()
				}).Once()
			} else {
				tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
				tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
					return ports.Frame{}, block()
				}).Once()
			}
			dialer := portsmocks.NewMockDialer(t)
			dialer.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

			errCh := make(chan error, 1)
			go func() {
				errCh <- runTestClient(ctx, testDependencies(dialer, term, clk, nil, nil), client.AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: false})
			}()

			var timerC chan time.Time
			select {
			case timerC = <-createdTimers:
			case <-time.After(time.Second):
				t.Fatal("pre-Welcome timer was not created")
			}
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("pre-Welcome operation did not start")
			}
			if tt.phase == "recv" {
				select {
				case timerC = <-createdTimers:
				case <-time.After(time.Second):
					t.Fatal("Welcome timer was not created")
				}
			}

			if tt.end == "cancel" {
				cancel()
			} else {
				timerC <- time.Time{}
			}

			select {
			case err := <-errCh:
				require.ErrorIs(t, err, tt.want)
			case <-time.After(time.Second):
				t.Fatal("Run did not return after pre-Welcome cancellation")
			}
			select {
			case <-operationExited:
			case <-time.After(time.Second):
				t.Fatal("pre-Welcome operation leaked after Run returned")
			}
		})
	}
}

func TestAttachHelloIncludesTrueColor(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "truecolor")

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	gotHello := make(chan ports.Hello, 1)
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).RunAndReturn(func(f ports.Frame) error {
		hello, err := ports.UnmarshalHello(f.Payload)
		require.NoError(t, err)
		gotHello <- hello
		return nil
	}).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)

	select {
	case hello := <-gotHello:
		require.Equal(t, "xterm-256color", hello.TermEnv)
		require.True(t, hello.TrueColor)
		require.Equal(t, uint8(8), hello.MaxOutputInFlight)
	case <-time.After(2 * time.Second):
		t.Fatal("hello frame was not sent")
	}
}

func TestAttachHelloIncludesCompleteLocalEnvironment(t *testing.T) {
	t.Setenv("VEV_CLIENT_ENV_TEST", "TOKEN=a=b=c")

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	gotHello := make(chan ports.Hello, 1)
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).RunAndReturn(func(f ports.Frame) error {
		hello, err := ports.UnmarshalHello(f.Payload)
		if err != nil {
			return err
		}
		gotHello <- hello
		return nil
	}).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	require.NoError(t, runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral}))
	require.Equal(t, os.Environ(), (<-gotHello).Env)
}

func TestAttachHelloRequestsSingleOutputForDatagramTransport(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).RunAndReturn(func(f ports.Frame) error {
		hello, err := ports.UnmarshalHello(f.Payload)
		require.NoError(t, err)
		require.Equal(t, uint8(1), hello.MaxOutputInFlight)
		return nil
	}).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	require.NoError(t, runTestClient(context.Background(), attachTestDependencies(markedDatagramTransport{tr}, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral}))
}

func TestAttachHappyPath(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1", SessionName: "main"}))},
		recvItem{f: frameOf(ports.MsgOutput, ports.MarshalOutput(ports.Output{Data: []byte("hello world")}))},
		recvItem{f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	require.Equal(t, "hello world", out.String())
	require.Equal(t, int32(1), restoreCount.Load(), "restore must run exactly once")
}

func TestAttachVersionMismatch(t *testing.T) {
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	// EnterRaw must NOT be called on the error-before-welcome path.

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Recv().Return(
		frameOf(ports.MsgError, ports.MarshalErrorMsg(ports.ErrorMsg{Code: ports.ErrVersionMismatch, Text: "version mismatch"})),
		nil,
	).Once()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentAttach, SessionName: "main"})
	require.Error(t, err)
	var pe *client.ProtocolError
	require.True(t, errors.As(err, &pe), "want *client.ProtocolError, got %T", err)
	require.Equal(t, ports.ErrVersionMismatch, pe.Code)
}

func TestAttachRestoredOnRecvErrorMidStream(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	boom := errors.New("connection reset")
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{err: boom},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.Error(t, err)
	require.Equal(t, int32(1), restoreCount.Load(), "restore must run after mid-stream Recv error")
}

func TestAttachDaemonVanishedOnEOF(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{err: io.EOF},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.Error(t, err)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, int32(1), restoreCount.Load())
}

func TestAttachStdinOSCColorResponseSendsThemeAndPreservesInput(t *testing.T) {
	t.Setenv("COLORTERM", "truecolor")

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	input := newOneShotBlockingReader([]byte("a\x1b]11;rgb:0101/0202/0303\x07b"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error {
		restoreCount.Add(1)
		return nil
	}, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotTheme := make(chan ports.Theme, 1)
	gotInput := make(chan []byte, 2)
	allowDetach := make(chan struct{})

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgInput)).RunAndReturn(func(f ports.Frame) error {
		in, err := ports.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		require.NotContains(t, string(in.Data), "\x1b]11;")
		gotInput <- append([]byte(nil), in.Data...)
		if bytes.Contains(in.Data, []byte("b")) {
			close(allowDetach)
		}
		return nil
	}).Maybe()
	tr.EXPECT().Send(isType(ports.MsgTheme)).RunAndReturn(func(f ports.Frame) error {
		th, err := ports.UnmarshalTheme(f.Payload)
		require.NoError(t, err)
		gotTheme <- th
		return nil
	}).Once()

	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return ports.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	require.Equal(t, int32(1), restoreCount.Load())

	select {
	case th := <-gotTheme:
		require.True(t, th.HasBackground)
		require.Equal(t, uint8(1), th.Background.R)
		require.Equal(t, uint8(2), th.Background.G)
		require.Equal(t, uint8(3), th.Background.B)
		require.True(t, th.TrueColor)
	case <-time.After(2 * time.Second):
		t.Fatal("theme frame was not sent")
	}

	var inputBytes []byte
	for {
		select {
		case b := <-gotInput:
			inputBytes = append(inputBytes, b...)
		default:
			require.Equal(t, []byte("ab"), inputBytes)
			return
		}
	}
}

func TestAttachStdinThemeTrueColorFalseWhenCOLORTERMNotTruecolor(t *testing.T) {
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("COLORTERM", "")

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	input := newOneShotBlockingReader([]byte("\x1b]10;#010203\x07"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotTheme := make(chan ports.Theme, 1)
	allowDetach := make(chan struct{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgTheme)).RunAndReturn(func(f ports.Frame) error {
		th, err := ports.UnmarshalTheme(f.Payload)
		require.NoError(t, err)
		gotTheme <- th
		close(allowDetach)
		return nil
	}).Once()
	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return ports.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case th := <-gotTheme:
		require.True(t, th.HasForeground)
		require.False(t, th.TrueColor)
	case <-time.After(2 * time.Second):
		t.Fatal("theme frame was not sent")
	}
}

func TestAttachForwardsStandaloneEscapeInput(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	input := newOneShotBlockingReader([]byte("\x1b"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput := make(chan []byte, 1)
	allowDetach := make(chan struct{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgInput)).RunAndReturn(func(f ports.Frame) error {
		in, err := ports.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- in.Data
		close(allowDetach)
		return nil
	}).Once()
	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return ports.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case got := <-gotInput:
		require.Equal(t, []byte("\x1b"), got)
	case <-time.After(2 * time.Second):
		t.Fatal("standalone escape input was not sent")
	}
}

func TestAttachStdinForwardsSGRMouseReportAsSingleFrame(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	input := newOneShotBlockingReader([]byte("\x1b[<0;1;1M"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput := make(chan []byte, 2)
	allowDetach := make(chan struct{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgInput)).RunAndReturn(func(f ports.Frame) error {
		in, err := ports.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- in.Data
		close(allowDetach)
		return nil
	}).Once()
	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return ports.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case got := <-gotInput:
		require.Equal(t, []byte("\x1b[<0;1;1M"), got, "SGR mouse report must arrive as one intact MsgInput frame")
	case <-time.After(2 * time.Second):
		t.Fatal("mouse report input was not sent")
	}
}

func TestAttachStdinCoalescesSplitBracketedPaste(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	paste := []byte("\x1b[200~hello\nworld\x1b[201~")
	input := newChunkedBlockingReader([]byte("\x1b[200~hello\n"), []byte("world\x1b[201~"))
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput := make(chan []byte, 1)
	allowDetach := make(chan struct{})
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgInput)).RunAndReturn(func(f ports.Frame) error {
		in, err := ports.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- in.Data
		close(allowDetach)
		return nil
	}).Once()
	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return ports.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case got := <-gotInput:
		require.Equal(t, paste, got, "split bracketed paste must arrive as one intact MsgInput frame")
	case <-time.After(2 * time.Second):
		t.Fatal("split bracketed paste input was not sent")
	}
}

func TestAttachForwardsResize(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	detachAfterResize := make(chan struct{})
	firstRecv := make(chan struct{})
	var firstRecvOnce sync.Once

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	// The resize frame is forwarded via the sender goroutine.
	gotResize := make(chan ports.Resize, 1)
	tr.EXPECT().Send(isType(ports.MsgResize)).RunAndReturn(func(f ports.Frame) error {
		r, _ := ports.UnmarshalResize(f.Payload)
		gotResize <- r
		close(detachAfterResize)
		return nil
	}).Once()

	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	recvCh := make(chan recvItem, 2)
	recvCh <- recvItem{f: welcome}
	done := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		firstRecvOnce.Do(func() { close(firstRecv) })
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-detachAfterResize:
			// Deliver the detach once the resize has been observed.
			select {
			case <-done:
				return ports.Frame{}, io.EOF
			default:
				close(done)
				return detached, nil
			}
		case <-done:
			return ports.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	// Push a resize event once attach begins receiving daemon frames.
	go func() {
		<-firstRecv
		resizeCh <- domain.Size{Cols: 120, Rows: 40}
	}()

	err := runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: ""})
	require.NoError(t, err)
	select {
	case r := <-gotResize:
		require.Equal(t, domain.Size{Cols: 120, Rows: 40}, r.Size)
	default:
		t.Fatal("resize was not forwarded")
	}
}

func TestRunRestoresRawModeAfterAttachError(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	boom := errors.New("connection reset")
	unblock := scriptRecv(tr,
		recvItem{f: frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))},
		recvItem{err: boom},
	)
	defer unblock()
	tr.EXPECT().Close().Return(nil).Once()
	d := portsmocks.NewMockDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, nil, nil), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: "", Remote: false})
	require.Error(t, err)
	require.Equal(t, int32(1), restoreCount.Load(), "Run must restore raw mode after attach attempt errors")
}

func TestRunDoesNotEnterRawBeforePreWelcomeError(t *testing.T) {
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	// EnterRaw must NOT be called when the daemon rejects Hello before Welcome.

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Recv().Return(
		frameOf(ports.MsgError, ports.MarshalErrorMsg(ports.ErrorMsg{Code: ports.ErrVersionMismatch, Text: "version mismatch"})),
		nil,
	).Once()
	tr.EXPECT().Close().Return(nil).Once()
	d := portsmocks.NewMockDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, nil, nil), client.AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: false})
	require.Error(t, err)
	var pe *client.ProtocolError
	require.True(t, errors.As(err, &pe), "want *client.ProtocolError, got %T", err)
}

func TestRunPhaseASingleAttempt(t *testing.T) {
	dialErr := errors.New("dial failed")
	d := portsmocks.NewMockDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(nil, dialErr).Once()
	tm := portsmocks.NewMockTerminal(t)

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, nil, nil), client.AttachRequest{Intent: ports.IntentEphemeral, SessionName: "", Remote: false})
	require.ErrorIs(t, err, dialErr)
}

type sequenceDialer struct {
	trs   []ports.Transport
	errs  []error
	calls atomic.Int32
}

func (d *sequenceDialer) Dial(context.Context) (ports.Transport, error) {
	i := int(d.calls.Add(1)) - 1
	if i < len(d.errs) && d.errs[i] != nil {
		return nil, d.errs[i]
	}
	if i >= len(d.trs) {
		return nil, io.EOF
	}
	return d.trs[i], nil
}

type recordingTransport struct {
	recvs  []recvItem
	sends  []ports.Frame
	closed atomic.Int32
}

func (t *recordingTransport) Send(f ports.Frame) error {
	t.sends = append(t.sends, f)
	return nil
}

func (t *recordingTransport) Recv() (ports.Frame, error) {
	if len(t.recvs) == 0 {
		return ports.Frame{}, io.EOF
	}
	it := t.recvs[0]
	t.recvs = t.recvs[1:]
	return it.f, it.err
}

func (t *recordingTransport) Close() error {
	t.closed.Add(1)
	return nil
}

type runTerminal struct {
	in           *blockingReader
	out          bytes.Buffer
	rawCount     atomic.Int32
	restoreCount atomic.Int32
	resizeCh     chan domain.Size
}

func newRunTerminal() *runTerminal {
	return &runTerminal{in: newBlockingReader(), resizeCh: make(chan domain.Size)}
}

func (t *runTerminal) EnterRaw() (func() error, error) {
	t.rawCount.Add(1)
	return func() error { t.restoreCount.Add(1); return nil }, nil
}
func (t *runTerminal) Size() (domain.Size, error)       { return domain.Size{Cols: 80, Rows: 24}, nil }
func (t *runTerminal) ResizeEvents() <-chan domain.Size { return t.resizeCh }
func (t *runTerminal) QueryColors() error               { return nil }
func (t *runTerminal) In() io.Reader                    { return t.in }
func (t *runTerminal) Out() io.Writer                   { return &t.out }
func (t *runTerminal) Flush() error                     { return nil }

func helloFromSend(t *testing.T, tr *recordingTransport) ports.Hello {
	t.Helper()
	require.NotEmpty(t, tr.sends)
	require.Equal(t, ports.MsgHello, tr.sends[0].Type)
	h, err := ports.UnmarshalHello(tr.sends[0].Payload)
	require.NoError(t, err)
	return h
}

func welcomeFrame(token uint64) ports.Frame {
	return frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1", SessionName: "main", ResumeToken: token, Capabilities: ports.CapabilityResume}))
}

func TestRunReconnectsWithRotatedTokenAndSameClientID(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	tr1 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(11)}, {err: io.EOF}}}
	tr2 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(22)}, {err: io.EOF}}}
	tr3 := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(33)}, {f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))}}}
	d := &sequenceDialer{trs: []ports.Transport{tr1, tr2, tr3}}

	err := runTestClient(context.Background(), testDependencies(d, term, realClock{}, nil, nil), client.AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: false})
	require.NoError(t, err)
	require.Equal(t, int32(3), d.calls.Load())
	require.Equal(t, int32(1), term.rawCount.Load())
	require.Equal(t, int32(1), term.restoreCount.Load())

	h1 := helloFromSend(t, tr1)
	h2 := helloFromSend(t, tr2)
	h3 := helloFromSend(t, tr3)
	require.Equal(t, ports.IntentAttach, h1.Intent)
	require.Zero(t, h1.ResumeToken)
	require.Equal(t, ports.IntentResume, h2.Intent)
	require.Equal(t, uint64(11), h2.ResumeToken)
	require.Equal(t, ports.IntentResume, h3.Intent)
	require.Equal(t, uint64(22), h3.ResumeToken)
	require.Equal(t, h1.ClientID, h2.ClientID)
	require.Equal(t, h1.ClientID, h3.ClientID)
	require.Contains(t, term.out.String(), "reconnecting…")
	require.Contains(t, term.out.String(), "\x1b[2K")
}

func TestRunDoesNotRetryTerminalDetachedError(t *testing.T) {
	term := newRunTerminal()
	defer term.in.unblock()
	tr := &recordingTransport{recvs: []recvItem{{f: welcomeFrame(11)}, {f: frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonSessionKilled}))}}}
	d := &sequenceDialer{trs: []ports.Transport{tr}}

	err := runTestClient(context.Background(), testDependencies(d, term, realClock{}, nil, nil), client.AttachRequest{Intent: ports.IntentAttach, SessionName: "main", Remote: false})
	require.Error(t, err)
	var de *client.DetachedError
	require.True(t, errors.As(err, &de))
	require.Equal(t, int32(1), d.calls.Load())
	require.Equal(t, int32(1), term.restoreCount.Load())
}

func TestAttachSchemeRequeryCreatesPaletteGenerationBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	allowSecond := make(chan struct{})
	inputDone := make(chan struct{})
	input := &queryBoundaryReader{
		first:       []byte("\x1b]4;1;#112233\a\x1b[?997;2n"),
		second:      []byte("\x1b]4;2;#445566\a"),
		secondRead:  make(chan struct{}, 1),
		allowSecond: allowSecond,
		done:        inputDone,
	}
	defer close(inputDone)

	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	queryStarted := make(chan struct{})
	allowQuery := make(chan struct{})
	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	tm.EXPECT().QueryColors().RunAndReturn(func() error {
		close(queryStarted)
		<-allowQuery
		return nil
	}).Once()

	themes := make(chan ports.Theme, 3)
	recvRelease := make(chan struct{})
	var welcomed atomic.Bool
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgTheme)).RunAndReturn(func(f ports.Frame) error {
		theme, err := ports.UnmarshalTheme(f.Payload)
		if err != nil {
			t.Errorf("decode sent theme: %v", err)
			return err
		}
		themes <- theme
		return nil
	}).Times(3)
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		if welcomed.CompareAndSwap(false, true) {
			return frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"})), nil
		}
		<-recvRelease
		return ports.Frame{}, io.EOF
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	runDone := make(chan error, 1)
	go func() {
		runDone <- runTestClient(ctx, attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral})
	}()

	select {
	case <-queryStarted:
	case <-time.After(time.Second):
		t.Fatal("main loop did not start the palette re-query")
	}
	select {
	case <-input.secondRead:
		t.Fatal("stdin read the next chunk before the cleared theme and query completed")
	default:
	}

	close(allowQuery)
	select {
	case <-input.secondRead:
	case <-time.After(time.Second):
		t.Fatal("stdin did not resume after the palette re-query completed")
	}
	close(allowSecond)

	var got [3]ports.Theme
	for i := range got {
		select {
		case got[i] = <-themes:
		case <-time.After(time.Second):
			t.Fatalf("theme %d was not sent", i)
		}
	}
	require.Equal(t, uint16(1<<1), got[0].PaletteKnown)
	require.True(t, got[0].SchemeKnown)
	require.Zero(t, got[1].PaletteKnown, "the old palette must be cleared before a later read is admitted")
	require.True(t, got[1].SchemeKnown)
	require.Equal(t, uint16(1<<2), got[2].PaletteKnown, "only the post-query chunk may populate the new palette generation")

	cancel()
	select {
	case err := <-runDone:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("attach did not return after cancellation")
	}
	close(recvRelease)
	require.Equal(t, int32(1), restoreCount.Load())
}

func TestAttachSchemeRequeryClearsStalePalette(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	input := newChunkedBlockingReader(
		[]byte("\x1b]11;#010203\a\x1b]4;1;#112233\a\x1b]4;14;#778899\a"),
		[]byte("\x1b[?997;2n"),
	)
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Once()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount.Add(1); return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()
	tm.EXPECT().QueryColors().Return(nil).Once()

	var themes []ports.Theme
	var themesMu sync.Mutex
	clearedPalette := make(chan struct{})
	var clearOnce sync.Once
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgTheme)).RunAndReturn(func(f ports.Frame) error {
		theme, err := ports.UnmarshalTheme(f.Payload)
		if err != nil {
			t.Errorf("decode sent theme: %v", err)
			return err
		}
		themesMu.Lock()
		themes = append(themes, theme)
		themesMu.Unlock()
		if theme.SchemeKnown && theme.PaletteKnown == 0 {
			clearOnce.Do(func() { close(clearedPalette) })
		}
		return nil
	}).Times(3)

	welcome := frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))
	detached := frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (ports.Frame, error) {
		select {
		case <-closed:
			return ports.Frame{}, io.EOF
		default:
		}
		select {
		case <-clearedPalette:
			close(closed)
			return detached, nil
		default:
			return welcome, nil
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	require.NoError(t, runTestClient(context.Background(), attachTestDependencies(tr, tm, realClock{}), client.AttachRequest{Intent: ports.IntentEphemeral}))
	require.Equal(t, int32(1), restoreCount.Load())

	themesMu.Lock()
	defer themesMu.Unlock()
	require.Len(t, themes, 3, "one complete snapshot per input chunk plus the re-query invalidation")
	require.Equal(t, uint16(1<<1|1<<14), themes[0].PaletteKnown)
	require.True(t, themes[0].HasBackground)
	require.Equal(t, uint16(1<<1|1<<14), themes[1].PaletteKnown, "scheme update retains palette until re-query")
	require.True(t, themes[1].SchemeKnown)
	require.Zero(t, themes[2].PaletteKnown, "re-query must invalidate stale palette entries")
	require.Equal(t, themes[0].Background, themes[2].Background)
	require.True(t, themes[2].SchemeKnown)
	require.True(t, themes[2].Light)
}
