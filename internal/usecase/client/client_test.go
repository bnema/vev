package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
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

// recvItem is one scripted Recv result.
type recvItem struct {
	f   ports.Frame
	err error
}

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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentAttach, "main")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
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

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
	require.NoError(t, err)
	select {
	case got := <-gotInput:
		require.Equal(t, []byte("\x1b[<0;1;1M"), got, "SGR mouse report must arrive as one intact MsgInput frame")
	case <-time.After(2 * time.Second):
		t.Fatal("mouse report input was not sent")
	}
}

func TestAttachForwardsResize(t *testing.T) {
	var out bytes.Buffer
	var restoreCount atomic.Int32
	resizeCh := make(chan domain.Size)
	tm, in := newHappyTerminal(t, &out, &restoreCount, resizeCh)
	defer in.unblock()

	detachAfterResize := make(chan struct{})

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

	// Push a resize event shortly after attach begins.
	go func() {
		time.Sleep(20 * time.Millisecond)
		resizeCh <- domain.Size{Cols: 120, Rows: 40}
	}()

	err := client.Attach(context.Background(), tr, tm, ports.IntentEphemeral, "")
	require.NoError(t, err)
	select {
	case r := <-gotResize:
		require.Equal(t, domain.Size{Cols: 120, Rows: 40}, r.Size)
	default:
		t.Fatal("resize was not forwarded")
	}
}

type testDialer struct {
	tr    ports.Transport
	err   error
	calls atomic.Int32
}

func (d *testDialer) Dial(context.Context) (ports.Transport, error) {
	d.calls.Add(1)
	return d.tr, d.err
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
	d := &testDialer{tr: tr}

	err := client.Run(context.Background(), d, tm, ports.IntentEphemeral, "")
	require.Error(t, err)
	require.Equal(t, int32(1), restoreCount.Load(), "Run must restore raw mode after attachOnce errors")
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
	d := &testDialer{tr: tr}

	err := client.Run(context.Background(), d, tm, ports.IntentAttach, "main")
	require.Error(t, err)
	var pe *client.ProtocolError
	require.True(t, errors.As(err, &pe), "want *client.ProtocolError, got %T", err)
	require.Equal(t, int32(1), d.calls.Load())
}

func TestRunPhaseASingleAttempt(t *testing.T) {
	dialErr := errors.New("dial failed")
	d := &testDialer{err: dialErr}
	tm := portsmocks.NewMockTerminal(t)

	err := client.Run(context.Background(), d, tm, ports.IntentEphemeral, "")
	require.ErrorIs(t, err, dialErr)
	require.Equal(t, int32(1), d.calls.Load(), "Phase A Run must not retry")
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

	err := client.Run(context.Background(), d, term, ports.IntentAttach, "main")
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

	err := client.Run(context.Background(), d, term, ports.IntentAttach, "main")
	require.Error(t, err)
	var de *client.DetachedError
	require.True(t, errors.As(err, &de))
	require.Equal(t, int32(1), d.calls.Load())
	require.Equal(t, int32(1), term.restoreCount.Load())
}
