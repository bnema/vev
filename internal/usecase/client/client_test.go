package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
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
