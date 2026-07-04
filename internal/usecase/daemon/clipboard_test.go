package daemon

import (
	"context"
	"encoding/base64"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	scopy "github.com/bnema/vev/internal/usecase/copy"
	"github.com/bnema/vev/pkg/renderer"
)

// chunkReadPTY returns a MockPTY whose Read yields each chunk in order, then
// reports io.EOF — driving ptyReader through exactly len(chunks) reads before
// it unwinds via reapPane, matching the style of TestPTYQueryGetsResponseWrittenBackToPTY.
func chunkReadPTY(t *testing.T, chunks ...[]byte) *portsmocks.MockPTY {
	t.Helper()
	p := portsmocks.NewMockPTY(t)
	p.EXPECT().Read(mock.Anything).RunAndReturn(func(buf []byte) (int, error) {
		if len(chunks) == 0 {
			return 0, io.EOF
		}
		n := copy(buf, chunks[0])
		if n == len(chunks[0]) {
			chunks = chunks[1:]
		} else {
			chunks[0] = chunks[0][n:]
		}
		return n, nil
	})
	p.EXPECT().Write(mock.Anything).Return(0, nil).Maybe()
	p.EXPECT().Close().Return(nil).Maybe()
	return p
}

func TestPTYReaderForwardsOSC52ClipboardToAttachedClient(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b]52;c;aGVsbG8=\x07"))

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "clip", name: "clip", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	f := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(f.Payload)
	require.NoError(t, err)
	require.Contains(t, string(out.Data), "\x1b]52;c;aGVsbG8=\x07")
}

func TestPTYReaderDropsOversizedClipboardPayload(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	big := strings.Repeat("a", scopy.OSC52MaxPayloadBytes+1)
	b64 := base64.StdEncoding.EncodeToString([]byte(big))
	p := chunkReadPTY(t, []byte("\x1b]52;c;"+b64+"\x07"))

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "clip-big", name: "clip-big", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case f := <-sends:
		require.NotEqual(t, ports.MsgOutput, f.Type)
	default:
	}
}

func TestPTYReaderDropsInvalidBase64Clipboard(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b]52;c;not-valid-base64!!\x07"))

	tr, sends := newCapturingTransport(t)
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sess := &session{id: "clip-bad", name: "clip-bad", tabs: []*tab{win}, ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	d.ptyReader(sess, win, win.focusedPane())

	select {
	case f := <-sends:
		require.NotEqual(t, ports.MsgOutput, f.Type)
	default:
	}
}

func TestPTYReaderClipboardNoAttachedClientDoesNotPanic(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	p := chunkReadPTY(t, []byte("\x1b]52;c;aGVsbG8=\x07"))

	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	win := newTestTabWithContext(p, sctx, cancel)
	sess := &session{id: "clip-noclient", name: "clip-noclient", tabs: []*tab{win}, ctx: sctx, cancel: cancel}
	d.sessions[sess.id] = sess

	d.sessWg.Add(1)
	require.NotPanics(t, func() {
		d.ptyReader(sess, win, win.focusedPane())
	})
}

func TestForwardClipboardAsyncSerializesClipboardWrites(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	tr := newBlockingClipboardTransport()
	ac := &attachedClient{tr: tr, rend: renderer.New(renderer.Capabilities{})}
	sctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sess := &session{id: "clip-order", name: "clip-order", ctx: sctx, cancel: cancel, client: ac}
	ac.setSession(sess)

	first := base64.StdEncoding.EncodeToString([]byte("first"))
	second := base64.StdEncoding.EncodeToString([]byte("second"))
	d.forwardClipboardAsync(sess, first)
	require.Equal(t, "first", <-tr.started)

	d.forwardClipboardAsync(sess, second)
	select {
	case got := <-tr.started:
		t.Fatalf("second clipboard send started before first completed: %q", got)
	case <-time.After(50 * time.Millisecond):
	}

	close(tr.releaseFirst)
	require.Equal(t, "first", <-tr.sent)
	require.Equal(t, "second", <-tr.started)
	require.Equal(t, "second", <-tr.sent)
}

type blockingClipboardTransport struct {
	started      chan string
	sent         chan string
	releaseFirst chan struct{}
}

func newBlockingClipboardTransport() *blockingClipboardTransport {
	return &blockingClipboardTransport{
		started:      make(chan string, 2),
		sent:         make(chan string, 2),
		releaseFirst: make(chan struct{}),
	}
}

func (tr *blockingClipboardTransport) Send(f ports.Frame) error {
	out, err := ports.UnmarshalOutput(f.Payload)
	if err != nil {
		return err
	}
	data := string(out.Data)
	var label string
	switch {
	case strings.Contains(data, base64.StdEncoding.EncodeToString([]byte("first"))):
		label = "first"
	case strings.Contains(data, base64.StdEncoding.EncodeToString([]byte("second"))):
		label = "second"
	default:
		label = data
	}
	tr.started <- label
	if label == "first" {
		<-tr.releaseFirst
	}
	tr.sent <- label
	return nil
}

func (tr *blockingClipboardTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (tr *blockingClipboardTransport) Close() error               { return nil }
