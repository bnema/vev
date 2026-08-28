package client_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
	"github.com/bnema/vev/internal/usecase/client"
)

// clipboardRunTestDeps wires a Run() attempt with an input reader that
// delivers data then blocks (so the pump stays parked until detach), a
// transport that captures every MsgInput/MsgImagePush send, and a Recv that
// answers Welcome then, once triggered, Detached — mirroring the pattern used
// by the OSC-color-response test elsewhere in this package.
func runRemoteClipboardTest(t *testing.T, remote bool, clip ports.ClipboardReader, stdin []byte, triggerByte byte) (gotInput chan []byte, gotImage chan protocol.ImagePush) {
	t.Helper()
	var out bytes.Buffer
	var restoreCount int32
	resizeCh := make(chan domain.Geometry)
	input := newOneShotBlockingReader(stdin)
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Maybe()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount++; return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput = make(chan []byte, 8)
	gotImage = make(chan protocol.ImagePush, 1)
	allowDetach := make(chan struct{})

	tr := newMockClientConnection(t)
	tr.EXPECT().Send(isType(wire.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(wire.MsgResize)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(wire.MsgInput)).RunAndReturn(func(f wire.Frame) error {
		in, err := wire.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- append([]byte(nil), in.Data...)
		if bytes.IndexByte(in.Data, triggerByte) >= 0 {
			closeOnce(allowDetach)
		}
		return nil
	}).Maybe()
	tr.EXPECT().Send(isType(wire.MsgImagePush)).RunAndReturn(func(f wire.Frame) error {
		ip, err := wire.UnmarshalImagePush(f.Payload)
		require.NoError(t, err)
		gotImage <- ip
		return nil
	}).Maybe()
	tr.EXPECT().Send(isType(wire.MsgClientNotice)).Return(nil).Maybe()

	welcome := frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))
	detached := frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))
	recvCh := make(chan recvItem, 1)
	recvCh <- recvItem{f: welcome}
	closed := make(chan struct{})
	tr.EXPECT().Recv().RunAndReturn(func() (wire.Frame, error) {
		select {
		case it := <-recvCh:
			return it.f, it.err
		case <-allowDetach:
			select {
			case <-closed:
				return wire.Frame{}, io.EOF
			default:
				close(closed)
				return detached, nil
			}
		case <-closed:
			return wire.Frame{}, io.EOF
		}
	}).Maybe()
	tr.EXPECT().Close().Return(nil).Once()

	d := newMockClientDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, clip, nil), client.AttachRequest{Intent: protocol.IntentEphemeral, Remote: remote})
	require.NoError(t, err)
	return gotInput, gotImage
}

func closeOnce(ch chan struct{}) {
	select {
	case <-ch:
	default:
		close(ch)
	}
}

func drainInput(gotInput chan []byte) []byte {
	var b []byte
	for {
		select {
		case chunk := <-gotInput:
			b = append(b, chunk...)
		default:
			return b
		}
	}
}

func TestRunRemoteClipboardCtrlVWithImageSendsImagePushNoCtrlVForwarded(t *testing.T) {
	clip := portsmocks.NewMockClipboardReader(t)
	clip.EXPECT().ReadImage(mock.Anything).Return("image/png", []byte("PNGDATA"), nil).Once()

	gotInput, gotImage := runRemoteClipboardTest(t, true, clip, []byte("a\x16b"), 'b')

	select {
	case ip := <-gotImage:
		require.Equal(t, uint64(2), ip.InputSeq)
		require.Equal(t, "image/png", ip.Mime)
		require.Equal(t, []byte("PNGDATA"), ip.Data)
	case <-time.After(2 * time.Second):
		t.Fatal("MsgImagePush was not sent")
	}
	require.Equal(t, []byte("ab"), drainInput(gotInput), "Ctrl+V must not be forwarded as input when an image is sent")
}

func TestRunRemoteClipboardCtrlVWithNoImageForwardsCtrlV(t *testing.T) {
	clip := portsmocks.NewMockClipboardReader(t)
	clip.EXPECT().ReadImage(mock.Anything).Return("", nil, ports.ErrNoClipboardImage).Once()

	gotInput, _ := runRemoteClipboardTest(t, true, clip, []byte("a\x16b"), 'b')

	require.Equal(t, []byte("a\x16b"), drainInput(gotInput), "Ctrl+V must be forwarded verbatim when the clipboard has no image")
}

type clipboardToastLifecycleTransport struct {
	recv  chan wire.Frame
	sends chan wire.Frame
}

func (t *clipboardToastLifecycleTransport) Send(f wire.Frame) error {
	t.sends <- f
	return nil
}

func (t *clipboardToastLifecycleTransport) Recv() (wire.Frame, error) { return <-t.recv, nil }
func (*clipboardToastLifecycleTransport) Close() error                { return nil }

type clipboardToastLifecycleDialer struct{ transport ports.Transport }

func (d clipboardToastLifecycleDialer) Dial(context.Context) (ports.ClientConnection, error) {
	return &rawClientConnection{raw: d.transport}, nil
}

func TestRunRemoteClipboardFailureNotifiesDaemonAndWritesOutputVerbatim(t *testing.T) {
	var out bytes.Buffer
	resizeEvents := make(chan domain.Geometry)
	input := newOneShotBlockingReader([]byte{0x16})
	defer input.unblock()
	term := portsmocks.NewMockTerminal(t)
	term.EXPECT().Geometry().Return(domain.Geometry{Size: domain.Size{Cols: 80, Rows: 24}}, nil).Maybe()
	term.EXPECT().EnterRaw().Return(func() error { return nil }, nil).Once()
	term.EXPECT().In().Return(input).Maybe()
	term.EXPECT().Out().Return(&out).Maybe()
	term.EXPECT().Flush().Return(nil).Maybe()
	term.EXPECT().ResizeEvents().Return((<-chan domain.Geometry)(resizeEvents)).Maybe()

	clipboard := portsmocks.NewMockClipboardReader(t)
	clipboard.EXPECT().ReadImage(mock.Anything).Return("", nil, errors.New("read failed")).Once()
	transport := &clipboardToastLifecycleTransport{recv: make(chan wire.Frame, 8), sends: make(chan wire.Frame, 16)}
	transport.recv <- frameOf(wire.MsgWelcome, wire.MarshalWelcome(protocol.Welcome{SessionID: "s1"}))

	result := make(chan error, 1)
	go func() {
		result <- client.NewRunner(client.Dependencies{Dialer: clipboardToastLifecycleDialer{transport: transport}, Terminal: term, Clock: realClock{}, DisableCapabilityProbe: true, Clipboard: clipboard}).Run(context.Background(), client.AttachRequest{Intent: protocol.IntentEphemeral, Remote: true})
	}()

	var gotNotice bool
	for !gotNotice {
		select {
		case sent := <-transport.sends:
			if sent.Type != wire.MsgClientNotice {
				continue
			}
			notice, err := wire.UnmarshalClientNotice(sent.Payload)
			require.NoError(t, err)
			require.Equal(t, protocol.ClientNoticeClipboardFallback, notice.Action)
			gotNotice = true
		case <-time.After(time.Second):
			t.Fatal("client did not notify daemon of clipboard failure")
		}
	}
	beforeOutput := out.String()
	transport.recv <- frameOf(wire.MsgOutput, mustMarshalOutput(protocol.Output{Epoch: 1, Base: 0, New: 1, Size: domain.Size{Cols: 1, Rows: 1}, Full: true, Data: []byte("incremental")}))
	transport.recv <- frameOf(wire.MsgDetached, wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach}))

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("client did not finish")
	}
	require.Equal(t, beforeOutput+"incremental", out.String(), "daemon output must remain verbatim with no local toast bytes")
}

func TestRunRemoteClipboardOversizedImageForwardsCtrlV(t *testing.T) {
	huge := bytes.Repeat([]byte{0xff}, 1<<20+1) // one byte over the image-push cap
	clip := portsmocks.NewMockClipboardReader(t)
	clip.EXPECT().ReadImage(mock.Anything).Return("image/png", huge, nil).Once()

	gotInput, _ := runRemoteClipboardTest(t, true, clip, []byte("a\x16b"), 'b')

	require.Equal(t, []byte("a\x16b"), drainInput(gotInput), "an oversized clipboard image must not be swallowed: Ctrl+V is forwarded instead")
}

func TestRunRemoteClipboardCtrlVInsideBracketedPasteIsNotIntercepted(t *testing.T) {
	// A MockClipboardReader with no .EXPECT() set fails the test if
	// ReadImage is ever called — which must never happen for a 0x16 that
	// arrives as part of pasted content, even when the whole paste (open
	// marker, content, close marker) lands in a single read alongside it.
	clip := portsmocks.NewMockClipboardReader(t)

	paste := []byte("\x1b[200~pa\x16ste\x1b[201~Z")
	gotInput, gotImage := runRemoteClipboardTest(t, true, clip, paste, 'Z')

	require.Equal(t, paste, drainInput(gotInput), "a Ctrl+V byte inside a bracketed paste must be forwarded as ordinary paste content, unsplit")
	select {
	case ip := <-gotImage:
		t.Fatalf("clipboard image must not be pushed for a Ctrl+V inside a paste, got %+v", ip)
	default:
	}
}

func TestRunLocalAttachDoesNotInterceptCtrlVEvenIfClipboardConfigured(t *testing.T) {
	// A MockClipboardReader with no .EXPECT() set fails the test if ReadImage
	// is ever called, which is exactly what a local (non-remote) attach must
	// never do.
	clip := portsmocks.NewMockClipboardReader(t)

	gotInput, _ := runRemoteClipboardTest(t, false, clip, []byte("a\x16b"), 'b')

	require.Equal(t, []byte("a\x16b"), drainInput(gotInput), "local attach must forward Ctrl+V untouched")
}
