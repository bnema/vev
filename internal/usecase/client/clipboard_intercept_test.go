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
	"github.com/bnema/vev/internal/usecase/client"
)

// clipboardRunTestDeps wires a Run() attempt with an input reader that
// delivers data then blocks (so the pump stays parked until detach), a
// transport that captures every MsgInput/MsgImagePush send, and a Recv that
// answers Welcome then, once triggered, Detached — mirroring the pattern used
// by the OSC-color-response test elsewhere in this package.
func runRemoteClipboardTest(t *testing.T, remote bool, clip ports.ClipboardReader, stdin []byte, triggerByte byte) (gotInput chan []byte, gotImage chan ports.ImagePush) {
	t.Helper()
	var out bytes.Buffer
	var restoreCount int32
	resizeCh := make(chan domain.Size)
	input := newOneShotBlockingReader(stdin)
	defer input.unblock()

	tm := portsmocks.NewMockTerminal(t)
	tm.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Maybe()
	tm.EXPECT().EnterRaw().Return(func() error { restoreCount++; return nil }, nil).Once()
	tm.EXPECT().In().Return(input).Maybe()
	tm.EXPECT().Out().Return(&out).Maybe()
	tm.EXPECT().Flush().Return(nil).Maybe()
	tm.EXPECT().ResizeEvents().Return(resizeCh).Maybe()

	gotInput = make(chan []byte, 8)
	gotImage = make(chan ports.ImagePush, 1)
	allowDetach := make(chan struct{})

	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(isType(ports.MsgTheme)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(ports.MsgHello)).Return(nil).Once()
	tr.EXPECT().Send(isType(ports.MsgResize)).Return(nil).Maybe()
	tr.EXPECT().Send(isType(ports.MsgInput)).RunAndReturn(func(f ports.Frame) error {
		in, err := ports.UnmarshalInput(f.Payload)
		require.NoError(t, err)
		gotInput <- append([]byte(nil), in.Data...)
		if bytes.IndexByte(in.Data, triggerByte) >= 0 {
			closeOnce(allowDetach)
		}
		return nil
	}).Maybe()
	tr.EXPECT().Send(isType(ports.MsgImagePush)).RunAndReturn(func(f ports.Frame) error {
		ip, err := ports.UnmarshalImagePush(f.Payload)
		require.NoError(t, err)
		gotImage <- ip
		return nil
	}).Maybe()

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

	d := portsmocks.NewMockDialer(t)
	d.EXPECT().Dial(mock.Anything).Return(tr, nil).Once()

	err := runTestClient(context.Background(), testDependencies(d, tm, realClock{}, clip, nil), client.AttachRequest{Intent: ports.IntentEphemeral, Remote: remote})
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
	recv  chan ports.Frame
	sends chan ports.Frame
}

func (t *clipboardToastLifecycleTransport) Send(f ports.Frame) error {
	t.sends <- f
	return nil
}

func (t *clipboardToastLifecycleTransport) Recv() (ports.Frame, error) { return <-t.recv, nil }
func (*clipboardToastLifecycleTransport) Close() error                 { return nil }

type clipboardToastLifecycleDialer struct{ transport ports.Transport }

func (d clipboardToastLifecycleDialer) Dial(context.Context) (ports.Transport, error) {
	return d.transport, nil
}

func TestRunRemoteClipboardFailureReconcilesOnResetFrame(t *testing.T) {
	var out bytes.Buffer
	resizeEvents := make(chan domain.Size)
	input := newOneShotBlockingReader([]byte{0x16})
	defer input.unblock()
	term := portsmocks.NewMockTerminal(t)
	term.EXPECT().Size().Return(domain.Size{Cols: 80, Rows: 24}, nil).Maybe()
	term.EXPECT().EnterRaw().Return(func() error { return nil }, nil).Once()
	term.EXPECT().In().Return(input).Maybe()
	term.EXPECT().Out().Return(&out).Maybe()
	term.EXPECT().Flush().Return(nil).Maybe()
	term.EXPECT().ResizeEvents().Return((<-chan domain.Size)(resizeEvents)).Maybe()

	clipboard := portsmocks.NewMockClipboardReader(t)
	clipboard.EXPECT().ReadImage(mock.Anything).Return("", nil, errors.New("read failed")).Once()
	transport := &clipboardToastLifecycleTransport{recv: make(chan ports.Frame, 8), sends: make(chan ports.Frame, 16)}
	transport.recv <- frameOf(ports.MsgWelcome, ports.MarshalWelcome(ports.Welcome{SessionID: "s1"}))

	result := make(chan error, 1)
	go func() {
		result <- client.NewRunner(client.Dependencies{Dialer: clipboardToastLifecycleDialer{transport: transport}, Terminal: term, Clock: realClock{}, Clipboard: clipboard}).Run(context.Background(), client.AttachRequest{Intent: ports.IntentEphemeral, Remote: true})
	}()

	for {
		select {
		case sent := <-transport.sends:
			if sent.Type != ports.MsgResize {
				continue
			}
			goto resized
		case <-time.After(time.Second):
			t.Fatal("client did not request a daemon reset after drawing clipboard toast")
		}
	}

resized:
	require.Contains(t, out.String(), "image paste failed; sent Ctrl+V")
	transport.recv <- frameOf(ports.MsgOutput, ports.MarshalOutput(ports.Output{BaseStateNum: 1, NewStateNum: 2, Data: []byte("incremental")}))
	for {
		select {
		case sent := <-transport.sends:
			if sent.Type != ports.MsgAck {
				continue
			}
			ack, err := ports.UnmarshalAck(sent.Payload)
			require.NoError(t, err)
			if ack.AckedStateNum == 2 {
				goto incrementalAcknowledged
			}
		case <-time.After(time.Second):
			t.Fatal("client did not ACK discarded incremental output")
		}
	}

incrementalAcknowledged:
	require.NotContains(t, out.String(), "incremental", "incremental output must not overwrite a local toast before reset")
	transport.recv <- frameOf(ports.MsgOutput, ports.MarshalOutput(ports.Output{BaseStateNum: 0, NewStateNum: 3, Data: []byte("authoritative reset")}))
	transport.recv <- frameOf(ports.MsgDetached, ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}))

	select {
	case err := <-result:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("client did not finish after reset frame")
	}
	got := out.String()
	require.NotContains(t, got, "incremental", "incremental output must not overwrite a local toast before reset")
	require.Contains(t, got, "authoritative reset")
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
