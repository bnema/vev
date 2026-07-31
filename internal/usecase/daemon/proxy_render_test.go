package daemon

import (
	"io"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProxyRenderComposesRemoteVTUnderExactlyOneLocalChrome(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 16, Rows: 6})
	require.NoError(t, err)
	proxy.mu.Lock()
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}}
	proxy.mu.Unlock()
	_, _, changed := proxy.applyOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("\x1b[2;3Hremote")})
	require.True(t, changed)

	state, ok := proxy.capturePrimary(&attachedClient{}, primaryCaptureRequest{
		bars:  barState{status: proxy.statusSegments(false)},
		reset: true,
	})
	require.True(t, ok)
	composed := composeFrame(*state, composeCacheInput{})

	require.Equal(t, 6, composed.frame.Height)
	require.Contains(t, proxyRowText(composed.frame.Row(0)), "shell", "one local top bar")
	require.Contains(t, proxyRowText(composed.frame.Row(5)), "work@arch", "one local bottom bar")
	require.NotContains(t, proxyRowText(composed.frame.Row(1)), "shell")
	require.NotContains(t, proxyRowText(composed.frame.Row(4)), "work@arch")
	require.Equal(t, 'r', composed.frame.At(2, 2).Rune, "ANSI was applied only by the local VT")
	require.False(t, composed.cursor.hidden)
	require.Equal(t, 2, composed.cursor.row, "cursor includes exactly the one local top bar")
	require.Contains(t, composed.damage, renderer.FullRedraw())
}

func TestProxyRenderPaintsAfterLocalToRemoteHandoff(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 16, Rows: 6})
	require.NoError(t, err)
	proxy.mu.Lock()
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}}
	proxy.mu.Unlock()
	_, _, changed := proxy.applyOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("remote")})
	require.True(t, changed)

	d := newTestDaemon(t, nil, stubClock{})
	transport, sent := newCapturingTransport(t)
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), size: domain.Size{Cols: 16, Rows: 6}}
	ac.setSession(proxy)
	proxy.sessionCore.mu.Lock()
	proxy.client = ac
	proxy.sessionCore.mu.Unlock()

	d.paint(proxy, ac, true, nil)
	frame := awaitTestValue(t, sent, "handoff to remote proxy did not paint")
	require.Equal(t, ports.MsgOutput, frame.Type, "handoff to remote proxy gets a local first paint")
}

func TestProxyRenderTransitionsPaintBothLocalAndRemoteSessions(t *testing.T) {
	d, local, ac, sent := newManualSessionWithPTYs(t, newQuietPTY())
	localCoordinator := d.attachCoordinator(local, nil, ac, true)
	localToken := attachmentToken(local, ac, ac.transport())
	localToken.lease = localCoordinator.attachmentLease(ac)

	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, ac.size)
	require.NoError(t, err)
	proxy.mu.Lock()
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}}
	proxy.mu.Unlock()
	_, _, changed := proxy.applyOutput(ports.Output{BaseStateNum: 0, NewStateNum: 1, Data: []byte("remote")})
	require.True(t, changed)
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	toProxy, err := d.transitionAttachment(attachmentTransitionRequest{
		source: local, target: proxy, next: ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: ac.transportSnapshot(), sourceToken: &localToken,
		action: "test transition", ready: true,
	})
	require.NoError(t, err)
	require.NotNil(t, toProxy.published.lease, "proxy transition installs a coordinator lease")
	require.True(t, d.firstPaintForTransition(toProxy.published))
	require.Equal(t, ports.MsgOutput, awaitTestValue(t, sent, "transition to proxy did not paint").Type)

	toLocal, err := d.transitionAttachment(attachmentTransitionRequest{
		source: proxy, target: local, next: ac,
		expectedRole: attachmentActive, targetRole: attachmentActive,
		expectedTransport: ac.transportSnapshot(), sourceToken: &toProxy.published,
		action: "test transition", ready: true,
	})
	require.NoError(t, err)
	require.True(t, d.firstPaintForTransition(toLocal.published))
	require.Equal(t, ports.MsgOutput, awaitTestValue(t, sent, "transition to local session did not paint").Type)
}

func TestProxyResizeUsesReducedRemoteGeometryOnce(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 12, Rows: 8})
	require.NoError(t, err)
	transport := newProxyTestTransport()
	proxy.mu.Lock()
	proxy.transport = transport
	proxy.linkGeneration = 1
	proxy.mu.Unlock()

	changed, sent := proxy.resize(domain.Size{Cols: 20, Rows: 10})
	require.True(t, changed)
	require.True(t, sent)
	frame := awaitTestValue(t, transport.sent, "proxy resize did not reach remote transport")
	require.Equal(t, ports.MsgResize, frame.Type)
	resize, err := ports.UnmarshalResize(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, domain.Size{Cols: 20, Rows: 8}, resize.Size)
	select {
	case extra := <-transport.sent:
		t.Fatalf("unexpected second remote resize: %#v", extra)
	default:
	}

	state, ok := proxy.capturePrimary(&attachedClient{}, primaryCaptureRequest{reset: true})
	require.True(t, ok)
	composed := composeFrame(*state, composeCacheInput{})
	require.Equal(t, 10, composed.frame.Height, "local viewport remains full-sized")
	require.Equal(t, 8, state.layout.area.Height)
}

// newAttachedProxyFixture publishes one proxy with a local thin client bound to
// clientTr and its remote link bound to remoteTr, using no render coordinator so
// invalidation paints synchronously.
func newAttachedProxyFixture(t *testing.T, d *Daemon, clientTr ports.Transport, remoteTr ports.Transport) (*proxySession, *attachedClient) {
	t.Helper()
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 16, Rows: 6})
	require.NoError(t, err)
	proxy.mu.Lock()
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}}
	proxy.transport = remoteTr
	proxy.linkGeneration = 1
	proxy.mu.Unlock()
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	ac := &attachedClient{tr: clientTr, output: newOutputStateStream(), size: domain.Size{Cols: 16, Rows: 6}}
	ac.setSession(proxy)
	proxy.sessionCore.mu.Lock()
	proxy.client = ac
	proxy.sessionCore.mu.Unlock()
	return proxy, ac
}

func newFailingTransport(t *testing.T) *portsmocks.MockTransport {
	t.Helper()
	tr := portsmocks.NewMockTransport(t)
	tr.EXPECT().Send(mock.Anything).Return(io.ErrClosedPipe).Maybe()
	tr.EXPECT().Close().Return(nil).Maybe()
	return tr
}

// TestProxyResizeRepaintsLocalChromeWhenRemoteSendFails covers the split
// between "the local content rectangle moved" and "the remote was told". The
// local VT has already been resized, so the client owes a repaint either way.
func TestProxyResizeRepaintsLocalChromeWhenRemoteSendFails(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	clientTr, sent := newCapturingTransport(t)
	remote := newProxyTestTransport()
	remote.sendFails.Store(true)
	proxy, ac := newAttachedProxyFixture(t, d, clientTr, remote)

	d.resizeProxyForLease(proxy, ac, nil, domain.Size{Cols: 24, Rows: 10})

	frame := awaitTestValue(t, sent, "failed remote resize did not repaint local chrome")
	require.Equal(t, ports.MsgOutput, frame.Type)
	proxy.mu.Lock()
	content := proxy.contentSize
	proxy.mu.Unlock()
	require.Equal(t, contentSize(domain.Size{Cols: 24, Rows: 10}, false), content, "local VT resized despite the failed send")
}

// TestProxyResizeWithoutGeometryChangeDoesNotRepaint is the other half: an
// identical size is not a reason to repaint.
func TestProxyResizeWithoutGeometryChangeDoesNotRepaint(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	clientTr, sent := newCapturingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTr, newProxyTestTransport())

	d.resizeProxyForLease(proxy, ac, nil, domain.Size{Cols: 16, Rows: 6})

	select {
	case frame := <-sent:
		t.Fatalf("no-op resize repainted: %v", frame.Type)
	default:
	}
}

// TestProxyPrepareFailureRepaintsWithoutLocalSession covers a failed render
// transaction on an attachment that has no local session to report through.
func TestProxyPrepareFailureRepaintsWithoutLocalSession(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	clientTr, sent := newCapturingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTr, newProxyTestTransport())

	ac.sendMu.Lock() // emitFrame releases the transaction lock.
	state, ok := proxy.capturePrimary(ac, primaryCaptureRequest{bars: barState{status: proxy.statusSegments(false)}, reset: true})
	require.True(t, ok)
	composed := composeFrame(*state, composeCacheInput{})
	// A malformed frame is what makes outputStateStream.prepare fail.
	composed.frame = renderer.Frame{Width: 1}

	require.True(t, d.emitFrame(proxy, ac, state, composed))
	frame := awaitTestValue(t, sent, "prepare failure on a proxy did not schedule a repaint")
	require.Equal(t, ports.MsgOutput, frame.Type)
}

// TestProxySendErrorDetachesProxyAttachment covers the transport-failure
// cleanup path for an attachment with no local session lifecycle.
func TestProxySendErrorDetachesProxyAttachment(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	clientTr := newFailingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTr, newProxyTestTransport())

	d.paint(proxy, ac, true, nil)

	proxy.sessionCore.mu.Lock()
	client := proxy.client
	proxy.sessionCore.mu.Unlock()
	require.Nil(t, client, "a failed send must release the proxy attachment")
	require.Nil(t, ac.currentAttachmentSession(), "the client must no longer point at the proxy")
}

func proxyRowText(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, cell := range row {
		runes[i] = cell.Rune
	}
	return string(runes)
}
