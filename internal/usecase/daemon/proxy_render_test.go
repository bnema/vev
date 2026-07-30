package daemon

import (
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/renderer"
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

	require.True(t, proxy.resize(domain.Size{Cols: 20, Rows: 10}))
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

func proxyRowText(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, cell := range row {
		runes[i] = cell.Rune
	}
	return string(runes)
}
