package daemon

import (
	"fmt"
	"io"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestProxyCaptureRetainsOwnedFrameAcrossSafeDamage(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 4, Rows: 3})
	require.NoError(t, err)
	proxy.mu.Lock()
	generation := proxy.linkGeneration
	snapshot := screenSnapshot(4, 3, "abcd", "efgh", "ijkl")
	require.NoError(t, proxy.screen.Apply(snapshot))
	proxy.screenReady = true
	proxy.appliedState = snapshot.NewStateNum
	proxy.mu.Unlock()

	ac := &attachedClient{}
	state, ok := proxy.captureRenderState(ac, renderCaptureRequest{})
	require.True(t, ok)
	require.Equal(t, "abcdefghijkl", proxyFrameText(state.panes[0].frame))
	commitDamageReceipts(state.receipts)

	proxy.mu.Lock()
	delta := ports.ScreenUpdate{
		Kind:         ports.ScreenUpdateDelta,
		BaseStateNum: 1,
		NewStateNum:  2,
		Size:         domain.Size{Cols: 4, Rows: 3},
		Spans:        []ports.ScreenSpan{{Y: 1, X: 1, Cells: cells("Z")}},
		Cursor:       ports.ScreenCursor{Visible: true},
	}
	require.NoError(t, proxy.screen.Apply(delta))
	proxy.mu.Unlock()

	cellsBefore := &ac.proxyCapture.frame.Cells[0]
	state, ok = proxy.captureRenderState(ac, renderCaptureRequest{})
	require.True(t, ok)
	require.Equal(t, "abcdeZghijkl", proxyFrameText(state.panes[0].frame))
	require.Equal(t, cellsBefore, &ac.proxyCapture.frame.Cells[0], "safe span damage must update the retained frame")
	require.Equal(t, generation, proxy.linkGeneration)
}

func TestProxyRenderComposesRemoteVTUnderExactlyOneLocalChrome(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 16, Rows: 6})
	require.NoError(t, err)
	proxy.mu.Lock()
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}}
	proxy.mu.Unlock()
	changed := applyTestScreenText(proxy, 1, 2, "remote")
	require.True(t, changed)

	state, ok := proxy.captureRenderState(&attachedClient{}, renderCaptureRequest{
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
	changed := applyTestScreenText(proxy, 0, 0, "remote")
	require.True(t, changed)

	d := newTestDaemon(t, nil, stubClock{})
	transport, sent := newCapturingTransport(t)
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), size: domain.Size{Cols: 16, Rows: 6}}
	ac.setSession(proxy)
	proxy.sessionCore.mu.Lock()
	proxy.sessionCore.registerAttachmentLocked(ac)
	proxy.sessionCore.mu.Unlock()

	d.paint(proxy, ac, true, nil)
	frame := awaitTestValue(t, sent, "handoff to remote proxy did not paint")
	require.Equal(t, ports.MsgOutput, frame.Type, "handoff to remote proxy gets a local first paint")
}

func TestProxyRenderTransitionsPaintBothLocalAndRemoteSessions(t *testing.T) {
	d, local, ac, sent := newManualSessionWithPTYs(t, newQuietPTY())
	localCoordinator := d.attachCoordinator(local, ac, true)
	localToken := attachmentToken(local, ac, ac.transport())
	localToken.lease = localCoordinator.attachmentLease(ac)

	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, ac.size)
	require.NoError(t, err)
	proxy.mu.Lock()
	proxy.meta = ports.SessionMeta{SessionName: "work", Tabs: []ports.SessionTabMeta{{Index: 0, Name: "shell"}}}
	proxy.mu.Unlock()
	changed := applyTestScreenText(proxy, 0, 0, "remote")
	require.True(t, changed)
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	toProxy, err := d.transitionAttachment(attachmentTransitionRequest{
		source: local, target: proxy, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &localToken,
		action: "test transition", ready: true,
	})
	require.NoError(t, err)
	require.NotNil(t, toProxy.published.lease, "proxy transition installs a coordinator lease")
	require.True(t, d.firstPaintForTransition(toProxy.published))
	require.Equal(t, ports.MsgOutput, awaitTestValue(t, sent, "transition to proxy did not paint").Type)

	toLocal, err := d.transitionAttachment(attachmentTransitionRequest{
		source: proxy, target: local, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &toProxy.published,
		action: "test transition", ready: true,
	})
	require.NoError(t, err)
	require.True(t, d.firstPaintForTransition(toLocal.published))
	require.Equal(t, ports.MsgOutput, awaitTestValue(t, sent, "transition to local session did not paint").Type)
}

func TestProxyResizeRejectsOversizedGeometryBeforeMutationOrForwarding(t *testing.T) {
	for _, size := range []domain.Size{
		{Cols: 512, Rows: 515},
		{Cols: 1000, Rows: 1000},
	} {
		t.Run(fmt.Sprintf("%dx%d", size.Cols, size.Rows), func(t *testing.T) {
			proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 80, Rows: 24})
			require.NoError(t, err)
			transport := newProxyTestTransport()
			proxy.mu.Lock()
			proxy.transport = transport
			proxy.linkGeneration = 1
			initial := proxy.contentSize
			proxy.mu.Unlock()

			changed, sent := proxy.resize(size)
			require.False(t, changed)
			require.False(t, sent)
			proxy.mu.Lock()
			require.Equal(t, initial, proxy.contentSize)
			proxy.mu.Unlock()
			select {
			case frame := <-transport.sent:
				t.Fatalf("oversized resize was forwarded: %#v", frame)
			default:
			}
		})
	}
}

func TestProxyResizeForLeaseRejectsOversizedGeometryBeforeMutationOrPaint(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	client, clientSent := newCapturingTransport(t)
	remote := newProxyTestTransport()
	proxy, ac := newAttachedProxyFixture(t, d, client, remote)
	initialContent := proxy.contentSize
	initialViewport := ac.size

	d.resizeProxyForLease(proxy, ac, nil, domain.Size{Cols: 1000, Rows: 1000})

	proxy.mu.Lock()
	require.Equal(t, initialContent, proxy.contentSize)
	proxy.mu.Unlock()
	require.Equal(t, initialViewport, ac.size)
	select {
	case frame := <-clientSent:
		t.Fatalf("oversized resize painted the client: %#v", frame)
	default:
	}
	select {
	case frame := <-remote.sent:
		t.Fatalf("oversized resize was forwarded: %#v", frame)
	default:
	}
}

func TestProxyResizeKeepsPlaceholderUntilSnapshot(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 4, Rows: 4})
	require.NoError(t, err)
	transport := newProxyTestTransport()
	proxy.mu.Lock()
	initial := screenSnapshot(4, 2, "abcd", "efgh")
	initial.NewStateNum = 7
	require.NoError(t, proxy.screen.Apply(initial))
	proxy.transport = transport
	proxy.linkGeneration = 1
	proxy.screenReady = true
	proxy.appliedState = initial.NewStateNum
	proxy.mu.Unlock()

	changed, sent := proxy.resize(domain.Size{Cols: 3, Rows: 5})
	require.True(t, changed)
	require.True(t, sent)
	awaitTestValue(t, transport.sent, "proxy resize did not reach remote transport")

	proxy.mu.Lock()
	require.Equal(t, domain.Size{Cols: 3, Rows: 3}, proxy.contentSize)
	require.Equal(t, "abcefg   ", proxyFrameText(proxy.screen.frame))
	require.Equal(t, uint64(7), proxy.appliedState, "resize must preserve the moving snapshot floor")
	require.False(t, proxy.screenReady)
	require.False(t, proxy.resetRequested)
	require.Zero(t, proxy.screen.stateNum)
	proxy.mu.Unlock()

	_, requestReset, changed := proxy.applyScreenUpdateForGeneration(1, ports.ScreenUpdate{
		Kind:         ports.ScreenUpdateDelta,
		BaseStateNum: 7,
		NewStateNum:  8,
		Size:         domain.Size{Cols: 3, Rows: 3},
		Cursor:       ports.ScreenCursor{Visible: true},
	})
	require.True(t, requestReset)
	require.False(t, changed)

	moving := screenSnapshot(3, 3, "123", "456", "789")
	moving.NewStateNum = 8
	_, requestReset, changed = proxy.applyScreenUpdateForGeneration(1, moving)
	require.False(t, requestReset)
	require.True(t, changed)
	proxy.mu.Lock()
	require.Equal(t, uint64(8), proxy.appliedState)
	require.Greater(t, proxy.appliedState, uint64(7))
	require.True(t, proxy.screenReady)
	require.False(t, proxy.resetRequested)
	proxy.mu.Unlock()
}

func TestProxyResizeUsesReducedRemoteGeometryOnce(t *testing.T) {
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, domain.Size{Cols: 12, Rows: 8})
	require.NoError(t, err)
	transport := newProxyTestTransport()
	proxy.mu.Lock()
	proxy.transport = transport
	proxy.linkGeneration = 1
	proxy.screenReady = true
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

	state, ok := proxy.captureRenderState(&attachedClient{}, renderCaptureRequest{reset: true})
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
	proxy.screenReady = true
	proxy.mu.Unlock()
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	ac := &attachedClient{tr: clientTr, output: newOutputStateStream(), size: domain.Size{Cols: 16, Rows: 6}}
	ac.setSession(proxy)
	proxy.sessionCore.mu.Lock()
	proxy.sessionCore.registerAttachmentLocked(ac)
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

func TestProxyStructuredSendFailureRetainsProxyDamage(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	clientTr := newFailingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, clientTr, newProxyTestTransport())
	ac.proxied = true
	ac.screenOutput = newStructuredOutputStream(ac.output)
	require.True(t, applyTestScreenText(proxy, 0, 0, "pending"))

	ac.sendMu.Lock()
	state, ok := proxy.captureRenderState(ac, renderCaptureRequest{reset: true})
	require.True(t, ok)
	composed := composeFrame(*state, composeCacheInput{})
	require.True(t, d.emitFrame(proxy, ac, state, composed))

	proxy.mu.Lock()
	damage := proxy.screen.CaptureDamage()
	proxy.mu.Unlock()
	require.NotEmpty(t, damage.Damage, "a failed structured send must leave proxy damage pending")
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
	state, ok := proxy.captureRenderState(ac, renderCaptureRequest{bars: barState{status: proxy.statusSegments(false)}, reset: true})
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
	attachments := proxy.sessionCore.snapshotAttachmentsLocked()
	proxy.sessionCore.mu.Unlock()
	require.Empty(t, attachments, "a failed send must release the proxy attachment")
	require.Nil(t, ac.currentAttachmentSession(), "the client must no longer point at the proxy")
}

func proxyRowText(row []renderer.Cell) string {
	runes := make([]rune, len(row))
	for i, cell := range row {
		runes[i] = cell.Rune
	}
	return string(runes)
}
