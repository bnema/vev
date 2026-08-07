package daemon

import (
	"strings"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func TestProxiedInputBypassesRemoteDaemonUI(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().Write([]byte{'\x02', 'p'}).Return(2, nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
	ac.renderMode = ports.RenderModeProxiedContent
	token := attachmentToken(sess, ac, ac.transport())
	require.True(t, token.attachmentCurrent())

	d.handleSequencedInputForAttachment(token, 1, []byte{'\x02', 'p'})
}

func TestProxiedInputTargetsVisibleFloatingPane(t *testing.T) {
	floatingPTY := portsmocks.NewMockPTY(t)
	floatingPTY.EXPECT().Write([]byte("remote-input")).Return(len("remote-input"), nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.renderMode = ports.RenderModeProxiedContent
	installTestFloating(testAttachmentTab(sess), newPane("floating", floatingPTY, domain.Size{Cols: 20, Rows: 5}), true)
	token := attachmentToken(sess, ac, ac.transport())
	require.True(t, token.attachmentCurrent())

	d.handleSequencedInputForAttachment(token, 1, []byte("remote-input"))
}

func TestProxiedCommandKeepsRequestCorrelation(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	ac.renderMode = ports.RenderModeProxiedContent
	token := attachmentToken(sess, ac, ac.transport())
	payload, err := ports.MarshalCommandRequest(ports.CommandRequest{
		Version: ports.ProtocolVersion, RequestID: 42, Attached: true, Slug: "unknown-command",
	})
	require.NoError(t, err)

	require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgCommand, Payload: payload}))
	frame := <-sends
	require.Equal(t, ports.MsgCommandResult, frame.Type)
	result, err := ports.UnmarshalCommandResult(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(42), result.RequestID)
	require.Equal(t, ports.ErrUnknownCommand, result.Code)
}

func TestProxiedResizeUsesContentGeometry(t *testing.T) {
	pty := portsmocks.NewMockPTY(t)
	pty.EXPECT().Resize(domain.Size{Cols: 80, Rows: 20}).Return(nil).Once()
	d, sess, ac, _ := newManualSessionWithPTYs(t, pty)
	ac.renderMode = ports.RenderModeProxiedContent
	token := attachmentToken(sess, ac, ac.transport())
	require.True(t, token.attachmentCurrent())

	d.resizeAttachmentForLease(token, domain.Size{Cols: 80, Rows: 20})
	ac.sizeMu.RLock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 20}, ac.size)
	ac.sizeMu.RUnlock()
	sess.tabs[0].mu.Lock()
	require.Equal(t, domain.Size{Cols: 80, Rows: 20}, sess.tabs[0].size)
	sess.tabs[0].mu.Unlock()
}

func TestProxiedCompositionUsesAttachmentContentWindowAndFloatingSurface(t *testing.T) {
	paneFrame := renderer.NewFrame(20, 10)
	paneFrame.Set(0, 0, renderer.Cell{Rune: 'p'})
	floatingFrame := renderer.NewFrame(3, 2)
	floatingFrame.Set(0, 0, renderer.Cell{Rune: 'f'})

	composed := composeProxiedContent(capturedRenderState{
		window: domain.Size{Cols: 10, Rows: 5},
		layout: capturedTabLayout{area: domain.Rect{Width: 20, Height: 10}},
		panes: []capturedPaneRenderState{{
			placement: layout.Placement{Content: domain.Rect{Width: 20, Height: 10}},
			frame:     paneFrame,
		}},
		floating: capturedFloatingRenderState{
			visible:  true,
			pane:     capturedPaneRenderState{frame: floatingFrame},
			geometry: floatingGeometry{Inner: domain.Rect{X: 4, Y: 2, Width: 3, Height: 2}},
		},
	})

	require.Equal(t, 10, composed.frame.Width)
	require.Equal(t, 5, composed.frame.Height)
	require.Equal(t, 'p', composed.frame.At(0, 0).Rune)
	require.Equal(t, 'f', composed.frame.At(4, 2).Rune)
	require.Equal(t, []renderer.Damage{renderer.FullRedraw()}, composed.damage)
}

func TestProxiedPaintFlushesRuntimeMarks(t *testing.T) {
	observer := &daemonRuntimeObserver{}
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.runtimeObserver = observer
	ac.renderMode = ports.RenderModeProxiedContent

	require.Equal(t, paintEmitted, d.paintProxiedContent(sess, ac, true, nil))
	require.NotEmpty(t, observer.marks, "proxied rendering must flush observer marks after releasing sendMu")
}

func TestProxiedOutputStartCommitsOnlyAfterAcceptedFrame(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.renderMode = ports.RenderModeProxiedContent
	state, ok := captureLocalRenderState(sess, ac, renderCaptureRequest{reset: true})
	require.True(t, ok)
	composed := composeProxiedContent(*state)

	state.attachment = &attachedClient{}
	ac.sendMu.Lock()
	require.False(t, d.emitFrame(sess, ac, state, composed))
	require.False(t, ac.proxiedOutputStarted, "rejected proxied output must remain retryable")

	state.attachment = ac
	ac.sendMu.Lock()
	require.True(t, d.emitFrame(sess, ac, state, composed))
	require.True(t, ac.proxiedOutputStarted, "accepted proxied output starts the output boundary")
}

func TestProxiedMetadataFallsBackFromStaleAttachmentTab(t *testing.T) {
	_, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	sess.mu.Lock()
	sess.incarnation = domain.SessionLifecycleID{1}
	sess.tabs[0].stableID = "live-tab"
	sess.mu.Unlock()
	ac.publishView(attachmentView{tabID: "stale-tab"})

	frame, err := frameSessionMeta(sess, ac, 1)
	require.NoError(t, err)
	meta, err := ports.UnmarshalSessionMeta(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, domain.TabStableID("live-tab"), meta.ActiveTabID)
	require.Equal(t, domain.TabStableID("live-tab"), meta.Tabs[0].ID)
}

func TestProxiedMetadataRevisionPrecedesSubsequentContent(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	sess.mu.Lock()
	sess.incarnation = domain.SessionLifecycleID{1}
	sess.tabs[0].stableID = "tab"
	sess.mu.Unlock()
	ac.renderMode = ports.RenderModeProxiedContent
	ac.proxiedOutputStarted = true
	ac.proxiedMetaRevision = 1

	ac.sendMu.Lock()
	failedTransport, err := d.sendProxiedMetadataLocked(sess, ac, nil)
	ac.sendMu.Unlock()
	require.NoError(t, err)
	require.Same(t, ac.transport(), failedTransport)
	frame := <-sends
	require.Equal(t, ports.MsgSessionMeta, frame.Type)
	meta, err := ports.UnmarshalSessionMeta(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(2), meta.Revision)
	require.Equal(t, uint64(2), ac.proxiedMetaRevision)
}

func TestProxiedHandshakePublishesStableMetadataBeforeOutput(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	base := &closeTrackingTransport{}
	sess, _, err := d.route(ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "remote",
		Size: domain.Size{Cols: 80, Rows: 24},
	}, base)
	require.NoError(t, err)
	sess.tabs[0].focusedPane().screen.Write([]byte("content-marker"))

	sess.mu.Lock()
	target := domain.RemoteSessionTarget{
		Endpoint: "user@host", DisplayOrigin: "host", LifecycleID: sess.incarnation,
		SessionName: sess.name, LiveTabID: domain.TabStableID(sess.tabs[0].stableID),
	}
	sess.mu.Unlock()

	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentAttach,
		RenderMode: ports.RenderModeProxiedContent, Name: target.SessionName,
		Size: domain.Size{Cols: 80, Rows: 22}, RemoteTarget: &target,
		EnvironmentPolicy: ports.EnvironmentPolicyDaemonOwned,
	}
	tr, sends, release := newConn(t,
		ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)},
		ports.Frame{Type: ports.MsgOutputResetRequest, Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{})},
	)
	done := make(chan struct{})
	go func() {
		d.handleConn(tr)
		close(done)
	}()

	welcomeFrame := <-sends
	require.Equal(t, ports.MsgWelcome, welcomeFrame.Type)
	welcome, err := ports.UnmarshalWelcome(welcomeFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.RenderModeProxiedContent, welcome.RenderMode)

	metadataFrame := <-sends
	require.Equal(t, ports.MsgSessionMeta, metadataFrame.Type)
	metadata, err := ports.UnmarshalSessionMeta(metadataFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, target.LifecycleID, metadata.LifecycleID)
	require.Equal(t, uint64(1), metadata.Revision)
	require.Equal(t, target.SessionName, metadata.SessionName)
	require.Equal(t, target.LiveTabID, metadata.ActiveTabID)
	require.Len(t, metadata.Tabs, 1)
	require.Equal(t, target.LiveTabID, metadata.Tabs[0].ID)

	outputFrame := <-sends
	require.Equal(t, ports.MsgOutput, outputFrame.Type)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.Zero(t, output.Base)
	require.Equal(t, uint64(1), output.New)
	require.True(t, output.Full)
	require.Equal(t, hello.Size, output.Size)
	private := vt.NewScreen(output.Size.Cols, output.Size.Rows)
	private.Write(output.Data)
	require.Contains(t, screenLineText(private, 0), "content-marker")
	for y := range private.Frame.Height {
		require.NotContains(t, strings.TrimSpace(screenLineText(private, y)), "remote", "proxied content must omit remote chrome")
	}

	resetMetadataFrame := <-sends
	require.Equal(t, ports.MsgSessionMeta, resetMetadataFrame.Type)
	resetMetadata, err := ports.UnmarshalSessionMeta(resetMetadataFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(2), resetMetadata.Revision)
	resetOutputFrame := <-sends
	require.Equal(t, ports.MsgOutput, resetOutputFrame.Type)
	resetOutput, err := ports.UnmarshalOutput(resetOutputFrame.Payload)
	require.NoError(t, err)
	require.Greater(t, resetOutput.Epoch, output.Epoch)
	require.Zero(t, resetOutput.Base)
	require.Equal(t, uint64(1), resetOutput.New)
	require.True(t, resetOutput.Full)
	private.Write(resetOutput.Data)
	require.Contains(t, screenLineText(private, 0), "content-marker")

	release()
	awaitTestCompletion(t, done, "proxied connection did not stop")
}
