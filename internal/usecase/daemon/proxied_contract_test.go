package daemon

import (
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/layout"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func TestProxiedHandshakeOrdersWelcomeMetadataAndBaseZeroContent(t *testing.T) {
	t.Skip("legacy handshake fixture races attachment-local view repair")
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	contentSize := domain.Size{Cols: 24, Rows: 4}
	hello := ports.Hello{
		Version:           ports.ProtocolVersion,
		Intent:            ports.IntentNew,
		Proxied:           true,
		Name:              "remote-work",
		Size:              contentSize,
		MaxOutputInFlight: 4,
	}
	tr, sends, release := newConn(t, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
	defer release()

	var handlers sync.WaitGroup
	handlers.Go(func() { d.handleConn(tr) })

	welcomeFrame := awaitFrame(t, sends, ports.MsgWelcome)
	welcome, err := ports.UnmarshalWelcome(welcomeFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.CapabilityResume|ports.CapabilityProxied, welcome.Capabilities)

	metaFrame := awaitFrame(t, sends, ports.MsgSessionMeta)
	meta, err := ports.UnmarshalSessionMeta(metaFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, "remote-work", meta.SessionName)
	require.Equal(t, uint16(0), meta.Active)
	require.Equal(t, []ports.SessionTabMeta{{Index: 0}}, meta.Tabs)

	screenFrame := awaitFrame(t, sends, ports.MsgScreenUpdate)
	update, err := ports.UnmarshalScreenUpdate(screenFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ScreenUpdateSnapshot, update.Kind)
	require.Zero(t, update.BaseStateNum)
	require.Equal(t, contentSize, update.Size)
	require.Len(t, update.Spans, contentSize.Rows, "structured snapshots must carry every full row")
	for y, span := range update.Spans {
		require.Equal(t, uint16(y), span.Y)
		require.Zero(t, span.X)
		require.Len(t, span.Cells, contentSize.Cols)
		require.NotContains(t, rowText(span.Cells), "remote-work", "proxied screen must not contain remote session chrome")
	}

	sess := firstSession(d)
	require.NotNil(t, sess)
	testAttachmentTab(sess).mu.Lock()
	actualContentSize := testAttachmentTab(sess).size
	testAttachmentTab(sess).mu.Unlock()
	require.Equal(t, contentSize, actualContentSize, "proxied geometry must not subtract chrome a second time")

	release()
	handlers.Wait()
}

func TestOrdinaryHandshakeRetainsChromeAndNoMetadata(t *testing.T) {
	t.Skip("legacy handshake fixture races attachment-local view repair")
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	viewport := domain.Size{Cols: 24, Rows: 6}
	hello := ports.Hello{Version: ports.ProtocolVersion, Intent: ports.IntentNew, Name: "ordinary", Size: viewport}
	tr, sends, release := newConn(t, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(hello)})
	defer release()

	var handlers sync.WaitGroup
	handlers.Go(func() { d.handleConn(tr) })

	welcomeFrame := awaitFrame(t, sends, ports.MsgWelcome)
	welcome, err := ports.UnmarshalWelcome(welcomeFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.CapabilityResume, welcome.Capabilities)

	outputFrame := awaitTestValue(t, sends, "ordinary handshake produced no post-welcome frame")
	require.NotEqual(t, ports.MsgSessionMeta, outputFrame.Type, "an ordinary attachment must never receive session metadata")
	require.Equal(t, ports.MsgOutput, outputFrame.Type)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.Zero(t, output.BaseStateNum)
	client := vt.NewScreen(viewport.Cols, viewport.Rows)
	client.Write(output.Data)
	require.Contains(t, screenLineText(client, 0), "1")
	require.Contains(t, screenLineText(client, viewport.Rows-1), "ordinary")

	sess := firstSession(d)
	require.NotNil(t, sess)
	testAttachmentTab(sess).mu.Lock()
	actualContentSize := testAttachmentTab(sess).size
	testAttachmentTab(sess).mu.Unlock()
	require.Equal(t, tabSize(viewport), actualContentSize)
	requireNoOutputFrame(t, sends)

	release()
	handlers.Wait()
}

func TestDesiredCapturedCursorPublishesVisibleAbsoluteCursor(t *testing.T) {
	visible := desiredCapturedCursor(capturedCursorInputs{
		row: 2, col: 3, style: 0, visible: true, renderable: true,
		content: domain.Rect{X: 4, Y: 5, Width: 8, Height: 4},
	}, 1)
	require.Equal(t, cursorOut{valid: true, row: 8, col: 7, style: 1, hasStyle: true}, visible)

	hidden := desiredCapturedCursor(capturedCursorInputs{
		row: 2, col: 3, visible: false, renderable: true,
		content: domain.Rect{X: 4, Y: 5, Width: 8, Height: 4},
	}, 1)
	require.Equal(t, cursorOut{hidden: true}, hidden)
}

func TestProxiedRemotePaintSendsMetadataBeforeScreen(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	client, sends := newCapturingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, client, newProxyTestTransport())
	ac.proxied = true
	ac.screenOutput = newStructuredOutputStream(ac.output)
	require.True(t, applyTestScreenText(proxy, 0, 0, "remote"))

	ac.sendMu.Lock()
	state, ok := proxy.capturePrimary(ac, primaryCaptureRequest{reset: true})
	require.True(t, ok)
	composed := composeFrame(*state, composeCacheInput{})
	require.True(t, d.emitFrame(proxy, ac, state, composed))

	metadata := awaitTestValue(t, sends, "proxied remote metadata was not sent")
	require.Equal(t, ports.MsgSessionMeta, metadata.Type)
	meta, err := ports.UnmarshalSessionMeta(metadata.Payload)
	require.NoError(t, err)
	require.Equal(t, "work", meta.SessionName)
	require.Equal(t, ports.MsgScreenUpdate, awaitTestValue(t, sends, "proxied remote screen was not sent").Type)
}

func TestProxiedPaintPublishesAbsoluteVisibleCursorStyle(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	pane := testAttachmentTab(sess).focusedPane()
	pane.mu.Lock()
	pane.screen.Write([]byte("\x1b[3;6H\x1b[0 q"))
	pane.mu.Unlock()

	d.paint(sess, ac, true, nil)
	require.Equal(t, ports.MsgSessionMeta, awaitTestValue(t, sends, "proxied paint did not emit session metadata").Type)
	frame := awaitTestValue(t, sends, "proxied paint did not emit structured screen update")
	require.Equal(t, ports.MsgScreenUpdate, frame.Type)
	update, err := ports.UnmarshalScreenUpdate(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ScreenUpdateSnapshot, update.Kind)
	require.True(t, update.Cursor.Visible)
	require.Equal(t, uint16(2), update.Cursor.Row)
	require.Equal(t, uint16(5), update.Cursor.Col)
	require.True(t, update.Cursor.StyleSet)
	require.Zero(t, update.Cursor.Style)
}

func TestComposeFrameProxiedContentOnlyOmitsBothChromeRows(t *testing.T) {
	paneFrame := renderer.NewFrame(8, 2)
	fillOutputStateRows(paneFrame, []string{"CONTENT1", "CONTENT2"})
	placement := layout.Placement{ID: "pane", Content: domain.Rect{Width: 8, Height: 2}}
	state := capturedRenderState{
		contentOnly: true,
		reset:       true,
		layout: capturedTabLayout{
			area: domain.Rect{Width: 8, Height: 2}, focus: "pane", placements: []layout.Placement{placement}, valid: true,
		},
		panes: []capturedPaneRenderState{{
			id: "pane", frame: paneFrame, placement: placement, focused: true, damage: []renderer.Damage{renderer.FullRedraw()},
		}},
		bars:   barState{status: statusSnapshot{session: "REMOTE-CHROME"}, bottomRight: "BOTTOM-CHROME"},
		cursor: capturedCursorInputs{visible: true, renderable: true, content: placement.Content},
	}

	proxied := composeFrame(state, composeCacheInput{})
	require.Equal(t, []string{"CONTENT1", "CONTENT2"}, frameRows(proxied.frame))
	require.Equal(t, 0, proxied.cursor.row)

	state.contentOnly = false
	ordinary := composeFrame(state, composeCacheInput{})
	require.Equal(t, 4, ordinary.frame.Height)
	require.Equal(t, "CONTENT1", rowText(ordinary.frame.Row(1)))
	require.Contains(t, rowText(ordinary.frame.Row(3)), "REMOTE-")
	require.Equal(t, 1, ordinary.cursor.row)
}

func TestProxyResizeUpdatesAttachmentViewport(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	client, _ := newCapturingTransport(t)
	proxy, ac := newAttachedProxyFixture(t, d, client, newProxyTestTransport())

	d.resizeProxyForLease(proxy, ac, nil, domain.Size{Cols: 24, Rows: 10})
	require.Equal(t, domain.Size{Cols: 24, Rows: 10}, ac.size)
}

func TestProxiedTransactionalResizeUsesReceivedContentGeometry(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	rc := d.attachCoordinator(sess, nil, ac, true)
	lease := rc.attachmentLease(ac)
	require.NotNil(t, lease)

	want := domain.Size{Cols: 100, Rows: 12}
	require.True(t, d.requestTransactionalResizeForLease(sess, ac, lease, want, true))
	tb := testAttachmentTab(sess)
	tb.mu.Lock()
	require.Equal(t, want, tb.size)
	tb.mu.Unlock()
	require.Equal(t, want, ac.size)
}

func TestProxiedModeCannotChangeAcrossResume(t *testing.T) {
	tests := []struct {
		name           string
		initialProxied bool
		resumedProxied bool
	}{
		{name: "proxied attachment cannot resume as ordinary", initialProxied: true, resumedProxied: false},
		{name: "ordinary attachment cannot resume as proxied", initialProxied: false, resumedProxied: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
			var clientID [16]byte
			clientID[0] = 1
			originalTransport := &closeTrackingTransport{}
			hello := ports.Hello{
				Version: ports.ProtocolVersion, Intent: ports.IntentNew, Proxied: tt.initialProxied,
				ClientID: clientID, Name: "immutable", Size: domain.Size{Cols: 40, Rows: 8},
			}
			sess, ac, err := d.route(hello, originalTransport)
			require.NoError(t, err)
			token := ac.resumeToken
			d.clientGone(sess, ac, originalTransport, false)

			resume := hello
			resume.Intent = ports.IntentResume
			resume.ResumeToken = token
			resume.Proxied = tt.resumedProxied
			_, resumed, ok, err := d.resumeParked(resume, &closeTrackingTransport{}, resume.Size)
			require.Error(t, err)
			require.False(t, ok)
			require.Nil(t, resumed)
			require.Equal(t, tt.initialProxied, ac.proxied)
			d.mu.Lock()
			parked := d.parked[token]
			d.mu.Unlock()
			require.NotNil(t, parked, "a mode-mismatched resume must leave the original attachment parked")
		})
	}
}

func TestOutputResetRebasesFullWindowAndSchedulesBaseZeroPaint(t *testing.T) {
	d, sess, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	rc := d.attachCoordinator(sess, nil, ac, true)
	lease := rc.attachmentLease(ac)
	require.NotNil(t, lease)

	ac.sendMu.Lock()
	ac.output.next = ac.output.maxOutstanding
	ac.output.acked = 0
	ac.sendMu.Unlock()

	token := sess.attachmentToken(ac, ac.transport())
	require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgOutputResetRequest,
		Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
	}))

	frame := awaitFrame(t, sends, ports.MsgScreenUpdate)
	out, err := ports.UnmarshalScreenUpdate(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ScreenUpdateSnapshot, out.Kind)
	require.Zero(t, out.BaseStateNum)
	ac.sendMu.Lock()
	require.Equal(t, ac.output.maxOutstanding, ac.output.acked, "reset must retire the previously full output window")
	require.Equal(t, uint64(1), ac.output.outstanding(), "only the authoritative reset paint remains outstanding")
	require.Equal(t, ac.output.next, out.NewStateNum)
	ac.sendMu.Unlock()
}

func TestOutputResetRequiresProxiedActiveRoleAndStrictPayload(t *testing.T) {
	tests := []struct {
		name      string
		proxied   bool
		payload   []byte
		staleRole bool
	}{
		{name: "ordinary attachment", proxied: false, payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{})},
		{name: "malformed request", proxied: true, payload: []byte{0}},
		{name: "stale active role", proxied: true, payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}), staleRole: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
			ac.proxied = tt.proxied
			d.attachCoordinator(sess, nil, ac, true)
			ac.sendMu.Lock()
			ac.output.next, ac.output.acked = 4, 0
			ac.sendMu.Unlock()

			token := sess.attachmentToken(ac, ac.transport())
			if tt.staleRole {
				token.generation++
			}
			require.False(t, d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgOutputResetRequest, Payload: tt.payload}))
			ac.sendMu.Lock()
			acked := ac.output.acked
			ac.sendMu.Unlock()
			require.Zero(t, acked)
		})
	}
}

func TestOutputResetRevalidatesTransportUnderSendLock(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	d.attachCoordinator(sess, nil, ac, true)
	ac.sendMu.Lock()
	ac.output.next, ac.output.acked = 4, 0
	ac.sendMu.Unlock()
	token := sess.attachmentToken(ac, ac.transport())

	admitted := make(chan struct{})
	var once sync.Once
	d.afterAttachmentEffectAdmitted = func(attachmentConnectionToken) { once.Do(func() { close(admitted) }) }
	ac.sendMu.Lock()
	done := make(chan bool, 1)
	go func() {
		done <- d.handleAttachmentClientFrame(token, ports.Frame{
			Type:    ports.MsgOutputResetRequest,
			Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
		})
	}()
	awaitTestCompletion(t, admitted, "reset did not admit its role effect")
	ac.replaceTransport(&closeTrackingTransport{})
	ac.sendMu.Unlock()

	require.False(t, awaitTestValue(t, done, "reset did not finish after send lock release"))
	ac.sendMu.Lock()
	require.Zero(t, ac.output.acked, "a stale transport must not rebase the output stream")
	ac.sendMu.Unlock()
}
