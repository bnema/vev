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

	outputFrame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.Zero(t, output.BaseStateNum)

	client := vt.NewScreen(contentSize.Cols, contentSize.Rows)
	client.Write(output.Data)
	require.Equal(t, contentSize.Rows, client.Frame.Height)
	for _, row := range frameRows(client.Frame) {
		require.NotContains(t, row, "remote-work", "proxied output must not contain remote session chrome")
	}

	sess := firstSession(d)
	require.NotNil(t, sess)
	sess.activeTab().mu.Lock()
	actualContentSize := sess.activeTab().size
	sess.activeTab().mu.Unlock()
	require.Equal(t, contentSize, actualContentSize, "proxied geometry must not subtract chrome a second time")

	release()
	handlers.Wait()
}

func TestOrdinaryHandshakeRetainsChromeAndNoMetadata(t *testing.T) {
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

	outputFrame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.Zero(t, output.BaseStateNum)
	client := vt.NewScreen(viewport.Cols, viewport.Rows)
	client.Write(output.Data)
	require.Contains(t, screenLineText(client, 0), "1")
	require.Contains(t, screenLineText(client, viewport.Rows-1), "ordinary")

	sess := firstSession(d)
	require.NotNil(t, sess)
	sess.activeTab().mu.Lock()
	actualContentSize := sess.activeTab().size
	sess.activeTab().mu.Unlock()
	require.Equal(t, tabSize(viewport), actualContentSize)
	requireNoOutputFrame(t, sends)

	release()
	handlers.Wait()
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

func TestProxiedTransactionalResizeUsesReceivedContentGeometry(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	rc := d.attachCoordinator(sess, nil, ac, true)
	lease := rc.attachmentLease(ac)
	require.NotNil(t, lease)

	want := domain.Size{Cols: 100, Rows: 12}
	require.True(t, d.requestTransactionalResizeForLease(sess, ac, lease, want, true))
	tb := sess.activeTab()
	tb.mu.Lock()
	require.Equal(t, want, tb.size)
	tb.mu.Unlock()
	require.Equal(t, want, ac.size)
}

func TestProxiedModeCannotChangeAcrossResume(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	var clientID [16]byte
	clientID[0] = 1
	originalTransport := &closeTrackingTransport{}
	hello := ports.Hello{
		Version: ports.ProtocolVersion, Intent: ports.IntentNew, Proxied: true,
		ClientID: clientID, Name: "immutable", Size: domain.Size{Cols: 40, Rows: 8},
	}
	sess, ac, err := d.route(hello, originalTransport)
	require.NoError(t, err)
	token := ac.resumeToken
	d.clientGone(sess, ac, originalTransport, false)

	resume := hello
	resume.Intent = ports.IntentResume
	resume.ResumeToken = token
	resume.Proxied = false
	_, resumed, ok, err := d.resumeParked(resume, &closeTrackingTransport{}, resume.Size)
	require.Error(t, err)
	require.False(t, ok)
	require.Nil(t, resumed)
	require.True(t, ac.proxied)
	d.mu.Lock()
	require.NotNil(t, d.parked[token], "a mode-mismatched resume must leave the original attachment parked")
	d.mu.Unlock()
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
	require.False(t, d.handleActiveClientFrame(token, ports.Frame{
		Type:    ports.MsgOutputResetRequest,
		Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
	}))

	frame := awaitFrame(t, sends, ports.MsgOutput)
	out, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
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
			ac.output.next, ac.output.acked = 4, 0

			token := sess.attachmentToken(ac, ac.transport())
			if tt.staleRole {
				token.role = attachmentSnatched
			}
			require.False(t, d.handleActiveClientFrame(token, ports.Frame{Type: ports.MsgOutputResetRequest, Payload: tt.payload}))
			require.Zero(t, ac.output.acked)
		})
	}
}

func TestOutputResetRevalidatesTransportUnderSendLock(t *testing.T) {
	d, sess, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	ac.proxied = true
	d.attachCoordinator(sess, nil, ac, true)
	ac.output.next, ac.output.acked = 4, 0
	token := sess.attachmentToken(ac, ac.transport())

	admitted := make(chan struct{})
	var once sync.Once
	d.afterRoleEffectAdmitted = func(attachmentRoleToken) { once.Do(func() { close(admitted) }) }
	ac.sendMu.Lock()
	done := make(chan bool, 1)
	go func() {
		done <- d.handleActiveClientFrame(token, ports.Frame{
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
