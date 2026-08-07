package daemon

import (
	"os"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/keys"
	"github.com/bnema/vev/pkg/renderer"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

func newRemoteFrameRoutingFixture(t *testing.T) (*Daemon, *remoteView, *attachedClient, attachmentConnectionToken, chan ports.Frame) {
	t.Helper()
	d := newTestDaemon(t, nil, stubClock{})
	tr, sends := newCapturingTransport(t)
	view := &remoteView{
		key: remoteViewKey{
			endpoint:    "host",
			lifecycleID: domain.SessionLifecycleID{1},
			sessionName: "remote",
		},
		screen: vt.NewScreen(80, 22),
	}
	view.screen.Write([]byte("remote content"))
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	ac := &attachedClient{tr: tr, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, tr)
	require.True(t, token.current())
	return d, view, ac, token, sends
}

func TestRemoteViewRenderSnapshotDoesNotResizeSharedScreen(t *testing.T) {
	view := &remoteView{screen: vt.NewScreen(80, 22)}

	snapshot, ok := view.renderSnapshot(domain.Size{Cols: 100, Rows: 28})

	require.True(t, ok)
	require.Equal(t, 80, snapshot.frame.Width)
	require.Equal(t, 22, snapshot.frame.Height)
	view.mu.Lock()
	require.Equal(t, 80, view.screen.Frame.Width)
	require.Equal(t, 22, view.screen.Frame.Height)
	view.mu.Unlock()
}

func TestRemoteFrameRoutingKeepsPickerNavigationLocal(t *testing.T) {
	d, source, ac, view, token := remoteBackFixture(t)
	remoteTransport := newRemoteLinkTestTransport()
	view.mu.Lock()
	view.linkGeneration = 1
	view.link = &remoteLink{view: view, generation: 1, transport: remoteTransport, active: true}
	view.mu.Unlock()

	otherTab := newTab(newQuietPTY(), domain.Size{Cols: 80, Rows: 22})
	otherTab.stableID = "other-tab"
	other := &session{sessionCore: sessionCore{id: "other", name: "other", attachments: make(map[*attachedClient]struct{})}, tabs: []*tab{otherTab}}
	publishTiledPaneOwners(other, otherTab)
	d.mu.Lock()
	d.sessions[other.id] = other
	d.mu.Unlock()

	d.enterPicker(source, ac)
	ac.overlays.pickerMu.Lock()
	before, ok := ac.overlays.picker.Selected()
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	d.handleAttachmentClientFrame(token, frameInput([]byte("j")))

	ac.overlays.pickerMu.Lock()
	selected, ok := ac.overlays.picker.Selected()
	ac.overlays.pickerMu.Unlock()
	require.True(t, ok)
	require.NotEqual(t, before.Session, selected.Session, "picker navigation remains local while viewing remote content")
	select {
	case frame := <-remoteTransport.sent:
		t.Fatalf("picker navigation was forwarded to remote link as %v", frame.Type)
	default:
	}
}

func TestRemoteFrameRoutingForwardsOrdinaryTerminalInput(t *testing.T) {
	d, _, _, view, token := remoteBackFixture(t)
	remoteTransport := newRemoteLinkTestTransport()
	view.mu.Lock()
	view.linkGeneration = 1
	view.link = &remoteLink{view: view, generation: 1, transport: remoteTransport, active: true}
	view.mu.Unlock()

	d.handleAttachmentClientFrame(token, frameInput([]byte("x")))

	frame := awaitFrame(t, remoteTransport.sent, ports.MsgInput)
	input, err := ports.UnmarshalInput(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, []byte("x"), input.Data)
}

func TestRemoteFrameRoutingOpensPaletteLocally(t *testing.T) {
	d, _, ac, view, token := remoteBackFixture(t)
	remoteTransport := newRemoteLinkTestTransport()
	view.mu.Lock()
	view.linkGeneration = 1
	view.link = &remoteLink{view: view, generation: 1, transport: remoteTransport, active: true}
	view.mu.Unlock()

	d.handleAttachmentClientFrame(token, frameInput([]byte{keys.ESC, ' '}))

	require.True(t, ac.overlays.paletteActive(), "the local palette remains reachable while remote content is active")
	d.handleAttachmentClientFrame(token, frameInput([]byte("SSP\r")))
	require.True(t, ac.overlays.pickerActive(), "the local session picker reopens from the remote view")
	select {
	case frame := <-remoteTransport.sent:
		t.Fatalf("palette shortcut was forwarded to remote link as %v", frame.Type)
	default:
	}
}

func TestRemoteFrameRoutingResetRepaintsComposedFrameAndAckAdvancesOutput(t *testing.T) {
	d, _, ac, token, sends := newRemoteFrameRoutingFixture(t)

	beforeEpoch := ac.output.currentEpoch()
	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgOutputResetRequest,
		Payload: ports.MarshalOutputResetRequest(ports.OutputResetRequest{}),
	})
	frame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full)
	require.Greater(t, output.Epoch, beforeEpoch)
	require.Nil(t, ac.currentAttachmentSession())
	require.Empty(t, d.sessions)

	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgAck,
		Payload: mustMarshalAck(ports.Ack{Epoch: output.Epoch, State: output.New}),
	})
	ac.sendMu.Lock()
	require.Equal(t, output.New, ac.output.acked)
	ac.sendMu.Unlock()
}

func TestRemoteFrameRoutingAckRepaintsPrivateVTAfterOutputWindowBackpressure(t *testing.T) {
	d, view, ac, token, sends := newRemoteFrameRoutingFixture(t)
	ac.sendMu.Lock()
	ac.output = newOutputStateStream(1)
	ac.sendMu.Unlock()

	require.Equal(t, paintEmitted, d.paintRemoteView(view, ac, true, token))
	first := awaitFrame(t, sends, ports.MsgOutput)
	firstOutput, err := ports.UnmarshalOutput(first.Payload)
	require.NoError(t, err)

	view.mu.Lock()
	view.screen.Write([]byte(" updated"))
	view.mu.Unlock()
	require.Equal(t, paintBlockedCapacity, d.paintRemoteView(view, ac, false, token))

	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgAck,
		Payload: mustMarshalAck(ports.Ack{Epoch: firstOutput.Epoch, State: firstOutput.New}),
	})
	second := awaitFrame(t, sends, ports.MsgOutput)
	secondOutput, err := ports.UnmarshalOutput(second.Payload)
	require.NoError(t, err)
	require.Equal(t, firstOutput.Epoch, secondOutput.Epoch)
	require.Equal(t, firstOutput.New, secondOutput.Base)
	require.Greater(t, secondOutput.New, firstOutput.New)
}

func TestRemoteFrameRoutingThemeUpdatesOnlyAttachmentChromeAndRepaints(t *testing.T) {
	d, _, ac, token, sends := newRemoteFrameRoutingFixture(t)
	theme := ports.Theme{
		HasForeground: true,
		Foreground:    renderer.RGB{R: 1, G: 2, B: 3},
		HasBackground: true,
		Background:    renderer.RGB{R: 4, G: 5, B: 6},
		SchemeKnown:   true,
		Light:         true,
	}

	d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgTheme, Payload: ports.MarshalTheme(theme)})
	frame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full)

	want := d.resolveAppliedTheme(themeFromMessage(theme))
	got := ac.getAppliedTheme()
	require.Equal(t, want.Raw, got.Raw)
	require.Equal(t, want.Resolved, got.Resolved)
	require.Greater(t, got.Generation, uint64(0))
	require.Nil(t, ac.currentAttachmentSession())
	require.Empty(t, d.sessions)
	require.Empty(t, d.notices.history())
}

func TestRemoteFrameRoutingResizeRebasesOuterAndPrivateScreenWithoutLocalPTY(t *testing.T) {
	d, view, ac, token, sends := newRemoteFrameRoutingFixture(t)
	beforeEpoch := ac.output.currentEpoch()

	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgResize,
		Payload: mustMarshalResize(ports.Resize{Size: domain.Size{Cols: 100, Rows: 30}}),
	})
	frame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(frame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full)
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, output.Size)
	require.Greater(t, ac.output.currentEpoch(), beforeEpoch)
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, ac.sizeSnapshot())

	view.mu.Lock()
	require.Equal(t, 100, view.screen.Frame.Width)
	require.Equal(t, 28, view.screen.Frame.Height)
	view.mu.Unlock()
	require.Nil(t, ac.currentAttachmentSession())
	require.Empty(t, d.sessions)
}

func TestRemoteFrameRoutingClientNoticeUsesAttachmentToastOnly(t *testing.T) {
	d, _, ac, token, sends := newRemoteFrameRoutingFixture(t)

	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgClientNotice,
		Payload: ports.MarshalClientNotice(ports.ClientNotice{Action: ports.ClientNoticeLinkDegraded}),
	})
	awaitFrame(t, sends, ports.MsgOutput)
	toasts, _ := visibleToasts(ac)
	require.Len(t, toasts, 1)
	require.Equal(t, domain.NoticeConnection, toasts[0].Code)
	require.Empty(t, toasts[0].SessionID)
	require.Empty(t, d.notices.history(), "remote client notices must not create local session history")

	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgClientNotice,
		Payload: ports.MarshalClientNotice(ports.ClientNotice{Action: ports.ClientNoticeLinkConnected}),
	})
	awaitFrame(t, sends, ports.MsgOutput)
	toasts, _ = visibleToasts(ac)
	require.Empty(t, toasts)
}

func TestRemoteFrameRoutingImageAndCommandFailClosed(t *testing.T) {
	d, _, _, token, sends := newRemoteFrameRoutingFixture(t)
	d.tempDir = t.TempDir()

	d.handleAttachmentClientFrame(token, ports.Frame{
		Type:    ports.MsgImagePush,
		Payload: ports.MarshalImagePush(ports.ImagePush{Mime: "image/png", Data: []byte("not written")}),
	})
	entries, err := os.ReadDir(d.tempDir)
	require.NoError(t, err)
	require.Empty(t, entries)

	payload, err := ports.MarshalCommandRequest(ports.CommandRequest{
		Version:   ports.ProtocolVersion,
		RequestID: 7,
		Attached:  true,
		Slug:      "new-session",
	})
	require.NoError(t, err)
	d.handleAttachmentClientFrame(token, ports.Frame{Type: ports.MsgCommand, Payload: payload})
	resultFrame := awaitFrame(t, sends, ports.MsgCommandResult)
	result, err := ports.UnmarshalCommandResult(resultFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, uint64(7), result.RequestID)
	require.False(t, result.OK)
	require.Equal(t, ports.ErrNoSuchTarget, result.Code)
	require.Empty(t, d.sessions)
}
