package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

type parkedRemoteResumeTransport struct {
	closeOnce sync.Once
	closed    chan struct{}
	sends     chan ports.Frame
}

func newParkedRemoteResumeTransport() *parkedRemoteResumeTransport {
	return &parkedRemoteResumeTransport{closed: make(chan struct{}), sends: make(chan ports.Frame, 4)}
}

func (t *parkedRemoteResumeTransport) Send(frame ports.Frame) error {
	t.sends <- frame
	return nil
}

func (t *parkedRemoteResumeTransport) Recv() (ports.Frame, error) {
	<-t.closed
	return ports.Frame{}, io.EOF
}

func (t *parkedRemoteResumeTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type failingRemoteTransport struct {
	closeTrackingTransport
}

func (*failingRemoteTransport) Send(ports.Frame) error {
	return errors.New("send failed")
}

func TestAttachedClientOwnerBindingNarrowsLocalOnlyBehavior(t *testing.T) {
	local := &session{sessionCore: sessionCore{id: "local"}}
	remote := &remoteView{id: 7}
	ac := &attachedClient{}

	ac.setSession(local)
	require.Same(t, local, ac.currentAttachmentOwner())
	require.Same(t, local, ac.currentAttachmentSession())

	ac.setAttachmentOwner(remote)
	require.Same(t, remote, ac.currentAttachmentOwner())
	require.Nil(t, ac.currentAttachmentSession(), "remote owners must not reach local-only paths")
}

func TestAttachedClientOwnerBindingNormalizesTypedNil(t *testing.T) {
	ac := &attachedClient{}

	ac.setSession(nil)
	require.Nil(t, ac.currentAttachmentOwner())

	var remote *remoteView
	ac.setAttachmentOwner(remote)
	require.Nil(t, ac.currentAttachmentOwner())
}

func TestAttachmentTokenRejectsAnOwnerReboundToRemoteView(t *testing.T) {
	transport := &closeTrackingTransport{}
	local := &session{sessionCore: sessionCore{id: "local"}}
	ac := &attachedClient{tr: transport}
	ac.setSession(local)
	local.mu.Lock()
	require.True(t, local.registerAttachmentLocked(ac))
	local.mu.Unlock()

	token := local.attachmentToken(ac, transport)
	require.True(t, token.current())

	ac.setAttachmentOwner(&remoteView{id: 1})
	require.False(t, token.current(), "a local token must not survive an exact owner rebind")
}

func TestRemoteAttachmentTokenUsesExactRemoteOwnerMembership(t *testing.T) {
	transport := &closeTrackingTransport{}
	view := &remoteView{id: 1}
	ac := &attachedClient{tr: transport}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()

	token := attachmentOwnerToken(view, ac, transport)
	require.True(t, token.current())
	require.True(t, token.attachmentCurrent(), "remote tokens do not acquire a local render lease")

	view.mu.Lock()
	require.True(t, view.unregisterAttachmentLocked(ac))
	view.mu.Unlock()
	require.False(t, token.current(), "remote membership is part of exact token identity")
}

func TestTransitionToRemoteViewPublishesOneAuthoritativeOwner(t *testing.T) {
	d, source, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	before := source.attachmentToken(ac, ac.transport())
	require.True(t, before.current())
	published, err := d.transitionToRemoteView(before, view)
	require.NoError(t, err)

	require.False(t, before.current())
	require.True(t, published.current())
	require.Same(t, view, ac.currentAttachmentOwner())
	require.Empty(t, source.snapshotAttachments())
	require.True(t, view.attachmentRegistered(ac))
	require.Same(t, source, ac.previousOwner.Get())
}

func TestRemoteViewFirstPaintComposesPrivateContentUnderLocalChrome(t *testing.T) {
	d, source, ac, sends := newManualSessionWithPTYs(t, newQuietPTY())
	view := &remoteView{
		key:    remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"},
		screen: vt.NewScreen(80, 22),
	}
	view.screen.Write([]byte("remote content"))
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	token, err := d.transitionToRemoteView(source.attachmentToken(ac, ac.transport()), view)
	require.NoError(t, err)
	require.True(t, d.firstPaintForTransition(token))

	output := mustApplyOutput(t, vt.NewScreen(80, 24), awaitOutputFrameWithoutSleep(t, sends))
	require.True(t, output.Full)
	terminal := vt.NewScreen(80, 24)
	terminal.Write(output.Data)
	require.Contains(t, rowText(terminal.Snapshot().Row(0)), "remote")
	require.Contains(t, rowText(terminal.Snapshot().Row(1)), "remote content")
}

func TestShutdownRetiresRemoteViewsWithoutLocalSession(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	transport := &closeTrackingTransport{}
	ac := &attachedClient{tr: transport}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, transport)
	require.True(t, token.current())

	d.shutdownAll(ports.ReasonServerShutdown)

	d.mu.Lock()
	require.Empty(t, d.remoteViews)
	require.Empty(t, d.remoteViewsByKey)
	d.mu.Unlock()
	require.False(t, token.current())
	require.Nil(t, ac.currentAttachmentOwner())
	require.True(t, transport.Closed())
}

func TestRemoteClientGoneParksExactOwnerBinding(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	transport := &closeTrackingTransport{}
	ac := &attachedClient{tr: transport, resumeCapable: true}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, transport)

	require.True(t, d.clientGoneRemote(view, token, false))
	require.Nil(t, ac.currentAttachmentOwner())
	require.False(t, view.attachmentRegistered(ac))
	d.mu.Lock()
	parked := d.parked[ac.resumeToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, view, parked.owner)
}

func TestRemoteClientResumeRebindsParkedAttachmentToExactView(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	oldTransport := &closeTrackingTransport{}
	ac := &attachedClient{
		tr:            oldTransport,
		output:        newOutputStateStream(),
		size:          domain.Size{Cols: 80, Rows: 24},
		resumeCapable: true,
		clientID:      [16]byte{1},
	}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	oldToken := attachmentOwnerToken(view, ac, oldTransport)
	require.True(t, d.clientGoneRemote(view, oldToken, false))
	parkedToken := ac.resumeToken

	newTransport := &closeTrackingTransport{}
	resumedView, resumed, handled, err := d.resumeParkedRemoteView(ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      ports.IntentResume,
		ResumeToken: parkedToken,
		ClientID:    ac.clientID,
		Size:        domain.Size{Cols: 100, Rows: 30},
	}, newTransport)
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, view, resumedView)
	require.Same(t, ac, resumed)
	require.Same(t, view, ac.currentAttachmentOwner())
	require.True(t, view.attachmentRegistered(ac))
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, ac.sizeSnapshot())
	require.NotEqual(t, parkedToken, ac.resumeToken)
	require.True(t, d.commitResumeClaim(ac))
	d.mu.Lock()
	require.NotContains(t, d.parked, parkedToken)
	require.Empty(t, d.sessions)
	d.mu.Unlock()
}

func TestRemoteClientLiveResumeParksOldTransportAndRebindsExactView(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	oldTransport := &closeTrackingTransport{}
	ac := &attachedClient{
		tr:            oldTransport,
		output:        newOutputStateStream(),
		size:          domain.Size{Cols: 80, Rows: 24},
		resumeCapable: true,
		clientID:      [16]byte{1},
	}
	d.mu.Lock()
	ac.resumeToken = d.nextResumeTokenLocked()
	d.mu.Unlock()
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	oldToken := ac.resumeToken

	newTransport := &closeTrackingTransport{}
	resumedView, resumed, handled, err := d.resumeLiveRemoteView(ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      ports.IntentResume,
		ResumeToken: oldToken,
		ClientID:    ac.clientID,
		Size:        domain.Size{Cols: 100, Rows: 30},
	}, newTransport)
	require.NoError(t, err)
	require.True(t, handled)
	require.Same(t, view, resumedView)
	require.Same(t, ac, resumed)
	require.True(t, oldTransport.Closed())
	require.Same(t, view, ac.currentAttachmentOwner())
	require.True(t, d.commitResumeClaim(ac))
}

func TestRemoteClientResumeWaitsForInFlightRemoteParking(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"}}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	oldTransport := &closeTrackingTransport{}
	ac := &attachedClient{
		tr:            oldTransport,
		output:        newOutputStateStream(),
		size:          domain.Size{Cols: 80, Rows: 24},
		resumeCapable: true,
		clientID:      [16]byte{1},
	}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	resumeToken := d.markParkingInFlightOwner(view, ac)
	d.clearParkingInFlight(resumeToken, ac)

	detachStarted := make(chan struct{})
	releaseDetach := make(chan struct{})
	d.beforeClientGoneDetach = func() {
		close(detachStarted)
		<-releaseDetach
	}
	waitArmed := make(chan struct{})
	d.afterParkingWaitArmed = func() { close(waitArmed) }
	gone := make(chan bool, 1)
	go func() {
		gone <- d.clientGoneRemote(view, attachmentOwnerToken(view, ac, oldTransport), false)
	}()
	awaitTestCompletion(t, detachStarted, "remote detach did not publish parking")

	type result struct {
		view    *remoteView
		ac      *attachedClient
		handled bool
		err     error
	}
	resumed := make(chan result, 1)
	go func() {
		resumedView, resumedAC, handled, err := d.resumeParkedRemoteView(ports.Hello{
			Version:     ports.ProtocolVersion,
			Intent:      ports.IntentResume,
			ResumeToken: resumeToken,
			ClientID:    ac.clientID,
			Size:        domain.Size{Cols: 80, Rows: 24},
		}, &closeTrackingTransport{})
		resumed <- result{view: resumedView, ac: resumedAC, handled: handled, err: err}
	}()
	awaitTestCompletion(t, waitArmed, "remote resume did not wait for in-flight parking")
	close(releaseDetach)
	require.True(t, awaitTestValue(t, gone, "remote detach did not finish"))
	got := awaitTestValue(t, resumed, "remote resume did not finish")
	require.NoError(t, got.err)
	require.True(t, got.handled)
	require.Same(t, view, got.view)
	require.Same(t, ac, got.ac)
	require.True(t, d.commitResumeClaim(ac))
}

func TestRemoteClientResumeHandshakeRestoresLocalComposition(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{
		key:    remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"},
		screen: vt.NewScreen(80, 22),
	}
	view.screen.Write([]byte("remote content"))
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	oldTransport := &closeTrackingTransport{}
	ac := &attachedClient{
		tr:            oldTransport,
		output:        newOutputStateStream(),
		size:          domain.Size{Cols: 80, Rows: 24},
		resumeCapable: true,
		clientID:      [16]byte{1},
	}
	ac.initOverlays()
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	oldToken := attachmentOwnerToken(view, ac, oldTransport)
	require.True(t, d.clientGoneRemote(view, oldToken, false))
	parkedToken := ac.resumeToken

	transport := newParkedRemoteResumeTransport()
	done := make(chan struct{})
	go func() {
		d.handleHello(transport, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
			Version:     ports.ProtocolVersion,
			Intent:      ports.IntentResume,
			ResumeToken: parkedToken,
			ClientID:    ac.clientID,
			Size:        domain.Size{Cols: 100, Rows: 30},
		})})
		close(done)
	}()

	welcomeFrame := awaitFrame(t, transport.sends, ports.MsgWelcome)
	welcome, err := ports.UnmarshalWelcome(welcomeFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, "remote", welcome.SessionName)
	require.NotEqual(t, parkedToken, welcome.ResumeToken)
	outputFrame := awaitFrame(t, transport.sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full)
	require.Equal(t, domain.Size{Cols: 100, Rows: 30}, output.Size)
	require.Same(t, view, ac.currentAttachmentOwner())
	require.True(t, view.attachmentRegistered(ac))

	require.NoError(t, transport.Close())
	awaitTestCompletion(t, done, "remote resume handshake did not finish")
	d.mu.Lock()
	parked := d.parked[welcome.ResumeToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, view, parked.owner)
}

func TestRemoteResumeHandshakeFailureRestoresParkedOwner(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{
		key:    remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"},
		screen: vt.NewScreen(80, 22),
	}
	view.screen.Write([]byte("retained content"))
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	oldTransport := &closeTrackingTransport{}
	ac := &attachedClient{
		tr:            oldTransport,
		output:        newOutputStateStream(),
		size:          domain.Size{Cols: 80, Rows: 24},
		resumeCapable: true,
		clientID:      [16]byte{1},
	}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	require.True(t, d.clientGoneRemote(view, attachmentOwnerToken(view, ac, oldTransport), false))
	parkedToken := ac.resumeToken

	failed := &resumeWelcomeFailureTransport{}
	d.handleHello(failed, ports.Frame{Type: ports.MsgHello, Payload: ports.MarshalHello(ports.Hello{
		Version:     ports.ProtocolVersion,
		Intent:      ports.IntentResume,
		ResumeToken: parkedToken,
		ClientID:    ac.clientID,
		Size:        domain.Size{Cols: 80, Rows: 24},
	})})

	require.Nil(t, ac.currentAttachmentOwner())
	require.False(t, view.attachmentRegistered(ac))
	view.mu.Lock()
	require.Equal(t, 80, view.screen.Frame.Width)
	require.Equal(t, 22, view.screen.Frame.Height)
	view.mu.Unlock()
	require.Equal(t, parkedToken, ac.resumeToken)
	d.mu.Lock()
	parked := d.parked[parkedToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.False(t, parked.claimed)
	require.Same(t, view, parked.owner)
}

func TestRemotePaintSendFailureParksExactOwnerBinding(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{
		key:    remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"},
		screen: vt.NewScreen(80, 22),
	}
	view.screen.Write([]byte("remote content"))
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	transport := &failingRemoteTransport{}
	ac := &attachedClient{
		tr:            transport,
		output:        newOutputStateStream(),
		size:          domain.Size{Cols: 80, Rows: 24},
		resumeCapable: true,
	}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, transport)
	cleanupDone := make(chan struct{})
	d.afterAttachmentSendErrorCleanup = func() { close(cleanupDone) }

	d.paintRemoteView(view, ac, true, token)
	awaitTestCompletion(t, cleanupDone, "remote send-error cleanup did not finish")
	require.Nil(t, ac.currentAttachmentOwner())
	require.False(t, view.attachmentRegistered(ac))
	d.mu.Lock()
	parked := d.parked[ac.resumeToken]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, view, parked.owner)
}

func TestRemoteParkingRetainsStableOwnerBinding(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	view := &remoteView{
		id:  1,
		key: remoteViewKey{endpoint: "host", lifecycleID: domain.SessionLifecycleID{1}, sessionName: "remote"},
	}
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()

	ac := &attachedClient{tr: &closeTrackingTransport{}, resumeCapable: true}
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()

	token := d.markParkingInFlightOwner(view, ac)
	require.NotZero(t, token)
	view.mu.Lock()
	require.True(t, view.unregisterAttachmentLocked(ac))
	view.mu.Unlock()
	ac.setAttachmentOwner(nil)
	require.True(t, d.parkAttachmentOwner(view, ac))

	d.mu.Lock()
	parked := d.parked[token]
	d.mu.Unlock()
	require.NotNil(t, parked)
	require.Same(t, view, parked.owner)
}

func TestRemoteViewRegistryUsesDaemonLocalIDAndExactRemoteLifecycleKey(t *testing.T) {
	target := domain.RemoteSessionTarget{
		Endpoint:      "user@host",
		DisplayOrigin: "host",
		LifecycleID:   domain.SessionLifecycleID{1},
		SessionName:   "remote",
		LiveTabID:     "tab-1",
	}
	key, err := remoteViewKeyForTarget(target)
	require.NoError(t, err)

	d := &Daemon{
		remoteViews:      make(map[remoteViewID]*remoteView),
		remoteViewsByKey: make(map[remoteViewKey]remoteViewID),
	}
	first := &remoteView{key: key}
	second := &remoteView{key: key}

	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(first))
	require.NotZero(t, first.id)
	require.Same(t, first, d.remoteViewByKeyLocked(key))
	require.Error(t, d.registerRemoteViewLocked(second), "a lifecycle key cannot alias another local view")
	require.Zero(t, second.id, "a rejected registration must not consume a local view ID")
	require.Len(t, d.remoteViews, 1)
	require.Len(t, d.remoteViewsByKey, 1)
	require.True(t, d.unregisterRemoteViewLocked(first))
	require.Nil(t, d.remoteViewByKeyLocked(key))
	d.mu.Unlock()
}
