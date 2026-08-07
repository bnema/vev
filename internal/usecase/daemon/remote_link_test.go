package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/pkg/vt"
	"github.com/stretchr/testify/require"
)

type remoteLinkTestReceive struct {
	frame ports.Frame
	err   error
}

type remoteLinkTestTransport struct {
	recv      chan remoteLinkTestReceive
	sent      chan ports.Frame
	closed    chan struct{}
	closeOnce sync.Once
}

func newRemoteLinkTestTransport() *remoteLinkTestTransport {
	return &remoteLinkTestTransport{
		recv:   make(chan remoteLinkTestReceive, 8),
		sent:   make(chan ports.Frame, 8),
		closed: make(chan struct{}),
	}
}

func (t *remoteLinkTestTransport) Send(frame ports.Frame) error {
	select {
	case <-t.closed:
		return io.EOF
	case t.sent <- frame:
		return nil
	}
}

func (t *remoteLinkTestTransport) Recv() (ports.Frame, error) {
	select {
	case <-t.closed:
		return ports.Frame{}, io.EOF
	case received := <-t.recv:
		return received.frame, received.err
	}
}

func (t *remoteLinkTestTransport) Close() error {
	t.closeOnce.Do(func() { close(t.closed) })
	return nil
}

type remoteLinkTestDialer struct {
	transport ports.Transport
	started   chan<- struct{}
	release   <-chan struct{}
	once      sync.Once
}

func (d *remoteLinkTestDialer) Dial(ctx context.Context) (ports.Transport, error) {
	if d.started != nil {
		d.once.Do(func() { close(d.started) })
	}
	if d.release != nil {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-d.release:
		}
	}
	return d.transport, nil
}

type remoteLinkTestFactory struct {
	dialer ports.Dialer
	calls  atomic.Int32
}

func (f *remoteLinkTestFactory) DialerForRemote(_ string, _ string, _ ports.RemoteTransportMode, _ *slog.Logger) (ports.Dialer, error) {
	f.calls.Add(1)
	return f.dialer, nil
}

type remoteLinkSequenceFactory struct {
	mu      sync.Mutex
	dialers []ports.Dialer
	calls   atomic.Int32
}

func (f *remoteLinkSequenceFactory) DialerForRemote(_ string, _ string, _ ports.RemoteTransportMode, _ *slog.Logger) (ports.Dialer, error) {
	call := int(f.calls.Add(1)) - 1
	f.mu.Lock()
	defer f.mu.Unlock()
	if call < 0 || call >= len(f.dialers) {
		return nil, io.EOF
	}
	return f.dialers[call], nil
}

func remoteLinkTestTarget() domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint:      "remote.example",
		DisplayOrigin: "remote.example",
		LifecycleID:   domain.SessionLifecycleID{1},
		SessionName:   "remote",
		LiveTabID:     "tab-1",
	}
}

func enqueueRemoteLinkHandshake(t *testing.T, transport *remoteLinkTestTransport, target domain.RemoteSessionTarget, size domain.Size) {
	t.Helper()
	enqueueRemoteLinkHandshakeWithContent(t, transport, target, size, "remote marker")
}

func enqueueRemoteLinkHandshakeWithContent(t *testing.T, transport *remoteLinkTestTransport, target domain.RemoteSessionTarget, size domain.Size, contentMarker string) {
	t.Helper()
	enqueueRemoteLinkHandshakeWithMetadata(t, transport, target, size, ports.SessionMeta{
		LifecycleID: target.LifecycleID,
		Revision:    1,
		SessionName: target.SessionName,
		ActiveTabID: target.LiveTabID,
		Tabs:        []ports.SessionTabMeta{{ID: target.LiveTabID, Name: "main"}},
	}, contentMarker)
}

func enqueueRemoteLinkHandshakeWithMetadata(t *testing.T, transport *remoteLinkTestTransport, target domain.RemoteSessionTarget, size domain.Size, metadata ports.SessionMeta, contentMarker string) {
	t.Helper()
	content := contentSize(size)
	metaPayload, err := ports.MarshalSessionMeta(metadata)
	require.NoError(t, err)
	outputPayload, err := ports.MarshalOutput(ports.Output{
		Epoch: 1, New: 1, Size: content, Full: true, Data: []byte(contentMarker),
	})
	require.NoError(t, err)
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID: "remote-id", SessionName: target.SessionName, RenderMode: ports.RenderModeProxiedContent,
		Capabilities: ports.CapabilityResume,
	})}}
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgSessionMeta, Payload: metaPayload}}
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgOutput, Payload: outputPayload}}
}

func newRemoteMetadataLinkFixture(t *testing.T) (*Daemon, *remoteView, *remoteLink, *remoteLinkTestTransport) {
	t.Helper()
	d := newTestDaemon(t, nil, stubClock{})
	transport := newRemoteLinkTestTransport()
	target := remoteLinkTestTarget()
	view := &remoteView{
		key: remoteViewKey{
			endpoint:    target.Endpoint,
			lifecycleID: target.LifecycleID,
			sessionName: target.SessionName,
		},
		screen:         vt.NewScreen(80, 22),
		linkGeneration: 1,
		metadata: ports.SessionMeta{
			LifecycleID: target.LifecycleID,
			Revision:    1,
			SessionName: target.SessionName,
			ActiveTabID: target.LiveTabID,
			Tabs:        []ports.SessionTabMeta{{ID: target.LiveTabID, Name: "main"}},
		},
	}
	link := &remoteLink{
		view:       view,
		generation: 1,
		target:     target,
		transport:  transport,
		cancel:     func() {},
		active:     true,
	}
	view.link = link
	d.mu.Lock()
	require.NoError(t, d.registerRemoteViewLocked(view))
	d.mu.Unlock()
	return d, view, link, transport
}

func attachRemoteMetadataClient(t *testing.T, view *remoteView) (*attachedClient, chan ports.Frame) {
	t.Helper()
	transport, sends := newCapturingTransport(t)
	ac := &attachedClient{tr: transport, output: newOutputStateStream(), size: domain.Size{Cols: 80, Rows: 24}}
	ac.initOverlays()
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	return ac, sends
}

func remoteMetadataFrame(t *testing.T, metadata ports.SessionMeta) ports.Frame {
	t.Helper()
	payload, err := ports.MarshalSessionMeta(metadata)
	require.NoError(t, err)
	return ports.Frame{Type: ports.MsgSessionMeta, Payload: payload}
}

func TestStopAndJoinRemoteLinkDoesNotWaitForUnstartedWorker(t *testing.T) {
	transport := newRemoteLinkTestTransport()
	link := &remoteLink{generation: 1, transport: transport, done: make(chan struct{})}
	done := make(chan struct{})
	go func() {
		stopAndJoinRemoteLink(link)
		close(done)
	}()
	awaitTestCompletion(t, done, "stopping an unstarted remote link blocked")
	select {
	case <-transport.closed:
	default:
		t.Fatal("stopping an unstarted remote link did not close its transport")
	}
}

func TestRemoteLinkDetachedPropagatesItsExactReason(t *testing.T) {
	d, view, link, _ := newRemoteMetadataLinkFixture(t)
	attachment, sends := attachRemoteMetadataClient(t, view)

	require.NoError(t, d.handleRemoteLinkFrame(link, ports.Frame{
		Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonDetach}),
	}))

	frame := awaitFrame(t, sends, ports.MsgDetached)
	detached, err := ports.UnmarshalDetached(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ReasonDetach, detached.Reason)
	require.Nil(t, attachment.currentAttachmentOwner())
}

func TestRemoteLinkAcceptedMetadataRepaintsEveryAttachedClientUnderLocalChrome(t *testing.T) {
	d, view, link, _ := newRemoteMetadataLinkFixture(t)
	first, firstSends := attachRemoteMetadataClient(t, view)
	second, secondSends := attachRemoteMetadataClient(t, view)
	callbacks := make(chan struct{}, 2)
	for _, ac := range []*attachedClient{first, second} {
		ac.beforeAttachmentTokenValidation = func() {
			view.mu.Lock()
			view.mu.Unlock()
			callbacks <- struct{}{}
		}
	}

	metadata := ports.SessionMeta{
		LifecycleID: view.key.lifecycleID,
		Revision:    2,
		SessionName: view.key.sessionName,
		ActiveTabID: "tab-1",
		Tabs:        []ports.SessionTabMeta{{ID: "tab-1", Name: "renamed"}},
	}
	require.NoError(t, d.handleRemoteLinkFrame(link, remoteMetadataFrame(t, metadata)))
	for range 2 {
		awaitTestValue(t, callbacks, "metadata repaint callback did not run outside remoteView.mu")
	}

	for _, sends := range []chan ports.Frame{firstSends, secondSends} {
		frame := awaitFrame(t, sends, ports.MsgOutput)
		output, err := ports.UnmarshalOutput(frame.Payload)
		require.NoError(t, err)
		require.Contains(t, string(output.Data), "renamed", "accepted metadata must repaint local chrome")
	}
	view.mu.Lock()
	require.Equal(t, uint64(2), view.metadata.Revision)
	require.Equal(t, "renamed", view.metadata.Tabs[0].Name)
	view.mu.Unlock()
}

func TestRemoteLinkIgnoresDuplicateAndOlderMetadataWithoutStopping(t *testing.T) {
	d, view, link, transport := newRemoteMetadataLinkFixture(t)
	metadata := ports.SessionMeta{
		LifecycleID: view.key.lifecycleID,
		Revision:    2,
		SessionName: view.key.sessionName,
		ActiveTabID: "tab-1",
		Tabs:        []ports.SessionTabMeta{{ID: "tab-1", Name: "new"}},
	}
	frame := remoteMetadataFrame(t, metadata)
	require.NoError(t, d.handleRemoteLinkFrame(link, frame))
	require.NoError(t, d.handleRemoteLinkFrame(link, frame), "duplicate metadata must be ignored")
	require.NoError(t, d.handleRemoteLinkFrame(link, remoteMetadataFrame(t, ports.SessionMeta{
		LifecycleID: view.key.lifecycleID,
		Revision:    1,
		SessionName: view.key.sessionName,
		ActiveTabID: "tab-1",
		Tabs:        []ports.SessionTabMeta{{ID: "tab-1", Name: "old"}},
	})), "older metadata must be ignored")

	view.mu.Lock()
	require.Same(t, link, view.link)
	require.True(t, link.active)
	require.Equal(t, uint64(2), view.metadata.Revision)
	require.Equal(t, "new", view.metadata.Tabs[0].Name)
	view.mu.Unlock()

	require.NoError(t, d.handleRemoteLinkFrame(link, ports.Frame{Type: ports.MsgPing, Payload: ports.MarshalPing(ports.Ping{})}))
	awaitFrame(t, transport.sent, ports.MsgPong)
}

func TestRemoteLinkRejectsInvalidMetadataIdentityAndOrdering(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, []byte)
	}{
		{
			name: "lifecycle identity",
			mutate: func(_ *testing.T, payload []byte) {
				payload[0] = 2
			},
		},
		{
			name: "session identity",
			mutate: func(t *testing.T, payload []byte) {
				metadata := ports.SessionMeta{
					LifecycleID: domain.SessionLifecycleID{1}, Revision: 2, SessionName: "remote!", ActiveTabID: "tab-1",
					Tabs: []ports.SessionTabMeta{{ID: "tab-1", Name: "new"}},
				}
				encoded, err := ports.MarshalSessionMeta(metadata)
				require.NoError(t, err)
				copy(payload, encoded)
			},
		},
		{
			name: "zero revision",
			mutate: func(_ *testing.T, payload []byte) {
				clear(payload[16:24])
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, view, link, _ := newRemoteMetadataLinkFixture(t)
			metadata := ports.SessionMeta{
				LifecycleID: view.key.lifecycleID,
				Revision:    2,
				SessionName: view.key.sessionName,
				ActiveTabID: "tab-1",
				Tabs:        []ports.SessionTabMeta{{ID: "tab-1", Name: "new"}},
			}
			payload, err := ports.MarshalSessionMeta(metadata)
			require.NoError(t, err)
			test.mutate(t, payload)
			err = d.handleRemoteLinkFrame(link, ports.Frame{Type: ports.MsgSessionMeta, Payload: payload})
			require.Error(t, err)
			view.mu.Lock()
			require.Same(t, link, view.link)
			require.Equal(t, uint64(1), view.metadata.Revision)
			view.mu.Unlock()
		})
	}
}

func TestValidateInitialRemoteLinkTargetResolvesStoppedSelectorsStrictly(t *testing.T) {
	baseTarget := remoteLinkTestTarget()
	baseTarget.LiveTabID = ""
	baseTarget.Stopped = true
	baseMetadata := ports.SessionMeta{
		LifecycleID: baseTarget.LifecycleID,
		Revision:    1,
		SessionName: baseTarget.SessionName,
		ActiveTabID: "tab-b",
		Tabs: []ports.SessionTabMeta{
			{ID: "tab-a", Name: "alpha"},
			{ID: "tab-b", Name: "beta"},
		},
	}
	tests := []struct {
		name    string
		mutate  func(*domain.RemoteSessionTarget, *ports.SessionMeta)
		wantErr string
	}{
		{
			name: "valid stable selector",
			mutate: func(target *domain.RemoteSessionTarget, _ *ports.SessionMeta) {
				target.StoppedTab = domain.NewStableTabSelector("tab-b")
			},
		},
		{
			name: "valid ordinal selector",
			mutate: func(target *domain.RemoteSessionTarget, _ *ports.SessionMeta) {
				target.StoppedTab = domain.NewOrdinalTabSelector(1, "beta", 2)
			},
		},
		{
			name: "wrong stable ID",
			mutate: func(target *domain.RemoteSessionTarget, _ *ports.SessionMeta) {
				target.StoppedTab = domain.NewStableTabSelector("tab-missing")
			},
			wantErr: "stopped tab selector mismatch",
		},
		{
			name: "missing stable ID",
			mutate: func(_ *domain.RemoteSessionTarget, metadata *ports.SessionMeta) {
				metadata.Tabs[0].ID = ""
			},
			wantErr: "invalid initial metadata",
		},
		{
			name: "duplicate stable IDs",
			mutate: func(_ *domain.RemoteSessionTarget, metadata *ports.SessionMeta) {
				metadata.Tabs[0].ID = metadata.Tabs[1].ID
			},
			wantErr: "invalid initial metadata",
		},
		{
			name: "reordered ordinal selector",
			mutate: func(target *domain.RemoteSessionTarget, metadata *ports.SessionMeta) {
				target.StoppedTab = domain.NewOrdinalTabSelector(1, "beta", 2)
				metadata.Tabs[0], metadata.Tabs[1] = metadata.Tabs[1], metadata.Tabs[0]
			},
			wantErr: "stopped tab selector mismatch",
		},
		{
			name: "ordinal name mismatch",
			mutate: func(target *domain.RemoteSessionTarget, _ *ports.SessionMeta) {
				target.StoppedTab = domain.NewOrdinalTabSelector(1, "renamed", 2)
			},
			wantErr: "stopped tab selector mismatch",
		},
		{
			name: "ordinal count mismatch",
			mutate: func(target *domain.RemoteSessionTarget, _ *ports.SessionMeta) {
				target.StoppedTab = domain.NewOrdinalTabSelector(1, "beta", 3)
			},
			wantErr: "stopped tab selector mismatch",
		},
		{
			name: "active tab mismatch",
			mutate: func(target *domain.RemoteSessionTarget, metadata *ports.SessionMeta) {
				target.StoppedTab = domain.NewStableTabSelector("tab-b")
				metadata.ActiveTabID = "tab-a"
			},
			wantErr: "metadata active tab mismatch",
		},
		{
			name: "running target validation remains strict",
			mutate: func(target *domain.RemoteSessionTarget, metadata *ports.SessionMeta) {
				*target = remoteLinkTestTarget()
				metadata.ActiveTabID = "tab-a"
				metadata.Tabs[1].ID = target.LiveTabID
			},
			wantErr: "metadata active tab mismatch",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			target := baseTarget
			metadata := baseMetadata
			metadata.Tabs = append([]ports.SessionTabMeta(nil), baseMetadata.Tabs...)
			test.mutate(&target, &metadata)

			err := validateInitialRemoteLinkTarget(metadata, target)
			if test.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, test.wantErr)
		})
	}
}

func TestOpenRemoteViewRejectsStoppedTargetBeforeCandidatePublication(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	target.LiveTabID = ""
	target.Stopped = true
	target.StoppedTab = domain.NewStableTabSelector("tab-b")
	transport := newRemoteLinkTestTransport()
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID: "remote-id", SessionName: target.SessionName, RenderMode: ports.RenderModeProxiedContent,
	})}}
	metadataPayload, err := ports.MarshalSessionMeta(ports.SessionMeta{
		LifecycleID: target.LifecycleID,
		Revision:    1,
		SessionName: target.SessionName,
		ActiveTabID: "tab-a",
		Tabs: []ports.SessionTabMeta{
			{ID: "tab-a", Name: "alpha"},
			{ID: "tab-b", Name: "beta"},
		},
	})
	require.NoError(t, err)
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgSessionMeta, Payload: metadataPayload}}
	d.remoteDialerFactory = &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}

	view, err := d.openRemoteView(context.Background(), target, domain.Size{Cols: 80, Rows: 24})
	require.Nil(t, view)
	require.ErrorContains(t, err, "metadata active tab mismatch")
	select {
	case <-transport.closed:
	default:
		t.Fatal("rejected stopped-target candidate transport was not closed")
	}
	d.mu.Lock()
	require.Empty(t, d.remoteViews)
	require.Empty(t, d.remoteViewConstructions)
	d.mu.Unlock()
}

func TestOpenRemoteViewPublishesHandshakeReadyCandidate(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, transport, target, size)
	factory := &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}
	d.remoteDialerFactory = factory
	d.remoteTransportMode = ports.RemoteTransportUDP

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	require.NotNil(t, view)
	require.Equal(t, int32(1), factory.calls.Load())

	helloFrame := awaitFrame(t, transport.sent, ports.MsgHello)
	hello, err := ports.UnmarshalHello(helloFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.IntentAttach, hello.Intent)
	require.Equal(t, ports.RenderModeProxiedContent, hello.RenderMode)
	require.Equal(t, contentSize(size), hello.Size)
	require.Equal(t, ports.EnvironmentPolicyDaemonOwned, hello.EnvironmentPolicy)
	require.Equal(t, &target, hello.RemoteTarget)

	ackFrame := awaitFrame(t, transport.sent, ports.MsgAck)
	ack, err := ports.UnmarshalAck(ackFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.Ack{Epoch: 1, State: 1}, ack)

	d.mu.Lock()
	require.Same(t, view, d.remoteViewByKeyLocked(view.key))
	d.mu.Unlock()
	view.mu.Lock()
	require.Contains(t, screenLineText(view.screen, 0), "remote marker")
	require.Equal(t, target.LifecycleID, view.metadata.LifecycleID)
	require.Equal(t, target.LiveTabID, view.metadata.ActiveTabID)
	view.mu.Unlock()

	require.NoError(t, transport.Close())
}

func TestOpenRemoteViewReconnectsInactiveExactView(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	firstTransport := newRemoteLinkTestTransport()
	secondTransport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshakeWithContent(t, firstTransport, target, size, "first generation")
	enqueueRemoteLinkHandshakeWithContent(t, secondTransport, target, size, "second generation")
	factory := &remoteLinkSequenceFactory{dialers: []ports.Dialer{
		&remoteLinkTestDialer{transport: firstTransport},
		&remoteLinkTestDialer{transport: secondTransport},
	}}
	d.remoteDialerFactory = factory

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, firstTransport.sent, ports.MsgHello)
	awaitFrame(t, firstTransport.sent, ports.MsgAck)

	localTransport := &closeTrackingTransport{}
	attachment := &attachedClient{tr: localTransport}
	attachment.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(attachment))
	oldLink := view.link
	oldGeneration := view.linkGeneration
	oldLink.active = false
	require.Contains(t, screenLineText(view.screen, 0), "first generation")
	view.mu.Unlock()

	reconnected, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	require.Same(t, view, reconnected)
	require.Equal(t, int32(2), factory.calls.Load(), "an inactive registry link must be re-established")
	awaitFrame(t, secondTransport.sent, ports.MsgHello)
	awaitFrame(t, secondTransport.sent, ports.MsgAck)

	view.mu.Lock()
	require.NotSame(t, oldLink, view.link)
	require.Equal(t, oldGeneration+1, view.linkGeneration)
	require.Same(t, view, view.link.view)
	require.Equal(t, view.linkGeneration, view.link.generation)
	require.True(t, view.link.active)
	require.Contains(t, screenLineText(view.screen, 0), "second generation")
	require.Contains(t, view.attachments, attachment, "reconnect must preserve local attachment membership")
	view.mu.Unlock()
	require.Same(t, view, attachment.currentAttachmentOwner())
	select {
	case <-firstTransport.closed:
	default:
		t.Fatal("reconnect did not close the replaced exact transport")
	}
	awaitTestCompletion(t, oldLink.done, "reconnect did not join the replaced exact transport")

	d.shutdownAll(ports.ReasonServerShutdown)
}

func TestRemoteLinkFailureReconnectsAttachedViewWithoutChangingLocalOwner(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	firstTransport := newRemoteLinkTestTransport()
	secondTransport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, firstTransport, target, size)
	enqueueRemoteLinkHandshake(t, secondTransport, target, size)
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	factory := &remoteLinkSequenceFactory{dialers: []ports.Dialer{
		&remoteLinkTestDialer{transport: firstTransport},
		&remoteLinkTestDialer{transport: secondTransport, started: reconnectStarted, release: releaseReconnect},
	}}
	d.remoteDialerFactory = factory

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, firstTransport.sent, ports.MsgHello)
	awaitFrame(t, firstTransport.sent, ports.MsgAck)
	attachment := &attachedClient{tr: &closeTrackingTransport{}}
	attachment.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(attachment))
	oldLink := view.link
	oldScreen := view.screen
	view.mu.Unlock()

	firstTransport.recv <- remoteLinkTestReceive{err: io.EOF}
	awaitTestCompletion(t, reconnectStarted, "remote link failure did not begin reconnect")
	view.mu.Lock()
	require.Equal(t, remoteViewLinkReconnecting, view.linkState)
	require.Same(t, oldLink, view.link)
	require.Same(t, oldScreen, view.screen)
	view.mu.Unlock()
	require.Same(t, view, attachment.currentAttachmentOwner())

	close(releaseReconnect)
	require.Eventually(t, func() bool {
		view.mu.Lock()
		defer view.mu.Unlock()
		return view.link != oldLink && view.linkState == remoteViewLinkHealthy && view.link != nil && view.link.active
	}, time.Second, 5*time.Millisecond, "validated reconnect should restore the same local remote view")
	require.Same(t, view, attachment.currentAttachmentOwner())
	require.Equal(t, int32(2), factory.calls.Load())
	d.shutdownAll(ports.ReasonServerShutdown)
}

func TestRemoteLinkFailedAutoReconnectMarksViewUnavailableWithoutDetachingLocalOwner(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	firstTransport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, firstTransport, target, size)
	factory := &remoteLinkSequenceFactory{dialers: []ports.Dialer{
		&remoteLinkTestDialer{transport: firstTransport},
		&remoteLinkTestDialer{},
	}}
	d.remoteDialerFactory = factory

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, firstTransport.sent, ports.MsgHello)
	awaitFrame(t, firstTransport.sent, ports.MsgAck)
	attachment := &attachedClient{tr: &closeTrackingTransport{}}
	attachment.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(attachment))
	oldLink := view.link
	view.mu.Unlock()

	firstTransport.recv <- remoteLinkTestReceive{err: io.EOF}
	require.Eventually(t, func() bool {
		view.mu.Lock()
		defer view.mu.Unlock()
		return view.link == oldLink && view.linkState == remoteViewLinkUnavailable && !oldLink.active
	}, time.Second, 5*time.Millisecond, "failed automatic reconnect should leave the local view available for navigation")
	snapshot, ok := view.renderSnapshot(size)
	require.True(t, ok)
	require.Contains(t, remoteStatusSnapshot(view, snapshot).session, "unavailable")
	require.Same(t, view, attachment.currentAttachmentOwner())
	require.Equal(t, int32(2), factory.calls.Load())
	d.shutdownAll(ports.ReasonServerShutdown)
}

func TestStaleFailedReconnectCannotDegradeReplacementGeneration(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	firstTransport := newRemoteLinkTestTransport()
	candidateTransport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, firstTransport, target, size)
	factory := &remoteLinkSequenceFactory{dialers: []ports.Dialer{
		&remoteLinkTestDialer{transport: firstTransport},
		&remoteLinkTestDialer{transport: candidateTransport},
	}}
	d.remoteDialerFactory = factory

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, firstTransport.sent, ports.MsgHello)
	awaitFrame(t, firstTransport.sent, ports.MsgAck)
	attachment := &attachedClient{tr: &closeTrackingTransport{}}
	attachment.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(attachment))
	oldLink := view.link
	view.mu.Unlock()

	firstTransport.recv <- remoteLinkTestReceive{err: io.EOF}
	awaitFrame(t, candidateTransport.sent, ports.MsgHello)

	// A newer publication wins while the single automatic attempt is still
	// waiting for its candidate handshake. Its later failure must not turn the
	// replacement view unavailable or start a second retry.
	replacementTransport := newRemoteLinkTestTransport()
	view.mu.Lock()
	view.linkGeneration++
	replacement := &remoteLink{
		view: view, generation: view.linkGeneration, target: target, transport: replacementTransport,
		cancel: func() {}, done: make(chan struct{}), active: true,
	}
	view.link = replacement
	view.linkState = remoteViewLinkHealthy
	view.reconnectGeneration++
	view.mu.Unlock()
	candidateTransport.recv <- remoteLinkTestReceive{err: io.EOF}
	awaitTestCompletion(t, candidateTransport.closed, "stale reconnect candidate was not closed")

	view.mu.Lock()
	require.Same(t, replacement, view.link)
	require.Equal(t, remoteViewLinkHealthy, view.linkState)
	view.mu.Unlock()
	require.Same(t, view, attachment.currentAttachmentOwner())
	require.Equal(t, int32(2), factory.calls.Load(), "a stale failed reconnect must not start another retry")

	d.markRemoteLinkUnavailable(oldLink)
	require.Equal(t, int32(2), factory.calls.Load(), "a retired link cannot start another retry")
	d.shutdownAll(ports.ReasonServerShutdown)
}

func TestOpenRemoteViewFailedReconnectPreservesViewUntilValidatedPublication(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	firstTransport := newRemoteLinkTestTransport()
	failedTransport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshakeWithContent(t, firstTransport, target, size, "retained generation")
	failedTransport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgWelcome, Payload: ports.MarshalWelcome(ports.Welcome{
		SessionID: "remote-id", SessionName: target.SessionName, RenderMode: ports.RenderModeProxiedContent,
	})}}
	invalidMetadata, err := ports.MarshalSessionMeta(ports.SessionMeta{
		LifecycleID: domain.SessionLifecycleID{9}, Revision: 1, SessionName: target.SessionName, ActiveTabID: target.LiveTabID,
		Tabs: []ports.SessionTabMeta{{ID: target.LiveTabID, Name: "main"}},
	})
	require.NoError(t, err)
	failedTransport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgSessionMeta, Payload: invalidMetadata}}
	factory := &remoteLinkSequenceFactory{dialers: []ports.Dialer{
		&remoteLinkTestDialer{transport: firstTransport},
		&remoteLinkTestDialer{transport: failedTransport},
	}}
	d.remoteDialerFactory = factory

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, firstTransport.sent, ports.MsgHello)
	awaitFrame(t, firstTransport.sent, ports.MsgAck)
	attachment := &attachedClient{tr: &closeTrackingTransport{}}
	attachment.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(attachment))
	oldLink := view.link
	oldGeneration := view.linkGeneration
	oldScreen := view.screen
	oldLink.active = false
	view.mu.Unlock()

	reconnected, err := d.openRemoteView(context.Background(), target, size)
	require.Nil(t, reconnected)
	require.ErrorContains(t, err, "metadata identity mismatch")
	require.Equal(t, int32(2), factory.calls.Load(), "the inactive registry hit must not be returned")
	select {
	case <-failedTransport.closed:
	default:
		t.Fatal("failed reconnect candidate transport was not closed")
	}

	d.mu.Lock()
	require.Same(t, view, d.remoteViewByKeyLocked(view.key))
	d.mu.Unlock()
	view.mu.Lock()
	require.Same(t, oldLink, view.link)
	require.Equal(t, oldGeneration, view.linkGeneration)
	require.Same(t, oldScreen, view.screen)
	require.Contains(t, screenLineText(view.screen, 0), "retained generation")
	require.Contains(t, view.attachments, attachment)
	view.mu.Unlock()
	require.Same(t, view, attachment.currentAttachmentOwner())

	d.shutdownAll(ports.ReasonServerShutdown)
}

func TestOpenRemoteViewReconnectPublicationRejectsStaleLinkGeneration(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	oldTransport := newRemoteLinkTestTransport()
	candidateTransport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, oldTransport, target, size)
	enqueueRemoteLinkHandshakeWithContent(t, candidateTransport, target, size, "stale candidate")
	reconnectStarted := make(chan struct{})
	releaseReconnect := make(chan struct{})
	factory := &remoteLinkSequenceFactory{dialers: []ports.Dialer{
		&remoteLinkTestDialer{transport: oldTransport},
		&remoteLinkTestDialer{transport: candidateTransport, started: reconnectStarted, release: releaseReconnect},
	}}
	d.remoteDialerFactory = factory

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, oldTransport.sent, ports.MsgHello)
	awaitFrame(t, oldTransport.sent, ports.MsgAck)
	view.mu.Lock()
	observedLink := view.link
	observedLink.active = false
	view.mu.Unlock()

	type result struct {
		view *remoteView
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		reconnected, reconnectErr := d.openRemoteView(context.Background(), target, size)
		resultCh <- result{view: reconnected, err: reconnectErr}
	}()
	awaitTestCompletion(t, reconnectStarted, "reconnect dial did not start")

	newerTransport := newRemoteLinkTestTransport()
	_, cancelNewer := context.WithCancel(context.Background())
	view.mu.Lock()
	view.linkGeneration++
	newerLink := &remoteLink{
		view: view, generation: view.linkGeneration, target: target, transport: newerTransport,
		cancel: cancelNewer, done: make(chan struct{}), active: true,
	}
	view.link = newerLink
	view.mu.Unlock()
	d.startRemoteLink(newerLink)
	close(releaseReconnect)

	got := awaitTestValue(t, resultCh, "reconnect did not finish")
	require.NoError(t, got.err)
	require.Same(t, view, got.view)
	view.mu.Lock()
	require.Same(t, newerLink, view.link, "stale reconnect publication replaced a newer generation")
	view.mu.Unlock()
	select {
	case <-candidateTransport.closed:
	default:
		t.Fatal("stale reconnect candidate transport was not closed")
	}
	select {
	case <-newerTransport.closed:
		t.Fatal("stale reconnect closed the newer exact transport")
	default:
	}
	select {
	case <-oldTransport.closed:
		t.Fatal("rejected reconnect closed a transport it did not replace")
	default:
	}

	observedLink.cancel()
	require.NoError(t, oldTransport.Close())
	awaitTestCompletion(t, observedLink.done, "superseded test link did not stop")
	d.shutdownAll(ports.ReasonServerShutdown)
}

func TestOpenRemoteViewCancellationClosesUnpublishedTransport(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	d.remoteDialerFactory = &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}

	ctx, cancel := context.WithCancel(context.Background())
	type result struct {
		view *remoteView
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		view, err := d.openRemoteView(ctx, target, size)
		resultCh <- result{view: view, err: err}
	}()
	awaitFrame(t, transport.sent, ports.MsgHello)
	cancel()

	got := <-resultCh
	require.Nil(t, got.view)
	require.ErrorIs(t, got.err, context.Canceled)
	select {
	case <-transport.closed:
	default:
		t.Fatal("canceled candidate transport was not closed")
	}
	d.mu.Lock()
	require.Empty(t, d.remoteViews)
	require.Empty(t, d.remoteViewConstructions)
	d.mu.Unlock()
}

func TestShutdownAllCancelsUnpublishedRemoteViewConstruction(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	d.remoteDialerFactory = &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}

	type result struct {
		view *remoteView
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		view, err := d.openRemoteView(context.Background(), target, size)
		resultCh <- result{view: view, err: err}
	}()
	awaitFrame(t, transport.sent, ports.MsgHello)

	d.shutdownAll(ports.ReasonServerShutdown)
	got := awaitTestValue(t, resultCh, "remote view construction did not stop at shutdown")
	require.Nil(t, got.view)
	require.ErrorIs(t, got.err, context.Canceled)
	select {
	case <-transport.closed:
	default:
		t.Fatal("shutdown did not close the unpublished remote transport")
	}
	d.mu.Lock()
	require.Empty(t, d.remoteViews)
	require.Empty(t, d.remoteViewConstructions)
	d.mu.Unlock()
}

func TestRemoteLinkAppliesAcceptedOutputAndRequestsOneResetForInvalidState(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, transport, target, size)
	d.remoteDialerFactory = &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, transport.sent, ports.MsgHello)
	awaitFrame(t, transport.sent, ports.MsgAck)

	content := contentSize(size)
	incrementalPayload, err := ports.MarshalOutput(ports.Output{
		Epoch: 1, Base: 1, New: 2, Size: content, Data: []byte(" updated"),
	})
	require.NoError(t, err)
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgOutput, Payload: incrementalPayload}}
	ackFrame := awaitFrame(t, transport.sent, ports.MsgAck)
	ack, err := ports.UnmarshalAck(ackFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.Ack{Epoch: 1, State: 2}, ack)
	view.mu.Lock()
	require.Contains(t, screenLineText(view.screen, 0), "remote marker updated")
	view.mu.Unlock()

	invalidPayload, err := ports.MarshalOutput(ports.Output{
		Epoch: 1, Base: 9, New: 10, Size: content, Data: []byte("must not apply"),
	})
	require.NoError(t, err)
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgOutput, Payload: invalidPayload}}
	awaitFrame(t, transport.sent, ports.MsgOutputResetRequest)
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgOutput, Payload: invalidPayload}}
	transport.recv <- remoteLinkTestReceive{frame: ports.Frame{Type: ports.MsgPing, Payload: ports.MarshalPing(ports.Ping{})}}
	afterInvalid := awaitTestValue(t, transport.sent, "remote link did not process duplicate invalid output")
	require.Equal(t, ports.MsgPong, afterInvalid.Type, "duplicate invalid state must not trigger another reset")

	view.mu.Lock()
	require.NotContains(t, screenLineText(view.screen, 0), "must not apply")
	view.mu.Unlock()
	require.NoError(t, transport.Close())
}

func TestShutdownAllInterruptsPublishedRemoteViewLink(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, transport, target, size)
	d.remoteDialerFactory = &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, transport.sent, ports.MsgHello)
	awaitFrame(t, transport.sent, ports.MsgAck)
	view.mu.Lock()
	link := view.link
	view.mu.Unlock()
	require.NotNil(t, link)

	d.shutdownAll(ports.ReasonServerShutdown)
	select {
	case <-transport.closed:
	default:
		t.Fatal("shutdown did not close the exact remote transport")
	}
	awaitTestCompletion(t, link.done, "remote link worker did not stop after shutdown")
	d.mu.Lock()
	require.Empty(t, d.remoteViews)
	d.mu.Unlock()
}

func TestRemoteLinkForwardsRemoteViewInputAndContentResize(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, transport, target, size)
	d.remoteDialerFactory = &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport}}

	view, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	awaitFrame(t, transport.sent, ports.MsgHello)
	awaitFrame(t, transport.sent, ports.MsgAck)

	clientTransport, _ := newCapturingTransport(t)
	ac := &attachedClient{tr: clientTransport, output: newOutputStateStream(), size: size}
	ac.initOverlays()
	ac.setAttachmentOwner(view)
	view.mu.Lock()
	require.True(t, view.registerAttachmentLocked(ac))
	view.mu.Unlock()
	token := attachmentOwnerToken(view, ac, clientTransport)
	require.True(t, token.attachmentCurrent())

	d.handleSequencedInputForAttachment(token, 42, []byte("remote input"))
	inputFrame := awaitFrame(t, transport.sent, ports.MsgInput)
	input, err := ports.UnmarshalInput(inputFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.Input{InputSeq: 42, Data: []byte("remote input")}, input)

	require.True(t, d.resizeAttachmentForLease(token, domain.Size{Cols: 100, Rows: 30}))
	resizeFrame := awaitFrame(t, transport.sent, ports.MsgResize)
	resize, err := ports.UnmarshalResize(resizeFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, domain.Size{Cols: 100, Rows: 28}, resize.Size)

	require.NoError(t, transport.Close())
}

func TestOpenRemoteViewElectsOneConstructorAndCanceledWaiterDoesNotPoisonWinner(t *testing.T) {
	d := newTestDaemon(t, nil, stubClock{})
	target := remoteLinkTestTarget()
	size := domain.Size{Cols: 80, Rows: 24}
	transport := newRemoteLinkTestTransport()
	enqueueRemoteLinkHandshake(t, transport, target, size)
	started := make(chan struct{})
	release := make(chan struct{})
	factory := &remoteLinkTestFactory{dialer: &remoteLinkTestDialer{transport: transport, started: started, release: release}}
	d.remoteDialerFactory = factory

	type result struct {
		view *remoteView
		err  error
	}
	ownerResult := make(chan result, 1)
	go func() {
		view, err := d.openRemoteView(context.Background(), target, size)
		ownerResult <- result{view: view, err: err}
	}()
	<-started

	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	cancelWaiter()
	view, err := d.openRemoteView(waiterCtx, target, size)
	require.Nil(t, view)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), factory.calls.Load())

	close(release)
	owner := <-ownerResult
	require.NoError(t, owner.err)
	require.NotNil(t, owner.view)
	require.Equal(t, int32(1), factory.calls.Load())

	warm, err := d.openRemoteView(context.Background(), target, size)
	require.NoError(t, err)
	require.Same(t, owner.view, warm)
	require.Equal(t, int32(1), factory.calls.Load())

	require.NoError(t, transport.Close())
}
