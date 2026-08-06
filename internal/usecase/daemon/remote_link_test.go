package daemon

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"

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
	content := contentSize(size)
	metaPayload, err := ports.MarshalSessionMeta(ports.SessionMeta{
		LifecycleID: target.LifecycleID,
		Revision:    1,
		SessionName: target.SessionName,
		ActiveTabID: target.LiveTabID,
		Tabs:        []ports.SessionTabMeta{{ID: target.LiveTabID, Name: "main"}},
	})
	require.NoError(t, err)
	outputPayload, err := ports.MarshalOutput(ports.Output{
		Epoch: 1, New: 1, Size: content, Full: true, Data: []byte("remote marker"),
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
