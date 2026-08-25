package daemon

import (
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
)

type parkedRouteFailTransport struct{}

func (parkedRouteFailTransport) Send(ports.Frame) error     { return errors.New("send failed") }
func (parkedRouteFailTransport) Recv() (ports.Frame, error) { return ports.Frame{}, io.EOF }
func (parkedRouteFailTransport) Close() error               { return nil }

type parkedRouteExpiryTransport struct {
	sends  chan ports.Frame
	closed chan struct{}
	once   sync.Once
}

func (t *parkedRouteExpiryTransport) Send(frame ports.Frame) error {
	t.sends <- frame
	return nil
}

func (t *parkedRouteExpiryTransport) Recv() (ports.Frame, error) {
	<-t.closed
	return ports.Frame{}, io.EOF
}

func (t *parkedRouteExpiryTransport) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}

type parkedRouteClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *parkedRouteClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *parkedRouteClock) NewTimer(time.Duration) ports.Timer { return stubTimer{} }

func (c *parkedRouteClock) setNow(now time.Time) {
	c.mu.Lock()
	c.now = now
	c.mu.Unlock()
}

func armAndPrepareParkedRoute(t *testing.T, d *Daemon, source *session, ac *attachedClient, sends chan ports.Frame) ports.ParkedRouteLeaseID {
	t.Helper()
	ac.navigationCapabilities |= ports.NavigationCapabilityHomePicker
	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	require.NoError(t, d.sendNavigationActionForAttachment(token, ports.NavigationOpenHomePicker))
	effect.End()
	directiveFrame := awaitFrame(t, sends, ports.MsgNavigationAction)
	directive, err := ports.UnmarshalNavigationDirective(directiveFrame.Payload)
	require.NoError(t, err)

	prepare := ports.ParkedRouteRequest{RequestID: 1, LeaseID: directive.LeaseID, Action: ports.ParkedRoutePrepare}
	require.False(t, d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(prepare)}))
	readyFrame := awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	ready, err := ports.UnmarshalParkedRouteResponse(readyFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ParkedRouteResponse{RequestID: 1, Status: ports.ParkedRouteReady}, ready)
	return directive.LeaseID
}

func TestParkedRouteSwitchesExactLiveTargetOnRetainedTransport(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker

	targetLifecycle := domain.SessionLifecycleID{7}
	target := &session{
		sessionCore: sessionCore{id: "target", name: "target", incarnation: targetLifecycle, attachments: make(map[*attachedClient]struct{})},
		ctx:         source.ctx, cancel: func() {},
		tabs: []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
	}
	publishTiledPaneOwners(target, target.tabs[0])
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)
	require.True(t, ac.parkedRouteOutput.Load())

	targetRequest := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: targetLifecycle,
		SessionName: "target", LiveTabID: domain.TabStableID(target.tabs[0].stableID),
	}
	switchRequest := ports.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: ports.ParkedRouteSwitch, Target: &targetRequest}
	require.False(t, d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(switchRequest)}))

	require.Same(t, target, ac.currentAttachmentSession())
	require.False(t, ac.parkedRouteOutput.Load())
	responseFrame := awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	response, err := ports.UnmarshalParkedRouteResponse(responseFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ParkedRouteResponse{RequestID: 2, Status: ports.ParkedRouteSwitched}, response)
}

func TestParkedRouteSwitchAcceptedBeforeLeaseExpiryCommitsAtomically(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	clock := &parkedRouteClock{}
	d.clock = clock
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker

	targetLifecycle := domain.SessionLifecycleID{7}
	target := &session{
		sessionCore: sessionCore{id: "target", name: "target", incarnation: targetLifecycle, attachments: make(map[*attachedClient]struct{})},
		ctx:         source.ctx, cancel: func() {},
		tabs: []*tab{newTab(nil, domain.Size{Cols: 80, Rows: 23})},
	}
	publishTiledPaneOwners(target, target.tabs[0])
	d.mu.Lock()
	d.sessions[target.id] = target
	d.mu.Unlock()
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)
	d.afterAttachmentEffectsFrozen = func() {
		clock.setNow(time.Time{}.Add(parkedRouteLeaseTTL))
	}

	targetRequest := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: targetLifecycle,
		SessionName: "target", LiveTabID: domain.TabStableID(target.tabs[0].stableID),
	}
	switchRequest := ports.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: ports.ParkedRouteSwitch, Target: &targetRequest}
	d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(switchRequest)})

	require.Same(t, target, ac.currentAttachmentSession())
	responseFrame := awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	response, err := ports.UnmarshalParkedRouteResponse(responseFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ParkedRouteSwitched, response.Status)
}

func TestParkedRouteRestoresExactStoppedTargetWithDaemonEnvironment(t *testing.T) {
	d := newTestDaemon(t, newFactory(t, newQuietPTY()), stubClock{})
	d.baseEnv = []string{"OWNER=daemon"}
	source, ac, sends := addRemoteRefreshPickerOwner(t, d, "source")
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker
	source.mu.Lock()
	source.env = []string{"OWNER=source"}
	source.mu.Unlock()

	lifecycle := domain.SessionLifecycleID{8}
	d.mu.Lock()
	d.inactive["stopped"] = inactiveSession{
		name: "stopped", cwd: "/remote/work", incarnation: lifecycle, state: ports.SessionDown,
		tabNames: []string{"alpha", "beta"},
		tabRecords: []domain.CatalogueTabRecord{
			{StableID: "tab-a", Name: "alpha"},
			{StableID: "tab-b", Name: "beta"},
		},
	}
	d.mu.Unlock()
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)

	target := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: lifecycle,
		SessionName: "stopped", Stopped: true, StoppedTab: domain.NewStableTabSelector("tab-b"),
	}
	switchRequest := ports.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: ports.ParkedRouteSwitch, Target: &target}
	d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(switchRequest)})

	restored := ac.currentAttachmentSession()
	require.NotNil(t, restored)
	require.Equal(t, "stopped", restored.name)
	require.Equal(t, lifecycle, restored.incarnation)
	require.Equal(t, domain.TabStableID("tab-b"), ac.viewSnapshot().tabID)
	restored.mu.Lock()
	require.Equal(t, []string{"OWNER=daemon"}, restored.env)
	restored.mu.Unlock()
}

func TestParkedRouteStaleTargetLeavesSourceResumable(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker
	ac.setRouteSnapshot(ports.RecentRouteSnapshot{Generation: 1})

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)

	missing := domain.RemoteSessionTarget{
		Endpoint: "remote", DisplayOrigin: "remote", LifecycleID: domain.SessionLifecycleID{9},
		SessionName: "missing", LiveTabID: "tab-1",
	}
	switchRequest := ports.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: ports.ParkedRouteSwitch, Target: &missing}
	d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(switchRequest)})
	staleFrame := awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	stale, err := ports.UnmarshalParkedRouteResponse(staleFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ParkedRouteStaleTarget, stale.Status)
	require.Same(t, source, ac.currentAttachmentSession())
	require.True(t, ac.parkedRouteOutput.Load())

	resume := ports.ParkedRouteRequest{RequestID: 3, LeaseID: leaseID, Action: ports.ParkedRouteResume}
	d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(resume)})
	resumedFrame := awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	resumed, err := ports.UnmarshalParkedRouteResponse(resumedFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ParkedRouteResumed, resumed.Status)
	require.False(t, ac.parkedRouteOutput.Load())
	outputFrame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full)
}

func TestParkedRouteDoesNotReplayOneShotEffectsAfterResume(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)
	require.NoError(t, d.boundedSendOutputErr(ac, []byte("one-shot effect")))
	select {
	case frame := <-sends:
		t.Fatalf("parked one-shot effect emitted frame type %d", frame.Type)
	default:
	}

	resume := ports.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: ports.ParkedRouteResume}
	d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(resume)})
	_ = awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	outputFrame := awaitFrame(t, sends, ports.MsgOutput)
	output, err := ports.UnmarshalOutput(outputFrame.Payload)
	require.NoError(t, err)
	require.True(t, output.Full)
	require.NotContains(t, string(output.Data), "one-shot effect")
	select {
	case frame := <-sends:
		t.Fatalf("parked one-shot effect replayed as frame type %d", frame.Type)
	default:
	}
}

func TestParkedRouteExpiredLeaseFailsClosed(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker

	leaseID := armAndPrepareParkedRoute(t, d, source, ac, sends)

	ac.parkedRouteMu.Lock()
	ac.parkedRoute.expiresAt = d.clock.Now()
	ac.parkedRouteMu.Unlock()
	resume := ports.ParkedRouteRequest{RequestID: 2, LeaseID: leaseID, Action: ports.ParkedRouteResume}
	d.handleAttachmentClientFrame(source.attachmentToken(ac, ac.transport()), ports.Frame{Type: ports.MsgParkedRouteRequest, Payload: ports.MarshalParkedRouteRequest(resume)})
	responseFrame := awaitFrame(t, sends, ports.MsgParkedRouteResponse)
	response, err := ports.UnmarshalParkedRouteResponse(responseFrame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.ParkedRouteExpired, response.Status)
	require.Same(t, source, ac.currentAttachmentSession())
	require.False(t, ac.parkedRouteOutput.Load())
}

func TestParkedRouteLeaseExpiryClosesExactRetainedTransport(t *testing.T) {
	d, source, ac, _, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	expiry := make(chan time.Time, 1)
	timer := portsmocks.NewMockTimer(t)
	timer.EXPECT().C().Return(expiry).Maybe()
	timer.EXPECT().Stop().Return(true).Maybe()
	clock := portsmocks.NewMockClock(t)
	clock.EXPECT().Now().Return(time.Time{}).Maybe()
	clock.EXPECT().NewTimer(mock.Anything).Return(timer).Once()
	d.clock = clock
	transport := &parkedRouteExpiryTransport{sends: make(chan ports.Frame, 8), closed: make(chan struct{})}
	ac.replaceTransport(transport)

	_ = armAndPrepareParkedRoute(t, d, source, ac, transport.sends)
	expiry <- time.Time{}.Add(parkedRouteLeaseTTL)

	select {
	case <-transport.closed:
	case <-time.After(time.Second):
		t.Fatal("parked transport remained open after its lease expired")
	}
}

func TestParkedRouteMalformedSwitchRequestsFailClosed(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	leaseID := ports.ParkedRouteLeaseID{1}
	for _, request := range []ports.ParkedRouteRequest{
		{RequestID: 1, LeaseID: leaseID, Action: ports.ParkedRouteSwitch},
		{RequestID: 2, Action: ports.ParkedRouteSwitch, Target: &domain.RemoteSessionTarget{}},
		{RequestID: 3, LeaseID: leaseID, Action: 99},
	} {
		d.handleParkedRouteRequest(source.attachmentToken(ac, ac.transport()), request)
	}
	require.Same(t, source, ac.currentAttachmentSession())
	select {
	case frame := <-sends:
		t.Fatalf("malformed parked route emitted frame type %d", frame.Type)
	default:
	}
}

func TestParkedRouteResponseFailureDoesNotDrainItsOwnFrameEffect(t *testing.T) {
	d, source, ac, _, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.replaceTransport(parkedRouteFailTransport{})
	token := source.attachmentToken(ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect

	result := make(chan bool, 1)
	go func() {
		result <- d.respondParkedRoute(token, ports.ParkedRouteResponse{RequestID: 1, Status: ports.ParkedRouteReady})
	}()
	require.False(t, awaitTestValue(t, result, "parked-route send failure cleanup"))
	d.attachmentCleanupWg.Wait()
}

func TestParkedRouteDetachClearsLeaseAndSuspension(t *testing.T) {
	d, source, ac, sends, releases := newManualTabSession(t, 1)
	defer releaseAll(releases)
	ac.navigationCapabilities = ports.NavigationCapabilityHomePicker

	_ = armAndPrepareParkedRoute(t, d, source, ac, sends)
	require.True(t, ac.parkedRouteOutput.Load())
	require.NoError(t, d.boundedSendOutputErr(ac, []byte("suppressed side effect")))
	select {
	case frame := <-sends:
		t.Fatalf("parked side effect emitted frame type %d", frame.Type)
	default:
	}

	d.clientGone(source, ac, ac.transport(), true)
	ac.parkedRouteMu.Lock()
	require.Nil(t, ac.parkedRoute)
	ac.parkedRouteMu.Unlock()
	require.False(t, ac.parkedRouteOutput.Load())
}
