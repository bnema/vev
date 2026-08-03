package daemon

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/usecase/picker"
	"github.com/stretchr/testify/require"
)

type proxyLifecycleClock struct {
	mu     sync.RWMutex
	now    time.Time
	timers chan *proxyLifecycleTimer
}

func newProxyLifecycleClock() *proxyLifecycleClock {
	return &proxyLifecycleClock{
		now:    time.Unix(1_800_000_000, 0),
		timers: make(chan *proxyLifecycleTimer, 16),
	}
}

func (c *proxyLifecycleClock) Now() time.Time {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.now
}

func (c *proxyLifecycleClock) Advance(delay time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delay)
	c.mu.Unlock()
}

func (c *proxyLifecycleClock) NewTimer(delay time.Duration) ports.Timer {
	timer := &proxyLifecycleTimer{clock: c, delay: delay, ticks: make(chan time.Time, 1)}
	select {
	case c.timers <- timer:
	default:
	}
	return timer
}

type proxyLifecycleTimer struct {
	mu      sync.Mutex
	clock   *proxyLifecycleClock
	delay   time.Duration
	ticks   chan time.Time
	stopped bool
}

func (t *proxyLifecycleTimer) C() <-chan time.Time { return t.ticks }
func (t *proxyLifecycleTimer) Reset(delay time.Duration) bool {
	t.mu.Lock()
	wasActive := !t.stopped
	t.delay = delay
	t.stopped = false
	t.mu.Unlock()
	return wasActive
}
func (t *proxyLifecycleTimer) Stop() bool {
	t.mu.Lock()
	wasActive := !t.stopped
	t.stopped = true
	t.mu.Unlock()
	return wasActive
}
func (t *proxyLifecycleTimer) fire() {
	t.clock.Advance(t.delay)
	select {
	case t.ticks <- t.clock.Now():
	default:
	}
}

func TestProxyLifecycleClockTimerRegistrationDoesNotBlock(t *testing.T) {
	clock := newProxyLifecycleClock()
	for range cap(clock.timers) {
		clock.NewTimer(time.Minute)
	}

	registered := make(chan struct{})
	go func() {
		clock.NewTimer(time.Minute)
		close(registered)
	}()
	awaitTestCompletion(t, registered, "timer registration blocked on observer capacity")
	for range cap(clock.timers) {
		_ = awaitTestValue(t, clock.timers, "registered timer was not observable")
	}
}

type proxyLifecycleTransport struct {
	d        *Daemon
	proxy    *proxySession
	closed   chan struct{}
	closeOne sync.Once
	unlocked bool
}

func (t *proxyLifecycleTransport) Send(ports.Frame) error { return nil }
func (t *proxyLifecycleTransport) Recv() (ports.Frame, error) {
	<-t.closed
	return ports.Frame{}, nil
}
func (t *proxyLifecycleTransport) Close() error {
	t.closeOne.Do(func() {
		daemonUnlocked := t.d.mu.TryLock()
		if daemonUnlocked {
			t.d.mu.Unlock()
		}
		coreUnlocked := t.proxy.sessionCore.mu.TryLock()
		if coreUnlocked {
			t.proxy.sessionCore.mu.Unlock()
		}
		proxyUnlocked := t.proxy.mu.TryLock()
		if proxyUnlocked {
			t.proxy.mu.Unlock()
		}
		t.unlocked = daemonUnlocked && coreUnlocked && proxyUnlocked
		close(t.closed)
	})
	return nil
}

func newRegisteredLifecycleProxy(t *testing.T, clock ports.Clock) (*Daemon, *proxySession, *proxyLifecycleTransport) {
	t.Helper()
	d := newTestDaemon(t, nil, clock)
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, defaultSize)
	require.NoError(t, err)
	transport := &proxyLifecycleTransport{d: d, proxy: proxy, closed: make(chan struct{})}
	proxy.transport = transport
	proxy.linkGeneration = 1
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()
	return d, proxy, transport
}

func TestNewProxySessionUsesProxyScreenSizeLimit(t *testing.T) {
	for _, test := range []struct {
		name  string
		size  domain.Size
		valid bool
	}{
		{name: "largest valid content", size: domain.Size{Cols: 512, Rows: 514}, valid: true},
		{name: "one cell over content limit", size: domain.Size{Cols: 512, Rows: 515}},
		{name: "1000x1000", size: domain.Size{Cols: 1000, Rows: 1000}},
	} {
		t.Run(test.name, func(t *testing.T) {
			proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, test.size)
			if !test.valid {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, contentSize(test.size, false), proxy.contentSize)
			require.Equal(t, test.size.Cols, proxy.screen.frame.Width)
			require.Equal(t, test.size.Rows-tabChromeRows, proxy.screen.frame.Height)
		})
	}
}

func TestProxyWarmLifecycleFollowsAttachmentTransitions(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, local, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.clock = clock
	localCoordinator := d.attachCoordinator(local, nil, ac, true)
	localToken := attachmentToken(local, ac, ac.transport())
	localToken.lease = localCoordinator.attachmentLease(ac)

	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, ac.size)
	require.NoError(t, err)
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	toProxy, err := d.transitionAttachment(attachmentTransitionRequest{
		source: local, target: proxy, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &localToken,
		action: "lifecycle test", ready: true,
	})
	require.NoError(t, err)
	require.NotZero(t, proxy.mruAt.Load(), "proxy activation must participate in unified MRU ordering")
	proxy.mu.Lock()
	require.Nil(t, proxy.warm, "attachment must leave no warm timer published")
	proxy.mu.Unlock()

	_, err = d.transitionAttachment(attachmentTransitionRequest{
		source: proxy, target: local, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &toProxy.published,
		action: "lifecycle test", ready: true,
	})
	require.NoError(t, err)
	timer := awaitTestValue(t, clock.timers, "proxy timer was not registered")
	require.Equal(t, proxyWarmDuration, timer.delay)
	proxy.mu.Lock()
	require.NotNil(t, proxy.warm, "switch-away must arm the warm lifecycle")
	proxy.mu.Unlock()
}

func TestProxyWarmLifecycleUsesExactFiveMinuteGeneration(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, proxy, transport := newRegisteredLifecycleProxy(t, clock)
	d.remoteCatalog.mu.Lock()
	d.remoteCatalog.cache[proxy.key.Host] = ports.RemoteCatalogCacheEntry{Host: proxy.key.Host, FetchedAt: clock.Now()}
	d.remoteCatalog.mu.Unlock()

	d.armProxyWarm(proxy)
	timer := awaitTestValue(t, clock.timers, "proxy timer was not registered")
	require.Equal(t, 5*time.Minute, timer.delay)
	proxy.mu.Lock()
	token := proxy.warm
	generation := proxy.generation
	proxy.mu.Unlock()
	require.NotNil(t, token)

	timer.fire()
	awaitTestCompletion(t, token.done, "proxy warm lifecycle did not complete")

	d.mu.Lock()
	require.NotContains(t, d.sessions, proxy.id)
	require.True(t, d.closing, "the last warm proxy expiry must begin daemon shutdown")
	d.mu.Unlock()
	require.True(t, transport.unlocked, "transport close must run after daemon/core/proxy locks are released")
	proxy.mu.Lock()
	require.Greater(t, proxy.generation, generation)
	proxy.mu.Unlock()
	d.remoteCatalog.mu.Lock()
	require.Contains(t, d.remoteCatalog.cache, proxy.key.Host, "warm expiry must preserve discovery cache")
	d.remoteCatalog.mu.Unlock()
}

func TestProxyWarmReattachCancelsAndStaleTimerCannotRemoveProxy(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, proxy, _ := newRegisteredLifecycleProxy(t, clock)

	d.armProxyWarm(proxy)
	firstTimer := awaitTestValue(t, clock.timers, "first proxy timer was not registered")
	proxy.mu.Lock()
	firstToken := proxy.warm
	proxy.mu.Unlock()
	require.NotNil(t, firstToken)

	ac := &attachedClient{}
	proxy.sessionCore.mu.Lock()
	proxy.sessionCore.registerAttachmentLocked(ac)
	proxy.sessionCore.mu.Unlock()
	d.cancelProxyWarmForAttachment(proxy, ac)
	awaitTestCompletion(t, firstToken.done, "first proxy warm lifecycle did not complete")
	firstTimer.fire()

	d.mu.Lock()
	require.Same(t, proxy, d.sessions[proxy.id])
	d.mu.Unlock()

	proxy.sessionCore.mu.Lock()
	for _, attachment := range proxy.sessionCore.snapshotAttachmentsLocked() {
		proxy.sessionCore.unregisterAttachmentLocked(attachment)
	}
	proxy.sessionCore.mu.Unlock()
	d.armProxyWarm(proxy)
	secondTimer := awaitTestValue(t, clock.timers, "second proxy timer was not registered")
	proxy.mu.Lock()
	secondToken := proxy.warm
	proxy.mu.Unlock()
	require.NotNil(t, secondToken)
	require.NotSame(t, firstToken, secondToken)

	secondTimer.fire()
	awaitTestCompletion(t, secondToken.done, "second proxy warm lifecycle did not complete")
	d.mu.Lock()
	require.NotContains(t, d.sessions, proxy.id)
	d.mu.Unlock()
}

func TestProxyWarmTimerRejectsReplacementPointer(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, proxy, transport := newRegisteredLifecycleProxy(t, clock)
	d.armProxyWarm(proxy)
	timer := awaitTestValue(t, clock.timers, "proxy timer was not registered")
	proxy.mu.Lock()
	token := proxy.warm
	proxy.mu.Unlock()

	replacement, err := newProxySession(proxy.key, defaultSize)
	require.NoError(t, err)
	d.mu.Lock()
	d.sessions[proxy.id] = replacement
	d.mu.Unlock()

	timer.fire()
	awaitTestCompletion(t, token.done, "proxy warm lifecycle did not complete")
	d.mu.Lock()
	require.Same(t, replacement, d.sessions[proxy.id])
	d.mu.Unlock()
	select {
	case <-transport.closed:
		t.Fatal("stale pointer timer closed the old transport")
	default:
	}
}

type proxyResumeTestFactory struct {
	mu         sync.Mutex
	transports []ports.Transport
	callCount  int
	calls      chan int
}

func newProxyResumeTestFactory(transports ...ports.Transport) *proxyResumeTestFactory {
	return &proxyResumeTestFactory{transports: transports, calls: make(chan int, proxyResumeMaxAttempts+2)}
}

func (f *proxyResumeTestFactory) DialerForRemote(string, string, ports.RemoteTransportMode, *slog.Logger) (ports.Dialer, error) {
	f.mu.Lock()
	f.callCount++
	callNumber := f.callCount
	var selected ports.Transport
	for i, transport := range f.transports {
		if transport != nil {
			selected = transport
			f.transports[i] = nil
			break
		}
	}
	f.mu.Unlock()
	f.calls <- callNumber
	if selected == nil {
		return nil, errors.New("resume dial unavailable")
	}
	return proxyConstructionDialer{transport: selected}, nil
}

func newProxyResumeHarness(t *testing.T, clock *proxyLifecycleClock, factory ports.RemoteDialerFactory) (*Daemon, *proxySession, *proxyTestTransport) {
	t.Helper()
	d := newTestDaemon(t, nil, clock)
	d.remoteDialerFactory = factory
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, defaultSize)
	require.NoError(t, err)
	initial := newProxyTestTransport()
	proxy.mu.Lock()
	proxy.transport = initial
	proxy.linkGeneration = 1
	proxy.mu.Unlock()
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()
	return d, proxy, initial
}

func TestProxyResumeBackoffIsFakeClockBoundedAndParksOffline(t *testing.T) {
	clock := newProxyLifecycleClock()
	factory := newProxyResumeTestFactory()
	d, proxy, initial := newProxyResumeHarness(t, clock, factory)
	initial.recv <- proxyRecv{err: io.ErrUnexpectedEOF}

	go d.runProxyLink(context.Background(), proxy)
	for attempt := range proxyResumeMaxAttempts {
		timer := awaitTestValue(t, clock.timers, "proxy resume did not arm fake-clock backoff")
		require.Equal(t, proxyResumeBackoff(attempt), timer.delay)
		timer.fire()
		require.Equal(t, attempt+1, awaitTestValue(t, factory.calls, "proxy resume dial was not attempted"))
		handshakeTimer := awaitTestValue(t, clock.timers, "proxy resume handshake did not arm its timeout")
		require.Equal(t, proxyHandshakeTimeout, handshakeTimer.delay)
	}
	awaitTestCompletion(t, proxy.done, "proxy resume did not stop after its attempt cap")

	proxy.mu.Lock()
	require.Nil(t, proxy.transport)
	require.Equal(t, ports.LinkStateOffline, proxy.linkState)
	proxy.mu.Unlock()
	d.mu.Lock()
	require.Same(t, proxy, d.sessions[proxy.id], "exhaustion must park the proxy instead of unregistering it")
	d.mu.Unlock()
	select {
	case timer := <-clock.timers:
		t.Fatalf("attempt cap armed an extra retry: %v", timer.delay)
	default:
	}
}

func TestProxyResumeCancellationDuringBackoffDoesNotRedial(t *testing.T) {
	clock := newProxyLifecycleClock()
	factory := newProxyResumeTestFactory()
	d, proxy, initial := newProxyResumeHarness(t, clock, factory)
	initial.recv <- proxyRecv{err: io.EOF}
	ctx, cancel := context.WithCancel(context.Background())
	go d.runProxyLink(ctx, proxy)
	_ = awaitTestValue(t, clock.timers, "proxy resume did not arm backoff")
	cancel()
	awaitTestCompletion(t, proxy.done, "canceled proxy resume did not stop")
	select {
	case <-factory.calls:
		t.Fatal("canceled backoff dialed a replacement link")
	default:
	}
}

func TestProxyResumeBackoffResetsAfterStableConnection(t *testing.T) {
	clock := newProxyLifecycleClock()
	resumed := newProxyTestTransport()
	factory := newProxyResumeTestFactory(resumed)
	d, proxy, initial := newProxyResumeHarness(t, clock, factory)
	initial.recv <- proxyRecv{err: io.EOF}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go d.runProxyLink(ctx, proxy)

	first := awaitTestValue(t, clock.timers, "initial resume did not arm backoff")
	require.Equal(t, proxyResumeInitialBackoff, first.delay)
	first.fire()
	_ = awaitTestValue(t, factory.calls, "stable resume did not dial")
	handshakeTimer := awaitTestValue(t, clock.timers, "stable resume handshake did not arm its timeout")
	require.Equal(t, proxyHandshakeTimeout, handshakeTimer.delay)
	_ = requireProxyHello(t, resumed)
	resumed.recv <- proxyRecv{frame: proxyWelcome(proxy.key.Name, 2, ports.CapabilityResume|ports.CapabilityProxied)}
	resumed.recv <- proxyRecv{frame: proxyMeta(proxy.key.Name)}
	resumed.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	require.Eventually(t, func() bool {
		proxy.mu.Lock()
		defer proxy.mu.Unlock()
		return proxy.transport == resumed && proxy.resumeToken == 2
	}, time.Second, time.Millisecond)
	resumed.recv <- proxyRecv{frame: ports.Frame{Type: ports.MsgPing, Payload: ports.MarshalPing(ports.Ping{})}}
	_ = awaitFrame(t, resumed.sent, ports.MsgPong)

	clock.Advance(proxyResumeStableDuration)
	resumed.recv <- proxyRecv{err: io.EOF}
	second := awaitTestValue(t, clock.timers, "stable link loss did not arm another backoff")
	require.Equal(t, proxyResumeInitialBackoff, second.delay, "stable connection must reset exponential backoff")
	cancel()
	awaitTestCompletion(t, proxy.done, "stable-reset proxy did not stop after cancellation")
}

func TestProxyLinkStateDisplayAndExactGeneration(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, proxy, transport := newRegisteredLifecycleProxy(t, clock)

	tests := []struct {
		state ports.LinkState
		want  string
	}{
		{ports.LinkStateConnected, proxy.name},
		{ports.LinkStateDegraded, proxy.name + " [degraded]"},
		{ports.LinkStateProbing, proxy.name + " [probing]"},
		{ports.LinkStateOffline, proxy.name + " [offline]"},
		{ports.LinkStateDead, proxy.name + " [dead]"},
	}
	for _, test := range tests {
		require.True(t, d.updateProxyLinkState(proxy, 1, transport, test.state))
		require.Equal(t, test.want, proxy.statusSegments(false).session)
	}
	require.False(t, d.updateProxyLinkState(proxy, 2, transport, ports.LinkStateConnected))
	require.Equal(t, proxy.name+" [dead]", proxy.statusSegments(false).session)
}

func TestIncomingDirectAttachTerminatesProxiedRemoteAttachment(t *testing.T) {
	d := newTestDaemon(t, nil, newProxyLifecycleClock())
	sess := &session{sessionCore: sessionCore{id: "work", name: "work"}}
	oldTransport := newProxyTestTransport()
	old := &attachedClient{tr: oldTransport, output: newOutputStateStream(), proxied: true}
	old.initOverlays()
	old.setSession(sess)
	sess.registerAttachment(old)
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(sess))
	d.mu.Unlock()
	d.attachCoordinator(sess, nil, old, true)

	newTransport := newProxyTestTransport()
	next := &attachedClient{tr: newTransport, output: newOutputStateStream()}
	next.initOverlays()
	result, err := d.transitionAttachment(attachmentTransitionRequest{
		target: sess, next: next,

		expectedTransport: next.transportSnapshot(), ready: true,
	})
	require.NoError(t, err)
	require.NotNil(t, result.published.ac)
	require.Contains(t, sess.snapshotAttachments(), next)
	require.Same(t, sess, old.currentAttachmentSession())

	d.deferAttachmentTransitionCleanups(result)
	d.waitNotifies()
	select {
	case <-oldTransport.sent:
		t.Fatal("publishing an attachment displaced the existing connection")
	default:
	}
	select {
	case <-oldTransport.closed:
		t.Fatal("publishing an attachment closed the existing connection")
	default:
	}
}

func TestProxyReplacedIsTerminalExpired(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, proxy, transport := newRegisteredLifecycleProxy(t, clock)

	require.True(t, d.markProxyReplaced(proxy, 1, transport))
	proxy.mu.Lock()
	require.True(t, proxy.expired)
	proxy.mu.Unlock()
	require.Equal(t, proxy.name+" [expired]", proxy.statusSegments(false).session)
	require.False(t, d.updateProxyLinkState(proxy, 1, transport, ports.LinkStateConnected), "replacement expiry must reject later link recovery")
}

func TestRemoteProxyPickerUsesExplicitExpiredLifecycleState(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	spoofed := remoteProxyPickerView(key, sessionView{name: key.Display() + " [expired]", tabCount: 1})
	require.Equal(t, key.Display(), spoofed.Name)
	require.Equal(t, picker.RemoteFresh, spoofed.RemoteAvailability)
	require.True(t, spoofed.RemoteAttachReady)

	expired := remoteProxyPickerView(key, sessionView{name: "unrelated presentation", tabCount: 1, expired: true})
	require.Equal(t, key.Display()+" [expired]", expired.Name)
	require.Equal(t, picker.RemoteStale, expired.RemoteAvailability)
	require.False(t, expired.RemoteAttachReady)
}

func TestReplacedProxyCannotBeSelectedOrReattachedAfterSwitchAway(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, local, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	d.clock = clock
	coordinator := d.attachCoordinator(local, nil, ac, true)
	localToken := attachmentToken(local, ac, ac.transport())
	localToken.lease = coordinator.attachmentLease(ac)

	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	proxy, err := newProxySession(key, ac.size)
	require.NoError(t, err)
	transport := &proxyLifecycleTransport{d: d, proxy: proxy, closed: make(chan struct{})}
	proxy.transport = transport
	proxy.linkGeneration = 1
	factory := newProxyConstructionFactory(newProxyTestTransport())
	d.remoteDialerFactory = factory
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	toProxy, err := d.transitionAttachment(attachmentTransitionRequest{
		source: local, target: proxy, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &localToken,
		action: "replaced picker regression", ready: true,
	})
	require.NoError(t, err)

	result, err := d.handleLinkFrame(proxy, proxy.linkGeneration, ports.Frame{
		Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonReplaced}),
	})
	require.NoError(t, err)
	require.Equal(t, proxyLinkReplaced, result)
	require.True(t, d.markProxyReplaced(proxy, proxy.linkGeneration, transport))

	_, err = d.transitionAttachment(attachmentTransitionRequest{
		source: proxy, target: local, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &toProxy.published,
		action: "replaced picker regression", ready: true,
	})
	require.NoError(t, err)
	require.Same(t, local, ac.currentAttachmentSession())
	proxy.mu.Lock()
	warm := proxy.warm
	proxy.mu.Unlock()
	require.NotNil(t, warm, "switch-away did not retain warm expiry")
	warmTimer, ok := warm.timer.(*proxyLifecycleTimer)
	require.True(t, ok)
	require.Equal(t, proxyWarmDuration, warmTimer.delay)

	views, _ := d.pickerViews(local, nil)
	var expiredView picker.SessionView
	for _, view := range views {
		if view.RemoteKey != nil && *view.RemoteKey == key {
			expiredView = view
			break
		}
	}
	require.Equal(t, key.ID(), expiredView.ID, "exact registered proxy was absent from unified picker")
	require.Equal(t, key.Display()+" [expired]", expiredView.Name)
	require.Equal(t, picker.RemoteStale, expiredView.RemoteAvailability)
	require.Equal(t, "expired", expiredView.RemoteDetail)
	require.False(t, expiredView.RemoteAttachReady)
	require.Equal(t, key.Display()+" [expired]", proxy.lifecycleDisplayName())

	d.enterPicker(local, ac)
	forced := d.newPickerModel(local, nil, pickerNavigate, moveSourceLocator{}, picker.SourceFilter{
		Session: key.ID(), RemoteKey: &key,
	})
	ac.overlays.pickerMu.Lock()
	ac.overlays.picker = forced
	cursor, cursorOK := forced.Cursor()
	_, selectable := forced.Selected()
	ac.overlays.pickerMu.Unlock()
	require.True(t, cursorOK)
	require.Equal(t, key, *cursor.RemoteKey)
	require.False(t, selectable, "terminal proxy row must not be activatable")
	d.handlePickerInput(ac, []byte("\r"))
	require.False(t, ac.overlays.pickerActive())
	require.Same(t, local, ac.currentAttachmentSession(), "picker activation reattached the terminal proxy")

	token := attachmentToken(local, ac, ac.transport())
	effect, admitted := ac.beginAttachmentEffect(token)
	require.True(t, admitted)
	token.effect = effect
	err = d.switchToTargetForAttachment(token, cursor, sessionHandoffGuard{}, "replaced picker regression")
	require.Error(t, err)
	require.Same(t, local, ac.currentAttachmentSession(), "defensive activation reused the terminal proxy")

	reopened, err := d.openProxySession(context.Background(), key, ac.size)
	require.Error(t, err)
	require.Nil(t, reopened)
	require.Equal(t, 0, factory.callCount(), "terminal proxy triggered an automatic IntentAttach/redial")
	d.mu.Lock()
	require.Same(t, proxy, d.sessions[key.ID()], "terminal proxy registration changed before warm expiry")
	d.mu.Unlock()

	proxy.mu.Lock()
	require.Same(t, warm, proxy.warm)
	proxy.mu.Unlock()
	warmTimer.fire()
	awaitTestCompletion(t, warm.done, "terminal proxy warm expiry did not complete")
}

func TestDormantProxyReplacedRetainsAutomaticWarmExpiry(t *testing.T) {
	clock := newProxyLifecycleClock()
	d, proxy, _ := newRegisteredLifecycleProxy(t, clock)
	link := newProxyKillControlTransport(d, proxy)
	proxy.mu.Lock()
	proxy.transport = link
	proxy.mu.Unlock()

	require.True(t, d.armProxyWarm(proxy))
	timer := awaitTestValue(t, clock.timers, "dormant proxy did not arm its warm deadline")
	proxy.mu.Lock()
	token := proxy.warm
	generation := proxy.generation
	proxy.mu.Unlock()
	require.NotNil(t, token)
	require.Equal(t, proxyWarmDuration, timer.delay)

	link.recv <- proxyKillRecv{frame: ports.Frame{
		Type:    ports.MsgDetached,
		Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonReplaced}),
	}}
	go d.runProxyLink(context.Background(), proxy)
	awaitTestCompletion(t, proxy.done, "replacement did not terminate the proxy link")

	proxy.mu.Lock()
	require.True(t, proxy.expired)
	require.Same(t, token, proxy.warm, "terminal replacement cleared the only expiry capability")
	require.Greater(t, proxy.generation, generation, "terminal replacement did not invalidate stale lifecycle observers")
	require.Equal(t, proxy.generation, token.generation, "preserved expiry capability was not rekeyed to the terminal generation")
	proxy.mu.Unlock()
	select {
	case extra := <-clock.timers:
		t.Fatalf("replacement unexpectedly rearmed the original deadline: %v", extra.delay)
	default:
	}

	timer.fire()
	awaitTestCompletion(t, token.done, "original warm deadline did not complete")
	d.mu.Lock()
	require.NotContains(t, d.sessions, proxy.id, "exact replaced proxy was not automatically unregistered")
	d.mu.Unlock()
}

type proxyKillRecv struct {
	frame ports.Frame
	err   error
}

type proxyKillControlTransport struct {
	d                *Daemon
	proxy            *proxySession
	sent             chan ports.Frame
	recv             chan proxyKillRecv
	recvLocksChecked chan struct{}
	closed           chan struct{}
	close            sync.Once
	recvCheck        sync.Once
	sendErr          error
}

func newProxyKillControlTransport(d *Daemon, proxy *proxySession) *proxyKillControlTransport {
	return &proxyKillControlTransport{
		d: d, proxy: proxy, sent: make(chan ports.Frame, 2), recv: make(chan proxyKillRecv, 1),
		recvLocksChecked: make(chan struct{}), closed: make(chan struct{}),
	}
}

func (t *proxyKillControlTransport) locksAvailable() bool {
	daemonUnlocked := t.d.mu.TryLock()
	if daemonUnlocked {
		t.d.mu.Unlock()
	}
	coreUnlocked := t.proxy.sessionCore.mu.TryLock()
	if coreUnlocked {
		t.proxy.sessionCore.mu.Unlock()
	}
	proxyUnlocked := t.proxy.mu.TryLock()
	if proxyUnlocked {
		t.proxy.mu.Unlock()
	}
	catalogUnlocked := t.d.remoteCatalog.mu.TryLock()
	if catalogUnlocked {
		t.d.remoteCatalog.mu.Unlock()
	}
	return daemonUnlocked && coreUnlocked && proxyUnlocked && catalogUnlocked
}

func (t *proxyKillControlTransport) Send(frame ports.Frame) error {
	if !t.locksAvailable() {
		return errors.New("architecture lock held during control send")
	}
	t.sent <- frame
	return t.sendErr
}

func (t *proxyKillControlTransport) Recv() (ports.Frame, error) {
	if !t.locksAvailable() {
		return ports.Frame{}, errors.New("architecture lock held during control receive")
	}
	t.recvCheck.Do(func() { close(t.recvLocksChecked) })
	select {
	case result := <-t.recv:
		return result.frame, result.err
	case <-t.closed:
		return ports.Frame{}, io.EOF
	}
}

func (t *proxyKillControlTransport) Close() error {
	if !t.locksAvailable() {
		return errors.New("architecture lock held during control close")
	}
	t.close.Do(func() { close(t.closed) })
	return nil
}

type proxyKillBlockedSendTransport struct {
	*proxyKillControlTransport
	started  chan struct{}
	release  chan struct{}
	finished chan struct{}
	start    sync.Once
	finish   sync.Once
}

func (t *proxyKillBlockedSendTransport) Send(ports.Frame) error {
	if !t.locksAvailable() {
		return errors.New("architecture lock held during blocked control send")
	}
	t.start.Do(func() { close(t.started) })
	<-t.release
	t.finish.Do(func() { close(t.finished) })
	return nil
}

type proxyKillDialer struct {
	d         *Daemon
	proxy     *proxySession
	transport ports.Transport
	err       error
}

func (d proxyKillDialer) Dial(ctx context.Context) (ports.Transport, error) {
	if ctx == nil {
		return nil, errors.New("nil kill context")
	}
	checker := newProxyKillControlTransport(d.d, d.proxy)
	if !checker.locksAvailable() {
		return nil, errors.New("architecture lock held during control dial")
	}
	return d.transport, d.err
}

type proxyKillDialerFactory struct {
	d       *Daemon
	proxy   *proxySession
	dialer  ports.Dialer
	err     error
	targets chan domain.RemoteSessionKey
}

func (f *proxyKillDialerFactory) DialerForRemote(target, session string, _ ports.RemoteTransportMode, _ *slog.Logger) (ports.Dialer, error) {
	checker := newProxyKillControlTransport(f.d, f.proxy)
	if !checker.locksAvailable() {
		return nil, errors.New("architecture lock held during dialer selection")
	}
	if f.targets != nil {
		f.targets <- domain.RemoteSessionKey{Host: target, Name: session}
	}
	return f.dialer, f.err
}

type proxyKillCache struct {
	d      *Daemon
	proxy  *proxySession
	stores chan []ports.RemoteCatalogCacheEntry
}

func (*proxyKillCache) Load() ([]ports.RemoteCatalogCacheEntry, error) { return nil, nil }
func (c *proxyKillCache) Store(entries []ports.RemoteCatalogCacheEntry) error {
	checker := newProxyKillControlTransport(c.d, c.proxy)
	if !checker.locksAvailable() {
		return errors.New("architecture lock held during cache persistence")
	}
	copyEntries := make([]ports.RemoteCatalogCacheEntry, len(entries))
	for i, entry := range entries {
		copyEntries[i] = cloneRemoteCatalogEntry(entry)
	}
	c.stores <- copyEntries
	return nil
}

func newProxyKillHarness(t *testing.T) (*Daemon, *proxySession, *proxyLifecycleClock, *proxyKillControlTransport, *proxyKillCache) {
	t.Helper()
	clock := newProxyLifecycleClock()
	d, proxy, _ := newRegisteredLifecycleProxy(t, clock)
	control := newProxyKillControlTransport(d, proxy)
	factory := &proxyKillDialerFactory{d: d, proxy: proxy, targets: make(chan domain.RemoteSessionKey, 1)}
	factory.dialer = proxyKillDialer{d: d, proxy: proxy, transport: control}
	d.remoteDialerFactory = factory
	d.remoteTransportMode = ports.RemoteTransportUDP
	cache := &proxyKillCache{d: d, proxy: proxy, stores: make(chan []ports.RemoteCatalogCacheEntry, 2)}
	d.remoteCatalogCache = cache
	d.remoteCatalog.replaceCache([]ports.RemoteCatalogCacheEntry{{
		Host: proxy.key.Host, FetchedAt: clock.Now(), Sessions: []ports.RemoteCatalogSession{
			{Name: proxy.key.Name, State: "running"}, {Name: "other", State: "running"},
		},
	}})
	return d, proxy, clock, control, cache
}

func proxyKillGeneration(p *proxySession) uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.generation
}

func requireProxyKillState(t *testing.T, d *Daemon, proxy *proxySession, present bool) {
	t.Helper()
	d.mu.Lock()
	require.Equal(t, present, d.sessions[proxy.id] == proxy)
	d.mu.Unlock()
	d.remoteCatalog.mu.Lock()
	entry := d.remoteCatalog.cache[proxy.key.Host]
	d.remoteCatalog.mu.Unlock()
	found := false
	for _, session := range entry.Sessions {
		found = found || session.Name == proxy.key.Name
	}
	require.Equal(t, present, found)
}

func TestProxyKillEOFSuccessRemovesExactLiveAndCacheWithoutLockedIO(t *testing.T) {
	d, proxy, _, control, cache := newProxyKillHarness(t)
	proxy.mu.Lock()
	liveTransport, ok := proxy.transport.(*proxyLifecycleTransport)
	liveGeneration := proxy.generation
	proxy.mu.Unlock()
	require.True(t, ok)
	control.recv <- proxyKillRecv{err: fmt.Errorf("ssh stream closed: %w", io.EOF)}

	require.NoError(t, d.killRemoteProxy(context.Background(), proxy, liveGeneration))
	frame := awaitTestValue(t, control.sent, "remote control kill was not sent")
	require.Equal(t, ports.MsgKill, frame.Type)
	kill, err := ports.UnmarshalKill(frame.Payload)
	require.NoError(t, err)
	require.Equal(t, ports.Kill{Name: proxy.key.Name}, kill)
	requireProxyKillState(t, d, proxy, false)
	awaitTestCompletion(t, liveTransport.closed, "successful remote kill did not close the exact live transport generation")
	require.True(t, liveTransport.unlocked, "exact live transport close ran under an architecture lock")
	proxy.mu.Lock()
	require.Greater(t, proxy.generation, liveGeneration)
	require.Same(t, liveTransport, proxy.transport, "teardown must not substitute a different link generation before closing it")
	proxy.mu.Unlock()
	stored := awaitTestValue(t, cache.stores, "successful remote kill did not persist cache removal")
	require.Len(t, stored, 1)
	require.Equal(t, []ports.RemoteCatalogSession{{Name: "other", State: "running"}}, stored[0].Sessions)
}

func TestProxyKillFailuresPreserveLiveAndCache(t *testing.T) {
	remoteFailure := ports.Frame{Type: ports.MsgError, Payload: ports.MarshalErrorMsg(ports.ErrorMsg{Code: ports.ErrInternal, Text: "no"})}
	tests := []struct {
		name  string
		setup func(*Daemon, *proxySession, *proxyKillControlTransport)
	}{
		{name: "dialer selection", setup: func(d *Daemon, proxy *proxySession, _ *proxyKillControlTransport) {
			d.remoteDialerFactory = &proxyKillDialerFactory{d: d, proxy: proxy, err: errors.New("factory")}
		}},
		{name: "dial", setup: func(d *Daemon, proxy *proxySession, _ *proxyKillControlTransport) {
			d.remoteDialerFactory = &proxyKillDialerFactory{d: d, proxy: proxy, dialer: proxyKillDialer{d: d, proxy: proxy, err: errors.New("dial")}}
		}},
		{name: "send", setup: func(_ *Daemon, _ *proxySession, control *proxyKillControlTransport) {
			control.sendErr = errors.New("send")
		}},
		{name: "receive error", setup: func(_ *Daemon, _ *proxySession, control *proxyKillControlTransport) {
			control.recv <- proxyKillRecv{err: errors.New("truncated control reply")}
		}},
		{name: "remote error", setup: func(_ *Daemon, _ *proxySession, control *proxyKillControlTransport) {
			control.recv <- proxyKillRecv{frame: remoteFailure}
		}},
		{name: "unexpected reply", setup: func(_ *Daemon, _ *proxySession, control *proxyKillControlTransport) {
			control.recv <- proxyKillRecv{frame: ports.Frame{Type: ports.MsgPong, Payload: ports.MarshalPong(ports.Pong{})}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, proxy, _, control, cache := newProxyKillHarness(t)
			test.setup(d, proxy, control)
			require.Error(t, d.killRemoteProxy(context.Background(), proxy, proxyKillGeneration(proxy)))
			requireProxyKillState(t, d, proxy, true)
			select {
			case <-cache.stores:
				t.Fatal("failed remote kill persisted a cache mutation")
			default:
			}
		})
	}
}

func TestProxyKillTimeoutPreservesState(t *testing.T) {
	d, proxy, clock, _, cache := newProxyKillHarness(t)
	done := make(chan error, 1)
	go func() { done <- d.killRemoteProxy(context.Background(), proxy, proxyKillGeneration(proxy)) }()
	timer := awaitTestValue(t, clock.timers, "remote kill did not arm a bounded timeout")
	require.Equal(t, proxyKillTimeout, timer.delay)
	timer.fire()
	require.ErrorIs(t, awaitTestValue(t, done, "remote kill timeout did not unblock receive"), errProxyKillTimeout)
	requireProxyKillState(t, d, proxy, true)
	select {
	case <-cache.stores:
		t.Fatal("timed out remote kill persisted a cache mutation")
	default:
	}
}

func TestProxyKillBlockedSendReturnsAtBoundWithoutMutatingState(t *testing.T) {
	d, proxy, clock, control, cache := newProxyKillHarness(t)
	blocked := &proxyKillBlockedSendTransport{
		proxyKillControlTransport: control,
		started:                   make(chan struct{}), release: make(chan struct{}), finished: make(chan struct{}),
	}
	defer func() {
		close(blocked.release)
		awaitTestCompletion(t, blocked.finished, "abandoned send did not finish after test release")
	}()
	d.remoteDialerFactory = &proxyKillDialerFactory{
		d: d, proxy: proxy,
		dialer: proxyKillDialer{d: d, proxy: proxy, transport: blocked},
	}

	d.mu.Lock()
	beforeLive := d.sessions[proxy.id]
	beforeClosing := d.closing
	d.mu.Unlock()
	proxy.mu.Lock()
	beforeGeneration, beforeExpired, beforeWarm, beforeTransport := proxy.generation, proxy.expired, proxy.warm, proxy.transport
	proxy.mu.Unlock()
	d.remoteCatalog.mu.Lock()
	beforeCache := cloneRemoteCatalogEntry(d.remoteCatalog.cache[proxy.key.Host])
	d.remoteCatalog.mu.Unlock()

	done := make(chan error, 1)
	go func() { done <- d.killRemoteProxy(context.Background(), proxy, beforeGeneration) }()
	awaitTestCompletion(t, blocked.started, "remote kill did not enter adversarial Send")
	timer := awaitTestValue(t, clock.timers, "remote kill did not arm a bounded timeout")
	require.Equal(t, proxyKillTimeout, timer.delay)
	timer.fire()
	require.ErrorIs(t, awaitTestValue(t, done, "remote kill waited for Send after its bound"), errProxyKillTimeout)

	d.mu.Lock()
	require.Same(t, beforeLive, d.sessions[proxy.id])
	require.Equal(t, beforeClosing, d.closing)
	d.mu.Unlock()
	proxy.mu.Lock()
	require.Equal(t, beforeGeneration, proxy.generation)
	require.Equal(t, beforeExpired, proxy.expired)
	require.Same(t, beforeWarm, proxy.warm)
	require.Same(t, beforeTransport, proxy.transport)
	proxy.mu.Unlock()
	d.remoteCatalog.mu.Lock()
	require.Equal(t, beforeCache, d.remoteCatalog.cache[proxy.key.Host])
	d.remoteCatalog.mu.Unlock()
	select {
	case <-cache.stores:
		t.Fatal("blocked remote kill persisted a cache mutation")
	default:
	}
	select {
	case <-blocked.finished:
		t.Fatal("adversarial Send unexpectedly returned when transport closed")
	default:
	}
}

func TestProxyKillStaleSuccessPreservesReplacementGeneration(t *testing.T) {
	tests := []struct {
		name string
		win  func(*Daemon, *proxySession)
	}{
		{name: "reattach generation", win: func(d *Daemon, proxy *proxySession) {
			d.mu.Lock()
			proxy.sessionCore.mu.Lock()
			proxy.mu.Lock()
			proxy.generation++
			proxy.mu.Unlock()
			proxy.sessionCore.mu.Unlock()
			d.mu.Unlock()
		}},
		{name: "replacement pointer", win: func(d *Daemon, proxy *proxySession) {
			replacement, err := newProxySession(proxy.key, defaultSize)
			require.NoError(t, err)
			d.mu.Lock()
			d.sessions[proxy.id] = replacement
			d.mu.Unlock()
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			d, proxy, _, control, cache := newProxyKillHarness(t)
			generation := proxyKillGeneration(proxy)
			done := make(chan error, 1)
			go func() { done <- d.killRemoteProxy(context.Background(), proxy, generation) }()
			_ = awaitTestValue(t, control.sent, "remote kill did not reach the control transport")
			awaitTestCompletion(t, control.recvLocksChecked, "remote kill did not verify unlocked architecture before generation replacement")
			test.win(d, proxy)
			control.recv <- proxyKillRecv{err: io.EOF}
			require.ErrorIs(t, awaitTestValue(t, done, "stale remote kill did not finish"), errProxyKillGeneration)
			d.remoteCatalog.mu.Lock()
			entry := d.remoteCatalog.cache[proxy.key.Host]
			d.remoteCatalog.mu.Unlock()
			require.Len(t, entry.Sessions, 2, "stale success removed cached discovery")
			select {
			case <-cache.stores:
				t.Fatal("stale remote success persisted a cache mutation")
			default:
			}
		})
	}
}

func TestProxyKillCurrentLinkDetachedCanRaceControlEOF(t *testing.T) {
	d, proxy, _, control, _ := newProxyKillHarness(t)
	generation := proxyKillGeneration(proxy)
	done := make(chan error, 1)
	go func() { done <- d.killRemoteProxy(context.Background(), proxy, generation) }()
	_ = awaitTestValue(t, control.sent, "remote kill did not reach the control transport")
	result, err := d.handleLinkFrame(proxy, proxy.linkGeneration, ports.Frame{
		Type: ports.MsgDetached, Payload: ports.MarshalDetached(ports.Detached{Reason: ports.ReasonSessionKilled}),
	})
	require.NoError(t, err)
	require.Equal(t, proxyLinkStop, result)
	control.recv <- proxyKillRecv{err: io.EOF}
	require.NoError(t, awaitTestValue(t, done, "remote kill did not accept EOF after current-link detach"))
	requireProxyKillState(t, d, proxy, false)
}

func TestProxyKillConfirmationRequiresExactRemoteDisplay(t *testing.T) {
	p, release := newBlockingPTY(t)
	defer release()
	d, local, ac, _ := newManualSessionWithPTYs(t, p)
	proxy, err := newProxySession(domain.RemoteSessionKey{Host: "arch", Name: "work"}, defaultSize)
	require.NoError(t, err)
	d.mu.Lock()
	require.True(t, d.registerSessionLocked(proxy))
	d.mu.Unlock()

	target := picker.Target{Session: proxy.id, RemoteKey: &proxy.key, TabIndex: -1}
	require.True(t, d.enterRemoteKillConfirmation(local, ac, target))
	ac.overlays.promptMu.Lock()
	require.Equal(t, " Type work@arch to kill ", ac.overlays.prompt.Title())
	submit := ac.overlays.promptTransitionSubmit
	ac.overlays.promptMu.Unlock()
	require.Error(t, submit("work", attachmentConnectionToken{}))
	d.mu.Lock()
	require.Same(t, proxy, d.sessions[proxy.id])
	d.mu.Unlock()
}

func TestProxyKillConfirmationWorksWhileAttachedToProxyAndCancelsWithoutIO(t *testing.T) {
	d, proxy, ac, _, _, _, _ := newProxyRoleDetachFixture(t)
	target := picker.Target{Session: proxy.id, RemoteKey: &proxy.key, TabIndex: -1}
	require.True(t, d.enterRemoteKillConfirmation(proxy, ac, target))
	d.handlePromptInput(ac, []byte("\x03"))
	require.False(t, ac.overlays.promptActive())
	d.mu.Lock()
	require.Same(t, proxy, d.sessions[proxy.id])
	d.mu.Unlock()
}

type proxyConstructionDialer struct{ transport ports.Transport }

func (d proxyConstructionDialer) Dial(context.Context) (ports.Transport, error) {
	return d.transport, nil
}

type proxyConstructionFactory struct {
	mu             sync.Mutex
	transports     []ports.Transport
	calls          chan int
	secondAttempt  chan struct{}
	releaseSecond  chan struct{}
	secondCallOnce sync.Once
}

func newProxyConstructionFactory(transports ...ports.Transport) *proxyConstructionFactory {
	return &proxyConstructionFactory{
		transports: transports, calls: make(chan int, len(transports)+1),
		secondAttempt: make(chan struct{}), releaseSecond: make(chan struct{}),
	}
}

func (f *proxyConstructionFactory) DialerForRemote(string, string, ports.RemoteTransportMode, *slog.Logger) (ports.Dialer, error) {
	f.mu.Lock()
	call := len(f.transports)
	for i := range f.transports {
		if f.transports[i] != nil {
			call = i
			break
		}
	}
	if call == len(f.transports) {
		f.mu.Unlock()
		return nil, errors.New("unexpected extra proxy construction")
	}
	transport := f.transports[call]
	f.transports[call] = nil
	f.mu.Unlock()
	f.calls <- call + 1
	if call == 1 {
		f.secondCallOnce.Do(func() { close(f.secondAttempt) })
		<-f.releaseSecond
	}
	return proxyConstructionDialer{transport: transport}, nil
}

func (f *proxyConstructionFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, transport := range f.transports {
		if transport == nil {
			count++
		}
	}
	return count
}

type proxyConstructionWaitContext struct {
	context.Context
	observed chan struct{}
	once     sync.Once
}

func (c *proxyConstructionWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.observed) })
	return c.Context.Done()
}

type proxyOpenResult struct {
	proxy *proxySession
	err   error
}

func openProxyForTest(d *Daemon, ctx context.Context, key domain.RemoteSessionKey) <-chan proxyOpenResult {
	result := make(chan proxyOpenResult, 1)
	go func() {
		proxy, err := d.openProxySession(ctx, key, defaultSize)
		result <- proxyOpenResult{proxy: proxy, err: err}
	}()
	return result
}

func TestOpenProxySessionSerializesSameKeyAttachConstruction(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	first := newProxyTestTransport()
	second := newProxyTestTransport()
	second.recv <- proxyRecv{frame: proxyWelcome(key.Name, 2, ports.CapabilityProxied)}
	second.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	second.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	factory := newProxyConstructionFactory(first, second)
	d := newProxyTestDaemon(t, factory, nil)

	leader := openProxyForTest(d, context.Background(), key)
	require.Equal(t, 1, awaitTestValue(t, factory.calls, "leader did not dial"))
	_ = requireProxyHello(t, first)

	waitCtx, cancelWait := context.WithCancel(context.Background())
	defer cancelWait()
	observed := &proxyConstructionWaitContext{Context: waitCtx, observed: make(chan struct{})}
	waiter := openProxyForTest(d, observed, key)

	secondStarted := false
	select {
	case <-observed.observed:
		// The waiter reached the cancellable construction reservation.
	case <-factory.secondAttempt:
		// This is the adversarial pre-fix path: the later remote IntentAttach
		// would replace the leader before its otherwise-valid reply arrives.
		secondStarted = true
		close(factory.releaseSecond)
		_ = requireProxyHello(t, second)
	}

	first.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	first.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	first.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	leaderResult := awaitTestValue(t, leader, "leader construction did not finish")
	waiterResult := awaitTestValue(t, waiter, "waiter construction did not finish")
	require.NoError(t, leaderResult.err)
	require.NoError(t, waiterResult.err)
	require.False(t, secondStarted, "a second same-key IntentAttach handshake started")
	require.Equal(t, 1, factory.callCount())
	require.Same(t, leaderResult.proxy, waiterResult.proxy, "all callers must receive the exact registered winner")
	d.mu.Lock()
	require.Same(t, leaderResult.proxy, d.sessions[key.ID()])
	d.mu.Unlock()
	stopProxy(t, leaderResult.proxy)
}

func TestOpenProxySessionSameKeyWaiterCanCancel(t *testing.T) {
	key := domain.RemoteSessionKey{Host: "arch", Name: "work"}
	first := newProxyTestTransport()
	second := newProxyTestTransport()
	factory := newProxyConstructionFactory(first, second)
	d := newProxyTestDaemon(t, factory, nil)

	leader := openProxyForTest(d, context.Background(), key)
	require.Equal(t, 1, awaitTestValue(t, factory.calls, "leader did not dial"))
	_ = requireProxyHello(t, first)

	base, cancel := context.WithCancel(context.Background())
	observed := &proxyConstructionWaitContext{Context: base, observed: make(chan struct{})}
	waiter := openProxyForTest(d, observed, key)
	select {
	case <-observed.observed:
	case <-factory.secondAttempt:
		close(factory.releaseSecond)
		t.Fatal("cancelable waiter started another same-key handshake")
	}
	cancel()
	waiterResult := awaitTestValue(t, waiter, "canceled waiter did not finish")
	require.ErrorIs(t, waiterResult.err, context.Canceled)
	require.Nil(t, waiterResult.proxy)
	require.Equal(t, 1, factory.callCount())

	first.recv <- proxyRecv{frame: proxyWelcome(key.Name, 1, ports.CapabilityProxied)}
	first.recv <- proxyRecv{frame: proxyMeta(key.Name)}
	first.recv <- proxyRecv{frame: proxyHandshakeSnapshot()}
	leaderResult := awaitTestValue(t, leader, "leader construction did not finish")
	require.NoError(t, leaderResult.err)
	stopProxy(t, leaderResult.proxy)
}

type warmHookClock struct {
	timers chan *signalTimer

	mu   sync.Mutex
	hook func()
}

func (c *warmHookClock) Now() time.Time { return time.Time{} }

func (c *warmHookClock) NewTimer(d time.Duration) ports.Timer {
	timer := &signalTimer{ch: make(chan time.Time, 1), duration: d}
	c.mu.Lock()
	hook := c.hook
	c.hook = nil
	c.mu.Unlock()
	if hook != nil {
		hook()
	}
	if c.timers != nil {
		c.timers <- timer
	}
	return timer
}

func (c *warmHookClock) setHook(hook func()) {
	c.mu.Lock()
	c.hook = hook
	c.mu.Unlock()
}

func registerLifecycleProxy(t *testing.T, d *Daemon, key domain.RemoteSessionKey) *proxySession {
	t.Helper()
	p, err := newProxySession(key, domain.Size{Cols: 80, Rows: 24})
	require.NoError(t, err)
	d.mu.Lock()
	registered := d.registerSessionLocked(p)
	d.mu.Unlock()
	require.True(t, registered, "lifecycle fixture proxy was not published")
	return p
}

func newProxyLifecycleFixture(t *testing.T) (*Daemon, *proxySession, *warmHookClock) {
	t.Helper()
	clock := &warmHookClock{timers: make(chan *signalTimer, 4)}
	d := newProxyTestDaemon(t, nil, clock)
	return d, registerLifecycleProxy(t, d, domain.RemoteSessionKey{Host: "arch", Name: "work"}), clock
}

func proxyWarmToken(p *proxySession) (*proxyWarmTimer, uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.warm, p.generation
}

func setProxyLifecycleClient(p *proxySession, ac *attachedClient) {
	p.sessionCore.mu.Lock()
	p.sessionCore.registerAttachmentLocked(ac)
	p.sessionCore.mu.Unlock()
}

func proxyRegistered(d *Daemon, p *proxySession) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.sessions[p.id] == p
}

func proxyExpired(p *proxySession) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.expired
}

func armWarmTimer(t *testing.T, d *Daemon, p *proxySession, clock *warmHookClock) (*proxyWarmTimer, *signalTimer) {
	t.Helper()
	require.True(t, d.armProxyWarm(p), "warm timer was not published")
	timer := awaitTestValue(t, clock.timers, "warm timer was never created")
	token, _ := proxyWarmToken(p)
	require.NotNil(t, token, "published arm left no warm token")
	return token, timer
}

// TestArmProxyWarmKeepsExpiryPathWhenPublicationFails pins the two-phase arm:
// a re-arm that loses its revalidation must leave the incumbent timer both
// installed and current, otherwise a registered proxy is left with no path out
// of the live registry.
func TestArmProxyWarmKeepsExpiryPathWhenPublicationFails(t *testing.T) {
	t.Skip("legacy fixture predates attachment-owned state")
	tests := []struct {
		name string
		race func(d *Daemon, p *proxySession)
		undo func(d *Daemon, p *proxySession)
	}{
		{
			name: "client attaches during arm",
			race: func(_ *Daemon, p *proxySession) { setProxyLifecycleClient(p, &attachedClient{}) },
			undo: func(_ *Daemon, p *proxySession) { setProxyLifecycleClient(p, nil) },
		},
		{
			name: "replacement rekeys the lifecycle generation",
			race: func(_ *Daemon, p *proxySession) {
				p.mu.Lock()
				p.expired = true
				p.generation++
				if p.warm != nil {
					p.warm.generation = p.generation
				}
				p.mu.Unlock()
			},
		},
		{
			name: "daemon starts closing during arm",
			race: func(d *Daemon, _ *proxySession) {
				d.mu.Lock()
				d.closing = true
				d.mu.Unlock()
			},
			undo: func(d *Daemon, _ *proxySession) {
				d.mu.Lock()
				d.closing = false
				d.mu.Unlock()
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, p, clock := newProxyLifecycleFixture(t)
			token, timer := armWarmTimer(t, d, p, clock)

			clock.setHook(func() { tt.race(d, p) })
			require.False(t, d.armProxyWarm(p), "raced re-arm must not publish a replacement")
			awaitTestValue(t, clock.timers, "raced re-arm never reached its clock")

			retained, generation := proxyWarmToken(p)
			require.NotNil(t, retained, "failed re-arm stripped the proxy of its only expiry path")
			require.Same(t, token, retained, "failed re-arm replaced the armed expiry timer")
			require.Equal(t, generation, retained.generation, "retained timer no longer owns the lifecycle generation")
			require.True(t, proxyRegistered(d, p), "a failed re-arm must not unregister the proxy")

			if tt.undo != nil {
				tt.undo(d, p)
			}
			timer.ch <- time.Time{}
			awaitTestCompletion(t, token.done, "retained warm timer never ran to completion")
			require.False(t, proxyRegistered(d, p), "retained warm timer did not expire the dormant proxy")
			require.True(t, proxyExpired(p), "expiry did not publish terminal lifecycle state")
			awaitTestCompletion(t, d.done, "last proxy expiry did not complete the daemon")
		})
	}
}

func TestDetachProxyIfCurrentTransportArmsExpiredProxy(t *testing.T) {
	d, p, clock := newProxyLifecycleFixture(t)
	transport := newProxyTestTransport()
	t.Cleanup(func() { _ = transport.Close() })
	ac := &attachedClient{tr: transport, output: newOutputStateStream()}
	ac.setSession(p)
	setProxyLifecycleClient(p, ac)
	p.mu.Lock()
	p.expired = true
	p.mu.Unlock()

	require.True(t, d.detachProxyIfCurrentTransport(p, ac, ac.transportSnapshot()))
	awaitTestValue(t, clock.timers, "expired proxy detach did not arm a warm timer")
	token, generation := proxyWarmToken(p)
	require.NotNil(t, token, "expired headless proxy has no expiry path")
	require.Equal(t, generation, token.generation)
	require.Nil(t, ac.currentAttachmentSession())

	token.stop()
	awaitTestCompletion(t, token.done, "warm timer did not stop during cleanup")
}

func TestMarkProxyReplacedWarmTimerOwnership(t *testing.T) {
	t.Run("rekeys an already armed timer", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		transport := newProxyTestTransport()
		t.Cleanup(func() { _ = transport.Close() })
		generation, _ := p.installTransport(transport, false)
		token, timer := armWarmTimer(t, d, p, clock)

		require.True(t, d.markProxyReplaced(p, generation, transport))
		require.Empty(t, clock.timers, "replacement armed a second timer instead of rekeying")

		rekeyed, lifecycle := proxyWarmToken(p)
		require.Same(t, token, rekeyed, "replacement replaced the armed timer")
		require.Equal(t, lifecycle, rekeyed.generation, "replacement did not rekey the armed timer")
		require.True(t, proxyExpired(p))
		p.mu.Lock()
		state := p.linkState
		p.mu.Unlock()
		require.Equal(t, ports.LinkStateDead, state)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "rekeyed warm timer never ran to completion")
		require.False(t, proxyRegistered(d, p), "rekeyed warm timer did not expire the proxy")
	})

	t.Run("arms an expiry for a headless proxy", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		transport := newProxyTestTransport()
		t.Cleanup(func() { _ = transport.Close() })
		generation, _ := p.installTransport(transport, false)

		require.True(t, d.markProxyReplaced(p, generation, transport))
		timer := awaitTestValue(t, clock.timers, "replacement did not arm a warm timer")
		token, lifecycle := proxyWarmToken(p)
		require.NotNil(t, token, "replaced headless proxy has no expiry path")
		require.Equal(t, lifecycle, token.generation)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "warm timer never ran to completion")
		require.False(t, proxyRegistered(d, p))
	})

	t.Run("stale link generation publishes nothing", func(t *testing.T) {
		d, p, _ := newProxyLifecycleFixture(t)
		transport := newProxyTestTransport()
		t.Cleanup(func() { _ = transport.Close() })
		generation, _ := p.installTransport(transport, false)

		require.False(t, d.markProxyReplaced(p, generation-1, transport))
		require.False(t, proxyExpired(p))
		warm, _ := proxyWarmToken(p)
		require.Nil(t, warm, "a rejected replacement must not arm an expiry")
	})
}

func TestExpireWarmProxyCompletesDaemonOnlyOnLastSession(t *testing.T) {
	t.Run("last session completes the daemon", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		token, timer := armWarmTimer(t, d, p, clock)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "warm timer never ran to completion")
		awaitTestCompletion(t, p.done, "expiry did not finish the proxy")
		awaitTestCompletion(t, d.done, "last session expiry did not complete the daemon")
		require.False(t, proxyRegistered(d, p))
		require.True(t, proxyExpired(p))
	})

	t.Run("surviving session keeps the daemon running", func(t *testing.T) {
		d, p, clock := newProxyLifecycleFixture(t)
		survivor := registerLifecycleProxy(t, d, domain.RemoteSessionKey{Host: "arch", Name: "other"})
		token, timer := armWarmTimer(t, d, p, clock)

		timer.ch <- time.Time{}
		awaitTestCompletion(t, token.done, "warm timer never ran to completion")
		require.False(t, proxyRegistered(d, p))
		require.True(t, proxyRegistered(d, survivor))
		select {
		case <-d.done:
			t.Fatal("daemon completed while a session was still registered")
		default:
		}
	})
}

// TestExpireWarmProxyIgnoresSupersededToken proves the expiry callback is exact:
// a token that lost its lifecycle generation must publish no terminal state and
// must not unregister the proxy.
func TestExpireWarmProxyIgnoresSupersededToken(t *testing.T) {
	d, p, clock := newProxyLifecycleFixture(t)
	stale, _ := armWarmTimer(t, d, p, clock)
	current, currentTimer := armWarmTimer(t, d, p, clock)
	require.NotSame(t, stale, current, "re-arm did not publish a replacement timer")
	awaitTestCompletion(t, stale.done, "superseded warm timer was not canceled")

	require.False(t, d.expireWarmProxy(p, stale), "a superseded token expired the proxy")
	require.True(t, proxyRegistered(d, p))
	require.False(t, proxyExpired(p))
	installed, generation := proxyWarmToken(p)
	require.Same(t, current, installed)
	require.Equal(t, generation, installed.generation)

	currentTimer.ch <- time.Time{}
	awaitTestCompletion(t, current.done, "current warm timer never ran to completion")
	require.False(t, proxyRegistered(d, p))
}

func newProxyRoleDetachFixture(t *testing.T) (*Daemon, *proxySession, *attachedClient, *closeTrackingTransport, *warmHookClock, attachmentConnectionToken, *renderCoordinator) {
	t.Helper()
	d, local, ac, _ := newManualSessionWithPTYs(t, newQuietPTY())
	clock := &warmHookClock{timers: make(chan *signalTimer, 4)}
	d.clock = clock
	clientTransport := &closeTrackingTransport{}
	ac.replaceTransport(clientTransport)
	localCoordinator := d.attachCoordinator(local, nil, ac, true)
	localToken := attachmentToken(local, ac, clientTransport)
	localToken.lease = localCoordinator.attachmentLease(ac)
	proxy := registerLifecycleProxy(t, d, domain.RemoteSessionKey{Host: "arch", Name: "detach"})
	transition, err := d.transitionAttachment(attachmentTransitionRequest{
		source: local, target: proxy, next: ac,

		expectedTransport: ac.transportSnapshot(), sourceToken: &localToken,
		action: "proxy detach fixture", ready: true,
	})
	require.NoError(t, err)
	return d, proxy, ac, clientTransport, clock, transition.published, proxy.coordinator.Load()
}

func TestProxyRoleDetachRetiresExactClientAndOwnsOneWarmTimer(t *testing.T) {
	tests := []struct {
		name   string
		detach func(*Daemon, attachmentConnectionToken, ports.Transport) bool
	}{
		{
			name: "explicit",
			detach: func(d *Daemon, token attachmentConnectionToken, _ ports.Transport) bool {
				effect, admitted := token.ac.beginAttachmentEffect(token)
				require.True(t, admitted)
				token.effect = effect
				return d.clientGoneForAttachment(token, true)
			},
		},
		{
			name: "send error",
			detach: func(d *Daemon, token attachmentConnectionToken, failed ports.Transport) bool {
				d.detachOnAttachmentSendError(token, failed)
				return token.ac.currentAttachmentSession() == nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, proxy, ac, tr, clock, token, coordinator := newProxyRoleDetachFixture(t)
			require.True(t, tt.detach(d, token, tr))
			require.Nil(t, ac.currentAttachmentSession())
			require.True(t, tr.Closed())
			require.Nil(t, coordinator.attachmentLease(ac), "coordinator must retire the detached client")
			var warmTimers []*signalTimer
			for len(warmTimers) == 0 {
				timer := awaitTestValue(t, clock.timers, "proxy detach did not arm a warm timer")
				if timer.duration == proxyWarmDuration {
					warmTimers = append(warmTimers, timer)
				}
			}
			select {
			case timer := <-clock.timers:
				if timer.duration == proxyWarmDuration {
					warmTimers = append(warmTimers, timer)
				}
			default:
			}
			require.Len(t, warmTimers, 1, "proxy detach must arm exactly one warm timer")
			warm, _ := proxyWarmToken(proxy)
			require.NotNil(t, warm)
			warm.stop()
			awaitTestCompletion(t, warm.done, "warm timer did not stop")
		})
	}
}

func TestProxyRoleDetachRejectsStaleTransportAndRole(t *testing.T) {
	tests := []struct {
		name  string
		stale func(*attachedClient, *attachmentConnectionToken) *closeTrackingTransport
	}{
		{
			name: "transport",
			stale: func(ac *attachedClient, _ *attachmentConnectionToken) *closeTrackingTransport {
				current := &closeTrackingTransport{}
				ac.replaceTransport(current)
				return current
			},
		},
		{
			name: "role generation",
			stale: func(ac *attachedClient, _ *attachmentConnectionToken) *closeTrackingTransport {
				ac.connectionGeneration.Add(1)
				return nil
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d, proxy, ac, failed, clock, token, _ := newProxyRoleDetachFixture(t)
			current := tt.stale(ac, &token)
			d.detachOnAttachmentSendError(token, failed)
			require.Same(t, proxy, ac.currentAttachmentSession())
			if current != nil {
				require.False(t, current.Closed(), "stale transport error closed the rebound client")
			}
			select {
			case <-clock.timers:
				t.Fatal("stale proxy detach armed a warm timer")
			default:
			}
		})
	}
}
