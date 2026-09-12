package client

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/wire"
)

// inventoryTestClock is a manually advanced clock. Firing advances past the
// poll interval so relay ticks stay deterministic.
type inventoryTestClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*inventoryTestTimer
}

func newInventoryTestClock() *inventoryTestClock {
	return &inventoryTestClock{now: time.Unix(1000, 0)}
}

func (c *inventoryTestClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *inventoryTestClock) NewTimer(time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &inventoryTestTimer{ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	return timer
}

func (c *inventoryTestClock) timerCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.timers)
}

func (c *inventoryTestClock) fire() {
	c.mu.Lock()
	c.now = c.now.Add(2 * inventoryPollInterval)
	timers := append([]*inventoryTestTimer(nil), c.timers...)
	c.mu.Unlock()
	for _, timer := range timers {
		select {
		case timer.ch <- c.Now():
		default:
		}
	}
}

type inventoryTestTimer struct{ ch chan time.Time }

func (t *inventoryTestTimer) C() <-chan time.Time      { return t.ch }
func (t *inventoryTestTimer) Reset(time.Duration) bool { return false }
func (t *inventoryTestTimer) Stop() bool               { return true }

// inventoryScript selects the serving-transport script after the publication.
type inventoryScript int

const (
	inventoryScriptClose inventoryScript = iota
	inventoryScriptSelect
	// inventoryScriptSilent models an old daemon that never sends
	// demands: the relay stays idle and dials nothing.
	inventoryScriptSilent
	// inventoryScriptLocalIgnore models demands on a local attachment:
	// open and close cross back to back with no publication gate, so the
	// single-threaded loop provably processes both before detach.
	inventoryScriptLocalIgnore
)

// inventoryAttemptTransport scripts a remote serving daemon: welcome, open
// demand, then either a close demand or a valid selection once the relay
// publishes. Raw sends record publications for assertions.
type inventoryAttemptTransport struct {
	mu            sync.Mutex
	calls         int
	sent          []wire.Frame
	publications  []protocol.NavigationInventoryPublication
	published     chan struct{}
	publishedOnce sync.Once
	release       chan struct{}
	detach        chan struct{}
	releaseClose  chan struct{}
	script        inventoryScript
}

func (t *inventoryAttemptTransport) Send(frame wire.Frame) error {
	var publication protocol.NavigationInventoryPublication
	if frame.Type == wire.MsgNavigationInventoryPublication {
		var err error
		publication, err = wire.UnmarshalNavigationInventoryPublication(frame.Payload)
		if err != nil {
			return err
		}
	}
	t.mu.Lock()
	t.sent = append(t.sent, frame)
	if frame.Type == wire.MsgNavigationInventoryPublication {
		t.publications = append(t.publications, publication)
	}
	t.mu.Unlock()
	if frame.Type == wire.MsgNavigationInventoryPublication {
		t.publishedOnce.Do(func() { close(t.published) })
	}
	return nil
}

func (t *inventoryAttemptTransport) Recv() (wire.Frame, error) {
	t.mu.Lock()
	t.calls++
	call := t.calls
	t.mu.Unlock()
	switch call {
	case 1:
		return wire.Frame{Type: wire.MsgWelcome, Payload: wire.MarshalWelcome(protocol.Welcome{SessionID: "s"})}, nil
	case 2:
		if t.script == inventoryScriptSilent {
			select {
			case <-t.detach:
				return wire.Frame{Type: wire.MsgDetached, Payload: wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})}, nil
			case <-t.release:
				return wire.Frame{}, io.EOF
			}
		}
		return wire.Frame{Type: wire.MsgNavigationInventoryDemand, Payload: wire.MarshalNavigationInventoryDemand(protocol.NavigationInventoryDemand{InteractionGeneration: 7, Open: true})}, nil
	case 3:
		if t.script == inventoryScriptLocalIgnore {
			return wire.Frame{Type: wire.MsgNavigationInventoryDemand, Payload: wire.MarshalNavigationInventoryDemand(protocol.NavigationInventoryDemand{InteractionGeneration: 7})}, nil
		}
		select {
		case <-t.published:
		case <-t.detach:
			return wire.Frame{Type: wire.MsgDetached, Payload: wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})}, nil
		case <-t.release:
			return wire.Frame{}, io.EOF
		}
		// The close script holds the close demand until the test releases
		// it, so polling assertions run against an open interaction.
		if t.script == inventoryScriptClose {
			select {
			case <-t.releaseClose:
			case <-t.release:
				return wire.Frame{}, io.EOF
			}
		}
		if t.script == inventoryScriptSelect {
			return wire.Frame{Type: wire.MsgNavigationInventorySelection, Payload: wire.MarshalNavigationInventorySelection(protocol.NavigationInventorySelection{
				CauseActionID: 9, InteractionGeneration: 7, PublicationGeneration: 1,
				SourceKey: "local", EntryKey: "aaa/alpha",
			})}, nil
		}
		return wire.Frame{Type: wire.MsgNavigationInventoryDemand, Payload: wire.MarshalNavigationInventoryDemand(protocol.NavigationInventoryDemand{InteractionGeneration: 7})}, nil
	default:
		select {
		case <-t.detach:
			return wire.Frame{Type: wire.MsgDetached, Payload: wire.MarshalDetached(protocol.Detached{Reason: protocol.ReasonDetach})}, nil
		case <-t.release:
			return wire.Frame{}, io.EOF
		}
	}
}

func (t *inventoryAttemptTransport) SendClient(m protocol.ClientMessage) error {
	f, err := testClientFrame(m)
	if err != nil {
		return err
	}
	return t.Send(f)
}

func (t *inventoryAttemptTransport) ReceiveServer() (protocol.ServerMessage, error) {
	f, err := t.Recv()
	if err != nil {
		return nil, err
	}
	return testServerMessage(f)
}

func (t *inventoryAttemptTransport) Capabilities() protocol.ConnectionCapabilities {
	return testClientCapabilities(t)
}
func (t *inventoryAttemptTransport) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (t *inventoryAttemptTransport) LinkEvents() <-chan ports.LinkEvent { return nil }
func (t *inventoryAttemptTransport) Close() error                       { return nil }

func (t *inventoryAttemptTransport) latestRouteSnapshot() (protocol.RecentRouteSnapshot, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	for i := len(t.sent) - 1; i >= 0; i-- {
		if t.sent[i].Type != wire.MsgRecentRouteSnapshot {
			continue
		}
		snapshot, err := wire.UnmarshalRecentRouteSnapshot(t.sent[i].Payload)
		if err != nil {
			panic(err)
		}
		return snapshot, true
	}
	return protocol.RecentRouteSnapshot{}, false
}

func (t *inventoryAttemptTransport) publicationCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.publications)
}

func (t *inventoryAttemptTransport) hello() protocol.Hello {
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, frame := range t.sent {
		if frame.Type == wire.MsgHello {
			hello, err := wire.UnmarshalHello(frame.Payload)
			if err != nil {
				panic(err)
			}
			return hello
		}
	}
	return protocol.Hello{}
}

// inventoryTestControlConn answers one snapshot or resolve query from canned
// groups without Hello, session creation, or attachment.
type inventoryTestControlConn struct {
	inventoryRequest protocol.NavigationInventoryRequest
	pickerRequest    protocol.PickerControlRequest
	onPicker         func()
}

func (c *inventoryTestControlConn) SendClient(message protocol.ClientMessage) error {
	switch request := message.(type) {
	case protocol.NavigationInventoryRequest:
		c.inventoryRequest = request
	case protocol.PickerControlRequest:
		c.pickerRequest = request
		if c.onPicker != nil {
			c.onPicker()
		}
	default:
		return io.ErrUnexpectedEOF
	}
	return nil
}

func (c *inventoryTestControlConn) ReceiveServer() (protocol.ServerMessage, error) {
	if c.pickerRequest.Operation == protocol.PickerControlObserve {
		observations := make([]protocol.PickerRouteObservation, len(c.pickerRequest.Targets))
		for i, target := range c.pickerRequest.Targets {
			observations[i] = protocol.PickerRouteObservation{Target: target, Presence: protocol.PickerRoutePresent, Attention: true}
		}
		return protocol.PickerControlResponse{RequestID: c.pickerRequest.RequestID, Operation: protocol.PickerControlObserve, Status: protocol.PickerSourceOK, Observations: observations}, nil
	}
	request := c.inventoryRequest
	switch request.Operation {
	case protocol.NavigationInventorySnapshot:
		return protocol.NavigationInventoryResponse{
			RequestID: request.RequestID, Operation: protocol.NavigationInventorySnapshot,
			Status: protocol.NavigationInventoryOK,
			Groups: []protocol.NavigationInventorySourceGroup{{
				SourceKey: "local", Status: protocol.NavigationInventorySourceOK,
				Entries: []protocol.NavigationInventoryEntry{
					{SourceKey: "local", EntryKey: "aaa/alpha", Name: "alpha", DisplayOrigin: "local", State: "up"},
				},
			}},
		}, nil
	case protocol.NavigationInventoryResolve:
		lifecycle := domain.SessionLifecycleID{7}
		return protocol.NavigationInventoryResponse{
			RequestID: request.RequestID, Operation: protocol.NavigationInventoryResolve,
			Status: protocol.NavigationInventoryOK,
			Resolved: &protocol.AttachTarget{
				Session: "alpha", Intent: protocol.IntentAttach,
				ExactTarget: &protocol.ExactSessionTarget{LifecycleID: lifecycle, SessionName: "alpha"},
			},
		}, nil
	default:
		return nil, io.ErrUnexpectedEOF
	}
}

func (c *inventoryTestControlConn) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{}
}
func (c *inventoryTestControlConn) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (c *inventoryTestControlConn) LinkEvents() <-chan ports.LinkEvent { return nil }
func (c *inventoryTestControlConn) Close() error                       { return nil }

type inventoryTestDialer struct {
	mu             sync.Mutex
	dials          int
	pickerRequests int
}

func (d *inventoryTestDialer) Dial(context.Context) (ports.ClientConnection, error) {
	d.mu.Lock()
	d.dials++
	d.mu.Unlock()
	return &inventoryTestControlConn{onPicker: d.notePickerRequest}, nil
}

func (d *inventoryTestDialer) notePickerRequest() {
	d.mu.Lock()
	d.pickerRequests++
	d.mu.Unlock()
}

func (d *inventoryTestDialer) pickerCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.pickerRequests
}

func (d *inventoryTestDialer) count() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.dials
}

func runInventoryAttempt(t *testing.T, clock *inventoryTestClock, transport *inventoryAttemptTransport, remote bool, dialer *inventoryTestDialer) attachResult {
	t.Helper()
	input := newPaletteAttachReader(nil)
	t.Cleanup(input.close)
	term := &paletteAttachTerminal{in: input, resize: make(chan domain.Geometry)}
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler), ledger: newRouteLedger()}
	attempt := &attachAttempt{
		runner: runner, dialer: dialer, transport: transport,
		request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "remote-work"},
		remote:  remote,
		inventoryHome: &attachRoute{
			dialer:  dialer,
			request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "home"},
		},
		inventoryDialer: dialer,
		milestones:      msForInventoryTest(), themeState: &terminalThemeState{},
		enterRaw:  func() error { return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}
	return attempt.run(context.Background())
}

func msForInventoryTest() *milestones { return &milestones{} }

// TestInventoryPollBoxKeepsNewestCompletion pins out-of-order delivery:
// a late outcome from an older query must not overwrite the newer one,
// or the newer completion is lost while its slot stays busy.
func TestRemoteAttemptPublishesLocalAttentionObservation(t *testing.T) {
	clock := newInventoryTestClock()
	dialer := &inventoryTestDialer{}
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), releaseClose: make(chan struct{}), script: inventoryScriptSilent,
	}
	t.Cleanup(func() { close(transport.release) })
	input := newPaletteAttachReader(nil)
	t.Cleanup(input.close)
	term := &paletteAttachTerminal{in: input, resize: make(chan domain.Geometry)}
	ledger := newRouteLedger()
	localTarget := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{1}, SessionName: "local"}
	_, err := ledger.commitAttach(routeCandidateForAttach(AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local", EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned}, protocol.CommittedRouteIdentity{Target: localTarget}, dialer, 0))
	require.NoError(t, err)
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler), ledger: ledger}
	attempt := &attachAttempt{
		runner: runner, dialer: dialer, transport: transport, remote: true,
		request:         AttachRequest{Intent: protocol.IntentAttach, SessionName: "remote-work", Origin: protocol.RouteOriginRemote, OriginKey: "remote", Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned},
		inventoryHome:   &attachRoute{dialer: dialer, request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local", EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned}},
		inventoryDialer: dialer, milestones: msForInventoryTest(), themeState: &terminalThemeState{}, enterRaw: func() error { return nil }, reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}
	done := make(chan attachResult, 1)
	go func() { done <- attempt.run(context.Background()) }()

	require.Eventually(t, func() bool { return clock.timerCount() >= 2 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		clock.fire()
		return dialer.pickerCount() > 0
	}, 5*time.Second, 10*time.Millisecond, "the observation worker must query the stable authority")
	require.Eventually(t, func() bool {
		clock.fire()
		snapshot, ok := transport.latestRouteSnapshot()
		if !ok {
			return false
		}
		for _, entry := range append(snapshot.Entries, snapshot.ActiveEntry) {
			if entry.Target == localTarget && entry.Attention {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	close(transport.detach)
	result := <-done
	require.NoError(t, result.err)
}

func TestLocalAttemptPublishesRemoteAttentionObservation(t *testing.T) {
	clock := newInventoryTestClock()
	localDialer := &inventoryTestDialer{}
	remoteDialer := &inventoryTestDialer{}
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), releaseClose: make(chan struct{}), script: inventoryScriptSilent,
	}
	t.Cleanup(func() { close(transport.release) })
	input := newPaletteAttachReader(nil)
	t.Cleanup(input.close)
	term := &paletteAttachTerminal{in: input, resize: make(chan domain.Geometry)}
	ledger := newRouteLedger()
	remoteTarget := protocol.ExactSessionTarget{LifecycleID: domain.SessionLifecycleID{2}, SessionName: "work"}
	remoteRequest := AttachRequest{Intent: protocol.IntentAttach, SessionName: "work", Origin: protocol.RouteOriginDiscovery, OriginKey: "remote.test", Remote: true, EnvironmentPolicy: protocol.EnvironmentPolicyDaemonOwned}
	_, err := ledger.commitAttach(routeCandidateForAttach(remoteRequest, protocol.CommittedRouteIdentity{Target: remoteTarget}, remoteDialer, 0))
	require.NoError(t, err)
	observers := newRouteObserverRegistry()
	observers.register(remoteRequest, remoteDialer)
	require.Len(t, ledger.observationTargets(protocol.RouteOriginDiscovery, "remote.test"), 1)
	require.Len(t, observers.snapshot(), 1)
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler), ledger: ledger, routeObservers: observers}
	attempt := &attachAttempt{
		runner: runner, dialer: localDialer, transport: transport,
		request:    AttachRequest{Intent: protocol.IntentAttach, SessionName: "local", Origin: protocol.RouteOriginLocal, OriginKey: "local", EnvironmentPolicy: protocol.EnvironmentPolicyClientOwned},
		milestones: msForInventoryTest(), themeState: &terminalThemeState{}, enterRaw: func() error { return nil }, reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}
	done := make(chan attachResult, 1)
	go func() { done <- attempt.run(context.Background()) }()

	require.Eventually(t, func() bool { return clock.timerCount() >= 2 }, 5*time.Second, 10*time.Millisecond)
	require.Eventually(t, func() bool {
		clock.fire()
		return remoteDialer.pickerCount() > 0
	}, 5*time.Second, 10*time.Millisecond, "the local attempt must query the retained remote authority")
	require.Eventually(t, func() bool {
		clock.fire()
		snapshot, ok := transport.latestRouteSnapshot()
		if !ok {
			return false
		}
		for _, entry := range append(snapshot.Entries, snapshot.ActiveEntry) {
			if entry.Target == remoteTarget && entry.Attention {
				return true
			}
		}
		return false
	}, 5*time.Second, 10*time.Millisecond)
	close(transport.detach)
	result := <-done
	require.NoError(t, result.err)
	require.Zero(t, localDialer.pickerCount(), "the active local authority has no committed target to observe")
}

func TestInventoryPollBoxKeepsNewestCompletion(t *testing.T) {
	box := newInventoryPollBox()
	box.offer(inventoryPollOutcome{interaction: 2, query: 2})
	box.offer(inventoryPollOutcome{interaction: 2, query: 1})
	outcome, ok := box.take()
	require.True(t, ok)
	require.Equal(t, uint64(2), outcome.query)
	if _, ok := box.take(); ok {
		t.Fatal("take must drain the single coalesced outcome")
	}
}

// TestInventoryAttemptPublishesAndStops pins the relay poll loop through a
// scripted remote serving daemon: the open demand triggers an immediate
// snapshot query and a delta publication, the tick re-polls without
// republishing unchanged inventory, and the close demand stops polling.
func TestInventoryAttemptPublishesAndStops(t *testing.T) {
	clock := newInventoryTestClock()
	dialer := &inventoryTestDialer{}
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), releaseClose: make(chan struct{}), script: inventoryScriptClose,
	}
	t.Cleanup(func() { close(transport.release) })

	done := make(chan attachResult, 1)
	go func() { done <- runInventoryAttempt(t, clock, transport, true, dialer) }()

	select {
	case <-transport.published:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not publish after the open demand")
	}
	// Fire until the tick lands: pushes buffer on every known timer, so a
	// fire racing the post-publication re-arm is retried instead of lost
	// on the discarded open timer.
	require.Eventually(t, func() bool {
		clock.fire()
		return dialer.count() >= 2
	}, 5*time.Second, 10*time.Millisecond, "the tick must drive a second poll")
	require.Eventually(t, func() bool { return transport.publicationCount() == 1 }, 5*time.Second, 10*time.Millisecond,
		"unchanged inventory must not republish, only release the poll slot")
	close(transport.releaseClose)
	close(transport.detach)

	result := <-done
	require.NoError(t, result.err)
	require.Nil(t, result.handoff)
	require.GreaterOrEqual(t, dialer.count(), 2, "the tick must drive a second poll that publishes nothing")
	require.Len(t, transport.publications, 1)
	publication := transport.publications[0]
	require.Equal(t, uint64(7), publication.InteractionGeneration)
	require.Equal(t, uint64(1), publication.PublicationGeneration)
	require.Len(t, publication.Groups, 1)
	require.Equal(t, "aaa/alpha", publication.Groups[0].Entries[0].EntryKey)
}

// TestInventoryAttemptResolvesSelectionToHandoff pins the selection path:
// the serving daemon's selection validates against published state, resolves
// through the local control dialer, and returns a handoff bound to the home
// authority; Run captures the serving recovery route after rebasing metadata.
func TestInventoryAttemptResolvesSelectionToHandoff(t *testing.T) {
	clock := newInventoryTestClock()
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), script: inventoryScriptSelect,
	}
	t.Cleanup(func() { close(transport.release) })

	result := runInventoryAttempt(t, clock, transport, true, &inventoryTestDialer{})
	require.NoError(t, result.err)
	hello := transport.hello()
	require.NotEqual(t, uint16(0), uint16(hello.NavigationCapabilities&protocol.NavigationCapabilityInventory),
		"the attempt must advertise inventory in Hello so the serving palette sends demands")
	require.NotNil(t, result.handoff)
	require.Empty(t, result.handoff.target.Endpoint)
	require.Equal(t, "alpha", result.handoff.target.Session)
	require.NotNil(t, result.handoff.target.ExactTarget)
	require.True(t, result.handoff.inventory)
}

// TestInventoryAttemptIdleWithoutDemands pins the no-demand pairing: without
// demands the relay stays idle, dials nothing, and publishes nothing, and
// the attachment lifecycle is unaffected.
func TestInventoryAttemptIdleWithoutDemands(t *testing.T) {
	clock := newInventoryTestClock()
	dialer := &inventoryTestDialer{}
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), script: inventoryScriptSilent,
	}
	t.Cleanup(func() { close(transport.release) })

	done := make(chan attachResult, 1)
	go func() { done <- runInventoryAttempt(t, clock, transport, true, dialer) }()
	clock.fire()
	close(transport.detach)

	result := <-done
	require.NoError(t, result.err)
	require.Nil(t, result.handoff)
	require.Zero(t, dialer.count(), "no demand must trigger zero control dials")
	require.Zero(t, transport.publicationCount())
}

// TestInventoryAttemptIgnoresDemandsWhenLocal pins the local-attachment
// pairing: demands on a local serving attachment never enable the relay,
// so no control dial crosses even though the frames decode.
func TestInventoryAttemptIgnoresDemandsWhenLocal(t *testing.T) {
	clock := newInventoryTestClock()
	dialer := &inventoryTestDialer{}
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), script: inventoryScriptLocalIgnore,
	}
	t.Cleanup(func() { close(transport.release) })

	done := make(chan attachResult, 1)
	go func() { done <- runInventoryAttempt(t, clock, transport, false, dialer) }()
	close(transport.detach)

	result := <-done
	require.NoError(t, result.err)
	require.Nil(t, result.handoff)
	require.Zero(t, dialer.count())
	require.Zero(t, transport.publicationCount())
}

// TestInventoryAttemptDirectRemoteNeverDialsControl pins the direct-remote
// pairing: a serving daemon that sends inventory demands against an
// attachment with no committed home route must not trigger a local control
// dial. The relay is hybrid-only; without a home route no relay exists, so
// demands decode to no-ops and the attachment lifecycle is unaffected.
func TestInventoryAttemptDirectRemoteNeverDialsControl(t *testing.T) {
	clock := newInventoryTestClock()
	dialer := &inventoryTestDialer{}
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), script: inventoryScriptLocalIgnore,
	}
	t.Cleanup(func() { close(transport.release) })

	input := newPaletteAttachReader(nil)
	t.Cleanup(input.close)
	term := &paletteAttachTerminal{in: input, resize: make(chan domain.Geometry)}
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler)}
	attempt := &attachAttempt{
		runner: runner, dialer: dialer, transport: transport,
		request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "remote-work"},
		remote:  true,
		// No inventoryHome: a direct-remote start has no committed home
		// route, so no relay may exist even when demands arrive.
		inventoryDialer: dialer,
		milestones:      msForInventoryTest(), themeState: &terminalThemeState{},
		enterRaw:  func() error { return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}

	done := make(chan attachResult, 1)
	go func() { done <- attempt.run(context.Background()) }()
	close(transport.detach)

	result := <-done
	require.NoError(t, result.err)
	require.Nil(t, result.handoff)
	require.Zero(t, dialer.count(), "direct remote without a home route must never dial local control")
	require.Zero(t, transport.publicationCount())
	hello := transport.hello()
	require.Zero(t, hello.NavigationCapabilities&protocol.NavigationCapabilityInventory,
		"direct remote must not advertise inventory without a home route")
}
