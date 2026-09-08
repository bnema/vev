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
	publications  []protocol.NavigationInventoryPublication
	published     chan struct{}
	publishedOnce sync.Once
	release       chan struct{}
	detach        chan struct{}
	script        inventoryScript
}

func (t *inventoryAttemptTransport) Send(frame wire.Frame) error {
	if frame.Type == wire.MsgNavigationInventoryPublication {
		publication, err := wire.UnmarshalNavigationInventoryPublication(frame.Payload)
		if err != nil {
			return err
		}
		t.mu.Lock()
		t.publications = append(t.publications, publication)
		t.mu.Unlock()
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

func (t *inventoryAttemptTransport) publicationCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.publications)
}

// inventoryTestControlConn answers one snapshot or resolve query from canned
// groups without Hello, session creation, or attachment.
type inventoryTestControlConn struct {
	request protocol.NavigationInventoryRequest
}

func (c *inventoryTestControlConn) SendClient(message protocol.ClientMessage) error {
	request, ok := message.(protocol.NavigationInventoryRequest)
	if !ok {
		return io.ErrUnexpectedEOF
	}
	c.request = request
	return nil
}

func (c *inventoryTestControlConn) ReceiveServer() (protocol.ServerMessage, error) {
	switch c.request.Operation {
	case protocol.NavigationInventorySnapshot:
		return protocol.NavigationInventoryResponse{
			RequestID: c.request.RequestID, Operation: protocol.NavigationInventorySnapshot,
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
			RequestID: c.request.RequestID, Operation: protocol.NavigationInventoryResolve,
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
	mu    sync.Mutex
	dials int
}

func (d *inventoryTestDialer) Dial(context.Context) (ports.ClientConnection, error) {
	d.mu.Lock()
	d.dials++
	d.mu.Unlock()
	return &inventoryTestControlConn{}, nil
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
	runner := &Runner{term: term, clock: clock, logger: slog.New(slog.DiscardHandler)}
	attempt := &attachAttempt{
		runner: runner, dialer: dialer, transport: transport,
		request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "remote-work"},
		remote:  remote,
		inventoryHome: &attachRoute{
			dialer:  dialer,
			request: AttachRequest{Intent: protocol.IntentAttach, SessionName: "home"},
		},
		milestones: msForInventoryTest(), themeState: &terminalThemeState{},
		enterRaw:  func() error { return nil },
		reconnect: &reconnectUI{term: term, rawEntered: new(bool)},
	}
	return attempt.run(context.Background())
}

func msForInventoryTest() *milestones { return &milestones{} }

// TestInventoryAttemptPublishesAndStops pins the relay poll loop through a
// scripted remote serving daemon: the open demand triggers an immediate
// snapshot query and a delta publication, the tick re-polls without
// republishing unchanged inventory, and the close demand stops polling.
func TestInventoryAttemptPublishesAndStops(t *testing.T) {
	clock := newInventoryTestClock()
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), detach: make(chan struct{}), script: inventoryScriptClose,
	}
	t.Cleanup(func() { close(transport.release) })

	done := make(chan attachResult, 1)
	go func() { done <- runInventoryAttempt(t, clock, transport, true, &inventoryTestDialer{}) }()

	select {
	case <-transport.published:
	case <-time.After(5 * time.Second):
		t.Fatal("relay did not publish after the open demand")
	}
	clock.fire()
	require.Eventually(t, func() bool { return transport.publicationCount() == 1 }, 5*time.Second, 10*time.Millisecond,
		"unchanged inventory must not republish, only release the poll slot")
	close(transport.detach)

	result := <-done
	require.NoError(t, result.err)
	require.Nil(t, result.handoff)
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
// authority with the serving route as the inventory return route.
func TestInventoryAttemptResolvesSelectionToHandoff(t *testing.T) {
	clock := newInventoryTestClock()
	transport := &inventoryAttemptTransport{
		published: make(chan struct{}), release: make(chan struct{}), script: inventoryScriptSelect,
	}
	t.Cleanup(func() { close(transport.release) })

	result := runInventoryAttempt(t, clock, transport, true, &inventoryTestDialer{})
	require.NoError(t, result.err)
	require.NotNil(t, result.handoff)
	require.Empty(t, result.handoff.target.Endpoint)
	require.Equal(t, "alpha", result.handoff.target.Session)
	require.NotNil(t, result.handoff.target.ExactTarget)
	require.NotNil(t, result.handoff.inventory)
	require.Equal(t, uint64(9), result.handoff.inventory.causeActionID)
	require.Equal(t, "remote-work", result.handoff.inventory.returnRoute.request.SessionName)
}

// TestInventoryAttemptMatrixOldDaemon pins the old-daemon pairing: without
// demands the relay stays idle, dials nothing, and publishes nothing, and
// the attachment lifecycle is unaffected.
func TestInventoryAttemptMatrixOldDaemon(t *testing.T) {
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
	require.Zero(t, dialer.count(), "an old daemon must trigger zero control dials")
	require.Zero(t, transport.publicationCount())
}

// TestInventoryAttemptMatrixLocalAttachment pins the local-attachment
// pairing: demands from a local serving daemon never enable the relay, so
// no control dial crosses even though the frames decode.
func TestInventoryAttemptMatrixLocalAttachment(t *testing.T) {
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
