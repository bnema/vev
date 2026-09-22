package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
)

// previewWatchConn is one scripted daemon watch stream. The test pushes each
// server step; ReceiveServer blocks until a step arrives or the stream closes,
// exactly like a live daemon connection.
type previewWatchConn struct {
	stream ports.BrokerStreamID
	sent   chan protocol.ClientMessage
	steps  chan previewStep
	done   chan struct{}
	once   sync.Once
}

type previewStep struct {
	message protocol.ServerMessage
	err     error
}

func newPreviewWatchConn(stream ports.BrokerStreamID) *previewWatchConn {
	return &previewWatchConn{stream: stream, sent: make(chan protocol.ClientMessage, 4), steps: make(chan previewStep, 8), done: make(chan struct{})}
}

func (c *previewWatchConn) SendClient(message protocol.ClientMessage) error {
	select {
	case <-c.done:
		return errors.New("closed")
	case c.sent <- message:
		return nil
	}
}

func (c *previewWatchConn) ReceiveServer() (protocol.ServerMessage, error) {
	select {
	case <-c.done:
		return nil, errors.New("closed")
	case step := <-c.steps:
		return step.message, step.err
	}
}

func (c *previewWatchConn) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen}
}
func (c *previewWatchConn) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (c *previewWatchConn) LinkEvents() <-chan ports.LinkEvent { return nil }
func (c *previewWatchConn) Done() <-chan struct{}              { return c.done }
func (c *previewWatchConn) Err() error                         { return nil }
func (c *previewWatchConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *previewWatchConn) push(message protocol.ServerMessage) {
	c.steps <- previewStep{message: message}
}

func (c *previewWatchConn) fail(err error) { c.steps <- previewStep{err: err} }

// previewTestConnector dials one scripted physical whose every logical open
// yields a fresh watch stream, published on opened for the test to drive.
type previewTestConnector struct {
	opened   chan *previewWatchConn
	scripted *fakePhysical
}

func (c *previewTestConnector) Connect(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
	if c.scripted == nil {
		c.scripted = &fakePhysical{endpoint: e, done: make(chan struct{}), open: func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
			conn := newPreviewWatchConn(r.Stream)
			c.opened <- conn
			return conn, nil
		}}
	}
	return c.scripted, nil
}

func previewTarget(lifecycle byte, tab string) domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint: ports.BrokerPreviewLocalEndpoint, DisplayOrigin: "local",
		LifecycleID: domain.SessionLifecycleID{lifecycle}, SessionName: "work", LiveTabID: domain.TabStableID(tab),
	}
}

func previewRequestFor(service ports.BrokerService, generation ports.BrokerPreviewGeneration, target domain.RemoteSessionTarget) ports.BrokerPreviewRequest {
	return ports.BrokerPreviewRequest{
		Epoch: 1, Connection: service.ConnectionID(), Generation: generation,
		Route:   ports.BrokerPreviewRoute{Local: true, Policy: poolPolicy()},
		Preview: protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 80, Height: 24},
	}
}

func previewAnswer(lifecycle byte, tab string, width, height int) protocol.RemotePreview {
	return protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK, LifecycleID: domain.SessionLifecycleID{lifecycle}, TabID: domain.TabStableID(tab), Revision: 7, Width: uint16(width), Height: uint16(height), Cells: make([]renderer.Cell, width*height)}
}

type previewFixture struct {
	t         *testing.T
	service   ports.BrokerService
	sub       ports.BrokerPreviewSubscription
	connector *previewTestConnector
	clock     *manualClock
}

// newPreviewFixture subscribes one generation-1 local preview for target on a
// fresh authority driven by a manual clock.
func newPreviewFixture(t *testing.T, target domain.RemoteSessionTarget) *previewFixture {
	t.Helper()
	connector := &previewTestConnector{opened: make(chan *previewWatchConn, 8)}
	clock := newManualClock(time.Unix(0, 0))
	registry, err := NewRegistryWithConfig(1, newTestStore(), nil, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	authority, _, _, _ := composeTestAuthority(t, 1, registry, connector.Connect, clock)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	sub, err := service.SubscribePreview(previewRequestFor(service, 1, target))
	require.NoError(t, err)
	t.Cleanup(sub.Close)
	return &previewFixture{t: t, service: service, sub: sub, connector: connector, clock: clock}
}

// awaitOpen returns the next opened watch stream and the watch it received.
func (f *previewFixture) awaitOpen() (*previewWatchConn, protocol.RemotePreviewWatch) {
	f.t.Helper()
	var conn *previewWatchConn
	select {
	case conn = <-f.connector.opened:
	case <-time.After(5 * time.Second):
		f.t.Fatal("the preview did not open a watch stream")
	}
	select {
	case message := <-conn.sent:
		watch, ok := message.(protocol.RemotePreviewWatch)
		require.True(f.t, ok, "the first message must be a watch, got %T", message)
		return conn, watch
	case <-time.After(5 * time.Second):
		f.t.Fatal("the preview did not send its watch")
		return nil, protocol.RemotePreviewWatch{}
	}
}

func (f *previewFixture) awaitPublication() ports.BrokerPreviewPublication {
	f.t.Helper()
	select {
	case <-f.sub.Changed():
		return f.sub.Latest()
	case <-time.After(5 * time.Second):
		f.t.Fatal("the preview subscription did not publish within the bound")
		return ports.BrokerPreviewPublication{}
	}
}

// advanceUntilOpen drives the manual clock until the reopen backoff fires.
func (f *previewFixture) advanceUntilOpen(step time.Duration) {
	f.t.Helper()
	require.Eventually(f.t, func() bool {
		f.clock.Advance(step)
		return len(f.connector.opened) > 0
	}, 5*time.Second, time.Millisecond)
}

func awaitClosed(t *testing.T, conn *previewWatchConn) {
	t.Helper()
	select {
	case <-conn.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the watch stream was not closed")
	}
}

func TestServicePreviewKeepsOneStreamAcrossFrames(t *testing.T) {
	target := previewTarget(0x11, "tab-alpha")
	f := newPreviewFixture(t, target)
	conn, watch := f.awaitOpen()
	require.Equal(t, previewLocalInterval, watch.MinInterval, "the broker picks the local rate")
	require.Equal(t, target, watch.Request.Target)
	require.NoError(t, protocol.ValidateRemotePreviewWatch(watch))

	for revision := range uint64(3) {
		frame := previewAnswer(0x11, "tab-alpha", 2, 1)
		frame.Revision = revision + 1
		conn.push(frame)
		publication := f.awaitPublication()
		require.NoError(t, publication.Err)
		require.Equal(t, frame.Revision, publication.Preview.Revision)
	}
	require.Empty(t, f.connector.opened, "every frame arrives on the one watch stream")
}

func TestServicePreviewFirstFrameDeadlinePublishesUnavailableAndKeepsWaiting(t *testing.T) {
	f := newPreviewFixture(t, previewTarget(0x11, "tab-alpha"))
	conn, _ := f.awaitOpen()
	require.Eventually(t, func() bool {
		f.clock.Advance(previewFirstFrameLimit)
		return len(f.sub.Changed()) > 0
	}, 5*time.Second, time.Millisecond)
	publication := f.awaitPublication()
	var brokerErr ports.BrokerError
	require.ErrorAs(t, publication.Err, &brokerErr)
	require.Equal(t, ports.BrokerErrorUnavailable, brokerErr.Code)

	conn.push(previewAnswer(0x11, "tab-alpha", 2, 1))
	require.NoError(t, f.awaitPublication().Err, "a slow stream still delivers once it answers")
	require.Empty(t, f.connector.opened)
}

func TestServicePreviewFailureReopensWithBackoff(t *testing.T) {
	cases := []struct {
		name string
		fail func(*previewWatchConn)
	}{
		{name: "stream error", fail: func(c *previewWatchConn) { c.fail(errors.New("stream lost")) }},
		{name: "foreign target", fail: func(c *previewWatchConn) { c.push(previewAnswer(0x22, "tab-alpha", 2, 1)) }},
		{name: "foreign tab", fail: func(c *previewWatchConn) { c.push(previewAnswer(0x11, "tab-beta", 2, 1)) }},
		{name: "oversized crop", fail: func(c *previewWatchConn) { c.push(previewAnswer(0x11, "tab-alpha", 81, 1)) }},
		{name: "wrong message", fail: func(c *previewWatchConn) { c.push(protocol.Pong{}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPreviewFixture(t, previewTarget(0x11, "tab-alpha"))
			first, _ := f.awaitOpen()
			tc.fail(first)
			publication := f.awaitPublication()
			require.Error(t, publication.Err)
			require.Equal(t, protocol.RemotePreview{}, publication.Preview, "a failure never publishes foreign content")
			awaitClosed(t, first)

			f.advanceUntilOpen(previewReopenMin)
			second, _ := f.awaitOpen()
			require.NotEqual(t, first.stream, second.stream, "a reopen allocates a new stream identity")
			second.push(previewAnswer(0x11, "tab-alpha", 2, 1))
			require.NoError(t, f.awaitPublication().Err)
		})
	}
}

func TestServicePreviewBackoffDoublesUntilCap(t *testing.T) {
	f := newPreviewFixture(t, previewTarget(0x11, "tab-alpha"))
	conn, _ := f.awaitOpen()
	want := []time.Duration{previewReopenMin, 2 * previewReopenMin, 4 * previewReopenMin}
	for _, delay := range want {
		conn.fail(errors.New("stream lost"))
		f.awaitPublication()
		awaitClosed(t, conn)
		start := f.clock.Now()
		f.advanceUntilOpen(previewReopenMin / 5)
		require.GreaterOrEqual(t, f.clock.Now().Sub(start), delay)
		conn, _ = f.awaitOpen()
	}
}

func TestServicePreviewNoSuchTargetStopsRecovery(t *testing.T) {
	f := newPreviewFixture(t, previewTarget(0x11, "tab-alpha"))
	conn, _ := f.awaitOpen()
	conn.push(protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewNoSuchTarget})
	publication := f.awaitPublication()
	require.NoError(t, publication.Err)
	require.Equal(t, protocol.RemotePreviewNoSuchTarget, publication.Preview.Status)
	awaitClosed(t, conn)
	require.Never(t, func() bool {
		f.clock.Advance(previewReopenMax)
		return len(f.connector.opened) > 0
	}, 100*time.Millisecond, 5*time.Millisecond, "a dead target is never reopened")
}

func TestServicePreviewCloseClosesTheStream(t *testing.T) {
	cases := []struct {
		name  string
		close func(*previewFixture)
	}{
		{name: "subscription close", close: func(f *previewFixture) { f.sub.Close() }},
		{name: "replacement", close: func(f *previewFixture) {
			replacement, err := f.service.SubscribePreview(previewRequestFor(f.service, 2, previewTarget(0x11, "tab-alpha")))
			require.NoError(f.t, err)
			f.t.Cleanup(replacement.Close)
		}},
		{name: "service close", close: func(f *previewFixture) { require.NoError(f.t, f.service.Close()) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newPreviewFixture(t, previewTarget(0x11, "tab-alpha"))
			conn, _ := f.awaitOpen()
			conn.push(previewAnswer(0x11, "tab-alpha", 2, 1))
			f.awaitPublication()
			tc.close(f)
			awaitClosed(t, conn)
			require.NotPanics(t, func() { _ = f.sub.Latest() }, "Latest stays observable after Close")
		})
	}
}

func TestServicePreviewRouteValidation(t *testing.T) {
	registration := domain.RemoteRegistration{Endpoint: "dev@host:22", Incarnation: [16]byte{1}, Generation: 1}
	remoteTarget := previewTarget(0x11, "tab-alpha")
	remoteTarget.Endpoint = "dev@host:22"
	cases := []struct {
		name   string
		mutate func(*ports.BrokerPreviewRequest)
	}{
		{name: "local route for remote target", mutate: func(r *ports.BrokerPreviewRequest) { r.Preview.Target = remoteTarget }},
		{name: "remote route for local target", mutate: func(r *ports.BrokerPreviewRequest) {
			r.Route = ports.BrokerPreviewRoute{Endpoint: registration.Endpoint, Registration: registration, Policy: poolPolicy()}
		}},
		{name: "remote route for another host", mutate: func(r *ports.BrokerPreviewRequest) {
			other := remoteTarget
			other.Endpoint = "other@host:22"
			r.Route = ports.BrokerPreviewRoute{Endpoint: registration.Endpoint, Registration: registration, Policy: poolPolicy()}
			r.Preview.Target = other
		}},
		{name: "remote route without registration", mutate: func(r *ports.BrokerPreviewRequest) {
			r.Route = ports.BrokerPreviewRoute{Endpoint: registration.Endpoint, Policy: poolPolicy()}
			r.Preview.Target = remoteTarget
		}},
		{name: "local route with registration", mutate: func(r *ports.BrokerPreviewRequest) { r.Route.Registration = registration }},
		{name: "zero generation", mutate: func(r *ports.BrokerPreviewRequest) { r.Generation = 0 }},
		{name: "invalid viewport", mutate: func(r *ports.BrokerPreviewRequest) { r.Preview.Width = 0 }},
	}
	f := newPreviewFixture(t, previewTarget(0x11, "tab-alpha"))
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			request := previewRequestFor(f.service, 5, previewTarget(0x11, "tab-alpha"))
			tc.mutate(&request)
			_, err := f.service.SubscribePreview(request)
			require.ErrorIs(t, err, ports.BrokerAdmissionInvalid)
		})
	}
	stale := previewRequestFor(f.service, 1, previewTarget(0x11, "tab-alpha"))
	_, err := f.service.SubscribePreview(stale)
	require.ErrorIs(t, err, ports.BrokerAdmissionStale, "a generation is never reused")
}
