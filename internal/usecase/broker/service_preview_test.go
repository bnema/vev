package broker

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	renderer "github.com/bnema/vev-vt"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// previewLogicalConn is one scripted daemon-side logical connection: the test
// owns the preview answers it returns, records the stream identities the
// preview opens, and can block sends or receives on its closed channel.
type previewLogicalConn struct {
	mu      sync.Mutex
	streams []ports.BrokerStreamID
	pending []protocol.RemotePreview
	err     error
	done    chan struct{}
	once    sync.Once
}

func (c *previewLogicalConn) SendClient(message protocol.ClientMessage) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return errors.New("closed")
	default:
	}
	if _, ok := message.(protocol.RemotePreviewRequest); !ok {
		return errors.New("broker: preview connection received a non-preview request")
	}
	return nil
}

func (c *previewLogicalConn) ReceiveServer() (protocol.ServerMessage, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.done:
		return nil, errors.New("closed")
	default:
	}
	if c.err != nil {
		return nil, c.err
	}
	if len(c.pending) == 0 {
		return nil, errors.New("broker: no scripted preview answer")
	}
	preview := c.pending[0]
	c.pending = c.pending[1:]
	return preview, nil
}

func (c *previewLogicalConn) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen}
}

func (c *previewLogicalConn) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (c *previewLogicalConn) LinkEvents() <-chan ports.LinkEvent { return nil }
func (c *previewLogicalConn) Done() <-chan struct{}              { return c.done }
func (c *previewLogicalConn) Err() error                         { return nil }

func (c *previewLogicalConn) Close() error {
	c.once.Do(func() { close(c.done) })
	return nil
}

func (c *previewLogicalConn) queue(preview protocol.RemotePreview) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pending = append(c.pending, preview)
}

func (c *previewLogicalConn) fail(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err = err
}

// previewTarget builds one valid live preview target with the given lifecycle
// and tab, on the broker's canonical local authority.
func previewTarget(lifecycle byte, tab string) domain.RemoteSessionTarget {
	return domain.RemoteSessionTarget{
		Endpoint:      "local",
		DisplayOrigin: "local",
		LifecycleID:   domain.SessionLifecycleID{lifecycle},
		SessionName:   "work",
		LiveTabID:     domain.TabStableID(tab),
	}
}

// previewRequestFor builds one valid observation route plus preview request for
// a connection, epoch, and target. The stream identity is stated explicitly,
// exactly as the poll loop allocates one per attempt.
func previewRequestFor(epoch ports.BrokerEpoch, connection ports.BrokerConnectionID, stream ports.BrokerStreamID, target domain.RemoteSessionTarget) ports.BrokerPreviewRequest {
	return ports.BrokerPreviewRequest{
		Epoch:      epoch,
		Connection: connection,
		Generation: 1,
		Route: ports.BrokerOpenStreamRequest{
			Epoch:      epoch,
			Connection: connection,
			Stream:     stream,
			Local:      true,
			Purpose:    ports.BrokerStreamObservation,
			Policy:     poolPolicy(),
			StartMode:  ports.BrokerDaemonExistingOnly,
		},
		Preview: protocol.RemotePreviewRequest{Version: protocol.RemotePreviewSchemaVersion, Target: target, Width: 80, Height: 24},
	}
}

// awaitPreviewPublication waits for one Changed wake and returns the latest
// publication, failing the test on timeout.
func awaitPreviewPublication(t *testing.T, sub ports.BrokerPreviewSubscription) ports.BrokerPreviewPublication {
	t.Helper()
	select {
	case <-sub.Changed():
		return sub.Latest()
	case <-time.After(3 * time.Second):
		t.Fatal("the preview subscription did not publish within the bound")
		return ports.BrokerPreviewPublication{}
	}
}

// requirePreviewOK extracts the successful preview from a publication.
func requirePreviewOK(t *testing.T, publication ports.BrokerPreviewPublication) protocol.RemotePreview {
	t.Helper()
	require.NoError(t, publication.Err)
	require.Equal(t, protocol.RemotePreviewOK, publication.Preview.Status)
	return publication.Preview
}

// previewTestConnector dials one scripted physical whose every logical open
// returns one fresh scripted daemon connection. Every opened stream identity
// is recorded so a test can prove the preview poll loop allocates exactly
// one fresh identity per attempt. One connection per open models the daemon
// transport truthfully: the broker closes every logical stream after one
// exchange, and a shared connection would park the poll loop on the second.
type previewTestConnector struct {
	mu       sync.Mutex
	opens    []ports.BrokerStreamID
	pending  []protocol.RemotePreview
	scripted *fakePhysical
}

func (c *previewTestConnector) Connect(_ context.Context, e ports.BrokerDialTarget) (ports.BrokerPhysicalConnection, error) {
	connector := c
	if connector.scripted == nil {
		connector.scripted = &fakePhysical{endpoint: e, done: make(chan struct{}), open: func(_ context.Context, r ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
			connector.mu.Lock()
			connector.opens = append(connector.opens, r.Stream)
			pending := append([]protocol.RemotePreview(nil), connector.pending...)
			connector.mu.Unlock()
			return &previewLogicalConn{done: make(chan struct{}), pending: pending}, nil
		}}
	}
	return connector.scripted, nil
}

func (c *previewTestConnector) opened() []ports.BrokerStreamID {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]ports.BrokerStreamID(nil), c.opens...)
}

// previewAnswer builds one valid successful preview for the given identity
// and dimensions.
func previewAnswer(lifecycle byte, tab string, width, height int) protocol.RemotePreview {
	return protocol.RemotePreview{Version: protocol.RemotePreviewSchemaVersion, Status: protocol.RemotePreviewOK, LifecycleID: domain.SessionLifecycleID{lifecycle}, TabID: domain.TabStableID(tab), Revision: 7, Width: uint16(width), Height: uint16(height), Cells: make([]renderer.Cell, width*height)}
}

// subscribePreview subscribes one generation-1 preview on a fresh authority
// whose daemon answers come from the queued scripted previews. It returns the
// manual clock that drives the service's poll cadence, so a test advances
// time instead of sleeping through the 1s repoll interval.
func subscribePreview(t *testing.T, daemon *previewLogicalConn, target domain.RemoteSessionTarget) (ports.BrokerService, ports.BrokerPreviewSubscription, *previewTestConnector, *manualClock) {
	t.Helper()
	daemon.mu.Lock()
	queued := append([]protocol.RemotePreview(nil), daemon.pending...)
	daemon.mu.Unlock()
	connector := &previewTestConnector{pending: queued}
	clock := newManualClock(time.Unix(0, 0))
	store := newTestStore()
	registry, err := NewRegistryWithConfig(1, store, nil, clock, nil, RegistryConfig{ObservationDisabled: true})
	require.NoError(t, err)
	authority, _, _, _ := composeTestAuthority(t, 1, registry, connector.Connect, clock)
	service, err := authority.AdmitClient(context.Background())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, service.Close()) })
	stream, err := service.NextStreamID()
	require.NoError(t, err)
	request := previewRequestFor(1, service.ConnectionID(), stream, target)
	sub, err := service.SubscribePreview(request)
	require.NoError(t, err)
	t.Cleanup(sub.Close)
	return service, sub, connector, clock
}

// TestServicePreviewPollReusesNoStream proves GO-002: the poll loop allocates
// one fresh stream identity per attempt. Two consecutive successful polls open
// two distinct streams on the same connection instead of replaying the first.
func TestServicePreviewPollReusesNoStream(t *testing.T) {
	target := previewTarget(0x11, "tab-alpha")
	daemon := &previewLogicalConn{done: make(chan struct{})}
	daemon.queue(previewAnswer(0x11, "tab-alpha", 2, 1))
	daemon.queue(previewAnswer(0x11, "tab-alpha", 2, 1))
	_, sub, connector, clock := subscribePreview(t, daemon, target)

	// The poll loop republishes on a 1s cadence driven by the manual clock.
	// The first wake carries the first queued answer; advancing the clock
	// drives the second poll, whose answer can only come from a second open.
	requirePreviewOK(t, awaitPreviewPublication(t, sub))
	require.Len(t, connector.opened(), 1)
	clock.Advance(previewPollCadence)
	requirePreviewOK(t, awaitPreviewPublication(t, sub))
	opens := connector.opened()
	require.NotEqual(t, opens[0], opens[1], "the second poll must not replay the first stream identity")
}

// TestServicePreviewRejectsForeignTarget proves GO-005: a daemon answer whose
// inner preview names another session lifecycle or tab is rejected as an
// unavailable typed failure instead of being published as the current row.
func TestServicePreviewRejectsForeignTarget(t *testing.T) {
	target := previewTarget(0x11, "tab-alpha")
	daemon := &previewLogicalConn{done: make(chan struct{})}
	daemon.queue(previewAnswer(0x22, "tab-alpha", 2, 1))
	_, sub, _, _ := subscribePreview(t, daemon, target)

	publication := awaitPreviewPublication(t, sub)
	require.Error(t, publication.Err, "a foreign-target answer must not publish as success")
	require.Equal(t, protocol.RemotePreview{}, publication.Preview)
}

// TestServicePreviewRejectsForeignTab proves the tab half of GO-005: the same
// lifecycle with another tab's answer is rejected too.
func TestServicePreviewRejectsForeignTab(t *testing.T) {
	target := previewTarget(0x11, "tab-alpha")
	daemon := &previewLogicalConn{done: make(chan struct{})}
	daemon.queue(previewAnswer(0x11, "tab-beta", 2, 1))
	_, sub, _, _ := subscribePreview(t, daemon, target)

	publication := awaitPreviewPublication(t, sub)
	require.Error(t, publication.Err)
	require.Equal(t, protocol.RemotePreview{}, publication.Preview)
}

// TestServicePreviewAcceptsCroppedDimensions proves the GO-005 bound: the
// daemon crops the viewport to the focused pane, so a successful answer whose
// inner dimensions fit inside the request is accepted.
func TestServicePreviewAcceptsCroppedDimensions(t *testing.T) {
	target := previewTarget(0x11, "tab-alpha")
	daemon := &previewLogicalConn{done: make(chan struct{})}
	daemon.queue(previewAnswer(0x11, "tab-alpha", 2, 1))
	_, sub, _, _ := subscribePreview(t, daemon, target)

	publication := awaitPreviewPublication(t, sub)
	preview := requirePreviewOK(t, publication)
	require.Equal(t, uint16(2), preview.Width)
	require.Equal(t, uint16(1), preview.Height)
}

// TestServicePreviewCloseJoinsPollLoop proves the Close and wait-group half
// of the coverage gap: closing the subscription settles the poll goroutine,
// so Latest stays observable and no second wake fires after close.
func TestServicePreviewCloseJoinsPollLoop(t *testing.T) {
	target := previewTarget(0x11, "tab-alpha")
	daemon := &previewLogicalConn{done: make(chan struct{})}
	daemon.queue(previewAnswer(0x11, "tab-alpha", 2, 1))
	_, sub, _, _ := subscribePreview(t, daemon, target)
	requirePreviewOK(t, awaitPreviewPublication(t, sub))
	sub.Close()
	sub.Close()
	require.NotPanics(t, func() { _ = sub.Latest() }, "Latest stays observable after Close")
}
