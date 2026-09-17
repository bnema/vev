package brokeripc

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
)

// testSocketPath returns a short, owner-only parent directory and one broker
// endpoint inside it, so a real AF_UNIX path is exercised well within the
// cross-platform pathname limit.
func testSocketPath(t *testing.T) string {
	t.Helper()
	root, err := os.MkdirTemp("/tmp", "v")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	return SocketPath(filepath.Join(root, "vev"))
}

// testPolicy builds one valid broker policy.
func testPolicy() ports.BrokerPolicy {
	return ports.BrokerPolicy{
		ProtocolVersion:      protocol.Version,
		CatalogSchemaVersion: catalogue.RemoteCatalogSchemaVersion,
		EnvironmentPolicy:    protocol.EnvironmentPolicyClientOwned,
		Transport:            "unix",
		Trust:                "trusted",
		Launch:               "explicit",
		Isolation:            "per-user",
	}
}

// testHostAt builds one catalogue-valid host projection for index i.
func testHostAt(i int) ports.RemoteHostSnapshot {
	endpoint := fmt.Sprintf("user%d@host%d:22", i, i)
	return ports.RemoteHostSnapshot{
		Endpoint:       endpoint,
		DisplayOrigin:  endpoint,
		Registration:   domain.RemoteRegistration{Endpoint: endpoint, Incarnation: [16]byte{0x10, byte(i + 1), 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xf0, 0x11}, Generation: domain.RemoteGeneration(i + 1)},
		Availability:   domain.RemoteAvailabilityReachable,
		LastSuccess:    time.Unix(1700000000+int64(i), 0).UTC(),
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{},
	}
}

// testSessionAt builds one catalogue-valid session for a host and index.
func testSessionAt(host, index int) catalogue.RemoteCatalogSession {
	return catalogue.RemoteCatalogSession{
		LifecycleID: domain.SessionLifecycleID{byte(host + 1), byte(index >> 8), byte(index), 0x77},
		Name:        fmt.Sprintf("s%02d-%03d", host, index),
		State:       catalogue.RemoteCatalogSessionDown,
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
}

// testSnapshot builds one valid publication for an epoch and revision.
func testSnapshot(epoch ports.BrokerEpoch, revision ports.BrokerRevision) ports.BrokerSnapshot {
	host := testHostAt(0)
	host.Sessions = []catalogue.RemoteCatalogSession{testSessionAt(0, 0), testSessionAt(0, 1)}
	return ports.BrokerSnapshot{Epoch: epoch, Revision: revision, Hosts: []ports.RemoteHostSnapshot{host}}
}

// snapshotHub is a multi-subscriber publication hub standing in for the broker
// registry: one immutable current snapshot, coalescing capacity-one subscribers.
type snapshotHub struct {
	mu       sync.Mutex
	snapshot ports.BrokerSnapshot
	subs     map[*fakeSubscription]struct{}
}

func newSnapshotHub(snapshot ports.BrokerSnapshot) *snapshotHub {
	return &snapshotHub{snapshot: snapshot, subs: make(map[*fakeSubscription]struct{})}
}

func (h *snapshotHub) current() ports.BrokerSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshot.Clone()
}

func (h *snapshotHub) subscribe() *fakeSubscription {
	sub := &fakeSubscription{hub: h, changed: make(chan struct{}, 1)}
	h.mu.Lock()
	h.subs[sub] = struct{}{}
	h.mu.Unlock()
	return sub
}

func (h *snapshotHub) publish(snapshot ports.BrokerSnapshot) {
	h.mu.Lock()
	h.snapshot = snapshot.Clone()
	subs := make([]*fakeSubscription, 0, len(h.subs))
	for sub := range h.subs {
		subs = append(subs, sub)
	}
	h.mu.Unlock()
	for _, sub := range subs {
		sub.notify()
	}
}

func (h *snapshotHub) remove(sub *fakeSubscription) {
	h.mu.Lock()
	delete(h.subs, sub)
	h.mu.Unlock()
}

// fakeSubscription is one coalescing capacity-one subscription.
type fakeSubscription struct {
	hub     *snapshotHub
	changed chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closed  bool
}

func (s *fakeSubscription) Changed() <-chan struct{} { return s.changed }

func (s *fakeSubscription) Close() {
	s.once.Do(func() {
		s.mu.Lock()
		s.closed = true
		s.mu.Unlock()
		s.hub.remove(s)
	})
}

func (s *fakeSubscription) notify() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

// fakeLogicalConn is one admitted core logical connection: it records messages
// the broker relays toward the daemon and lets a test inject daemon replies.
type fakeLogicalConn struct {
	fromClient chan protocol.ClientMessage
	toClient   chan protocol.ServerMessage
	done       chan struct{}
	once       sync.Once
	mu         sync.Mutex
	err        error
	closed     bool
}

func newFakeLogicalConn() *fakeLogicalConn {
	return &fakeLogicalConn{
		fromClient: make(chan protocol.ClientMessage, 64),
		toClient:   make(chan protocol.ServerMessage, 64),
		done:       make(chan struct{}),
	}
}

func (c *fakeLogicalConn) SendClient(message protocol.ClientMessage) error {
	select {
	case c.fromClient <- message:
		return nil
	case <-c.done:
		return ports.BrokerAdmissionClosed
	}
}

func (c *fakeLogicalConn) ReceiveServer() (protocol.ServerMessage, error) {
	select {
	case message := <-c.toClient:
		return message, nil
	case <-c.done:
		if err := c.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}
}

func (c *fakeLogicalConn) Capabilities() protocol.ConnectionCapabilities {
	return protocol.ConnectionCapabilities{OutputDataLimit: protocol.MaxOutputDataLen}
}

func (c *fakeLogicalConn) LinkState() ports.LinkState         { return ports.LinkStateConnected }
func (c *fakeLogicalConn) LinkEvents() <-chan ports.LinkEvent { return nil }
func (c *fakeLogicalConn) Done() <-chan struct{}              { return c.done }
func (c *fakeLogicalConn) Err() error                         { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

func (c *fakeLogicalConn) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
	})
	return nil
}

// fail publishes one terminal core failure.
func (c *fakeLogicalConn) fail(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	_ = c.Close()
}

// fakeCore is one admitted per-connection broker service.
type fakeCore struct {
	mu        sync.Mutex
	id        ports.BrokerConnectionID
	hub       *snapshotHub
	streams   map[ports.BrokerStreamID]*fakeLogicalConn
	opens     []ports.BrokerOpenStreamRequest
	closedIDs []ports.BrokerStreamID
	added     []string
	removed   []string
	reconcile []string
	openErr   error
	blockAdd  chan struct{}
	closed    bool
	done      chan struct{}
	once      sync.Once
}

func (c *fakeCore) ConnectionID() ports.BrokerConnectionID { return c.id }

func (c *fakeCore) Done() <-chan struct{} { return c.done }

func (c *fakeCore) Err() error { return nil }

func (c *fakeCore) Snapshot() ports.BrokerSnapshot { return c.hub.current() }

func (c *fakeCore) Subscribe() (ports.BrokerSubscription, error) { return c.hub.subscribe(), nil }

func (c *fakeCore) OpenStream(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerLogicalConnection, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.openErr != nil {
		return nil, c.openErr
	}
	if c.closed {
		return nil, ports.BrokerAdmissionClosed
	}
	conn := newFakeLogicalConn()
	c.streams[request.Stream] = conn
	c.opens = append(c.opens, request)
	return conn, nil
}

func (c *fakeCore) CloseStream(connection ports.BrokerConnectionID, stream ports.BrokerStreamID) error {
	if connection != c.id {
		return ports.BrokerAdmissionStale
	}
	c.mu.Lock()
	conn, ok := c.streams[stream]
	if ok {
		delete(c.streams, stream)
	}
	c.closedIDs = append(c.closedIDs, stream)
	c.mu.Unlock()
	if !ok {
		return ports.BrokerAdmissionStale
	}
	return conn.Close()
}

func (c *fakeCore) AddHost(ctx context.Context, target string) error {
	if c.blockAdd != nil {
		select {
		case <-c.blockAdd:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.added = append(c.added, target)
	c.mu.Unlock()
	return nil
}

func (c *fakeCore) RemoveHost(ctx context.Context, target string) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.mu.Lock()
	c.removed = append(c.removed, target)
	c.mu.Unlock()
	return target == "known", nil
}

func (c *fakeCore) RequestReconcile(endpoint string) {
	c.mu.Lock()
	c.reconcile = append(c.reconcile, endpoint)
	c.mu.Unlock()
}

func (c *fakeCore) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		conns := make([]*fakeLogicalConn, 0, len(c.streams))
		for _, conn := range c.streams {
			conns = append(conns, conn)
		}
		c.streams = make(map[ports.BrokerStreamID]*fakeLogicalConn)
		c.mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		close(c.done)
	})
	return nil
}

// logicalConn returns one admitted core connection by stream identity.
func (c *fakeCore) logicalConn(stream ports.BrokerStreamID) *fakeLogicalConn {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[stream]
}

// newTestCore builds one admitted core service for a session test: a fresh hub
// and a connection identity bound to epoch.
func newTestCore(epoch ports.BrokerEpoch) *fakeCore {
	var id ports.BrokerConnectionID
	binary.BigEndian.PutUint64(id[:8], uint64(epoch))
	id[15] = 1
	return &fakeCore{
		id:      id,
		hub:     newSnapshotHub(ports.BrokerSnapshot{}),
		streams: make(map[ports.BrokerStreamID]*fakeLogicalConn),
		done:    make(chan struct{}),
	}
}

// fakeAuthority admits clients for one listener and hands out per-connection
// core services.
type fakeAuthority struct {
	mu       sync.Mutex
	hub      *snapshotHub
	epoch    ports.BrokerEpoch
	next     uint64
	clients  []*fakeCore
	admitErr error
}

func newFakeAuthority(epoch ports.BrokerEpoch, snapshot ports.BrokerSnapshot) *fakeAuthority {
	return &fakeAuthority{hub: newSnapshotHub(snapshot), epoch: epoch}
}

func (a *fakeAuthority) AdmitClient(ctx context.Context) (ports.BrokerService, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.admitErr != nil {
		return nil, a.admitErr
	}
	a.next++
	var id ports.BrokerConnectionID
	binary.BigEndian.PutUint64(id[:8], uint64(a.epoch))
	binary.BigEndian.PutUint64(id[8:], a.next)
	core := &fakeCore{id: id, hub: a.hub, streams: make(map[ports.BrokerStreamID]*fakeLogicalConn), done: make(chan struct{})}
	a.clients = append(a.clients, core)
	return core, nil
}

func (a *fakeAuthority) last() *fakeCore {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.clients) == 0 {
		return nil
	}
	return a.clients[len(a.clients)-1]
}

func (a *fakeAuthority) publish(snapshot ports.BrokerSnapshot) { a.hub.publish(snapshot) }

// endpoint is one running broker IPC listener with its authority.
type endpoint struct {
	t         *testing.T
	path      string
	listener  ports.BrokerListener
	authority *fakeAuthority
	epoch     ports.BrokerEpoch
}

func startEndpoint(t *testing.T, cfg Config) *endpoint {
	t.Helper()
	epoch := ports.BrokerEpoch(0x51)
	authority := newFakeAuthority(epoch, ports.BrokerSnapshot{})
	listener, err := Listen(testSocketPath(t), epoch, authority, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return &endpoint{t: t, path: listener.Addr(), listener: listener, authority: authority, epoch: epoch}
}

// accept returns the next admitted session.
func (e *endpoint) accept() ports.BrokerService {
	e.t.Helper()
	type result struct {
		service ports.BrokerService
		err     error
	}
	out := make(chan result, 1)
	go func() {
		service, err := e.listener.Accept()
		out <- result{service, err}
	}()
	select {
	case got := <-out:
		require.NoError(e.t, got.err)
		return got.service
	case <-time.After(5 * time.Second):
		e.t.Fatal("listener.Accept did not return")
		return nil
	}
}

// dial connects one client adapter with the default test configuration.
func (e *endpoint) dial() ports.BrokerService { return e.dialWith(Config{}) }

// dialWith connects one client adapter with an explicit configuration.
func (e *endpoint) dialWith(cfg Config) ports.BrokerService {
	e.t.Helper()
	cfg.HandshakeTimeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := Dial(ctx, e.path, cfg)
	require.NoError(e.t, err)
	e.t.Cleanup(func() { _ = client.Close() })
	return client
}

// pair dials one client and accepts its session.
func (e *endpoint) pair() (ports.BrokerService, ports.BrokerService) {
	e.t.Helper()
	client := e.dial()
	session := e.accept()
	return client, session
}

// publish makes one snapshot current and wakes every subscriber.
func (e *endpoint) publish(epoch ports.BrokerEpoch, revision ports.BrokerRevision) ports.BrokerSnapshot {
	e.t.Helper()
	snapshot := testSnapshot(epoch, revision)
	e.authority.publish(snapshot)
	return snapshot
}

// recordingTransport is one in-memory server carriage for a session: it decodes
// and records every complete snapshot transfer the session writes and lets a
// test wait for a generation to publish. It never yields an inbound frame, so
// only the code under test drives publication.
type recordingTransport struct {
	mu      sync.Mutex
	ends    map[brokerwire.SubscriptionGeneration]map[ports.BrokerRevision]int
	changed chan struct{}
	closedC chan struct{}
	once    sync.Once
}

func newRecordingTransport() *recordingTransport {
	return &recordingTransport{
		ends:    make(map[brokerwire.SubscriptionGeneration]map[ports.BrokerRevision]int),
		changed: make(chan struct{}, 1),
		closedC: make(chan struct{}),
	}
}

// Send decodes one server frame and records a completed snapshot transfer.
func (t *recordingTransport) Send(envelope wire.Envelope) error {
	message, err := brokerwire.DecodeServer(envelope.Payload, brokerwire.MaxBrokerEnvelopeBytes, brokerwire.MaxStreamChunkBytes)
	if err != nil {
		return err
	}
	part, ok := message.(brokerwire.SnapshotPart)
	if !ok {
		return nil
	}
	if _, ok := part.Part.(brokerwire.SnapshotEnd); !ok {
		return nil
	}
	t.mu.Lock()
	if t.ends[part.Generation] == nil {
		t.ends[part.Generation] = make(map[ports.BrokerRevision]int)
	}
	t.ends[part.Generation][part.Revision]++
	t.mu.Unlock()
	select {
	case t.changed <- struct{}{}:
	default:
	}
	return nil
}

func (t *recordingTransport) RecvBounded(uint64) (wire.Envelope, error) {
	<-t.closedC
	return wire.Envelope{}, io.EOF
}

func (t *recordingTransport) Recv() (wire.Envelope, error) { return t.RecvBounded(0) }

func (t *recordingTransport) Close() error {
	t.once.Do(func() { close(t.closedC) })
	return nil
}

// transferCount reports how many complete transfers of one generation committed
// one revision.
func (t *recordingTransport) transferCount(generation brokerwire.SubscriptionGeneration, revision ports.BrokerRevision) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ends[generation][revision]
}

// waitForTransfer blocks until generation committed revision at least once.
func (t *recordingTransport) waitForTransfer(generation brokerwire.SubscriptionGeneration, revision ports.BrokerRevision, timeout time.Duration) bool {
	return t.waitForTransferCount(generation, revision, 1, timeout)
}

// waitForTransferCount blocks until generation committed revision at least
// minimum times, or the timeout elapses.
func (t *recordingTransport) waitForTransferCount(generation brokerwire.SubscriptionGeneration, revision ports.BrokerRevision, minimum int, timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		if t.transferCount(generation, revision) >= minimum {
			return true
		}
		select {
		case <-t.changed:
		case <-deadline.C:
			return t.transferCount(generation, revision) >= minimum
		}
	}
}
