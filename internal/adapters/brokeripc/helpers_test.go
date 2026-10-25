package brokeripc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/bnema/vev/internal/adapters/brokerwire"
	"github.com/bnema/vev/internal/adapters/sessionwire"
	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/bnema/vev/internal/protocol/wire"
	wiremocks "github.com/bnema/vev/internal/protocol/wire/mocks"
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

// openRequest builds one valid control-purpose open request for exactly stream,
// with this connection's start-if-needed authorization. After the cutover the
// calling service allocates every identity with NextStreamID, so a test that
// names a stream always states it explicitly.
func openRequest(stream ports.BrokerStreamID) ports.BrokerOpenStreamRequest {
	return ports.BrokerOpenStreamRequest{
		Purpose:   ports.BrokerStreamControl,
		Local:     true,
		Stream:    stream,
		Policy:    testPolicy(),
		StartMode: ports.BrokerDaemonStartIfNeeded,
	}
}

// attachmentRequest builds one valid attachment request for exactly stream with
// the given admission variant and creation name.
func attachmentRequest(stream ports.BrokerStreamID, admission ports.BrokerStreamAdmission, name string) ports.BrokerOpenStreamRequest {
	request := openRequest(stream)
	request.Purpose = ports.BrokerStreamAttachment
	request.Admission = admission
	request.Name = name
	return request
}

// nextStreamID reads one fresh identity from the service's only allocator,
// exactly as the composed supervisor and broker operations do.
func nextStreamID(t *testing.T, service ports.BrokerSession) ports.BrokerStreamID {
	t.Helper()
	stream, err := service.NextStreamID()
	require.NoError(t, err)
	return stream
}

// membershipProbeEndpoint is the endpoint every liveness probe adds through the
// real membership delegation path.
const membershipProbeEndpoint = "probe@host:22"

// requireConnectionDelegates proves one connection still admits a full
// membership round trip after a stream-local or subscriber-local event: the
// probe adds one host through the same delegated path a real caller uses and
// asserts the registration comes back for exactly the requested endpoint.
func requireConnectionDelegates(t *testing.T, service ports.BrokerSession) {
	t.Helper()
	require.NoError(t, connectionDelegates(context.Background(), service))
}

// connectionDelegates is the error-returning form of requireConnectionDelegates,
// safe to call from a test goroutine where FailNow is not.
func connectionDelegates(ctx context.Context, service ports.BrokerSession) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	registration, err := service.AddHost(ctx, membershipProbeEndpoint, testPolicy())
	if err != nil {
		return err
	}
	if registration.Endpoint != membershipProbeEndpoint {
		return fmt.Errorf("AddHost answered registration for %q, want %q", registration.Endpoint, membershipProbeEndpoint)
	}
	return nil
}

// recvWithin reads one server message under an explicit bound, so a session that
// never answers settles as a bounded test failure instead of hanging the package
// on a read that can never complete.
func (r *rawCarriage) recvWithin(t *testing.T, timeout time.Duration) brokerwire.ServerMessage {
	t.Helper()
	type result struct {
		message brokerwire.ServerMessage
		err     error
	}
	done := make(chan result, 1)
	go func() {
		envelope, err := r.transport.RecvBounded(r.ceilings.MaxReceiveEnvelopeBytes)
		if err != nil {
			done <- result{nil, err}
			return
		}
		message, err := brokerwire.DecodeServer(envelope.Payload, r.ceilings.MaxReceiveEnvelopeBytes, r.ceilings.StreamChunkLimit)
		done <- result{message, err}
	}()
	select {
	case got := <-done:
		require.NoError(t, got.err, "the broker must answer with a decodable server frame")
		return got.message
	case <-time.After(timeout):
		t.Fatal("the broker did not answer within the bound")
		return nil
	}
}

// awaitReadError proves one raw read fails under an explicit bound, so a session
// that never settles is a bounded test failure rather than a hang.
func (r *rawCarriage) awaitReadError(t *testing.T, timeout time.Duration) error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := r.transport.RecvBounded(r.ceilings.MaxReceiveEnvelopeBytes)
		done <- err
	}()
	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		t.Fatal("the connection must settle within the bound")
		return nil
	}
}

// testDaemonAt builds one catalogue-valid remote daemon projection for index i.
func testDaemonAt(i int) ports.BrokerDaemonObservation {
	endpoint := fmt.Sprintf("user%d@host%d:22", i, i)
	return ports.BrokerDaemonObservation{
		Endpoint:       endpoint,
		DisplayOrigin:  endpoint,
		Registration:   domain.RemoteRegistration{Endpoint: endpoint, Incarnation: [16]byte{0x10, byte(i + 1), 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xf0, 0x11}, Generation: domain.RemoteGeneration(i + 1)},
		Policy:         testPolicy(),
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
	daemon := testDaemonAt(0)
	daemon.Sessions = []catalogue.RemoteCatalogSession{testSessionAt(0, 0), testSessionAt(0, 1)}
	return ports.BrokerSnapshot{Epoch: epoch, Revision: revision, Daemons: []ports.BrokerDaemonObservation{daemon}}
}

// testLocalSnapshot builds one valid publication whose first daemon is local:
// the local daemon with one session followed by one remote daemon with one.
func testLocalSnapshot(epoch ports.BrokerEpoch, revision ports.BrokerRevision) ports.BrokerSnapshot {
	local := ports.BrokerDaemonObservation{
		Local:          true,
		DisplayOrigin:  "local",
		Policy:         testPolicy(),
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{testSessionAt(0, 0)},
	}
	remote := testDaemonAt(0)
	remote.Sessions = []catalogue.RemoteCatalogSession{testSessionAt(1, 0)}
	return ports.BrokerSnapshot{Epoch: epoch, Revision: revision, Daemons: []ports.BrokerDaemonObservation{local, remote}}
}

// snapshotHub is a multi-subscriber publication hub standing in for the broker
// registry: one immutable current snapshot, coalescing capacity-one subscribers.
// It is plain state and implements no interface.
type snapshotHub struct {
	mu       sync.Mutex
	snapshot ports.BrokerSnapshot
	subs     map[*subscriptionState]struct{}
}

func newSnapshotHub(snapshot ports.BrokerSnapshot) *snapshotHub {
	return &snapshotHub{snapshot: snapshot, subs: make(map[*subscriptionState]struct{})}
}

func (h *snapshotHub) current() ports.BrokerSnapshot {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.snapshot.Clone()
}

// subscribe registers one coalescing capacity-one subscription and returns its
// mock.
func (h *snapshotHub) subscribe(t *testing.T) *portsmocks.MockBrokerSubscription {
	m, state := newSubscriptionMock(t, h)
	_ = state
	return m
}

func (h *snapshotHub) publish(snapshot ports.BrokerSnapshot) {
	h.mu.Lock()
	h.snapshot = snapshot.Clone()
	subs := make([]*subscriptionState, 0, len(h.subs))
	for sub := range h.subs {
		subs = append(subs, sub)
	}
	h.mu.Unlock()
	for _, sub := range subs {
		sub.notify()
	}
}

func (h *snapshotHub) remove(sub *subscriptionState) {
	h.mu.Lock()
	delete(h.subs, sub)
	h.mu.Unlock()
}

// subscriptionState is the plain state behind one hub subscription.
type subscriptionState struct {
	hub     *snapshotHub
	changed chan struct{}
	once    sync.Once
	mu      sync.Mutex
	closed  bool
}

// newSubscriptionMock builds one coalescing capacity-one subscription on hub.
func newSubscriptionMock(t *testing.T, hub *snapshotHub, opts ...func(*portsmocks.MockBrokerSubscription, *subscriptionState)) (*portsmocks.MockBrokerSubscription, *subscriptionState) {
	t.Helper()
	state := &subscriptionState{hub: hub, changed: make(chan struct{}, 1)}
	m := portsmocks.NewMockBrokerSubscription(t)
	for _, opt := range opts {
		opt(m, state)
	}
	m.EXPECT().Changed().RunAndReturn(func() <-chan struct{} { return state.changed }).Maybe()
	m.EXPECT().Close().Run(func() {
		state.once.Do(func() {
			state.mu.Lock()
			state.closed = true
			state.mu.Unlock()
			hub.remove(state)
		})
	}).Maybe()
	hub.mu.Lock()
	hub.subs[state] = struct{}{}
	hub.mu.Unlock()
	return m, state
}

func (s *subscriptionState) notify() {
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

// newPreviewSubscriptionMock builds one server-side preview subscription whose
// Changed channel a test could drive explicitly; it never publishes by default.
func newPreviewSubscriptionMock(t *testing.T) *portsmocks.MockBrokerPreviewSubscription {
	t.Helper()
	changed := make(chan struct{}, 1)
	m := portsmocks.NewMockBrokerPreviewSubscription(t)
	m.EXPECT().Changed().RunAndReturn(func() <-chan struct{} { return changed }).Maybe()
	m.EXPECT().Latest().RunAndReturn(func() ports.BrokerPreviewPublication { return ports.BrokerPreviewPublication{} }).Maybe()
	m.EXPECT().Close().Return().Maybe()
	return m
}

// logicalState is the plain state behind one admitted core logical connection:
// it records messages the broker relays toward the daemon and lets a test inject
// daemon replies.
type logicalState struct {
	fromClient chan protocol.ClientMessage
	toClient   chan protocol.ServerMessage
	// preamble carries the canned session preamble acceptance: the raw relay
	// forwards the client's preamble request like any other envelope.
	preamble chan []byte
	done     chan struct{}
	once     sync.Once
	mu       sync.Mutex
	err      error
	closed   bool
}

// newLogicalStreamMock builds one admitted core logical connection.
func newLogicalStreamMock(t *testing.T, opts ...func(*portsmocks.MockBrokerEnvelopeStream, *logicalState)) (*portsmocks.MockBrokerEnvelopeStream, *logicalState) {
	t.Helper()
	c := &logicalState{
		fromClient: make(chan protocol.ClientMessage, 64),
		toClient:   make(chan protocol.ServerMessage, 64),
		preamble:   make(chan []byte, 1),
		done:       make(chan struct{}),
	}
	m := portsmocks.NewMockBrokerEnvelopeStream(t)
	for _, opt := range opts {
		opt(m, c)
	}
	m.EXPECT().SendEnvelope(mock.Anything).RunAndReturn(func(payload []byte) error {
		if request, err := sessionwire.EncodePreambleRequestForTest(); err == nil && bytes.Equal(payload, request) {
			response, err := sessionwire.EncodePreambleResponseForTest()
			if err != nil {
				return err
			}
			c.preamble <- response
			return nil
		}
		message, err := sessionwire.DecodeClientEnvelope(payload)
		if err != nil {
			return err
		}
		select {
		case c.fromClient <- message:
			return nil
		case <-c.done:
			return ports.BrokerAdmissionClosed
		}
	}).Maybe()
	m.EXPECT().RecvEnvelope().RunAndReturn(func() ([]byte, error) {
		// A message already queued when Close ran must still be observed first: a
		// real carriage never reports the connection done before a message the
		// peer sent ahead of an orderly close has been delivered. Draining
		// non-blocking before the select keeps that ordering deterministic.
		select {
		case response := <-c.preamble:
			return response, nil
		case message := <-c.toClient:
			return sessionwire.EncodeServerMessage(message)
		default:
		}
		select {
		case response := <-c.preamble:
			return response, nil
		case message := <-c.toClient:
			return sessionwire.EncodeServerMessage(message)
		case <-c.done:
			if err := c.Err(); err != nil {
				return nil, err
			}
			return nil, io.EOF
		}
	}).Maybe()
	m.EXPECT().Done().RunAndReturn(func() <-chan struct{} { return c.done }).Maybe()
	m.EXPECT().Err().RunAndReturn(c.Err).Maybe()
	m.EXPECT().Close().RunAndReturn(c.Close).Maybe()
	return m, c
}

func (c *logicalState) Err() error { c.mu.Lock(); defer c.mu.Unlock(); return c.err }

func (c *logicalState) Close() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.mu.Unlock()
		close(c.done)
	})
	return nil
}

// fail publishes one terminal core failure.
func (c *logicalState) fail(err error) {
	c.mu.Lock()
	c.err = err
	c.mu.Unlock()
	_ = c.Close()
}

// addHostCall records one delegated membership addition: the exact arguments
// the admitted core received and the exact result it returned. Recording both
// is what lets a test prove the adapter delegated once with the caller's own
// authority instead of re-deriving a policy or registration on the way.
type addHostCall struct {
	Endpoint string
	Policy   ports.BrokerPolicy
	Result   domain.RemoteRegistration
	Err      error
}

// removeHostCall records one delegated exact-registration removal.
type removeHostCall struct {
	Expected domain.RemoteRegistration
	Removed  bool
	Err      error
}

// updateHostPolicyCall records one delegated exact-registration policy update.
type updateHostPolicyCall struct {
	Expected domain.RemoteRegistration
	Policy   ports.BrokerPolicy
	Result   domain.RemoteRegistration
	Err      error
}

// coreState is the plain state behind one admitted per-connection broker
// service. It implements no interface; the core mock's closures capture it.
type coreState struct {
	t         *testing.T
	mu        sync.Mutex
	id        ports.BrokerConnectionID
	hub       *snapshotHub
	streams   map[ports.BrokerStreamID]*logicalState
	opens     []ports.BrokerOpenStreamRequest
	closedIDs []ports.BrokerStreamID
	added     []addHostCall
	removed   []removeHostCall
	updated   []updateHostPolicyCall
	reconcile []string
	openErr   error
	// subscribes records each core subscription's reader kind (true = passive).
	subscribes []bool
	closed     bool
	done       chan struct{}
	once       sync.Once

	// Membership result hooks. Their zero values keep the deterministic
	// defaults that mirror the real registry: a fresh generation-1 registration
	// for a new endpoint, a generation advanced by exactly one for a policy
	// update, and removal reported from removeRemoved.
	addErr        error
	removeErr     error
	updateErr     error
	addResult     *domain.RemoteRegistration
	updateResult  *domain.RemoteRegistration
	removeRemoved bool

	nextStream ports.BrokerStreamID
}

// passiveCore is the admitted core mock plus the optional passive-reader
// capability the server probes for with a type assertion. The generated
// BrokerCoreService mock cannot carry it because it is not part of that port, so
// the one extra method delegates straight to the shared state.
type passiveCore struct {
	*portsmocks.MockBrokerCoreService
	state *coreState
}

func (c passiveCore) SubscribePassive() (ports.BrokerSubscription, error) {
	return c.state.recordSubscribe(true), nil
}

// newCoreMock builds one admitted per-connection broker service on hub. Options
// register overrides before the defaults.
func newCoreMock(t *testing.T, id ports.BrokerConnectionID, hub *snapshotHub, opts ...func(*portsmocks.MockBrokerCoreService, *coreState)) (ports.BrokerCoreService, *coreState) {
	t.Helper()
	c := &coreState{
		t:       t,
		id:      id,
		hub:     hub,
		streams: make(map[ports.BrokerStreamID]*logicalState),
		done:    make(chan struct{}),
	}
	m := portsmocks.NewMockBrokerCoreService(t)
	for _, opt := range opts {
		opt(m, c)
	}
	m.EXPECT().ConnectionID().RunAndReturn(func() ports.BrokerConnectionID { return c.id }).Maybe()
	m.EXPECT().NextStreamID().RunAndReturn(func() (ports.BrokerStreamID, error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.nextStream++
		return c.nextStream, nil
	}).Maybe()
	m.EXPECT().Done().RunAndReturn(func() <-chan struct{} { return c.done }).Maybe()
	m.EXPECT().Err().Return(nil).Maybe()
	m.EXPECT().Snapshot().RunAndReturn(func() ports.BrokerSnapshot { return c.hub.current() }).Maybe()
	m.EXPECT().Subscribe().RunAndReturn(func() (ports.BrokerSubscription, error) {
		return c.recordSubscribe(false), nil
	}).Maybe()
	m.EXPECT().SubscribePreview(mock.Anything).RunAndReturn(func(ports.BrokerPreviewRequest) (ports.BrokerPreviewSubscription, error) {
		return newPreviewSubscriptionMock(t), nil
	}).Maybe()
	m.EXPECT().OpenEnvelopeStream(mock.Anything, mock.Anything).RunAndReturn(c.openStream).Maybe()
	m.EXPECT().CloseStream(mock.Anything, mock.Anything).RunAndReturn(c.closeStream).Maybe()
	m.EXPECT().AddHost(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(c.addHost).Maybe()
	m.EXPECT().RemoveHost(mock.Anything, mock.Anything).RunAndReturn(c.removeHost).Maybe()
	m.EXPECT().UpdateHostPolicy(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(c.updateHost).Maybe()
	m.EXPECT().RequestReconcile(mock.Anything).Run(func(endpoint string) {
		c.mu.Lock()
		c.reconcile = append(c.reconcile, endpoint)
		c.mu.Unlock()
	}).Maybe()
	m.EXPECT().Close().RunAndReturn(c.closeAll).Maybe()
	return passiveCore{MockBrokerCoreService: m, state: c}, c
}

func (c *coreState) recordSubscribe(passive bool) ports.BrokerSubscription {
	c.mu.Lock()
	c.subscribes = append(c.subscribes, passive)
	c.mu.Unlock()
	return c.hub.subscribe(c.t)
}

func (c *coreState) subscribeKinds() []bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]bool(nil), c.subscribes...)
}

func (c *coreState) openStream(_ context.Context, request ports.BrokerOpenStreamRequest) (ports.BrokerEnvelopeStream, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.openErr != nil {
		return nil, c.openErr
	}
	if c.closed {
		return nil, ports.BrokerAdmissionClosed
	}
	m, conn := newLogicalStreamMock(c.t)
	c.streams[request.Stream] = conn
	c.opens = append(c.opens, request)
	return m, nil
}

func (c *coreState) closeStream(connection ports.BrokerConnectionID, stream ports.BrokerStreamID) error {
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

// mapMembershipError mirrors the admitted core's own mapping of a registry
// refusal onto the closed broker error taxonomy. The real broker.Service already
// returns these typed codes, so the mock applies the same rule: a caller of the
// adapter must never see a bare registry sentinel as if it were an untyped
// failure.
func mapMembershipError(err error) error {
	if err == nil {
		return nil
	}
	code := ports.BrokerErrorCode(0)
	switch {
	case errors.Is(err, ports.ErrBrokerHostConflict):
		code = ports.BrokerErrorHostConflict
	case errors.Is(err, ports.ErrBrokerMembershipImmutable):
		code = ports.BrokerErrorMembershipImmutable
	default:
		var unknown ports.BrokerStoreOutcomeUnknownError
		var unknownPointer *ports.BrokerStoreOutcomeUnknownError
		if errors.As(err, &unknown) || errors.As(err, &unknownPointer) {
			code = ports.BrokerErrorOutcomeUnknown
		}
	}
	if code == 0 {
		return err
	}
	return ports.BrokerError{Code: code, Cause: err}
}

// addHost records one delegated membership addition and returns its exact
// result. The default is a fresh generation-1 registration for the requested
// endpoint, exactly as the real registry mints one for a new endpoint.
func (c *coreState) addHost(ctx context.Context, endpoint string, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	if err := ctx.Err(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	registration := domain.RemoteRegistration{}
	var err error
	switch {
	case c.addErr != nil:
		err = mapMembershipError(c.addErr)
		c.addErr = nil
	case c.addResult != nil:
		registration = *c.addResult
	default:
		registration, err = domain.NewRemoteRegistration(endpoint, [16]byte{0x01})
	}
	c.added = append(c.added, addHostCall{Endpoint: endpoint, Policy: policy, Result: registration, Err: err})
	return registration, err
}

// removeHost records one delegated exact-registration removal and reports
// whether the host is present. The state owns no membership, so the answer is
// the configured removeRemoved value for every exact registration.
func (c *coreState) removeHost(ctx context.Context, expected domain.RemoteRegistration) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	removed := false
	var err error
	if c.removeErr != nil {
		err = mapMembershipError(c.removeErr)
		c.removeErr = nil
	} else {
		removed = c.removeRemoved
	}
	c.removed = append(c.removed, removeHostCall{Expected: expected, Removed: removed, Err: err})
	return removed, err
}

// updateHost records one delegated exact-registration policy update. The
// default advances the generation by exactly one, mirroring the real registry's
// replacement-policy path, and preserves the expected incarnation.
func (c *coreState) updateHost(ctx context.Context, expected domain.RemoteRegistration, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	if err := ctx.Err(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	registration := domain.RemoteRegistration{}
	var err error
	switch {
	case c.updateErr != nil:
		err = mapMembershipError(c.updateErr)
		c.updateErr = nil
	case c.updateResult != nil:
		registration = *c.updateResult
	default:
		registration = expected
		if registration.Generation != ^domain.RemoteGeneration(0) {
			registration.Generation++
		}
	}
	c.updated = append(c.updated, updateHostPolicyCall{Expected: expected, Policy: policy, Result: registration, Err: err})
	return registration, err
}

// addCalls snapshots the delegated additions.
func (c *coreState) addCalls() []addHostCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]addHostCall(nil), c.added...)
}

// removeCalls snapshots the delegated removals.
func (c *coreState) removeCalls() []removeHostCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]removeHostCall(nil), c.removed...)
}

// updateCalls snapshots the delegated policy updates.
func (c *coreState) updateCalls() []updateHostPolicyCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]updateHostPolicyCall(nil), c.updated...)
}

// refuseAddHost makes the next delegated addition fail with err. The refusal is
// one-shot, so a later call is free to succeed rather than pin the whole
// connection as immutable forever.
func (c *coreState) refuseAddHost(err error) { c.mu.Lock(); c.addErr = err; c.mu.Unlock() }

// refuseRemoveHost makes the next delegated removal fail with err, one-shot.
func (c *coreState) refuseRemoveHost(err error) { c.mu.Lock(); c.removeErr = err; c.mu.Unlock() }

// reportRemoved configures the answer a delegated removal returns.
func (c *coreState) reportRemoved(removed bool) {
	c.mu.Lock()
	c.removeRemoved = removed
	c.mu.Unlock()
}

// zeroAddRegistration makes a delegated addition succeed without returning any
// registration authority, so a test can prove the server never publishes an
// addition success that cannot be fenced.
func (c *coreState) zeroAddRegistration() {
	c.mu.Lock()
	zero := domain.RemoteRegistration{}
	c.addResult = &zero
	c.mu.Unlock()
}

func (c *coreState) closeAll() error {
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		conns := make([]*logicalState, 0, len(c.streams))
		for _, conn := range c.streams {
			conns = append(conns, conn)
		}
		c.streams = make(map[ports.BrokerStreamID]*logicalState)
		c.mu.Unlock()
		for _, conn := range conns {
			_ = conn.Close()
		}
		close(c.done)
	})
	return nil
}

// logicalConn returns one admitted core connection by stream identity.
func (c *coreState) logicalConn(stream ports.BrokerStreamID) *logicalState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.streams[stream]
}

// newTestCore builds one admitted core service for a session test: a fresh hub
// and a connection identity bound to epoch.
func newTestCore(t *testing.T, epoch ports.BrokerEpoch) (ports.BrokerCoreService, *coreState) {
	t.Helper()
	var id ports.BrokerConnectionID
	binary.BigEndian.PutUint64(id[:8], uint64(epoch))
	id[15] = 1
	return newCoreMock(t, id, newSnapshotHub(ports.BrokerSnapshot{}))
}

// authorityState is the plain state behind one listener's authority: it hands
// out per-connection core services over one shared hub.
type authorityState struct {
	mu       sync.Mutex
	hub      *snapshotHub
	epoch    ports.BrokerEpoch
	next     uint64
	clients  []*coreState
	admitErr error
}

// newAuthorityMock builds an authority that admits clients for one listener.
// Options register overrides before the defaults.
func newAuthorityMock(t *testing.T, epoch ports.BrokerEpoch, snapshot ports.BrokerSnapshot, opts ...func(*portsmocks.MockBrokerAuthority, *authorityState)) (*portsmocks.MockBrokerAuthority, *authorityState) {
	t.Helper()
	a := &authorityState{hub: newSnapshotHub(snapshot), epoch: epoch}
	m := portsmocks.NewMockBrokerAuthority(t)
	for _, opt := range opts {
		opt(m, a)
	}
	m.EXPECT().AdmitClient(mock.Anything).RunAndReturn(func(ctx context.Context) (ports.BrokerCoreService, error) {
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
		core, state := newCoreMock(t, id, a.hub)
		a.clients = append(a.clients, state)
		return core, nil
	}).Maybe()
	return m, a
}

func (a *authorityState) last() *coreState {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.clients) == 0 {
		return nil
	}
	return a.clients[len(a.clients)-1]
}

func (a *authorityState) publish(snapshot ports.BrokerSnapshot) { a.hub.publish(snapshot) }

// endpoint is one running broker IPC listener with its authority.
type endpoint struct {
	t         *testing.T
	path      string
	listener  ports.BrokerListener
	authority *authorityState
	epoch     ports.BrokerEpoch
}

func startEndpoint(t *testing.T, cfg Config) *endpoint {
	t.Helper()
	epoch := ports.BrokerEpoch(0x51)
	authorityMock, authority := newAuthorityMock(t, epoch, ports.BrokerSnapshot{})
	listener, err := Listen(testSocketPath(t), epoch, authorityMock, cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = listener.Close() })
	return &endpoint{t: t, path: listener.Addr(), listener: listener, authority: authority, epoch: epoch}
}

// accept returns the next admitted session.
func (e *endpoint) accept() ports.BrokerCoreService {
	e.t.Helper()
	type result struct {
		service ports.BrokerCoreService
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
func (e *endpoint) pair() (ports.BrokerService, ports.BrokerCoreService) {
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

// recordingState is the plain state behind one recording server carriage: the
// complete snapshot transfers the session wrote, keyed by generation and
// revision. It implements no interface; the mock closures capture it.
type recordingState struct {
	mu      sync.Mutex
	ends    map[brokerwire.SubscriptionGeneration]map[ports.BrokerRevision]int
	closedC chan struct{}
	once    sync.Once
}

// newRecordingTransportMock builds one in-memory server carriage for a session.
// It decodes and records every complete snapshot transfer the session writes and
// never yields an inbound frame, so only the code under test drives publication.
// Options register overrides before the defaults.
func newRecordingTransportMock(t *testing.T, opts ...func(*wiremocks.MockBoundedTransport, *recordingState)) (*wiremocks.MockBoundedTransport, *recordingState) {
	t.Helper()
	state := &recordingState{
		ends:    make(map[brokerwire.SubscriptionGeneration]map[ports.BrokerRevision]int),
		closedC: make(chan struct{}),
	}
	m := wiremocks.NewMockBoundedTransport(t)
	for _, opt := range opts {
		opt(m, state)
	}
	m.EXPECT().Send(mock.Anything).RunAndReturn(func(envelope wire.Envelope) error {
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
		state.mu.Lock()
		if state.ends[part.Generation] == nil {
			state.ends[part.Generation] = make(map[ports.BrokerRevision]int)
		}
		state.ends[part.Generation][part.Revision]++
		state.mu.Unlock()
		return nil
	}).Maybe()
	m.EXPECT().RecvBounded(mock.Anything).RunAndReturn(func(uint64) (wire.Envelope, error) {
		<-state.closedC
		return wire.Envelope{}, io.EOF
	}).Maybe()
	m.EXPECT().Recv().RunAndReturn(func() (wire.Envelope, error) {
		<-state.closedC
		return wire.Envelope{}, io.EOF
	}).Maybe()
	m.EXPECT().Close().RunAndReturn(func() error {
		state.once.Do(func() { close(state.closedC) })
		return nil
	}).Maybe()
	return m, state
}

// transferCount reports how many complete transfers of one generation committed
// one revision.
func (s *recordingState) transferCount(generation brokerwire.SubscriptionGeneration, revision ports.BrokerRevision) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.ends[generation][revision]
}
