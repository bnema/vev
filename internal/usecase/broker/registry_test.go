package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
	"github.com/stretchr/testify/require"
)

// probeGrace bounds a negative scheduling assertion: a probe may not start
// inside this window, so the test observes "nothing happened" without
// sleeping on the behavior under test.
const probeGrace = 25 * time.Millisecond

// manualClock is a deterministic ports.Clock/ports.Timer pair: time only moves
// when Advance is called, and timers with equal deadlines fire in creation
// order. Scheduling assertions are therefore exact instead of racing wall time.
type manualClock struct {
	mu     sync.Mutex
	now    time.Time
	order  uint64
	timers map[*manualTimer]struct{}
}

func newManualClock(start time.Time) *manualClock {
	return &manualClock{now: start, timers: make(map[*manualTimer]struct{})}
}

func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *manualClock) NewTimer(d time.Duration) ports.Timer {
	timer := &manualTimer{clock: c, ch: make(chan time.Time, 1)}
	c.mu.Lock()
	defer c.mu.Unlock()
	timer.arm(d)
	return timer
}

// Advance moves time forward and fires every timer that came due.
func (c *manualClock) Advance(d time.Duration) {
	if d < 0 {
		panic("broker: manualClock.Advance requires a non-negative duration")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	due := make([]*manualTimer, 0, len(c.timers))
	for timer := range c.timers {
		if !timer.when.After(c.now) {
			due = append(due, timer)
		}
	}
	sort.Slice(due, func(i, j int) bool {
		if due[i].when.Equal(due[j].when) {
			return due[i].order < due[j].order
		}
		return due[i].when.Before(due[j].when)
	})
	for _, timer := range due {
		timer.active = false
		delete(c.timers, timer)
		select {
		case timer.ch <- c.now:
		default:
		}
	}
}

type manualTimer struct {
	clock  *manualClock
	ch     chan time.Time
	when   time.Time
	order  uint64
	active bool
}

func (t *manualTimer) C() <-chan time.Time { return t.ch }

func (t *manualTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if !t.active {
		return false
	}
	t.active = false
	delete(t.clock.timers, t)
	return true
}

func (t *manualTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	wasActive := t.active
	if t.active {
		t.active = false
		delete(t.clock.timers, t)
	}
	select {
	case <-t.ch:
	default:
	}
	t.arm(d)
	return wasActive
}

// arm schedules the timer relative to the clock's current time. The caller
// must hold clock.mu.
func (t *manualTimer) arm(d time.Duration) {
	t.when = t.clock.now.Add(d)
	t.order = t.clock.order
	t.clock.order++
	if d <= 0 {
		t.active = false
		select {
		case t.ch <- t.clock.now:
		default:
		}
		return
	}
	t.active = true
	t.clock.timers[t] = struct{}{}
}

// identityJitter removes scheduling jitter so a test can assert exact
// deadlines; the production jitter is covered separately.
func identityJitter(d time.Duration, _ string, _ uint64) time.Duration { return d }

// syncBuffer captures log output written from the registry's persistence
// goroutine as well as the test goroutine.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (w *syncBuffer) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

func (w *syncBuffer) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.String()
}

func testLogger() (*slog.Logger, *syncBuffer) {
	logs := &syncBuffer{}
	return slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelDebug})), logs
}

// testStore is a race-safe in-memory durable store whose writes signal a
// capacity-one channel so tests wait on a write instead of on wall time. It
// implements the whole BrokerHostStore seam: authority defaults to the hosts of
// the restored snapshot, so a fixture that only names a durable snapshot still
// keeps exercising the rule that observations intersect authoritative
// membership.
type testStore struct {
	mu       sync.Mutex
	loaded   ports.BrokerSnapshot
	loadErr  error
	hosts    ports.BrokerHosts
	hostsErr error
	stored   ports.BrokerSnapshot
	storeErr error
	writes   int
	changes  chan struct{}
}

func (s *testStore) Load() (ports.BrokerSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.loadErr != nil {
		return ports.BrokerSnapshot{}, s.loadErr
	}
	return s.loaded.Clone(), nil
}

func (s *testStore) LoadHosts() (ports.BrokerHosts, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hostsErr != nil {
		return ports.BrokerHosts{}, s.hostsErr
	}
	if s.hosts.Revision != 0 || len(s.hosts.Hosts) > 0 {
		return s.hosts, nil
	}
	authority := ports.BrokerHosts{Revision: 1}
	for _, host := range s.loaded.Daemons {
		authority.Hosts = append(authority.Hosts, ports.BrokerHostRecord{Registration: host.Registration, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), host.Registration.Endpoint)})
	}
	return authority, nil
}

func (s *testStore) ReplaceHosts(expected uint64, hosts []ports.BrokerHostRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hostsErr != nil {
		return s.hostsErr
	}
	if s.hosts.Revision == 0 {
		s.hosts.Revision = 1
	}
	if expected != s.hosts.Revision {
		return ports.ErrBrokerHostConflict
	}
	s.hosts.Hosts = append([]ports.BrokerHostRecord(nil), hosts...)
	s.hosts.Revision++
	return nil
}

func (s *testStore) Close() error { return nil }

func (s *testStore) Store(snapshot ports.BrokerSnapshot) error {
	s.mu.Lock()
	s.stored = snapshot.Clone()
	s.writes++
	err := s.storeErr
	s.mu.Unlock()
	s.signal()
	return err
}

func (s *testStore) storedSnapshot() ports.BrokerSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.stored.Clone()
}

func (s *testStore) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func (s *testStore) setStoreErr(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeErr = err
}

func (s *testStore) signal() {
	s.mu.Lock()
	if s.changes == nil {
		s.changes = make(chan struct{}, 1)
	}
	changes := s.changes
	s.mu.Unlock()
	select {
	case changes <- struct{}{}:
	default:
	}
}

func (s *testStore) changesChan() chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.changes == nil {
		s.changes = make(chan struct{}, 1)
	}
	return s.changes
}

// blockingStore holds the first durable write inside Store so a test can prove
// that publication never waits behind persistence.
type blockingStore struct {
	mu        sync.Mutex
	revisions []ports.BrokerRevision
	entered   chan ports.BrokerRevision
	released  chan struct{}
	first     sync.Once
}

func newBlockingStore() *blockingStore {
	return &blockingStore{entered: make(chan ports.BrokerRevision, 1), released: make(chan struct{})}
}

func (s *blockingStore) Load() (ports.BrokerSnapshot, error) { return ports.BrokerSnapshot{}, nil }

func (s *blockingStore) LoadHosts() (ports.BrokerHosts, error) {
	return ports.BrokerHosts{Revision: 1}, nil
}

func (s *blockingStore) ReplaceHosts(uint64, []ports.BrokerHostRecord) error { return nil }

func (s *blockingStore) Close() error { return nil }

func (s *blockingStore) Store(snapshot ports.BrokerSnapshot) error {
	s.mu.Lock()
	s.revisions = append(s.revisions, snapshot.Revision)
	s.mu.Unlock()
	s.first.Do(func() {
		select {
		case s.entered <- snapshot.Revision:
		default:
		}
		<-s.released
	})
	return nil
}

func (s *blockingStore) releaseFirst() { close(s.released) }

func (s *blockingStore) writtenRevisions() []ports.BrokerRevision {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]ports.BrokerRevision(nil), s.revisions...)
}

// blockingCASStore parks the first durable membership CAS until it is released,
// so a test can prove publication and Snapshot never wait behind membership
// durability. Load, LoadHosts, and Store keep the race-safe testStore behavior,
// and every later CAS is admitted without blocking.
type blockingCASStore struct {
	testStore
	entered  chan struct{}
	released chan struct{}
	block    sync.Once
	open     sync.Once
}

func newBlockingCASStore() *blockingCASStore {
	return &blockingCASStore{entered: make(chan struct{}, 1), released: make(chan struct{})}
}

func (s *blockingCASStore) ReplaceHosts(expected uint64, hosts []ports.BrokerHostRecord) error {
	s.block.Do(func() {
		s.entered <- struct{}{}
		<-s.released
	})
	return s.testStore.ReplaceHosts(expected, hosts)
}

// releaseCAS lets the parked CAS commit. It is idempotent so a failing test can
// unblock the parked mutator from cleanup.
func (s *blockingCASStore) releaseCAS() { s.open.Do(func() { close(s.released) }) }

// testProbe scripts one observation per received call: the test owns the
// completion, so a probe never depends on wall time.
type testProbe struct {
	calls    chan probeCall
	canceled chan struct{}
}

func newTestProbe(capacity int) *testProbe {
	return &testProbe{calls: make(chan probeCall, capacity), canceled: make(chan struct{}, 1)}
}

type probeCall struct {
	registration domain.RemoteRegistration
	result       chan probeAnswer
}

type probeAnswer struct {
	snapshot ports.BrokerDaemonObservation
	err      error
}

func (p *testProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
	call := probeCall{registration: registration, result: make(chan probeAnswer, 1)}
	select {
	case p.calls <- call:
	case <-ctx.Done():
		p.noteCanceled()
		return ports.BrokerDaemonObservation{}, ctx.Err()
	}
	select {
	case answer := <-call.result:
		return answer.snapshot, answer.err
	case <-ctx.Done():
		p.noteCanceled()
		return ports.BrokerDaemonObservation{}, ctx.Err()
	}
}

func (p *testProbe) noteCanceled() {
	select {
	case p.canceled <- struct{}{}:
	default:
	}
}

// churnProbe is a ctx-honoring probe used by the churn regression test. It
// tracks, per endpoint, how many probes were simultaneously live with an
// uncancelled context: an attempt that is retired without cancelling its
// context stays live and shows up as an overlap with its replacement.
type churnProbe struct {
	release    chan struct{}
	released   sync.Once
	startedSig chan struct{}

	mu       sync.Mutex
	contexts map[context.Context]string
	maxLive  map[string]int
	started  int
}

func newChurnProbe() *churnProbe {
	return &churnProbe{
		release:    make(chan struct{}),
		startedSig: make(chan struct{}, 256),
		contexts:   make(map[context.Context]string),
		maxLive:    make(map[string]int),
	}
}

func (p *churnProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.BrokerDaemonObservation, error) {
	p.mu.Lock()
	live := 0
	for other, endpoint := range p.contexts {
		if endpoint == registration.Endpoint && other.Err() == nil {
			live++
		}
	}
	// The attempt is registered even when its context is already cancelled (a
	// probe launched into a settling run), but only an uncancelled context is
	// counted live: an attempt retired without cancelling its context stays in
	// the map with Err()==nil and still overlaps its replacement.
	p.contexts[ctx] = registration.Endpoint
	if ctx.Err() == nil {
		live++
	}
	if live > p.maxLive[registration.Endpoint] {
		p.maxLive[registration.Endpoint] = live
	}
	p.started++
	p.mu.Unlock()
	select {
	case p.startedSig <- struct{}{}:
	default:
	}
	defer func() {
		p.mu.Lock()
		delete(p.contexts, ctx)
		p.mu.Unlock()
	}()
	select {
	case <-ctx.Done():
	case <-p.release:
	}
	return ports.BrokerDaemonObservation{}, ctx.Err()
}

// openRelease lets every still-blocked probe return, so a failing test never
// leaks probe goroutines.
func (p *churnProbe) openRelease() { p.released.Do(func() { close(p.release) }) }

func (p *churnProbe) startedCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.started
}

func (p *churnProbe) maxLiveFor(endpoint string) int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.maxLive[endpoint]
}

func (p *churnProbe) observedEndpoints() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	endpoints := make([]string, 0, len(p.maxLive))
	for endpoint := range p.maxLive {
		endpoints = append(endpoints, endpoint)
	}
	sort.Strings(endpoints)
	return endpoints
}

func (p *churnProbe) inflight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.contexts)
}

func registration(t *testing.T, endpoint string, marker byte) domain.RemoteRegistration {
	t.Helper()
	var incarnation [16]byte
	incarnation[0] = marker
	result, err := domain.NewRemoteRegistration(endpoint, incarnation)
	require.NoError(t, err)
	return result
}

// canonicalRoute derives the exact route production would write for one test
// policy and endpoint. Test policies only use the closed remote transport
// vocabulary, so the mapping cannot fail; a fixture that hand-builds membership
// therefore matches what AddHost or UpgradeBrokerHostRoutes would persist.
func canonicalRoute(policy ports.BrokerPolicy, endpoint string) ports.BrokerRouteSpec {
	route, err := ports.BrokerRouteForTransport(policy.Transport, endpoint)
	if err != nil {
		panic(err)
	}
	return route
}

// hostRecord builds one pinned authority record with the shared test policy and
// the canonical route production derives from it. The projection-only setHosts
// seam takes records now, so membership carries the same configured authority
// (registration, policy, and route) as production.
func hostRecord(reg domain.RemoteRegistration) ports.BrokerHostRecord {
	return ports.BrokerHostRecord{Registration: reg, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)}
}

func hostRecords(registrations ...domain.RemoteRegistration) []ports.BrokerHostRecord {
	records := make([]ports.BrokerHostRecord, 0, len(registrations))
	for _, reg := range registrations {
		records = append(records, hostRecord(reg))
	}
	return records
}

// hostSession builds one catalogue-valid session. The registry applies the
// same durable projection rules as the store before adopting an observation, so
// a fixture session must carry a non-zero lifecycle identity and a non-nil tab
// list to count as durable.
func hostSession(name string) catalogue.RemoteCatalogSession {
	sum := sha256.Sum256([]byte("lifecycle:" + name))
	lifecycle := domain.SessionLifecycleID{}
	copy(lifecycle[:], sum[:])
	return catalogue.RemoteCatalogSession{
		LifecycleID: lifecycle,
		Name:        name,
		State:       catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{},
	}
}

// hostSessions builds count minimal catalogue-valid sessions for the projection
// session bound tests.
func hostSessions(count int) []catalogue.RemoteCatalogSession {
	sessions := make([]catalogue.RemoteCatalogSession, 0, count)
	for index := 0; index < count; index++ {
		sessions = append(sessions, hostSession(fmt.Sprintf("session-%d", index)))
	}
	return sessions
}

// newTestStore returns the minimal authoritative durable store: a valid
// revision series over empty membership. NewRegistry requires a
// BrokerHostStore, so every registry fixture names this store explicitly
// instead of relying on an optional seam.
func newTestStore() *testStore { return &testStore{} }

// newTestRegistry builds a registry with exact scheduling and no logger so a
// failure never depends on jitter.
func newTestRegistry(t *testing.T, epoch ports.BrokerEpoch, store ports.BrokerHostStore, probe ports.BrokerHostProbe, clock *manualClock) *Registry {
	t.Helper()
	registry, err := NewRegistry(epoch, store, probe, clock, nil)
	require.NoError(t, err)
	registry.jitter = identityJitter
	return registry
}

func startRegistry(t *testing.T, r *Registry) context.CancelFunc {
	t.Helper()
	r.SetDemand(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done; r.SetDemand(false) })
	return cancel
}

func receiveCall(t *testing.T, probe *testProbe) probeCall {
	t.Helper()
	select {
	case call := <-probe.calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for probe")
		return probeCall{}
	}
}

func requireNoProbeWithin(t *testing.T, probe *testProbe, window time.Duration) {
	t.Helper()
	timer := time.NewTimer(window)
	defer timer.Stop()
	select {
	case call := <-probe.calls:
		t.Fatalf("unexpected probe for %q", call.registration.Endpoint)
	case <-timer.C:
	}
}

func waitSnapshot(t *testing.T, r *Registry, predicate func(ports.BrokerSnapshot) bool) ports.BrokerSnapshot {
	t.Helper()
	sub := r.Subscribe()
	defer sub.Close()
	deadline := time.After(time.Second)
	for {
		snapshot := r.Snapshot()
		if predicate(snapshot) {
			return snapshot
		}
		select {
		case <-sub.Changed():
		case <-deadline:
			t.Fatal("timed out waiting for snapshot")
		}
	}
}

func waitHost(t *testing.T, r *Registry, endpoint string, predicate func(ports.BrokerDaemonObservation) bool) ports.BrokerDaemonObservation {
	t.Helper()
	snapshot := waitSnapshot(t, r, func(snapshot ports.BrokerSnapshot) bool {
		host, ok := snapshot.Find(endpoint)
		return ok && predicate(host)
	})
	host, _ := snapshot.Find(endpoint)
	return host
}

// requireHostStableWithin asserts a stale completion has no delayed effect:
// the published host stays exactly as observed for a bounded window, and any
// intervening publication fails immediately.
func requireHostStableWithin(t *testing.T, r *Registry, endpoint string, want ports.BrokerDaemonObservation, window time.Duration) {
	t.Helper()
	sub := r.Subscribe()
	defer sub.Close()
	deadline := time.After(window)
	for {
		got, ok := r.Snapshot().Find(endpoint)
		require.True(t, ok)
		require.Equal(t, want, got)
		select {
		case <-sub.Changed():
		case <-deadline:
			return
		}
	}
}

// waitStored waits for a durable write satisfying predicate.
func waitStored(t *testing.T, store *testStore, predicate func(ports.BrokerSnapshot) bool) ports.BrokerSnapshot {
	t.Helper()
	deadline := time.After(2 * time.Second)
	for {
		if snapshot := store.storedSnapshot(); predicate(snapshot) {
			return snapshot
		}
		select {
		case <-store.changesChan():
		case <-deadline:
			t.Fatal("timed out waiting for durable snapshot")
			return ports.BrokerSnapshot{}
		}
	}
}

// waitWriterRevision waits until the serializer reports the revision durable.
func waitWriterRevision(t *testing.T, r *Registry, revision ports.BrokerRevision) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		r.store.mu.Lock()
		written := r.store.written
		r.store.mu.Unlock()
		if written >= revision {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for durable revision %d", revision)
		}
		time.Sleep(time.Millisecond)
	}
}

// waitForLog waits until captured log output contains want, so a persistence
// fault is proven observable instead of silently dropped.
func waitForLog(t *testing.T, logs *syncBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(logs.String(), want) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for log %q in %q", want, logs.String())
		}
		time.Sleep(time.Millisecond)
	}
}

// attemptFor returns the attempt token currently in flight for an endpoint,
// or zero when no attempt is admitted.
func attemptFor(r *Registry, endpoint string) uint64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if attempt := r.inflight[endpoint]; attempt != nil {
		return attempt.token
	}
	return 0
}

func TestRegistryRestoresPersistedSnapshotUnderFreshEpoch(t *testing.T) {
	reg := registration(t, "example.test", 1)
	sessions := []catalogue.RemoteCatalogSession{{
		LifecycleID: domain.SessionLifecycleID{1},
		Name:        "work",
		State:       catalogue.RemoteCatalogSessionUp,
		Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Name: "shell"}},
	}}
	stored := ports.BrokerSnapshot{
		Epoch:    7,
		Revision: 9,
		Daemons: []ports.BrokerDaemonObservation{{
			Endpoint:       reg.Endpoint,
			Registration:   reg,
			Availability:   domain.RemoteAvailabilityReachable,
			Checking:       true,
			LastSuccess:    time.Unix(50, 0),
			NextDue:        time.Unix(80, 0),
			InventoryKnown: true,
			Sessions:       sessions,
		}},
	}
	store := &testStore{loaded: stored}
	registry, err := NewRegistry(11, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
	require.NoError(t, err)

	snapshot := registry.Snapshot()
	require.Equal(t, ports.BrokerEpoch(11), snapshot.Epoch)
	// A restored broker process starts a fresh revision series: revisions
	// never compare across epochs.
	require.Equal(t, ports.BrokerRevision(1), snapshot.Revision)
	require.Len(t, snapshot.Daemons, 1)
	require.Empty(t, snapshot.Removed)

	host := snapshot.Daemons[0]
	require.True(t, host.Registration.Equal(reg))
	require.Equal(t, domain.RemoteAvailabilityReachable, host.Availability)
	require.Equal(t, time.Unix(80, 0), host.NextDue)
	require.Equal(t, sessions, host.Sessions)
	// In-flight observation state never survives a restart.
	require.False(t, host.Checking)

	// Restoring is read-only: the registry republishes under its own epoch
	// without rewriting the durable store.
	require.Equal(t, ports.BrokerRevision(0), store.storedSnapshot().Revision)
	require.Zero(t, store.writeCount())
}

func TestRegistryRestoreRejectsInvalidInputs(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	reg := registration(t, "example.test", 1)

	_, err := NewRegistry(0, newTestStore(), newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, newTestStore(), nil, clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, newTestStore(), newTestProbe(1), nil, nil)
	require.Error(t, err)

	// The store is a required dependency: a literal nil store is rejected, and
	// a typed nil behind the interface is rejected without panicking.
	_, err = NewRegistry(1, nil, newTestProbe(1), clock, nil)
	require.Error(t, err)
	var typedNilStore *testStore
	_, err = NewRegistry(1, typedNilStore, newTestProbe(1), clock, nil)
	require.Error(t, err)

	loadErr := errors.New("load failed")
	_, err = NewRegistry(1, &testStore{loadErr: loadErr}, newTestProbe(1), clock, nil)
	require.ErrorIs(t, err, loadErr)

	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 7}}, newTestProbe(1), clock, nil)
	require.Error(t, err)

	// A durable record without an epoch is inconsistent, never silently empty.
	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 0, Daemons: []ports.BrokerDaemonObservation{{Endpoint: reg.Endpoint, Registration: reg}}}}, newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 0, Removed: []ports.BrokerHostTombstone{{Endpoint: reg.Endpoint, Registration: reg, RetiredRevision: 3}}}}, newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 0, Revision: 4}}, newTestProbe(1), clock, nil)
	require.Error(t, err)

	empty, err := NewRegistry(1, &testStore{}, newTestProbe(1), clock, nil)
	require.NoError(t, err)
	require.Equal(t, ports.BrokerRevision(1), empty.Snapshot().Revision)
	require.Empty(t, empty.Snapshot().Daemons)
}

func TestRegistryRestoreRejectsPersistedSnapshotFromItsOwnEpoch(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	reg := registration(t, "example.test", 1)

	// A fresh broker process samples a fresh epoch and restarts its revision
	// series at one, so a durable record from its own epoch can never be
	// adopted: the restored series would not be monotonic and a stale record
	// would be indistinguishable from current authority.
	currentEpoch := ports.BrokerSnapshot{
		Epoch:    7,
		Revision: 9,
		Daemons: []ports.BrokerDaemonObservation{{
			Endpoint:     reg.Endpoint,
			Registration: reg,
			Availability: domain.RemoteAvailabilityReachable,
		}},
	}
	_, err := NewRegistry(7, &testStore{loaded: currentEpoch}, newTestProbe(1), clock, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "matches the new registry epoch")

	// The same rejection holds for a tombstone-only and for an empty-hosts
	// record from the current epoch.
	_, err = NewRegistry(7, &testStore{loaded: ports.BrokerSnapshot{Epoch: 7, Revision: 1, Removed: []ports.BrokerHostTombstone{{
		Endpoint: reg.Endpoint, Registration: reg, RetiredRevision: 1,
	}}}}, newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(7, &testStore{loaded: ports.BrokerSnapshot{Epoch: 7, Revision: 1}}, newTestProbe(1), clock, nil)
	require.Error(t, err)

	// A different epoch is still adopted normally.
	registry, err := NewRegistry(8, &testStore{loaded: currentEpoch}, newTestProbe(1), clock, nil)
	require.NoError(t, err)
	require.Len(t, registry.Snapshot().Daemons, 1)
}

func TestRegistrySetHostsRejectsInvalidRegistrations(t *testing.T) {
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)))

	reg := registration(t, "example.test", 1)
	require.Error(t, registry.setHosts(hostRecords(reg, reg)))
	require.Error(t, registry.setHosts(hostRecords(domain.RemoteRegistration{Endpoint: reg.Endpoint})))
	require.Empty(t, registry.Snapshot().Daemons)
}

func TestRegistrySetHostsRejectsHostLimitWithoutMutation(t *testing.T) {
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)))
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))

	oversized := make([]ports.BrokerHostRecord, 0, ports.BrokerMaxHosts+1)
	for index := 0; index <= ports.BrokerMaxHosts; index++ {
		oversized = append(oversized, hostRecord(registration(t, fmt.Sprintf("host-%d.test", index), byte(index+1))))
	}
	before := registry.Snapshot()
	require.Error(t, registry.setHosts(oversized))
	// The rejected call mutates nothing: same membership, same revision.
	require.Equal(t, before, registry.Snapshot())

	// The boundary itself is accepted.
	require.NoError(t, registry.setHosts(oversized[:ports.BrokerMaxHosts]))
	require.Len(t, registry.Snapshot().Daemons, ports.BrokerMaxHosts)
}

func TestRegistryUnchangedSetHostsIsNoOpAndReorderRepublishes(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	first := registration(t, "example.test", 1)
	second := registration(t, "other.test", 2)
	require.NoError(t, registry.setHosts(hostRecords(first, second)))
	startRegistry(t, registry)

	// Every configured host is observed exactly once.
	calls := make(map[string]probeCall, 2)
	for range 2 {
		call := receiveCall(t, probe)
		calls[call.registration.Endpoint] = call
	}
	require.Len(t, calls, 2)
	// Dispatch starts each probe while it holds the registry lock and publishes
	// the in-flight projection at the end of the same critical section, so a
	// received call is not yet a visible Checking state. Wait for the
	// publication instead of assuming it has landed.
	for endpoint := range calls {
		waitHost(t, registry, endpoint, func(host ports.BrokerDaemonObservation) bool { return host.Checking })
	}
	before := registry.Snapshot()
	for _, host := range before.Daemons {
		require.Truef(t, host.Checking, "host %q is not checking: %+v", host.Endpoint, before.Daemons)
	}

	// Identical membership in the same order never republishes and never
	// restarts an in-flight observation.
	require.NoError(t, registry.setHosts(hostRecords(first, second)))
	require.Equal(t, before, registry.Snapshot())
	requireNoProbeWithin(t, probe, probeGrace)

	// A reordering of identical records is a membership change: publication
	// order and the presentation rank stamped from it follow durable membership
	// authority, so the registry republishes both without cancelling the
	// in-flight observation.
	require.NoError(t, registry.setHosts(hostRecords(second, first)))
	reordered := registry.Snapshot()
	require.Greater(t, reordered.Revision, before.Revision)
	require.Equal(t, []string{"other.test", "example.test"}, []string{reordered.Daemons[0].Endpoint, reordered.Daemons[1].Endpoint})
	require.Equal(t, 0, reordered.Daemons[0].Rank)
	require.Equal(t, 1, reordered.Daemons[1].Rank)
	for _, host := range reordered.Daemons {
		require.True(t, host.Checking)
	}
	requireNoProbeWithin(t, probe, probeGrace)

	// Restoring the configured order restores the published order and ranks.
	require.NoError(t, registry.setHosts(hostRecords(first, second)))
	require.Equal(t, before.Daemons, registry.Snapshot().Daemons)
	requireNoProbeWithin(t, probe, probeGrace)

	// The in-flight observations still complete normally.
	for endpoint, call := range calls {
		call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
		waitHost(t, registry, endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
		})
	}
}

// TestRegistrySanitizesDerivedDisplayOrigin pins the boundary between routing
// authority and presentation: a target the routing validator accepts may carry
// bidi controls or line separators, and a hint the registry derives from it must
// never render them. The sanitized hint is published, satisfies the contract
// gate, and leaves the host observable.
func TestRegistrySanitizesDerivedDisplayOrigin(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{name: "login prefix strips the bidi rune from the origin", endpoint: "user@ho\u202est.test", want: "host.test"},
		{name: "bare target drops the bidi rune", endpoint: "ho\u202est.test", want: "host.test"},
		{name: "right-to-left mark dropped", endpoint: "user@ho\u200fst.test", want: "host.test"},
		{name: "ordinary target unchanged", endpoint: "user@host.test", want: "host.test"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clock := newManualClock(time.Unix(100, 0))
			probe := newTestProbe(1)
			registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
			require.NoError(t, registry.setHosts(hostRecords(registration(t, tc.endpoint, 1))))

			host := registry.Snapshot().Daemons[0]
			require.Equal(t, tc.endpoint, host.Endpoint, "the routing target stays authoritative")
			require.Equal(t, tc.want, host.DisplayOrigin)
			require.NoError(t, host.Validate())
			require.NoError(t, registry.Snapshot().Validate())

			// Sanitizing the hint never makes the host unobservable.
			startRegistry(t, registry)
			call := receiveCall(t, probe)
			call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
			waitHost(t, registry, tc.endpoint, func(host ports.BrokerDaemonObservation) bool {
				return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
			})
		})
	}
}

// TestRegistryRefusesUnpublishableMembership pins the fail-closed membership
// boundary: a target whose derived hint holds only refused runes has no
// displayable origin, and membership that cannot be published is refused where
// authority is owned instead of freezing every later publication.
func TestRegistryRefusesUnpublishableMembership(t *testing.T) {
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)))
	err := registry.setHosts(hostRecords(registration(t, "user@\u202e", 1)))
	require.ErrorContains(t, err, "not publishable")
	require.Empty(t, registry.Snapshot().Daemons)
}

// TestRegistryRefusesInvalidPublication pins the last-resort publication guard:
// a projection that reached the live set unpublished is never published, because
// a snapshot the wire refuses aborts every client connection on subscribe, and a
// refused publication never advances the revision series.
func TestRegistryRefusesInvalidPublication(t *testing.T) {
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)))
	require.NoError(t, registry.setHosts(hostRecords(registration(t, "example.test", 1))))
	before := registry.Snapshot()

	// Inject behind the membership boundary, which now refuses such a record.
	registry.mu.Lock()
	broken := before.Daemons[0].Clone()
	broken.DisplayOrigin = ""
	registry.hosts[broken.Endpoint] = broken
	registry.publishLocked(false)
	registry.mu.Unlock()

	require.Equal(t, before, registry.Snapshot())
}

func TestRegistrySnapshotIsImmutable(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions: []catalogue.RemoteCatalogSession{{
			LifecycleID: domain.SessionLifecycleID{1},
			Name:        "work",
			State:       catalogue.RemoteCatalogSessionUp,
			Tabs:        []catalogue.RemoteCatalogTab{{ID: "tab-1", Name: "shell"}},
		}},
	}}
	observed := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && len(host.Sessions) == 1
	})
	require.Equal(t, "work", observed.Sessions[0].Name)

	// Mutating a returned projection never reaches the registry.
	observed.Endpoint = "mutated"
	observed.Sessions[0].Name = "mutated"
	observed.Sessions[0].Tabs[0].Name = "mutated"
	current := registry.Snapshot()
	require.Equal(t, reg.Endpoint, current.Daemons[0].Endpoint)
	require.Equal(t, "work", current.Daemons[0].Sessions[0].Name)
	require.Equal(t, "shell", current.Daemons[0].Sessions[0].Tabs[0].Name)

	// A later publication never mutates a snapshot already handed out.
	earlier := registry.Snapshot()
	registry.RequestProbe(reg.Endpoint)
	next := receiveCall(t, probe)
	next.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{hostSession("other")},
	}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && len(host.Sessions) == 1 && host.Sessions[0].Name == "other"
	})
	require.Equal(t, "work", earlier.Daemons[0].Sessions[0].Name)
}

func TestRegistrySchedulesFreshObservationAfterSuccess(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}

	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, start, host.LastAttempt)
	require.Equal(t, start, host.LastSuccess)
	require.Equal(t, start.Add(defaultDemandFreshForRemote), host.NextDue)
	require.Zero(t, host.ConsecutiveFailures)
	require.Zero(t, host.FailureEpisode)

	// A fresh host is not observed again before its scheduled deadline.
	clock.Advance(defaultDemandFreshForRemote - time.Nanosecond)
	requireNoProbeWithin(t, probe, probeGrace)
	clock.Advance(time.Nanosecond)
	next := receiveCall(t, probe)
	require.True(t, next.registration.Equal(reg))
}

// TestRegistryAppliesSubscriptionDemandFreshness checks that apply/applyLocal
// schedule NextDue on RegistryConfig's demand cadence while Registry.SetDemand
// reports a live client subscription. It exercises both the remote and the local
// observation paths on the same registry, since service.go's Subscribe/Close
// wiring feeds one shared demand counter for both.
func TestRegistryAppliesSubscriptionDemandFreshness(t *testing.T) {
	const (
		demandLocal  = 250 * time.Millisecond
		demandRemote = 500 * time.Millisecond
	)
	tests := []struct {
		name      string
		local     bool
		demand    bool
		wantAfter time.Duration
	}{
		{name: "remote demand uses faster cadence", local: false, demand: true, wantAfter: demandRemote},
		{name: "local demand uses faster cadence", local: true, demand: true, wantAfter: demandLocal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			clock := newManualClock(start)
			remoteProbe := newTestProbe(1)
			localProbe := newScriptedLocalProbe(1)
			registry, err := NewRegistryWithConfig(1, newTestStore(), remoteProbe, clock, nil, RegistryConfig{
				Local:                &LocalObservation{DisplayOrigin: "local", Policy: poolPolicy(), Probe: localProbe},
				DemandFreshForLocal:  demandLocal,
				DemandFreshForRemote: demandRemote,
			})
			require.NoError(t, err)
			registry.jitter = identityJitter
			reg := registration(t, "example.test", 1)
			require.NoError(t, registry.setHosts(hostRecords(reg)))
			startRegistry(t, registry)

			if tt.demand {
				registry.SetDemand(true)
				t.Cleanup(func() { registry.SetDemand(false) })
			}

			if tt.local {
				call := receiveLocalCall(t, localProbe)
				call.result <- localProbeAnswer{snapshot: ports.BrokerDaemonObservation{
					Identity:        "local-daemon",
					Incarnation:     ports.BrokerDaemonIncarnation{9},
					ProtocolVersion: protocol.Version,
					Availability:    domain.RemoteAvailabilityReachable,
				}}
				host := waitLocal(t, registry, func(o ports.BrokerDaemonObservation) bool {
					return !o.Checking && o.Availability == domain.RemoteAvailabilityReachable
				})
				require.Equal(t, start.Add(tt.wantAfter), host.NextDue)
				return
			}
			call := receiveCall(t, remoteProbe)
			call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
			host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
				return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
			})
			require.Equal(t, start.Add(tt.wantAfter), host.NextDue)
		})
	}
}

// TestRegistrySetDemandIsCountedNotBoolean checks that subscriptions balance:
// observation stops after the last closes and resumes immediately on reopen.
func TestRegistrySetDemandIsCountedNotBoolean(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	registry.demandFreshForRemote = 500 * time.Millisecond
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	// Two subscriptions open; only one closes, so demand must stay active.
	registry.SetDemand(true)
	registry.SetDemand(true)
	registry.SetDemand(false)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, start.Add(500*time.Millisecond), host.NextDue, "one remaining demand unit must still use the faster cadence")

	// The last subscription closes: no scheduled observation runs for nobody.
	registry.SetDemand(false)
	registry.SetDemand(false) // balance the fixture's first subscription
	clock.Advance(time.Hour)
	requireNoProbeWithin(t, probe, probeGrace)
	registry.SetDemand(true)
	next := receiveCall(t, probe)
	next.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.LastSuccess.Equal(clock.Now())
	})
	require.Equal(t, clock.Now().Add(500*time.Millisecond), host.NextDue)
}

func TestRegistryDemandOverridesFreshSchedule(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})

	// Explicit demand is admitted immediately, before the fresh deadline.
	registry.RequestProbe(reg.Endpoint)
	forced := receiveCall(t, probe)
	require.True(t, forced.registration.Equal(reg))
	require.Equal(t, start, clock.Now())
}

func TestRegistryCoalescesConcurrentDemand(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	for range 10 {
		registry.RequestProbe(reg.Endpoint)
	}
	select {
	case duplicate := <-probe.calls:
		t.Fatalf("concurrent demand started a duplicate probe for %q", duplicate.registration.Endpoint)
	default:
	}
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return host.Availability == domain.RemoteAvailabilityReachable && host.LastSuccess.Equal(start)
	})
	require.Equal(t, start.Add(defaultDemandFreshForRemote), host.NextDue)

	// The single coalesced follow-up is admitted once the first completes.
	followUp := receiveCall(t, probe)
	require.True(t, followUp.registration.Equal(reg))
	followUp.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
}

func TestRegistryIgnoresDemandForUnknownEndpoint(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	registry.RequestProbe("unknown.test")
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	require.True(t, call.registration.Equal(reg))
	// No probe is ever scheduled for an endpoint outside configured membership.
	requireNoProbeWithin(t, probe, probeGrace)
}

func TestRegistryPublishesReachabilityAndPersists(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	store := &testStore{}
	registry := newTestRegistry(t, 3, store, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{hostSession("work")},
	}}
	snapshot := waitSnapshot(t, registry, func(snapshot ports.BrokerSnapshot) bool {
		host, ok := snapshot.Find(reg.Endpoint)
		return ok && !host.Checking && host.Availability == domain.RemoteAvailabilityReachable && len(host.Sessions) == 1
	})

	stored := waitStored(t, store, func(stored ports.BrokerSnapshot) bool {
		return stored.Revision >= snapshot.Revision && len(stored.Daemons) == 1
	})
	require.Equal(t, ports.BrokerEpoch(3), stored.Epoch)
	require.Len(t, stored.Daemons, 1)
	require.Equal(t, "work", stored.Daemons[0].Sessions[0].Name)
	require.Equal(t, reg.Endpoint, stored.Daemons[0].Endpoint)
	require.False(t, stored.Daemons[0].Checking)
}

func TestRegistryNonReachableOutcomesUseTypedFailureBackoff(t *testing.T) {
	dialErr := errors.New("dial failed")
	tests := []struct {
		name             string
		answer           probeAnswer
		wantAvailability domain.RemoteAvailability
		wantKind         domain.RemoteFailureKind
		wantErr          error
	}{
		{
			name:             "unreachable without error",
			answer:           probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnreachable}},
			wantAvailability: domain.RemoteAvailabilityUnreachable,
			wantKind:         domain.RemoteFailureTransport,
		},
		{
			name:             "unknown outcome is not reachability",
			answer:           probeAnswer{snapshot: ports.BrokerDaemonObservation{}},
			wantAvailability: domain.RemoteAvailabilityUnreachable,
			wantKind:         domain.RemoteFailureTransport,
		},
		{
			name:             "incompatible",
			answer:           probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityIncompatible}},
			wantAvailability: domain.RemoteAvailabilityIncompatible,
			wantKind:         domain.RemoteFailureIncompatible,
		},
		{
			name:             "authentication",
			answer:           probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityAuthFailed}},
			wantAvailability: domain.RemoteAvailabilityAuthFailed,
			wantKind:         domain.RemoteFailureAuthentication,
		},
		{
			name:             "invalid response",
			answer:           probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityInvalidResponse}},
			wantAvailability: domain.RemoteAvailabilityInvalidResponse,
			wantKind:         domain.RemoteFailureInvalidResponse,
		},
		{
			name:             "no daemon after a prior success",
			answer:           probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityNoDaemon}},
			wantAvailability: domain.RemoteAvailabilityNoDaemon,
			wantKind:         domain.RemoteFailureNone,
		},
		{
			name:             "transport error",
			answer:           probeAnswer{err: dialErr},
			wantAvailability: domain.RemoteAvailabilityUnreachable,
			wantKind:         domain.RemoteFailureTransport,
			wantErr:          dialErr,
		},
		{
			name:             "deadline exceeded",
			answer:           probeAnswer{err: context.DeadlineExceeded},
			wantAvailability: domain.RemoteAvailabilityUnreachable,
			wantKind:         domain.RemoteFailureTimeout,
			wantErr:          context.DeadlineExceeded,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			start := time.Unix(100, 0)
			clock := newManualClock(start)
			probe := newTestProbe(1)
			registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
			reg := registration(t, "example.test", 1)
			require.NoError(t, registry.setHosts(hostRecords(reg)))
			startRegistry(t, registry)
			if tt.wantAvailability == domain.RemoteAvailabilityNoDaemon {
				reachable := probeAnswer{snapshot: ports.BrokerDaemonObservation{
					Availability: domain.RemoteAvailabilityReachable, Identity: ports.BrokerDaemonIdentity("remote-daemon"),
					Incarnation: ports.BrokerDaemonIncarnation{1}, ProtocolVersion: protocol.Version,
					InventoryKnown: true, Sessions: []catalogue.RemoteCatalogSession{hostSession("old")},
				}}
				call := receiveCall(t, probe)
				call.result <- reachable
				waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
					return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable && host.Identity == ports.BrokerDaemonIdentity("remote-daemon")
				})
				clock.Advance(5 * time.Second)
				registry.RequestProbe(reg.Endpoint)
				call = receiveCall(t, probe)
				call.result <- reachable
				waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
					return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
				})
				clock.Advance(20 * time.Second)
				registry.RequestProbe(reg.Endpoint)
			}

			call := receiveCall(t, probe)
			call.result <- tt.answer
			host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
				return !host.Checking && (host.ConsecutiveFailures == 1 || tt.wantAvailability == domain.RemoteAvailabilityNoDaemon && host.Availability == domain.RemoteAvailabilityNoDaemon)
			})
			require.Equal(t, tt.wantAvailability, host.Availability)
			require.Equal(t, tt.wantKind, host.LastFailure.Kind)
			if tt.wantAvailability == domain.RemoteAvailabilityNoDaemon {
				require.Zero(t, host.ConsecutiveFailures)
				require.Zero(t, host.FailureEpisode)
				require.Equal(t, start.Add(25*time.Second), host.LastSuccess)
				require.Equal(t, start.Add(40*time.Second), host.NextDue)
				require.Empty(t, host.Identity)
				require.True(t, host.Incarnation.IsZero())
				require.Zero(t, host.ProtocolVersion)
				require.Zero(t, host.Capabilities)
				require.False(t, host.InventoryKnown)
				require.Empty(t, host.Sessions)
				require.Equal(t, domain.RemoteFailureNone, host.LastFailure.Kind)
			} else {
				require.Equal(t, uint(1), host.ConsecutiveFailures)
				require.Equal(t, uint64(1), host.FailureEpisode)
				// No observation succeeded yet, so there is no last success.
				require.True(t, host.LastSuccess.IsZero())
				// A watched host retries on the demand cadence.
				require.Equal(t, start.Add(defaultDemandFreshForRemote), host.NextDue)
			}
			if tt.wantErr != nil {
				require.ErrorIs(t, host.LastFailure, tt.wantErr)
			} else {
				require.Nil(t, host.LastFailure.Err)
			}
		})
	}
}

func TestRegistryClassifiesOversizedProbeProjectionAsInvalidResponse(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)
	sub := registry.Subscribe()
	defer sub.Close()

	// A reachable observation establishes the last-known inventory.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{hostSession("work")},
	}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})

	// A projection carrying one session more than the durable session bound is
	// a malformed response: it is classified as an invalid response and never
	// reaches publication, because the store enforces the same catalogue rule.
	// The loop checks every observed publication, so an invalid snapshot can
	// never pass unnoticed.
	clock.Advance(defaultFreshFor)
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       hostSessions(ports.BrokerMaxSessionsPerHost + 1),
	}}
	deadline := time.After(time.Second)
	var host ports.BrokerDaemonObservation
	for {
		snapshot := registry.Snapshot()
		require.NoError(t, snapshot.Validate())
		current, ok := snapshot.Find(reg.Endpoint)
		if ok && !current.Checking && current.ConsecutiveFailures == 1 {
			host = current
			break
		}
		select {
		case <-sub.Changed():
		case <-deadline:
			t.Fatal("timed out waiting for the invalid-response failure")
		}
	}
	require.Equal(t, domain.RemoteAvailabilityInvalidResponse, host.Availability)
	require.Equal(t, domain.RemoteFailureInvalidResponse, host.LastFailure.Kind)
	// The last-known inventory is retained: a malformed projection never
	// replaces it.
	require.Len(t, host.Sessions, 1)
	require.Equal(t, "work", host.Sessions[0].Name)
	require.Equal(t, clock.Now().Add(defaultDemandFreshForRemote), host.NextDue)
}

func TestRegistryAcceptsProbeProjectionAtSessionBound(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	// The bound itself is valid: exactly BrokerMaxSessionsPerHost sessions are
	// accepted and published, and the resulting snapshot validates.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       hostSessions(ports.BrokerMaxSessionsPerHost),
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Len(t, host.Sessions, ports.BrokerMaxSessionsPerHost)
	require.Zero(t, host.ConsecutiveFailures)
	snapshot := registry.Snapshot()
	require.NoError(t, snapshot.Validate())
	require.Len(t, snapshot.Daemons[0].Sessions, ports.BrokerMaxSessionsPerHost)
}

// TestRegistryRejectsNonDurableObservation proves the registry and the durable
// store agree on what may be persisted: a reachable projection the store would
// reject is classified as an invalid response, so the last-known inventory
// stays authoritative instead of a snapshot that can never be written.
func TestRegistryRejectsNonDurableObservation(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{hostSession("work")},
	}}
	waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})

	// A probe answer that violates the catalogue rules the store enforces (here
	// a zero lifecycle identity and an absent tab list) is a malformed response.
	clock.Advance(defaultFreshFor)
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp}},
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.ConsecutiveFailures == 1
	})
	require.Equal(t, domain.RemoteAvailabilityInvalidResponse, host.Availability)
	require.Equal(t, domain.RemoteFailureInvalidResponse, host.LastFailure.Kind)
	require.Len(t, host.Sessions, 1)
	require.Equal(t, "work", host.Sessions[0].Name)
	require.Equal(t, start, host.LastSuccess)
	snapshot := registry.Snapshot()
	require.NoError(t, snapshot.Validate())
	require.NoError(t, ports.ValidateDurableHostProjection(snapshot.Daemons[0]))
}

// TestRegistryRevisionOverflowFailsClosed pins the revision series: zero is not
// a valid revision and revisions never regress, so an exhausted counter fails
// closed instead of wrapping. The last valid snapshot stays published, the
// registry is poisoned, and every mutation path reports a typed error rather
// than emitting revision zero or a revision below the newest one, either of
// which would silently freeze durable persistence.
func TestRegistryRevisionOverflowFailsClosed(t *testing.T) {
	logger, logs := testLogger()
	registry, err := NewRegistry(1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)), logger)
	require.NoError(t, err)
	first := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(first)))
	before := registry.Snapshot()
	require.NoError(t, before.Validate())
	require.Equal(t, ports.BrokerRevision(2), before.Revision)
	authority := registry.authority

	// The next publication would overflow the series to zero.
	registry.revision = ^ports.BrokerRevision(0)

	// Both mutation paths refuse the change before touching the store or the
	// projection, so neither durable authority nor the publication series moves.
	require.ErrorIs(t, registry.setHosts(hostRecords(registration(t, "other.test", 2))), ports.ErrBrokerRevisionExhausted)
	require.ErrorIs(t, registry.ReplaceHosts([]ports.BrokerHostRecord{{Registration: first, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), first.Endpoint)}}), ports.ErrBrokerRevisionExhausted)
	require.Equal(t, authority, registry.authority)

	// The publication path itself fails closed too: a direct attempt neither
	// wraps the counter nor replaces the last valid snapshot.
	registry.mu.Lock()
	registry.publishLocked(false)
	registry.mu.Unlock()

	after := registry.Snapshot()
	require.Equal(t, before, after)
	require.NoError(t, after.Validate())
	require.Greater(t, after.Revision, ports.BrokerRevision(0))
	require.Equal(t, ^ports.BrokerRevision(0), registry.revision)
	require.Equal(t, first, after.Daemons[0].Registration)
	// The refusal is observable: an exhausted series logs instead of failing
	// silently behind a frozen snapshot.
	require.Contains(t, logs.String(), "revision series exhausted")
}

func TestRegistryPreservesLastSuccessAndOwnsFailureEpisode(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	// A successful observation owns the success time and opens no episode.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{hostSession("work")},
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, start, host.LastSuccess)
	require.Zero(t, host.FailureEpisode)

	// A failure preserves the last success and opens exactly one episode.
	clock.Advance(defaultFreshFor)
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Availability:   domain.RemoteAvailabilityIncompatible,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{{Name: "stale", State: catalogue.RemoteCatalogSessionUp}},
	}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.ConsecutiveFailures == 1
	})
	require.Equal(t, start, host.LastSuccess)
	require.Equal(t, uint64(1), host.FailureEpisode)
	// The last-known inventory is retained: a failed observation never
	// replaces it.
	require.Equal(t, "work", host.Sessions[0].Name)

	// Further failures in the same streak preserve the episode identity so
	// notice policy does not re-notify.
	clock.Advance(defaultRetryBase)
	call = receiveCall(t, probe)
	call.result <- probeAnswer{err: errors.New("offline")}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.ConsecutiveFailures == 2
	})
	require.Equal(t, start, host.LastSuccess)
	require.Equal(t, uint64(1), host.FailureEpisode)

	// Recovery clears the streak, keeps the episode, and advances the success.
	clock.Advance(2 * defaultRetryBase)
	recovered := clock.Now()
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.ConsecutiveFailures == 0
	})
	require.Equal(t, recovered, host.LastSuccess)
	require.Equal(t, uint64(1), host.FailureEpisode)
	require.Equal(t, domain.RemoteFailureNone, host.LastFailure.Kind)
	require.Nil(t, host.LastFailure.Err)
}

func TestRegistryDemandCapsFailureBackoff(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	reg := registration(t, "demand.test", 1)
	store := newTestStore()
	require.NoError(t, store.ReplaceHosts(1, hostRecords(reg)))
	registry := newTestRegistry(t, 1, store, probe, clock)
	startRegistry(t, registry)
	registry.SetDemand(true)
	t.Cleanup(func() { registry.SetDemand(false) })

	for failure := uint(1); failure <= 9; failure++ {
		call := receiveCall(t, probe)
		call.result <- probeAnswer{err: errors.New("daemon not available")}
		host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && host.ConsecutiveFailures == failure
		})
		require.LessOrEqual(t, host.NextDue.Sub(clock.Now()), defaultDemandFreshForRemote, "demanded retry gap must never exceed the freshness interval")
		clock.Advance(host.NextDue.Sub(clock.Now()))
		registry.RequestProbe(reg.Endpoint)
	}
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Zero(t, host.ConsecutiveFailures)
}

func TestRegistryBackoffIsExponentialAndCapped(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	probeErr := errors.New("offline")
	// A watched host retries on its two-second demand cadence even as the
	// underlying exponential failure count increases.
	want := []time.Duration{
		defaultDemandFreshForRemote, defaultDemandFreshForRemote,
		defaultDemandFreshForRemote, defaultDemandFreshForRemote,
		defaultDemandFreshForRemote, defaultDemandFreshForRemote,
		defaultDemandFreshForRemote,
	}
	for index, delay := range want {
		call := receiveCall(t, probe)
		at := clock.Now()
		call.result <- probeAnswer{err: probeErr}
		host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && host.ConsecutiveFailures == uint(index+1)
		})
		require.Equal(t, domain.RemoteAvailabilityUnreachable, host.Availability)
		require.Equal(t, domain.RemoteFailureTransport, host.LastFailure.Kind)
		require.ErrorIs(t, host.LastFailure, probeErr)
		require.Equal(t, at.Add(delay), host.NextDue)

		if index == len(want)-1 {
			break
		}
		// The retry is not admitted early: only the backoff deadline wakes it.
		clock.Advance(delay - time.Nanosecond)
		requireNoProbeWithin(t, probe, probeGrace)
		clock.Advance(time.Nanosecond)
	}
}

func TestRegistryJitterAppliesToRetryAndHealthyRefresh(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	registry.jitter = func(base time.Duration, _ string, _ uint64) time.Duration { return base / 2 }
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{err: errors.New("offline")}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityUnreachable
	})
	require.Equal(t, start.Add(defaultDemandFreshForRemote/2), host.NextDue)

	clock.Advance(defaultDemandFreshForRemote / 2)
	next := receiveCall(t, probe)
	successAt := clock.Now()
	next.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	// The healthy refresh is jittered too: a fleet of hosts never converges
	// on one instant.
	require.Equal(t, successAt.Add(defaultDemandFreshForRemote/2), host.NextDue)
}

func TestRegistryJitterIsDeterministicAndBounded(t *testing.T) {
	bases := []time.Duration{defaultRetryBase, defaultFreshFor, defaultRetryLimit}
	endpoints := []string{"example.test", "other.test", "user@host:2222"}
	for _, base := range bases {
		minimum, maximum := base*9/10, base*11/10
		for _, endpoint := range endpoints {
			spread := make(map[time.Duration]struct{})
			for attempt := uint64(1); attempt <= 64; attempt++ {
				got := jittered(base, endpoint, attempt)
				require.GreaterOrEqual(t, got, minimum)
				require.LessOrEqual(t, got, maximum)
				// Identical endpoint and attempt always yield the identical
				// duration: no wall clock and no shared PRNG participate.
				require.Equal(t, got, jittered(base, endpoint, attempt))
				spread[got] = struct{}{}
			}
			require.Greater(t, len(spread), 1, "jitter must vary across attempts")
		}
	}
	// A non-positive base has no spread to apply.
	require.Zero(t, jittered(0, "example.test", 1))
}

func TestRegistryFencesStaleCompletionAcrossRegistrationChanges(t *testing.T) {
	replacementAnswer := func(name string) probeAnswer {
		return probeAnswer{snapshot: ports.BrokerDaemonObservation{
			Availability:   domain.RemoteAvailabilityReachable,
			InventoryKnown: true,
			Sessions:       []catalogue.RemoteCatalogSession{hostSession(name)},
		}}
	}
	assertFenced := func(t *testing.T, current, replacement domain.RemoteRegistration) {
		t.Helper()
		clock := newManualClock(time.Unix(100, 0))
		probe := newTestProbe(2)
		registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
		require.NoError(t, registry.setHosts(hostRecords(current)))
		startRegistry(t, registry)
		currentCall := receiveCall(t, probe)
		require.True(t, currentCall.registration.Equal(current))

		require.NoError(t, registry.setHosts(hostRecords(replacement)))
		replacementCall := receiveCall(t, probe)
		require.True(t, replacementCall.registration.Equal(replacement))

		// The replacement completes and becomes authoritative...
		replacementCall.result <- replacementAnswer("replacement")
		applied := waitHost(t, registry, replacement.Endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable && len(host.Sessions) == 1
		})
		require.True(t, applied.Registration.Equal(replacement))
		require.Equal(t, "replacement", applied.Sessions[0].Name)

		// ...and the completion bound to the retired registration, arriving
		// afterwards, never touches it.
		currentCall.result <- replacementAnswer("retired")
		requireHostStableWithin(t, registry, replacement.Endpoint, applied, probeGrace)

		// The retired completion also never holds the replacement's slot: a
		// further observation is still admitted and still applies.
		registry.RequestProbe(replacement.Endpoint)
		next := receiveCall(t, probe)
		require.True(t, next.registration.Equal(replacement))
		next.result <- replacementAnswer("next")
		waitHost(t, registry, replacement.Endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && len(host.Sessions) == 1 && host.Sessions[0].Name == "next"
		})
	}

	t.Run("incarnation", func(t *testing.T) {
		assertFenced(t, registration(t, "example.test", 1), registration(t, "example.test", 2))
	})
	t.Run("generation", func(t *testing.T) {
		current := registration(t, "example.test", 1)
		replacement := current
		replacement.Generation++
		assertFenced(t, current, replacement)
	})
}

func TestRegistryFencesRetiredAttemptsAcrossRemovalAndIdenticalReAdd(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan probeResult, 1)
	dispatch := func() { registry.dispatch(ctx, clock.Now(), results) }

	registry.RequestProbe(reg.Endpoint)
	dispatch()
	retiredCall := receiveCall(t, probe)
	require.True(t, retiredCall.registration.Equal(reg))
	retired := attemptFor(registry, reg.Endpoint)
	require.NotZero(t, retired)

	// Removal retires a bounded tombstone and drops the in-flight token.
	require.NoError(t, registry.setHosts(nil))
	removed := registry.Snapshot()
	require.Empty(t, removed.Daemons)
	require.Len(t, removed.Removed, 1)
	require.Equal(t, removed.Revision, removed.Removed[0].RetiredRevision)
	require.True(t, removed.Removed[0].Fences(reg))
	require.NoError(t, removed.Validate())

	// The retired attempt can never revive a removed endpoint.
	registry.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: retired, snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}, at: clock.Now()})
	require.Empty(t, registry.Snapshot().Daemons)
	require.Len(t, registry.Snapshot().Removed, 1)

	// Re-adding the identical registration supersedes the tombstone, so only
	// the attempt token can fence the retired completion.
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	require.Empty(t, registry.Snapshot().Removed)
	registry.RequestProbe(reg.Endpoint)
	dispatch()
	freshCall := receiveCall(t, probe)
	require.True(t, freshCall.registration.Equal(reg))
	fresh := attemptFor(registry, reg.Endpoint)
	require.Greater(t, fresh, retired)

	registry.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: retired, snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}, at: clock.Now()})
	stale, ok := registry.Snapshot().Find(reg.Endpoint)
	require.True(t, ok)
	require.True(t, stale.Checking)
	require.Equal(t, domain.RemoteAvailabilityUnknown, stale.Availability)

	registry.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: fresh, snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}, at: clock.Now()})
	applied, ok := registry.Snapshot().Find(reg.Endpoint)
	require.True(t, ok)
	require.False(t, applied.Checking)
	require.Equal(t, domain.RemoteAvailabilityReachable, applied.Availability)
}

func TestRegistryRetiresInflightAttemptsOnReplacementAndRemoval(t *testing.T) {
	current := registration(t, "example.test", 1)
	replacement := registration(t, "example.test", 2)
	tests := []struct {
		name string
		next []ports.BrokerHostRecord
	}{
		{name: "replacement", next: []ports.BrokerHostRecord{hostRecord(replacement)}},
		{name: "removal", next: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newManualClock(time.Unix(100, 0))
			probe := newTestProbe(2)
			registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
			require.NoError(t, registry.setHosts(hostRecords(current)))
			startRegistry(t, registry)

			call := receiveCall(t, probe)
			require.True(t, call.registration.Equal(current))

			// Replacing or removing the registration retires the in-flight
			// attempt: the probe's derived context is cancelled before the
			// replacement can start, so a ctx-honoring probe stops instead of
			// overlapping its successor.
			require.NoError(t, registry.setHosts(tt.next))
			select {
			case <-probe.canceled:
			case <-time.After(time.Second):
				t.Fatal("retired probe did not observe cancellation")
			}
		})
	}
}

func TestRegistryChurnKeepsOneActiveProbePerEndpoint(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newChurnProbe()
	defer probe.openRelease()
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)

	first := registration(t, "example.test", 1)
	second := registration(t, "example.test", 2)
	require.NoError(t, registry.setHosts(hostRecords(first)))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	registry.RequestProbe(first.Endpoint)

	// Drop the initial attempt's start signal so each lockstep round below
	// consumes the start of its own replacement attempt.
	select {
	case <-probe.startedSig:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for the initial probe")
	}
	require.Equal(t, 1, probe.startedCount())

	// Lockstep churn: every round replaces the registration, which must cancel
	// the retired attempt before the replacement starts. An uncancelled
	// retirement would stay live and overlap its successor, and the exact start
	// count catches an attempt that never restarts.
	const rounds = 8
	for round := 0; round < rounds; round++ {
		next := first
		if round%2 == 0 {
			next = second
		}
		require.NoError(t, registry.setHosts(hostRecords(next)))
		registry.RequestProbe(next.Endpoint)
		select {
		case <-probe.startedSig:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for the replacement attempt to start")
		}
		require.Equal(t, round+2, probe.startedCount())
	}

	// Concurrent churn and demand: the same invariant must hold under the race
	// detector while membership and demand churn from several goroutines.
	failures := make([]error, 4)
	var wg sync.WaitGroup
	for worker := 0; worker < len(failures); worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for round := 0; round < 32; round++ {
				next := first
				if (worker+round)%2 == 0 {
					next = second
				}
				if err := registry.setHosts(hostRecords(next)); err != nil {
					failures[worker] = err
					return
				}
				registry.RequestProbe(first.Endpoint)
				registry.Snapshot()
			}
		}(worker)
	}
	wg.Wait()
	for _, err := range failures {
		require.NoError(t, err)
	}

	endpoints := probe.observedEndpoints()
	require.NotEmpty(t, endpoints)
	for _, endpoint := range endpoints {
		require.Equal(t, 1, probe.maxLiveFor(endpoint), "endpoint %q had overlapping live probes", endpoint)
	}
	require.GreaterOrEqual(t, probe.startedCount(), rounds+1)

	// Settling releases every attempt: no probe stays live once Run returns.
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("registry did not stop after cancellation")
	}
	deadline := time.Now().Add(time.Second)
	for probe.inflight() != 0 {
		if time.Now().After(deadline) {
			t.Fatalf("%d probes still live after settle", probe.inflight())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRegistryPublishesBoundedTombstones(t *testing.T) {
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)))

	// Two full membership rounds with distinct endpoints: without pruning the
	// retired set would double.
	for round := 0; round < 2; round++ {
		registrations := make([]ports.BrokerHostRecord, 0, ports.BrokerMaxHosts)
		for index := 0; index < ports.BrokerMaxHosts; index++ {
			registrations = append(registrations, hostRecord(registration(t, fmt.Sprintf("round-%d-host-%d.test", round, index), byte(round*ports.BrokerMaxHosts+index+1))))
		}
		require.NoError(t, registry.setHosts(registrations))
		require.NoError(t, registry.setHosts(nil))
	}

	snapshot := registry.Snapshot()
	require.NoError(t, snapshot.Validate())
	require.Len(t, snapshot.Removed, ports.BrokerMaxTombstones)
	for index, tombstone := range snapshot.Removed {
		require.NoError(t, tombstone.Validate())
		// Only the newest retirements survive the bound.
		require.Equal(t, snapshot.Revision, tombstone.RetiredRevision)
		if index > 0 {
			require.GreaterOrEqual(t, snapshot.Removed[index-1].RetiredRevision, tombstone.RetiredRevision)
		}
	}
}

func TestRegistryPersistenceRunsOffLockAndCoalescesWrites(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	store := newBlockingStore()
	registry, err := NewRegistry(1, store, probe, clock, nil)
	require.NoError(t, err)
	defer registry.store.close()

	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	// The writer is now blocked inside the first durable write.
	first := <-store.entered
	require.Equal(t, ports.BrokerRevision(2), first)

	// Publication is never blocked behind persistence and never holds the
	// registry lock across a store call.
	var setErr error
	published := make(chan struct{})
	go func() {
		defer close(published)
		for generation := domain.RemoteGeneration(2); generation <= 6; generation++ {
			reg.Generation = generation
			if err := registry.setHosts(hostRecords(reg)); err != nil {
				setErr = err
				return
			}
		}
	}()
	select {
	case <-published:
	case <-time.After(time.Second):
		t.Fatal("publication blocked behind durable persistence")
	}
	require.NoError(t, setErr)

	last := registry.Snapshot().Revision
	store.releaseFirst()
	waitWriterRevision(t, registry, last)

	// Writes stay ordered and coalesced: only the first and the newest
	// revision ever reach the store.
	require.Equal(t, []ports.BrokerRevision{2, last}, store.writtenRevisions())
}

// TestRegistryReplaceHostsBlockedCASKeepsSnapshotLockFree proves membership
// durability and publication stay independent: a ReplaceHosts parked inside the
// durable CAS may hold the registry lock, but Snapshot is served from the atomic
// publication, no observation may be applied behind it, and the run loop
// resumes scheduling as soon as the CAS commits.
func TestRegistryReplaceHostsBlockedCASKeepsSnapshotLockFree(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(2)
	store := newBlockingCASStore()
	registry, err := NewRegistry(1, store, probe, clock, nil)
	require.NoError(t, err)

	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)
	call := receiveCall(t, probe)
	require.True(t, waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return host.Checking
	}).Checking)

	// The replacement takes the registry lock and parks inside the durable CAS.
	replaced := make(chan error, 1)
	go func() {
		replaced <- registry.ReplaceHosts([]ports.BrokerHostRecord{{Registration: reg, Pinned: true, Policy: poolPolicy(), Route: canonicalRoute(poolPolicy(), reg.Endpoint)}})
	}()
	<-store.entered
	t.Cleanup(store.releaseCAS)

	// Snapshot stays lock-free: the atomic publication is readable while the
	// mutator is parked inside the store, and it still carries the pre-CAS
	// projection.
	observed := make(chan ports.BrokerSnapshot, 1)
	go func() { observed <- registry.Snapshot() }()
	select {
	case snapshot := <-observed:
		require.NoError(t, snapshot.Validate())
		require.Len(t, snapshot.Daemons, 1)
		require.Equal(t, reg, snapshot.Daemons[0].Registration)
	case <-time.After(time.Second):
		t.Fatal("Snapshot blocked behind the durable membership CAS")
	}

	// Wake the run loop and complete the observation while the CAS is still
	// parked: applying the result needs the registry lock, so the in-flight
	// projection must stay exactly as published until membership commits.
	registry.hint()
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	pending, ok := registry.Snapshot().Find(reg.Endpoint)
	require.True(t, ok)
	require.True(t, pending.Checking)
	stable := time.NewTimer(probeGrace)
	defer stable.Stop()

window:
	for {
		current, found := registry.Snapshot().Find(reg.Endpoint)
		require.True(t, found)
		require.Equal(t, pending, current)
		select {
		case <-stable.C:
			break window
		case <-time.After(time.Millisecond):
		}
	}

	// Committing membership releases the mutator and unparks the run loop, which
	// applies the observation and resumes scheduling the next one.
	store.releaseCAS()
	require.NoError(t, <-replaced)
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, clock.Now(), host.LastSuccess)
	clock.Advance(defaultFreshFor)
	receiveCall(t, probe)
}

func TestRegistryStoreErrorsAreObservableAndDoNotStallPublication(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	logger, logs := testLogger()
	storeErr := errors.New("disk full")
	store := &testStore{storeErr: storeErr}
	registry, err := NewRegistry(1, store, probe, clock, logger)
	require.NoError(t, err)
	defer registry.store.close()

	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	waitForLog(t, logs, "durable snapshot write failed")
	require.Contains(t, logs.String(), storeErr.Error())

	// A failed write is not fatal and never blocks the next publication.
	store.setStoreErr(nil)
	reg.Generation++
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return snapshot.Revision == registry.Snapshot().Revision
	})
	require.Len(t, stored.Daemons, 1)
	require.True(t, stored.Daemons[0].Registration.Equal(reg))
}

func TestRegistryPersistedCopyClearsChecking(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	store := &testStore{}
	registry := newTestRegistry(t, 3, store, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	// The observation is in flight: the published projection carries Checking,
	// the durable copy never does.
	require.True(t, waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return host.Checking
	}).Checking)
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Daemons) == 1
	})
	require.False(t, stored.Daemons[0].Checking)

	// Completing the observation persists the settled projection.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	settled := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Daemons) == 1 && snapshot.Daemons[0].Availability == domain.RemoteAvailabilityReachable
	})
	require.False(t, settled.Daemons[0].Checking)
}

// TestRegistryNeverPersistsRetirementTombstones proves retirement tombstones
// are process-local fencing state: the durable copy carries no Removed, and a
// fresh registry reopened over that snapshot neither resurrects the retired
// host nor sees a stale tombstone.
func TestRegistryNeverPersistsRetirementTombstones(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := &testStore{}
	registry := newTestRegistry(t, 1, store, newTestProbe(1), clock)
	defer registry.store.close()

	reg := registration(t, "retired.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	require.NoError(t, registry.setHosts(nil))

	// Removal publishes a bounded tombstone and drops the live host in memory.
	require.Empty(t, registry.Snapshot().Daemons)
	require.Len(t, registry.Snapshot().Removed, 1)

	// The durable copy drops the tombstone before the store ever sees it.
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Daemons) == 0 && snapshot.Revision == registry.Snapshot().Revision
	})
	require.Empty(t, stored.Removed)

	// Reopening over the durable snapshot adopts empty membership without the
	// stale tombstone.
	reopened, err := NewRegistry(2, &testStore{loaded: stored}, newTestProbe(1), clock, nil)
	require.NoError(t, err)
	require.Empty(t, reopened.Snapshot().Daemons)
	require.Empty(t, reopened.Snapshot().Removed)
}

func TestRegistryBoundsSlowSubscribersAndStopsOnCancellation(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)

	sub := registry.Subscribe()
	reg := registration(t, "example.test", 1)
	for generation := domain.RemoteGeneration(1); generation <= 10; generation++ {
		reg.Generation = generation
		require.NoError(t, registry.setHosts(hostRecords(reg)))
	}
	// A subscriber that never drains is bounded to one pending wake and
	// never blocks publication.
	require.Len(t, sub.Changed(), 1)

	// Drain the pending wake, then close: a closed subscriber never receives
	// another notification, and closing is idempotent.
	<-sub.Changed()
	sub.Close()
	sub.Close()
	reg.Generation++
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	select {
	case <-sub.Changed():
		t.Fatal("closed subscription received a notification")
	default:
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	registry.RequestProbe(reg.Endpoint)
	receiveCall(t, probe)
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("registry did not stop after cancellation")
	}
	select {
	case <-probe.canceled:
	case <-time.After(time.Second):
		t.Fatal("in-flight probe did not observe cancellation")
	}
	// Cleanup settles the projection: no host stays in flight.
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking
	})
	require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)
}

func TestRegistryRunIsSingleShotAndCleansUpInFlightState(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	logger, logs := testLogger()
	store := &testStore{}
	registry, err := NewRegistry(1, store, probe, clock, logger)
	require.NoError(t, err)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
	registry.RequestProbe(reg.Endpoint)

	call := receiveCall(t, probe)
	require.True(t, waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return host.Checking
	}).Checking)

	// A concurrent Run is refused instead of duplicating scheduling.
	refused := make(chan struct{})
	go func() { defer close(refused); registry.Run(context.Background()) }()
	select {
	case <-refused:
	case <-time.After(time.Second):
		t.Fatal("second Run did not return")
	}
	require.Contains(t, logs.String(), "Run refused")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("registry did not stop after cancellation")
	}
	// Cleanup publishes the settled projection: nothing stays in flight.
	require.False(t, waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking
	}).Checking)

	// The retired completion can never be applied once the run is over.
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
	settled := registry.Snapshot()
	require.Equal(t, domain.RemoteAvailabilityUnknown, settled.Daemons[0].Availability)
	require.Zero(t, settled.Daemons[0].ConsecutiveFailures)

	// Persistence is flushed before shutdown and no write outlives it.
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return snapshot.Revision >= settled.Revision
	})
	require.Equal(t, settled.Revision, stored.Revision)
	require.False(t, stored.Daemons[0].Checking)
	writes := store.writeCount()
	require.NoError(t, registry.setHosts(nil))
	require.Equal(t, writes, store.writeCount())
	require.Contains(t, logs.String(), "refused after shutdown")

	// A restart is refused rather than racing a stale scheduler.
	restarted := make(chan struct{})
	go func() { defer close(restarted); registry.Run(context.Background()) }()
	select {
	case <-restarted:
	case <-time.After(time.Second):
		t.Fatal("restarted Run did not return")
	}
	requireNoProbeWithin(t, probe, probeGrace)
}

func TestRegistryRevisionsIncreaseMonotonicallyWithinEpoch(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 42, newTestStore(), probe, clock)

	var last ports.BrokerRevision
	observe := func() {
		t.Helper()
		snapshot := registry.Snapshot()
		require.Equal(t, ports.BrokerEpoch(42), snapshot.Epoch)
		require.Greater(t, snapshot.Revision, last)
		last = snapshot.Revision
	}
	observe()

	first := registration(t, "a.test", 1)
	second := registration(t, "b.test", 2)
	require.NoError(t, registry.setHosts(hostRecords(first)))
	observe()
	require.NoError(t, registry.setHosts(hostRecords(first, second)))
	observe()

	startRegistry(t, registry)
	for range 2 {
		call := receiveCall(t, probe)
		call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityReachable}}
		waitHost(t, registry, call.registration.Endpoint, func(host ports.BrokerDaemonObservation) bool {
			return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
		})
		observe()
	}
}

// TestRegistryProbeCannotOverrideConfiguredAuthority proves the ownership rule:
// a probe supplies observed state only. Configured authority (local flag,
// endpoint, registration, policy, display origin, rank) is stamped by the
// registry from its own host records and a hostile probe result can never
// override it.
func TestRegistryProbeCannotOverrideConfiguredAuthority(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "user@host:22", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	require.True(t, call.registration.Equal(reg))

	// Every authority field is invented by the probe, including a local flag,
	// a foreign endpoint and registration, a different policy, and display/rank
	// hints. None of it may reach the published projection.
	hostilePolicy := poolPolicy()
	hostilePolicy.Trust = "attacker"
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Local:         true,
		Endpoint:      "attacker@evil",
		DisplayOrigin: "evil",
		Rank:          42,
		Registration:  registration(t, "attacker@evil", 9),
		Policy:        hostilePolicy,
		Availability:  domain.RemoteAvailabilityReachable,
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.False(t, host.Local)
	require.Equal(t, reg.Endpoint, host.Endpoint)
	require.Equal(t, domain.RemoteDisplayOrigin(reg.Endpoint), host.DisplayOrigin)
	require.Zero(t, host.Rank)
	require.True(t, host.Registration.Equal(reg))
	require.Equal(t, poolPolicy(), host.Policy)
	require.NotEqual(t, hostilePolicy, host.Policy)
	require.Len(t, registry.Snapshot().Daemons, 1)
}

// TestRegistryPublishesObservedDaemonIdentity proves observed identity,
// incarnation, protocol version, and capabilities flow from a probe result into
// the published snapshot; configured authority is stamped alongside them.
func TestRegistryPublishesObservedDaemonIdentity(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "user@host:22", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Identity:        "authed-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{7, 11},
		ProtocolVersion: protocol.Version,
		Capabilities:    protocol.CapabilityResume | protocol.CapabilityUDP,
		Availability:    domain.RemoteAvailabilityReachable,
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.Identity != ""
	})
	require.Equal(t, ports.BrokerDaemonIdentity("authed-daemon"), host.Identity)
	require.Equal(t, ports.BrokerDaemonIncarnation{7, 11}, host.Incarnation)
	require.Equal(t, protocol.Version, host.ProtocolVersion)
	require.Equal(t, protocol.CapabilityResume|protocol.CapabilityUDP, host.Capabilities)
	require.Equal(t, poolPolicy(), host.Policy)
	require.NoError(t, registry.Snapshot().Validate())
}

// TestRegistryRejectsObservedIdentityWithoutProtocolVersion pins the partial
// identity rule: an observation that reports identity and incarnation without a
// protocol version fails ports validation and is classified as an invalid
// response, so it never publishes a half-observed daemon.
func TestRegistryRejectsObservedIdentityWithoutProtocolVersion(t *testing.T) {
	// The contract itself rejects partial identity.
	require.Error(t, (ports.BrokerDaemonObservation{
		DisplayOrigin: "host",
		Policy:        poolPolicy(),
		Identity:      "authed-daemon",
		Incarnation:   ports.BrokerDaemonIncarnation{7},
		Availability:  domain.RemoteAvailabilityReachable,
	}).Validate())

	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, newTestStore(), probe, clock)
	reg := registration(t, "user@host:22", 1)
	require.NoError(t, registry.setHosts(hostRecords(reg)))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.BrokerDaemonObservation{
		Identity:     "authed-daemon",
		Incarnation:  ports.BrokerDaemonIncarnation{7},
		Availability: domain.RemoteAvailabilityReachable,
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return !host.Checking && host.ConsecutiveFailures == 1
	})
	require.Equal(t, domain.RemoteAvailabilityInvalidResponse, host.Availability)
	require.Equal(t, domain.RemoteFailureInvalidResponse, host.LastFailure.Kind)
	require.Empty(t, host.Identity, "partial identity must never be published")
	require.True(t, host.Incarnation.IsZero())
}

// TestRegistryPublishesRemoteDaemonsInRegistrationOrder pins the publication
// invariant: remote daemons appear in durable membership registration order (no
// local producer exists yet, so the snapshot carries remotes only).
func TestRegistryPublishesRemoteDaemonsInRegistrationOrder(t *testing.T) {
	registry := newTestRegistry(t, 1, newTestStore(), newTestProbe(1), newManualClock(time.Unix(100, 0)))
	first := registration(t, "a.test", 1)
	second := registration(t, "b.test", 2)
	third := registration(t, "c.test", 3)
	require.NoError(t, registry.setHosts(hostRecords(first, second)))
	require.Equal(t, []string{"a.test", "b.test"}, daemonEndpoints(registry.Snapshot()))

	// Membership changes publish in the new registration order, not sorted and
	// not map order.
	require.NoError(t, registry.setHosts(hostRecords(third, second, first)))
	require.Equal(t, []string{"c.test", "b.test", "a.test"}, daemonEndpoints(registry.Snapshot()))
}

func daemonEndpoints(snapshot ports.BrokerSnapshot) []string {
	endpoints := make([]string, 0, len(snapshot.Daemons))
	for _, daemon := range snapshot.Daemons {
		endpoints = append(endpoints, daemon.Endpoint)
	}
	return endpoints
}

// TestDurableProjectionDropsLocalObservationAndPolicy pins the durable format
// decision: the persistable copy carries remote observations only (a local
// daemon observation is process-local) and omits policy, because membership is
// the single policy authority and the loader re-stamps it.
func TestDurableProjectionDropsLocalObservationAndPolicy(t *testing.T) {
	reg := registration(t, "user@host:22", 1)
	snapshot := ports.BrokerSnapshot{Epoch: 1, Revision: 1, Daemons: []ports.BrokerDaemonObservation{
		{Local: true, DisplayOrigin: "local", Policy: poolPolicy(), Availability: domain.RemoteAvailabilityReachable},
		{Endpoint: reg.Endpoint, DisplayOrigin: domain.RemoteDisplayOrigin(reg.Endpoint), Registration: reg, Policy: poolPolicy(), Availability: domain.RemoteAvailabilityReachable},
	}}
	out := durable(snapshot)
	require.Len(t, out.Daemons, 1)
	require.False(t, out.Daemons[0].Local)
	require.Equal(t, reg.Endpoint, out.Daemons[0].Endpoint)
	require.Equal(t, ports.BrokerPolicy{}, out.Daemons[0].Policy)
	require.Nil(t, out.Removed)
	// The published snapshot the caller handed in is untouched.
	require.Len(t, snapshot.Daemons, 2)
	require.Equal(t, poolPolicy(), snapshot.Daemons[0].Policy)
}

// TestHostDisplayOriginIsBounded pins the presentation-hint bound: a derived
// origin is clamped rune-safely at the ports limit so an over-long configured
// endpoint still yields a valid publication instead of an unobservable host.
func TestHostDisplayOriginIsBounded(t *testing.T) {
	require.Equal(t, "host:22", hostDisplayOrigin("user@host:22"))
	require.Equal(t, "plain.test", hostDisplayOrigin("plain.test"))

	origin := hostDisplayOrigin("user@" + strings.Repeat("€", 100))
	require.LessOrEqual(t, len(origin), ports.BrokerMaxDisplayOriginBytes)
	require.True(t, utf8.ValidString(origin))
	require.NotEmpty(t, origin)
}
