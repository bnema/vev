package broker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
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
// capacity-one channel so tests wait on a write instead of on wall time.
type testStore struct {
	mu       sync.Mutex
	loaded   ports.BrokerSnapshot
	loadErr  error
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
	snapshot ports.RemoteHostSnapshot
	err      error
}

func (p *testProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.RemoteHostSnapshot, error) {
	call := probeCall{registration: registration, result: make(chan probeAnswer, 1)}
	select {
	case p.calls <- call:
	case <-ctx.Done():
		p.noteCanceled()
		return ports.RemoteHostSnapshot{}, ctx.Err()
	}
	select {
	case answer := <-call.result:
		return answer.snapshot, answer.err
	case <-ctx.Done():
		p.noteCanceled()
		return ports.RemoteHostSnapshot{}, ctx.Err()
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

func (p *churnProbe) Probe(ctx context.Context, registration domain.RemoteRegistration) (ports.RemoteHostSnapshot, error) {
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
	return ports.RemoteHostSnapshot{}, ctx.Err()
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

// hostSessions builds count minimal catalogue sessions for the projection
// session bound tests.
func hostSessions(count int) []catalogue.RemoteCatalogSession {
	sessions := make([]catalogue.RemoteCatalogSession, 0, count)
	for index := 0; index < count; index++ {
		sessions = append(sessions, catalogue.RemoteCatalogSession{
			Name:  fmt.Sprintf("session-%d", index),
			State: catalogue.RemoteCatalogSessionUp,
		})
	}
	return sessions
}

// newTestRegistry builds a registry with exact scheduling and no logger so a
// failure never depends on jitter.
func newTestRegistry(t *testing.T, epoch ports.BrokerEpoch, store ports.BrokerSnapshotStore, probe ports.BrokerHostProbe, clock *manualClock) *Registry {
	t.Helper()
	registry, err := NewRegistry(epoch, store, probe, clock, nil)
	require.NoError(t, err)
	registry.jitter = identityJitter
	return registry
}

func startRegistry(t *testing.T, r *Registry) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); r.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })
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

func waitHost(t *testing.T, r *Registry, endpoint string, predicate func(ports.RemoteHostSnapshot) bool) ports.RemoteHostSnapshot {
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
func requireHostStableWithin(t *testing.T, r *Registry, endpoint string, want ports.RemoteHostSnapshot, window time.Duration) {
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
		Name:  "work",
		State: catalogue.RemoteCatalogSessionUp,
		Tabs:  []catalogue.RemoteCatalogTab{{ID: "tab-1", Name: "shell"}},
	}}
	stored := ports.BrokerSnapshot{
		Epoch:    7,
		Revision: 9,
		Hosts: []ports.RemoteHostSnapshot{{
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
	require.Len(t, snapshot.Hosts, 1)
	require.Empty(t, snapshot.Removed)

	host := snapshot.Hosts[0]
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

	_, err := NewRegistry(0, nil, newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, nil, nil, clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, nil, newTestProbe(1), nil, nil)
	require.Error(t, err)

	loadErr := errors.New("load failed")
	_, err = NewRegistry(1, &testStore{loadErr: loadErr}, newTestProbe(1), clock, nil)
	require.ErrorIs(t, err, loadErr)

	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 7}}, newTestProbe(1), clock, nil)
	require.Error(t, err)

	// A durable record without an epoch is inconsistent, never silently empty.
	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 0, Hosts: []ports.RemoteHostSnapshot{{Endpoint: reg.Endpoint, Registration: reg}}}}, newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 0, Removed: []ports.BrokerHostTombstone{{Endpoint: reg.Endpoint, Registration: reg, RetiredRevision: 3}}}}, newTestProbe(1), clock, nil)
	require.Error(t, err)
	_, err = NewRegistry(1, &testStore{loaded: ports.BrokerSnapshot{Epoch: 0, Revision: 4}}, newTestProbe(1), clock, nil)
	require.Error(t, err)

	empty, err := NewRegistry(1, &testStore{}, newTestProbe(1), clock, nil)
	require.NoError(t, err)
	require.Equal(t, ports.BrokerRevision(1), empty.Snapshot().Revision)
	require.Empty(t, empty.Snapshot().Hosts)
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
		Hosts: []ports.RemoteHostSnapshot{{
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
	require.Len(t, registry.Snapshot().Hosts, 1)
}

func TestRegistrySetHostsRejectsInvalidRegistrations(t *testing.T) {
	registry := newTestRegistry(t, 1, nil, newTestProbe(1), newManualClock(time.Unix(100, 0)))

	reg := registration(t, "example.test", 1)
	require.Error(t, registry.SetHosts([]domain.RemoteRegistration{reg, reg}))
	require.Error(t, registry.SetHosts([]domain.RemoteRegistration{{Endpoint: reg.Endpoint}}))
	require.Empty(t, registry.Snapshot().Hosts)
}

func TestRegistrySetHostsRejectsHostLimitWithoutMutation(t *testing.T) {
	registry := newTestRegistry(t, 1, nil, newTestProbe(1), newManualClock(time.Unix(100, 0)))
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))

	oversized := make([]domain.RemoteRegistration, 0, ports.BrokerMaxHosts+1)
	for index := 0; index <= ports.BrokerMaxHosts; index++ {
		oversized = append(oversized, registration(t, fmt.Sprintf("host-%d.test", index), byte(index+1)))
	}
	before := registry.Snapshot()
	require.Error(t, registry.SetHosts(oversized))
	// The rejected call mutates nothing: same membership, same revision.
	require.Equal(t, before, registry.Snapshot())

	// The boundary itself is accepted.
	require.NoError(t, registry.SetHosts(oversized[:ports.BrokerMaxHosts]))
	require.Len(t, registry.Snapshot().Hosts, ports.BrokerMaxHosts)
}

func TestRegistryUnchangedSetHostsIsNoOp(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(2)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	first := registration(t, "example.test", 1)
	second := registration(t, "other.test", 2)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{first, second}))
	startRegistry(t, registry)

	// Every configured host is observed exactly once.
	calls := make(map[string]probeCall, 2)
	for range 2 {
		call := receiveCall(t, probe)
		calls[call.registration.Endpoint] = call
	}
	require.Len(t, calls, 2)
	before := registry.Snapshot()
	for _, host := range before.Hosts {
		require.True(t, host.Checking)
	}

	// Identical membership, including in a different order, never republishes
	// and never restarts an in-flight observation.
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{second, first}))
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{first, second}))
	require.Equal(t, before, registry.Snapshot())
	requireNoProbeWithin(t, probe, probeGrace)

	// The in-flight observations still complete normally.
	for endpoint, call := range calls {
		call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
		waitHost(t, registry, endpoint, func(host ports.RemoteHostSnapshot) bool {
			return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
		})
	}
}

func TestRegistrySnapshotIsImmutable(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityReachable,
		Sessions: []catalogue.RemoteCatalogSession{{
			Name:  "work",
			State: catalogue.RemoteCatalogSessionUp,
			Tabs:  []catalogue.RemoteCatalogTab{{ID: "tab-1", Name: "shell"}},
		}},
	}}
	observed := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && len(host.Sessions) == 1
	})
	require.Equal(t, "work", observed.Sessions[0].Name)

	// Mutating a returned projection never reaches the registry.
	observed.Endpoint = "mutated"
	observed.Sessions[0].Name = "mutated"
	observed.Sessions[0].Tabs[0].Name = "mutated"
	current := registry.Snapshot()
	require.Equal(t, reg.Endpoint, current.Hosts[0].Endpoint)
	require.Equal(t, "work", current.Hosts[0].Sessions[0].Name)
	require.Equal(t, "shell", current.Hosts[0].Sessions[0].Tabs[0].Name)

	// A later publication never mutates a snapshot already handed out.
	earlier := registry.Snapshot()
	registry.RequestProbe(reg.Endpoint)
	next := receiveCall(t, probe)
	next.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityReachable,
		Sessions:     []catalogue.RemoteCatalogSession{{Name: "other", State: catalogue.RemoteCatalogSessionUp}},
	}}
	waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && len(host.Sessions) == 1 && host.Sessions[0].Name == "other"
	})
	require.Equal(t, "work", earlier.Hosts[0].Sessions[0].Name)
}

func TestRegistrySchedulesFreshObservationAfterSuccess(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}

	host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, start, host.LastAttempt)
	require.Equal(t, start, host.LastSuccess)
	require.Equal(t, start.Add(defaultFreshFor), host.NextDue)
	require.Zero(t, host.ConsecutiveFailures)
	require.Zero(t, host.FailureEpisode)

	// A fresh host is not observed again before its scheduled deadline.
	clock.Advance(defaultFreshFor - time.Nanosecond)
	requireNoProbeWithin(t, probe, probeGrace)
	clock.Advance(time.Nanosecond)
	next := receiveCall(t, probe)
	require.True(t, next.registration.Equal(reg))
}

func TestRegistryDemandOverridesFreshSchedule(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
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
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return host.Availability == domain.RemoteAvailabilityReachable && host.LastSuccess.Equal(start)
	})
	require.Equal(t, start.Add(defaultFreshFor), host.NextDue)

	// The single coalesced follow-up is admitted once the first completes.
	followUp := receiveCall(t, probe)
	require.True(t, followUp.registration.Equal(reg))
	followUp.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
}

func TestRegistryIgnoresDemandForUnknownEndpoint(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
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
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability:   domain.RemoteAvailabilityReachable,
		InventoryKnown: true,
		Sessions:       []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp}},
	}}
	snapshot := waitSnapshot(t, registry, func(snapshot ports.BrokerSnapshot) bool {
		host, ok := snapshot.Find(reg.Endpoint)
		return ok && !host.Checking && host.Availability == domain.RemoteAvailabilityReachable && len(host.Sessions) == 1
	})

	stored := waitStored(t, store, func(stored ports.BrokerSnapshot) bool {
		return stored.Revision >= snapshot.Revision && len(stored.Hosts) == 1
	})
	require.Equal(t, ports.BrokerEpoch(3), stored.Epoch)
	require.Len(t, stored.Hosts, 1)
	require.Equal(t, "work", stored.Hosts[0].Sessions[0].Name)
	require.Equal(t, reg.Endpoint, stored.Hosts[0].Endpoint)
	require.False(t, stored.Hosts[0].Checking)
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
			answer:           probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityUnreachable}},
			wantAvailability: domain.RemoteAvailabilityUnreachable,
			wantKind:         domain.RemoteFailureTransport,
		},
		{
			name:             "unknown outcome is not reachability",
			answer:           probeAnswer{snapshot: ports.RemoteHostSnapshot{}},
			wantAvailability: domain.RemoteAvailabilityUnreachable,
			wantKind:         domain.RemoteFailureTransport,
		},
		{
			name:             "incompatible",
			answer:           probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityIncompatible}},
			wantAvailability: domain.RemoteAvailabilityIncompatible,
			wantKind:         domain.RemoteFailureIncompatible,
		},
		{
			name:             "authentication",
			answer:           probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityAuthFailed}},
			wantAvailability: domain.RemoteAvailabilityAuthFailed,
			wantKind:         domain.RemoteFailureAuthentication,
		},
		{
			name:             "invalid response",
			answer:           probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityInvalidResponse}},
			wantAvailability: domain.RemoteAvailabilityInvalidResponse,
			wantKind:         domain.RemoteFailureInvalidResponse,
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
			registry := newTestRegistry(t, 1, nil, probe, clock)
			reg := registration(t, "example.test", 1)
			require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
			startRegistry(t, registry)

			call := receiveCall(t, probe)
			call.result <- tt.answer
			host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
				return !host.Checking && host.ConsecutiveFailures == 1
			})
			require.Equal(t, tt.wantAvailability, host.Availability)
			require.Equal(t, tt.wantKind, host.LastFailure.Kind)
			require.Equal(t, uint(1), host.ConsecutiveFailures)
			require.Equal(t, uint64(1), host.FailureEpisode)
			// No observation succeeded yet, so there is no last success.
			require.True(t, host.LastSuccess.IsZero())
			// Every non-reachable outcome retries on the capped cadence.
			require.Equal(t, start.Add(defaultRetryBase), host.NextDue)
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
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)
	sub := registry.Subscribe()
	defer sub.Close()

	// A reachable observation establishes the last-known inventory.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityReachable,
		Sessions:     []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp}},
	}}
	waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})

	// A projection carrying one session more than the publication bound is a
	// malformed response: it is classified as an invalid response and never
	// reaches publication, because such a snapshot fails Validate. The loop
	// checks every observed publication, so an invalid snapshot can never pass
	// unnoticed.
	clock.Advance(defaultFreshFor)
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityReachable,
		Sessions:     hostSessions(ports.BrokerMaxSessionsPerHost + 1),
	}}
	deadline := time.After(time.Second)
	var host ports.RemoteHostSnapshot
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
	require.Equal(t, clock.Now().Add(defaultRetryBase), host.NextDue)
}

func TestRegistryAcceptsProbeProjectionAtSessionBound(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	// The bound itself is valid: exactly BrokerMaxSessionsPerHost sessions are
	// accepted and published, and the resulting snapshot validates.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityReachable,
		Sessions:     hostSessions(ports.BrokerMaxSessionsPerHost),
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Len(t, host.Sessions, ports.BrokerMaxSessionsPerHost)
	require.Zero(t, host.ConsecutiveFailures)
	snapshot := registry.Snapshot()
	require.NoError(t, snapshot.Validate())
	require.Len(t, snapshot.Hosts[0].Sessions, ports.BrokerMaxSessionsPerHost)
}

func TestRegistryPreservesLastSuccessAndOwnsFailureEpisode(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	// A successful observation owns the success time and opens no episode.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityReachable,
		Sessions:     []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp}},
	}}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	require.Equal(t, start, host.LastSuccess)
	require.Zero(t, host.FailureEpisode)

	// A failure preserves the last success and opens exactly one episode.
	clock.Advance(defaultFreshFor)
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{
		Availability: domain.RemoteAvailabilityIncompatible,
		Sessions:     []catalogue.RemoteCatalogSession{{Name: "stale", State: catalogue.RemoteCatalogSessionUp}},
	}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
	host = waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.ConsecutiveFailures == 2
	})
	require.Equal(t, start, host.LastSuccess)
	require.Equal(t, uint64(1), host.FailureEpisode)

	// Recovery clears the streak, keeps the episode, and advances the success.
	clock.Advance(2 * defaultRetryBase)
	recovered := clock.Now()
	call = receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.ConsecutiveFailures == 0
	})
	require.Equal(t, recovered, host.LastSuccess)
	require.Equal(t, uint64(1), host.FailureEpisode)
	require.Equal(t, domain.RemoteFailureNone, host.LastFailure.Kind)
	require.Nil(t, host.LastFailure.Err)
}

func TestRegistryBackoffIsExponentialAndCapped(t *testing.T) {
	start := time.Unix(100, 0)
	clock := newManualClock(start)
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	probeErr := errors.New("offline")
	// The retry cadence matches the legacy remotes monitor: a 5s base doubling
	// to a 60s cap.
	want := []time.Duration{
		5 * time.Second,
		10 * time.Second,
		20 * time.Second,
		40 * time.Second,
		time.Minute, // 80s capped at defaultRetryLimit
		time.Minute,
		time.Minute,
	}
	for index, delay := range want {
		call := receiveCall(t, probe)
		at := clock.Now()
		call.result <- probeAnswer{err: probeErr}
		host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
	registry := newTestRegistry(t, 1, nil, probe, clock)
	registry.jitter = func(base time.Duration, _ string, _ uint64) time.Duration { return base / 2 }
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	call := receiveCall(t, probe)
	call.result <- probeAnswer{err: errors.New("offline")}
	host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityUnreachable
	})
	require.Equal(t, start.Add(defaultRetryBase/2), host.NextDue)

	clock.Advance(defaultRetryBase / 2)
	next := receiveCall(t, probe)
	successAt := clock.Now()
	next.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	host = waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
	})
	// The healthy refresh is jittered too: a fleet of hosts never converges
	// on one instant.
	require.Equal(t, successAt.Add(defaultFreshFor/2), host.NextDue)
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
		return probeAnswer{snapshot: ports.RemoteHostSnapshot{
			Availability: domain.RemoteAvailabilityReachable,
			Sessions:     []catalogue.RemoteCatalogSession{{Name: name, State: catalogue.RemoteCatalogSessionUp}},
		}}
	}
	assertFenced := func(t *testing.T, current, replacement domain.RemoteRegistration) {
		t.Helper()
		clock := newManualClock(time.Unix(100, 0))
		probe := newTestProbe(2)
		registry := newTestRegistry(t, 1, nil, probe, clock)
		require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{current}))
		startRegistry(t, registry)
		currentCall := receiveCall(t, probe)
		require.True(t, currentCall.registration.Equal(current))

		require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{replacement}))
		replacementCall := receiveCall(t, probe)
		require.True(t, replacementCall.registration.Equal(replacement))

		// The replacement completes and becomes authoritative...
		replacementCall.result <- replacementAnswer("replacement")
		applied := waitHost(t, registry, replacement.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
		waitHost(t, registry, replacement.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
	registry := newTestRegistry(t, 1, nil, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	results := make(chan probeResult, 1)
	dispatch := func() { registry.dispatch(ctx, clock.Now(), results) }

	dispatch()
	retiredCall := receiveCall(t, probe)
	require.True(t, retiredCall.registration.Equal(reg))
	retired := attemptFor(registry, reg.Endpoint)
	require.NotZero(t, retired)

	// Removal retires a bounded tombstone and drops the in-flight token.
	require.NoError(t, registry.SetHosts(nil))
	removed := registry.Snapshot()
	require.Empty(t, removed.Hosts)
	require.Len(t, removed.Removed, 1)
	require.Equal(t, removed.Revision, removed.Removed[0].RetiredRevision)
	require.True(t, removed.Removed[0].Fences(reg))
	require.NoError(t, removed.Validate())

	// The retired attempt can never revive a removed endpoint.
	registry.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: retired, snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}, at: clock.Now()})
	require.Empty(t, registry.Snapshot().Hosts)
	require.Len(t, registry.Snapshot().Removed, 1)

	// Re-adding the identical registration supersedes the tombstone, so only
	// the attempt token can fence the retired completion.
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	require.Empty(t, registry.Snapshot().Removed)
	dispatch()
	freshCall := receiveCall(t, probe)
	require.True(t, freshCall.registration.Equal(reg))
	fresh := attemptFor(registry, reg.Endpoint)
	require.Greater(t, fresh, retired)

	registry.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: retired, snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}, at: clock.Now()})
	stale, ok := registry.Snapshot().Find(reg.Endpoint)
	require.True(t, ok)
	require.True(t, stale.Checking)
	require.Equal(t, domain.RemoteAvailabilityUnknown, stale.Availability)

	registry.apply(probeResult{endpoint: reg.Endpoint, registration: reg, attempt: fresh, snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}, at: clock.Now()})
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
		next []domain.RemoteRegistration
	}{
		{name: "replacement", next: []domain.RemoteRegistration{replacement}},
		{name: "removal", next: nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clock := newManualClock(time.Unix(100, 0))
			probe := newTestProbe(2)
			registry := newTestRegistry(t, 1, nil, probe, clock)
			require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{current}))
			startRegistry(t, registry)

			call := receiveCall(t, probe)
			require.True(t, call.registration.Equal(current))

			// Replacing or removing the registration retires the in-flight
			// attempt: the probe's derived context is cancelled before the
			// replacement can start, so a ctx-honoring probe stops instead of
			// overlapping its successor.
			require.NoError(t, registry.SetHosts(tt.next))
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
	registry := newTestRegistry(t, 1, nil, probe, clock)

	first := registration(t, "example.test", 1)
	second := registration(t, "example.test", 2)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{first}))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()

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
		require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{next}))
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
				if err := registry.SetHosts([]domain.RemoteRegistration{next}); err != nil {
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
	registry := newTestRegistry(t, 1, nil, newTestProbe(1), newManualClock(time.Unix(100, 0)))

	// Two full membership rounds with distinct endpoints: without pruning the
	// retired set would double.
	for round := 0; round < 2; round++ {
		registrations := make([]domain.RemoteRegistration, 0, ports.BrokerMaxHosts)
		for index := 0; index < ports.BrokerMaxHosts; index++ {
			registrations = append(registrations, registration(t, fmt.Sprintf("round-%d-host-%d.test", round, index), byte(round*ports.BrokerMaxHosts+index+1)))
		}
		require.NoError(t, registry.SetHosts(registrations))
		require.NoError(t, registry.SetHosts(nil))
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
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
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
			if err := registry.SetHosts([]domain.RemoteRegistration{reg}); err != nil {
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
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	waitForLog(t, logs, "durable snapshot write failed")
	require.Contains(t, logs.String(), storeErr.Error())

	// A failed write is not fatal and never blocks the next publication.
	store.setStoreErr(nil)
	reg.Generation++
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return snapshot.Revision == registry.Snapshot().Revision
	})
	require.Len(t, stored.Hosts, 1)
	require.True(t, stored.Hosts[0].Registration.Equal(reg))
}

func TestRegistryPersistedCopyClearsChecking(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	store := &testStore{}
	registry := newTestRegistry(t, 3, store, probe, clock)
	reg := registration(t, "example.test", 1)
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	startRegistry(t, registry)

	// The observation is in flight: the published projection carries Checking,
	// the durable copy never does.
	require.True(t, waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return host.Checking
	}).Checking)
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Hosts) == 1
	})
	require.False(t, stored.Hosts[0].Checking)

	// Completing the observation persists the settled projection.
	call := receiveCall(t, probe)
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	settled := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return len(snapshot.Hosts) == 1 && snapshot.Hosts[0].Availability == domain.RemoteAvailabilityReachable
	})
	require.False(t, settled.Hosts[0].Checking)
}

func TestRegistryBoundsSlowSubscribersAndStopsOnCancellation(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	probe := newTestProbe(1)
	registry := newTestRegistry(t, 1, nil, probe, clock)

	sub := registry.Subscribe()
	reg := registration(t, "example.test", 1)
	for generation := domain.RemoteGeneration(1); generation <= 10; generation++ {
		reg.Generation = generation
		require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
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
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))
	select {
	case <-sub.Changed():
		t.Fatal("closed subscription received a notification")
	default:
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()
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
	host := waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{reg}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); registry.Run(ctx) }()

	call := receiveCall(t, probe)
	require.True(t, waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
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
	require.False(t, waitHost(t, registry, reg.Endpoint, func(host ports.RemoteHostSnapshot) bool {
		return !host.Checking
	}).Checking)

	// The retired completion can never be applied once the run is over.
	call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
	settled := registry.Snapshot()
	require.Equal(t, domain.RemoteAvailabilityUnknown, settled.Hosts[0].Availability)
	require.Zero(t, settled.Hosts[0].ConsecutiveFailures)

	// Persistence is flushed before shutdown and no write outlives it.
	stored := waitStored(t, store, func(snapshot ports.BrokerSnapshot) bool {
		return snapshot.Revision >= settled.Revision
	})
	require.Equal(t, settled.Revision, stored.Revision)
	require.False(t, stored.Hosts[0].Checking)
	writes := store.writeCount()
	require.NoError(t, registry.SetHosts(nil))
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
	registry := newTestRegistry(t, 42, nil, probe, clock)

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
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{first}))
	observe()
	require.NoError(t, registry.SetHosts([]domain.RemoteRegistration{first, second}))
	observe()

	startRegistry(t, registry)
	for range 2 {
		call := receiveCall(t, probe)
		call.result <- probeAnswer{snapshot: ports.RemoteHostSnapshot{Availability: domain.RemoteAvailabilityReachable}}
		waitHost(t, registry, call.registration.Endpoint, func(host ports.RemoteHostSnapshot) bool {
			return !host.Checking && host.Availability == domain.RemoteAvailabilityReachable
		})
		observe()
	}
}
