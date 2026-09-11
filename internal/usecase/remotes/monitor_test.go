package remotes

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

var errMonitorTestFailure = errors.New("monitor test failure")

// fakeTimer is a manually fired ports.Timer.
type fakeTimer struct {
	clock    *fakeClock
	ch       chan time.Time
	deadline time.Time
	stopped  bool
}

func (t *fakeTimer) C() <-chan time.Time { return t.ch }

func (t *fakeTimer) Reset(d time.Duration) bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.deadline = t.clock.now.Add(d)
	t.stopped = false
	return true
}

func (t *fakeTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	t.stopped = true
	return true
}

// fakeClock is a manually advanced ports.Clock.
type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock(now time.Time) *fakeClock { return &fakeClock{now: now} }

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) NewTimer(d time.Duration) ports.Timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &fakeTimer{clock: c, ch: make(chan time.Time, 1), deadline: c.now.Add(d)}
	c.timers = append(c.timers, t)
	return t
}

// Advance moves the clock forward and fires due timers without blocking.
func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
	for _, t := range c.timers {
		if t.stopped || t.deadline.After(c.now) {
			continue
		}
		select {
		case t.ch <- c.now:
		default:
		}
	}
}

// fakeRuntime is a scripted ports.RemoteRuntime: the test observes admitted
// jobs and feeds completions explicitly.
type fakeRuntime struct {
	incoming chan ports.RemoteJob
	outgoing chan ports.RemoteJobResult
	err      error
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{
		incoming: make(chan ports.RemoteJob, 16),
		outgoing: make(chan ports.RemoteJobResult, 16),
	}
}

func (f *fakeRuntime) Run(ctx context.Context, jobs <-chan ports.RemoteJob, results chan<- ports.RemoteJobResult) error {
	for {
		select {
		case <-ctx.Done():
			return f.err
		case job, ok := <-jobs:
			if !ok {
				return f.err
			}
			select {
			case f.incoming <- job:
			case <-ctx.Done():
				return f.err
			}
		case res, ok := <-f.outgoing:
			if !ok {
				return f.err
			}
			select {
			case results <- res:
			case <-ctx.Done():
				return f.err
			}
		}
	}
}

func testRegistration(endpoint string, incarnation byte) domain.RemoteRegistration {
	return domain.RemoteRegistration{Endpoint: endpoint, Incarnation: [16]byte{incarnation}, Generation: 1}
}

func receiveJob(t *testing.T, ch <-chan ports.RemoteJob, what string) ports.RemoteJob {
	t.Helper()
	select {
	case job := <-ch:
		return job
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		return ports.RemoteJob{}
	}
}

// receiveObserve returns the next admitted catalogue observation,
// skipping independent lane traffic such as registry polls or stores.
func receiveObserve(t *testing.T, ch <-chan ports.RemoteJob, what string) ports.RemoteJob {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case job := <-ch:
			if job.Kind == ports.RemoteJobCatalogObserve {
				return job
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", what)
			return ports.RemoteJob{}
		}
	}
}

func requireNoJob(t *testing.T, ch <-chan ports.RemoteJob, what string) {
	t.Helper()
	select {
	case job := <-ch:
		t.Fatalf("unexpected %s: %+v", what, job)
	case <-time.After(100 * time.Millisecond):
	}
}

// requireNoObserve drains lane traffic for a window and fails only on a
// duplicate catalogue observation: registry polls and cache stores are
// legitimate concurrent lane activity.
func requireNoObserve(t *testing.T, ch <-chan ports.RemoteJob, what string) {
	t.Helper()
	deadline := time.After(150 * time.Millisecond)
	for {
		select {
		case job := <-ch:
			if job.Kind == ports.RemoteJobCatalogObserve {
				t.Fatalf("unexpected %s: %+v", what, job)
			}
		case <-deadline:
			return
		}
	}
}

func waitWake(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func startMonitor(t *testing.T, clock *fakeClock, runtime *fakeRuntime) (*Monitor, context.CancelFunc) {
	t.Helper()
	monitor := NewMonitor(runtime, clock, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- monitor.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("monitor did not stop")
		}
	})
	return monitor, cancel
}

func TestMonitorInitialSnapshotIsRenderable(t *testing.T) {
	monitor := NewMonitor(newFakeRuntime(), newFakeClock(time.Unix(1_000, 0)), nil)
	snapshot := monitor.Snapshot()
	if snapshot.Revision != 0 || snapshot.Initialized || len(snapshot.Hosts) != 0 {
		t.Fatalf("initial snapshot = %+v, want empty uninitialized", snapshot)
	}
	sub := monitor.Subscribe()
	defer sub.Close()
	select {
	case <-sub.Changed():
		t.Fatal("no publication expected before Run")
	case <-time.After(50 * time.Millisecond):
	}
}

func TestMonitorBootstrapsRegistryAndCacheIndependently(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	registryJob := receiveJob(t, runtime.incoming, "registry read")
	if registryJob.Kind != ports.RemoteJobRegistryRead {
		t.Fatalf("first job = %v, want registry_read", registryJob.Kind)
	}
	cacheJob := receiveJob(t, runtime.incoming, "cache load")
	if cacheJob.Kind != ports.RemoteJobCacheLoad {
		t.Fatalf("second job = %v, want cache_load", cacheJob.Kind)
	}

	// Cache answers first: the snapshot publishes but stays uninitialized
	// with no hosts until the registry is authoritative.
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad, Entries: []catalogue.RemoteCatalogCacheEntry{{
		Host: "user@arch", FetchedAt: clock.Now(), Incarnation: [16]byte{1},
		Sessions: []catalogue.RemoteCatalogSession{{Name: "cached", State: catalogue.RemoteCatalogSessionUp}},
	}}}
	waitWake(t, sub.Changed(), "cache publication")
	snapshot := monitor.Snapshot()
	if snapshot.Initialized || len(snapshot.Hosts) != 0 {
		t.Fatalf("cache-only snapshot = %+v, want uninitialized without hosts", snapshot)
	}

	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")
	snapshot = monitor.Snapshot()
	if !snapshot.Initialized || snapshot.Revision == 0 {
		t.Fatalf("registry snapshot = %+v, want initialized", snapshot)
	}
	host, ok := snapshot.Find("user@arch")
	if !ok {
		t.Fatalf("hosts = %+v, want user@arch", snapshot.Hosts)
	}
	if host.Availability != domain.RemoteAvailabilityUnknown {
		t.Fatalf("fresh host = %+v, want unknown availability", host)
	}
	// Discovery admits the first check in the same iteration that
	// publishes, so the host may already be checking here; admission is
	// proven by the observe job below.
	if !host.InventoryKnown || len(host.Sessions) != 1 || host.Sessions[0].Name != "cached" {
		t.Fatalf("host = %+v, want seeded cached inventory", host)
	}
	if host.Registration != registration {
		t.Fatalf("host registration = %+v, want %+v", host.Registration, registration)
	}

	// The seeded host is due immediately: exactly one observation is admitted.
	observeJob := receiveJob(t, runtime.incoming, "catalog observe")
	if observeJob.Kind != ports.RemoteJobCatalogObserve || observeJob.Endpoint != "user@arch" {
		t.Fatalf("observe job = %+v, want catalog_observe for user@arch", observeJob)
	}
	if observeJob.Deadline != observeTimeout {
		t.Fatalf("observe deadline = %v, want %v", observeJob.Deadline, observeTimeout)
	}
}

func TestMonitorEmptyRegistryStillPublishesInitialized(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	_ = receiveJob(t, runtime.incoming, "registry read")
	_ = receiveJob(t, runtime.incoming, "cache load")

	// A registry read with no registration is an authoritative empty state,
	// not an unfinished check: readers must stop reporting "checking".
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead}
	waitWake(t, sub.Changed(), "empty registry publication")
	snapshot := monitor.Snapshot()
	if !snapshot.Initialized || len(snapshot.Hosts) != 0 || snapshot.Revision == 0 {
		t.Fatalf("empty registry snapshot = %+v, want initialized with no hosts", snapshot)
	}
}

func TestMonitorCacheSeedingRules(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")

	matching := testRegistration("user@arch", 1)
	other := testRegistration("user@mule", 2)
	runtime.outgoing <- ports.RemoteJobResult{
		Kind:    ports.RemoteJobCacheLoad,
		Entries: []catalogue.RemoteCatalogCacheEntry{{Host: "user@arch", FetchedAt: clock.Now(), Incarnation: [16]byte{9}, Sessions: []catalogue.RemoteCatalogSession{{Name: "stale-owner"}}}}, // mismatched incarnation
	}
	waitWake(t, sub.Changed(), "cache publication")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{matching, other},
	}
	waitWake(t, sub.Changed(), "registry publication")
	// Drain the two admitted observations without answering.
	first := receiveJob(t, runtime.incoming, "first observe")
	second := receiveJob(t, runtime.incoming, "second observe")
	_ = first
	_ = second

	snapshot := monitor.Snapshot()
	arch, ok := snapshot.Find("user@arch")
	if !ok || arch.InventoryKnown || len(arch.Sessions) != 0 {
		t.Fatalf("mismatched cache must not seed: %+v", arch)
	}
}

// TestMonitorIdenticalRegistryPollPublishesNothing proves steady-state polls
// are silent: unchanged registrations with already-seeded cache produce no
// revision, while the seeded sessions were published exactly once.
func TestMonitorIdenticalRegistryPollPublishesNothing(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration},
	}
	waitWake(t, sub.Changed(), "registry publication")
	// Leave the admitted observation unanswered: no live inventory may
	// confirm and mask the cache seeding below.
	receiveObserve(t, runtime.incoming, "catalog observe")

	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCacheLoad,
		Entries: []catalogue.RemoteCatalogCacheEntry{{Host: "user@arch", FetchedAt: clock.Now(),
			Incarnation: [16]byte{1}, Sessions: []catalogue.RemoteCatalogSession{{Name: "work"}}}},
	}
	waitWake(t, sub.Changed(), "cache publication")
	host, ok := monitor.Snapshot().Find("user@arch")
	if !ok || !host.InventoryKnown || len(host.Sessions) != 1 {
		t.Fatalf("cache must seed visible sessions: %+v", host)
	}

	// An identical poll changes neither membership nor seeded state.
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration},
	}
	select {
	case <-sub.Changed():
		t.Fatal("identical registry poll published a revision")
	case <-time.After(150 * time.Millisecond):
	}
	host, _ = monitor.Snapshot().Find("user@arch")
	if len(host.Sessions) != 1 {
		t.Fatalf("seeded sessions must survive silent polls: %+v", host)
	}
}

func TestMonitorObserveSuccessSchedulesHealthyCheck(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")

	observe := receiveObserve(t, runtime.incoming, "catalog observe")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: observe.Generation,
		Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp}}},
	}
	waitSnapshot(t, monitor, "success publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
		host, ok := snapshot.Find("user@arch")
		return ok && host.Availability == domain.RemoteAvailabilityReachable && !host.Checking
	})
	host, _ := monitor.Snapshot().Find("user@arch")
	if !host.InventoryKnown || len(host.Sessions) != 1 {
		t.Fatalf("host = %+v, want confirmed inventory", host)
	}
	if host.ConsecutiveFailures != 0 {
		t.Fatalf("failures = %d, want 0", host.ConsecutiveFailures)
	}

	// Healthy cadence is 15s ±10%: nothing due at 13s, due by 17s.
	clock.Advance(13 * time.Second)
	requireNoObserve(t, runtime.incoming, "early healthy recheck")
	clock.Advance(4 * time.Second)
	followUp := receiveObserve(t, runtime.incoming, "healthy recheck")
	if followUp.Generation == observe.Generation {
		t.Fatalf("recheck reused generation %d", followUp.Generation)
	}
}

func TestMonitorRetryBackoffSequence(t *testing.T) {
	tests := []struct {
		name         string
		failureKind  domain.RemoteFailureKind
		availability domain.RemoteAvailability
		minDelay     time.Duration
		maxDelay     time.Duration
	}{
		{name: "transport", failureKind: domain.RemoteFailureTransport, availability: domain.RemoteAvailabilityUnreachable, minDelay: 4500 * time.Millisecond, maxDelay: 5500 * time.Millisecond},
		{name: "timeout maps to transport default", failureKind: domain.RemoteFailureNone, availability: domain.RemoteAvailabilityUnreachable, minDelay: 4500 * time.Millisecond, maxDelay: 5500 * time.Millisecond},
		{name: "authentication", failureKind: domain.RemoteFailureAuthentication, availability: domain.RemoteAvailabilityAuthFailed, minDelay: 4500 * time.Millisecond, maxDelay: 5500 * time.Millisecond},
		{name: "incompatible", failureKind: domain.RemoteFailureIncompatible, availability: domain.RemoteAvailabilityIncompatible, minDelay: 4500 * time.Millisecond, maxDelay: 5500 * time.Millisecond},
		{name: "invalid response", failureKind: domain.RemoteFailureInvalidResponse, availability: domain.RemoteAvailabilityInvalidResponse, minDelay: 4500 * time.Millisecond, maxDelay: 5500 * time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			clock := newFakeClock(time.Unix(1_000, 0))
			runtime := newFakeRuntime()
			monitor, _ := startMonitor(t, clock, runtime)
			sub := monitor.Subscribe()
			defer sub.Close()

			receiveJob(t, runtime.incoming, "registry read")
			receiveJob(t, runtime.incoming, "cache load")
			runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
			waitWake(t, sub.Changed(), "cache publication")
			registration := testRegistration("user@arch", 1)
			runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
			waitWake(t, sub.Changed(), "registry publication")
			observe := receiveObserve(t, runtime.incoming, "catalog observe")
			runtime.outgoing <- ports.RemoteJobResult{
				Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
				Registration: registration, Generation: observe.Generation,
				FailureKind: test.failureKind, Err: errMonitorTestFailure,
			}
			waitSnapshot(t, monitor, "failure publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
				host, ok := snapshot.Find("user@arch")
				return ok && host.Availability == test.availability && host.ConsecutiveFailures == 1
			})
			host, _ := monitor.Snapshot().Find("user@arch")
			if host.ConsecutiveFailures != 1 || host.LastFailure.Kind != test.failureKind && !(test.failureKind == domain.RemoteFailureNone && host.LastFailure.Kind == domain.RemoteFailureTransport) {
				t.Fatalf("host = %+v, want one classified failure", host)
			}
			clock.Advance(test.minDelay - time.Second)
			requireNoObserve(t, runtime.incoming, "early retry")
			clock.Advance(2 * time.Second)
			receiveObserve(t, runtime.incoming, "retry")
		})
	}
}

func TestMonitorRetryBackoffGrowsAndCaps(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")

	// Expected pre-jitter backoffs: 5, 10, 20, 40, 60, 60. Jitter is ±10%,
	// so assert one window per attempt below and above the nominal value.
	windows := [][2]time.Duration{
		{4500 * time.Millisecond, 5500 * time.Millisecond},
		{9 * time.Second, 11 * time.Second},
		{18 * time.Second, 22 * time.Second},
		{36 * time.Second, 44 * time.Second},
		{54 * time.Second, 66 * time.Second},
		{54 * time.Second, 66 * time.Second},
	}
	for i, window := range windows {
		observe := receiveObserve(t, runtime.incoming, "catalog observe")
		runtime.outgoing <- ports.RemoteJobResult{
			Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
			Registration: registration, Generation: observe.Generation,
			FailureKind: domain.RemoteFailureTimeout, Err: errMonitorTestFailure,
		}
		waitSnapshot(t, monitor, "failure publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
			host, ok := snapshot.Find("user@arch")
			return ok && host.ConsecutiveFailures == uint(i+1)
		})
		host, _ := monitor.Snapshot().Find("user@arch")
		// Prior inventory is retained with its age on failure.
		if host.InventoryKnown {
			t.Fatalf("attempt %d: failed check must not invent inventory", i)
		}
		clock.Advance(window[0] - time.Second)
		requireNoObserve(t, runtime.incoming, "early retry")
		clock.Advance(window[1] - window[0] + 2*time.Second)
	}
	// The last window leaves the next retry pending: it must arrive now.
	receiveObserve(t, runtime.incoming, "capped retry")
}

func TestMonitorFencesReRegistration(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	first := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{first}}
	waitWake(t, sub.Changed(), "registry publication")
	observe := receiveObserve(t, runtime.incoming, "catalog observe")

	// Re-registration with a new incarnation while the check is in flight.
	clock.Advance(registryPollInterval)
	poll := receiveJob(t, runtime.incoming, "registry poll")
	if poll.Kind != ports.RemoteJobRegistryRead {
		t.Fatalf("poll = %+v, want registry_read", poll)
	}
	second := testRegistration("user@arch", 2)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{second}}
	// The fresh registration resets: no inventory, unknown reachability,
	// and not checking, since the outstanding check belongs to the
	// previous registration.
	snapshot := waitSnapshot(t, monitor, "re-registration publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
		host, ok := snapshot.Find("user@arch")
		return ok && host.Registration == second
	})
	host, _ := snapshot.Find("user@arch")
	if host.Availability != domain.RemoteAvailabilityUnknown || host.Checking {
		t.Fatalf("re-registered host = %+v, want unknown without checking", host)
	}

	// The late result bound to the old incarnation fences out: it retires
	// its credit, never applies inventory or reachability, and never
	// clears the fresh registration's observation.
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: first, Generation: observe.Generation,
		Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "ghost"}}},
	}
	fresh := receiveObserve(t, runtime.incoming, "fresh observe")
	if fresh.Registration != second {
		t.Fatalf("fresh observe = %+v, want registration %+v", fresh.Registration, second)
	}
	snapshot = waitSnapshot(t, monitor, "fresh observe admission", func(snapshot ports.RemoteDirectorySnapshot) bool {
		host, ok := snapshot.Find("user@arch")
		return ok && host.Checking
	})
	host, _ = snapshot.Find("user@arch")
	if host.Availability != domain.RemoteAvailabilityUnknown || len(host.Sessions) != 0 {
		t.Fatalf("fenced result applied: %+v", host)
	}
}

// TestMonitorFencesRemoveReAddAcrossReads covers the ABA path simple
// replacement misses: the endpoint vanishes in one registry read (dropping
// its policy while its observation is outstanding), returns with a new
// incarnation in a later read, and both completions arrive in both orders.
// The old result must never clear the new registration's active check nor
// admit a second concurrent observation for the endpoint.
func TestMonitorFencesRemoveReAddAcrossReads(t *testing.T) {
	for _, order := range []string{"old-first", "new-first"} {
		t.Run(order, func(t *testing.T) {
			clock := newFakeClock(time.Unix(1_000, 0))
			runtime := newFakeRuntime()
			monitor, _ := startMonitor(t, clock, runtime)
			sub := monitor.Subscribe()
			defer sub.Close()

			receiveJob(t, runtime.incoming, "registry read")
			receiveJob(t, runtime.incoming, "cache load")
			runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
			waitWake(t, sub.Changed(), "cache publication")
			first := testRegistration("user@arch", 1)
			runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{first}}
			waitWake(t, sub.Changed(), "registry publication")
			old := receiveObserve(t, runtime.incoming, "catalog observe")

			// The endpoint vanishes while its check is outstanding, then
			// returns with a new incarnation. Generations restart at
			// one, so only the registration identity fences the old
			// result out.
			clock.Advance(registryPollInterval)
			receiveJob(t, runtime.incoming, "registry poll")
			runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead}
			waitSnapshot(t, monitor, "removal publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
				_, ok := snapshot.Find("user@arch")
				return !ok
			})
			clock.Advance(registryPollInterval)
			receiveJob(t, runtime.incoming, "registry poll")
			second := testRegistration("user@arch", 2)
			runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{second}}
			waitSnapshot(t, monitor, "re-add publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
				host, ok := snapshot.Find("user@arch")
				return ok && host.Registration == second
			})
			// While the old request is outstanding no fresh check is
			// admitted: exactly one observation per endpoint even
			// across removal and re-add.
			requireNoObserve(t, runtime.incoming, "concurrent observe across re-add")
			// Retire the old request first: it fences out silently and
			// leaves the new policy (unknown, not checking) untouched.
			oldRetire := ports.RemoteJobResult{
				Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
				Registration: first, Generation: old.Generation,
				Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "ghost"}}},
			}
			runtime.outgoing <- oldRetire
			fresh := receiveObserve(t, runtime.incoming, "fresh observe")
			if fresh.Registration != second {
				t.Fatalf("fresh observe = %+v, want registration %+v", fresh.Registration, second)
			}
			oldResult := ports.RemoteJobResult{
				Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
				Registration: first, Generation: old.Generation,
				Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "ghost"}}},
			}
			newResult := ports.RemoteJobResult{
				Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
				Registration: second, Generation: fresh.Generation,
				Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "work", State: catalogue.RemoteCatalogSessionUp}}},
			}
			if order == "old-first" {
				// A stale duplicate while the fresh check is
				// outstanding must retire silently: no ghost, no
				// cleared observation, no third check admitted.
				runtime.outgoing <- oldResult
				requireNoObserve(t, runtime.incoming, "third observe after stale duplicate")
				runtime.outgoing <- newResult
			} else {
				runtime.outgoing <- newResult
				runtime.outgoing <- oldResult
			}
			snapshot := waitSnapshot(t, monitor, "applied observation", func(snapshot ports.RemoteDirectorySnapshot) bool {
				host, ok := snapshot.Find("user@arch")
				return ok && host.Availability == domain.RemoteAvailabilityReachable
			})
			host, _ := snapshot.Find("user@arch")
			if len(host.Sessions) != 1 || host.Sessions[0].Name != "work" {
				t.Fatalf("host = %+v, want applied new inventory without ghost", host)
			}
			// Exactly two observations were ever admitted for the
			// endpoint: the stale result retired silently instead of
			// clearing the new check and triggering a third. A short
			// advance stays inside the healthy cadence, so only
			// registry polls (ignored here) may arrive.
			clock.Advance(time.Second)
			requireNoObserve(t, runtime.incoming, "duplicate observe after fencing")
		})
	}
}

func TestMonitorRegistryFailurePreservesHosts(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")
	observe := receiveObserve(t, runtime.incoming, "catalog observe")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: observe.Generation,
		Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "work"}}},
	}
	waitWake(t, sub.Changed(), "success publication")
	// Drain the admitted cache store.
	store := receiveJob(t, runtime.incoming, "cache store")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheStore, Entries: store.Entries}

	clock.Advance(registryPollInterval)
	receiveJob(t, runtime.incoming, "registry poll")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Err: errMonitorTestFailure}
	waitWake(t, sub.Changed(), "registry failure publication")
	snapshot := monitor.Snapshot()
	if !snapshot.Initialized || snapshot.RegistryError == nil {
		t.Fatalf("snapshot = %+v, want initialized with registry error", snapshot)
	}
	host, ok := snapshot.Find("user@arch")
	if !ok || host.Availability != domain.RemoteAvailabilityReachable || len(host.Sessions) != 1 {
		t.Fatalf("registry failure must preserve known host: %+v", snapshot.Hosts)
	}
}

func TestMonitorSingleObservationPerEndpoint(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	receiveObserve(t, runtime.incoming, "catalog observe")

	// Hints, polls and large clock advances never admit a second concurrent
	// check for the same endpoint.
	monitor.RequestReconcile("user@arch")
	monitor.RegistryChanged()
	clock.Advance(time.Hour)
	requireNoObserve(t, runtime.incoming, "duplicate concurrent observe")
}

func TestMonitorHintPermitsOneFollowUp(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")
	observe := receiveObserve(t, runtime.incoming, "catalog observe")

	// A verify hint during the in-flight check converts to exactly one
	// follow-up without forcing a registry read: the hinted endpoint is
	// re-checked as soon as its current check completes.
	monitor.RequestReconcile("user@arch")
	clock.Advance(time.Second)
	requireNoJob(t, runtime.incoming, "registry read forced by verify hint")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: observe.Generation,
		Catalog: catalogue.RemoteCatalog{},
	}
	followUp := receiveObserve(t, runtime.incoming, "follow-up observe")
	if followUp.Generation == observe.Generation {
		t.Fatalf("follow-up reused generation %d", followUp.Generation)
	}

	// No further follow-up without another hint: the host returns to the
	// healthy cadence after the follow-up completes.
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: followUp.Generation,
		Catalog: catalogue.RemoteCatalog{},
	}
	waitSnapshot(t, monitor, "follow-up publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
		host, ok := snapshot.Find("user@arch")
		return ok && host.Availability == domain.RemoteAvailabilityReachable && !host.Checking
	})
	clock.Advance(13 * time.Second)
	requireNoObserve(t, runtime.incoming, "unhinted extra observe")
}

// TestMonitorVerifyHintAdmitsIdleHost proves a verify hint checks an idle
// endpoint immediately without waiting for its cadence and without forcing
// a registry read.
func TestMonitorVerifyHintAdmitsIdleHost(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")
	observe := receiveObserve(t, runtime.incoming, "catalog observe")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: observe.Generation,
		Catalog: catalogue.RemoteCatalog{},
	}
	waitSnapshot(t, monitor, "observe publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
		host, ok := snapshot.Find("user@arch")
		return ok && !host.Checking
	})

	// The host is idle with its next check 15s out. A verify hint admits
	// a check on the next pass; no registry read is forced.
	monitor.RequestReconcile("user@arch")
	hinted := receiveObserve(t, runtime.incoming, "hinted observe")
	if hinted.Registration != registration {
		t.Fatalf("hinted observe = %+v, want registration %+v", hinted.Registration, registration)
	}
	requireNoJob(t, runtime.incoming, "registry read forced by verify hint")
}

// TestMonitorRegistryChangedExpeditesPoll proves a registry mutation hint
// expedites the next registry read without waiting for the poll cadence.
func TestMonitorRegistryChangedExpeditesPoll(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")
	receiveObserve(t, runtime.incoming, "catalog observe")

	// One second later the periodic poll is not due, but the mutation
	// hint expedites it.
	clock.Advance(time.Second)
	monitor.RegistryChanged()
	poll := receiveJob(t, runtime.incoming, "expedited registry poll")
	if poll.Kind != ports.RemoteJobRegistryRead {
		t.Fatalf("poll = %+v, want registry_read", poll)
	}
}

func TestMonitorCacheStoreSupersession(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")

	observe := receiveObserve(t, runtime.incoming, "catalog observe")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: observe.Generation,
		Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "v1"}}},
	}
	waitWake(t, sub.Changed(), "success publication")
	firstStore := receiveJob(t, runtime.incoming, "cache store")
	if len(firstStore.Entries) != 1 || len(firstStore.Entries[0].Sessions) != 1 || firstStore.Entries[0].Sessions[0].Name != "v1" {
		t.Fatalf("first store = %+v, want v1 inventory", firstStore.Entries)
	}

	// The writer stays blocked: a newer success supersedes the pending
	// snapshot instead of queueing behind it.
	clock.Advance(17 * time.Second)
	second := receiveObserve(t, runtime.incoming, "second observe")
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "user@arch",
		Registration: registration, Generation: second.Generation,
		Catalog: catalogue.RemoteCatalog{Sessions: []catalogue.RemoteCatalogSession{{Name: "v2"}}},
	}
	waitWake(t, sub.Changed(), "second success publication")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheStore, Entries: firstStore.Entries}
	// The superseding write is admitted next; unrelated lane traffic such
	// as registry polls may arrive first on the shared job channel.
	var superseding ports.RemoteJob
	deadline := time.After(5 * time.Second)
	for superseding.Kind != ports.RemoteJobCacheStore {
		select {
		case superseding = <-runtime.incoming:
		case <-deadline:
			t.Fatal("timed out waiting for superseding store")
		}
	}
	if len(superseding.Entries) != 1 || len(superseding.Entries[0].Sessions) != 1 || superseding.Entries[0].Sessions[0].Name != "v2" {
		t.Fatalf("superseding store = %+v, want newest v2 inventory without loss", superseding.Entries)
	}
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheStore, Entries: superseding.Entries}
}

func TestMonitorCooperativeCancel(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor := NewMonitor(runtime, clock, nil)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- monitor.Run(ctx) }()
	receiveJob(t, runtime.incoming, "registry read")
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

// systemTestClock is a real-time ports.Clock for the bounded spin test:
// blocked lanes must park the loop however time advances.
type systemTestClock struct{}

func (systemTestClock) Now() time.Time { return time.Now() }

func (systemTestClock) NewTimer(d time.Duration) ports.Timer {
	return systemTestTimer{Timer: time.NewTimer(d)}
}

type systemTestTimer struct {
	*time.Timer
}

func (t systemTestTimer) C() <-chan time.Time { return t.Timer.C }

// TestMonitorBlockedRegistryDoesNotSpin proves a blocked initial registry
// read cannot busy-spin the policy loop: with real time running and no
// completion ever arriving, only the single initial admission happens.
func TestMonitorBlockedRegistryDoesNotSpin(t *testing.T) {
	runtime := newFakeRuntime()
	monitor := NewMonitor(runtime, systemTestClock{}, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- monitor.Run(ctx) }()
	// The initial registry read and cache load are admitted once; the
	// blocked lanes never complete, so no further work can become
	// admittable and the loop must park instead of spinning.
	admissions := 0
	deadline := time.After(300 * time.Millisecond)
	collecting := true
	for collecting {
		select {
		case <-runtime.incoming:
			admissions++
		case <-deadline:
			collecting = false
		}
	}
	if admissions != 2 {
		t.Fatalf("admissions = %d, want exactly the initial registry read and cache load", admissions)
	}
}

// TestMonitorInitialRegistryFailureRetriesBounded proves a failed first
// registry read publishes the error without hosts and retries on the
// bounded poll cadence instead of spinning.
func TestMonitorInitialRegistryFailureRetriesBounded(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Err: errMonitorTestFailure}
	snapshot := waitSnapshot(t, monitor, "registry failure publication", func(snapshot ports.RemoteDirectorySnapshot) bool {
		return snapshot.RegistryError != nil
	})
	if snapshot.Initialized || len(snapshot.Hosts) != 0 {
		t.Fatalf("failed initial read = %+v, want uninitialized without hosts", snapshot)
	}
	// No immediate retry while fake time is held: the failure backs off
	// to the poll cadence.
	clock.Advance(time.Second)
	requireNoJob(t, runtime.incoming, "immediate registry retry")
	clock.Advance(registryPollInterval)
	retry := receiveJob(t, runtime.incoming, "registry retry")
	if retry.Kind != ports.RemoteJobRegistryRead {
		t.Fatalf("retry = %+v, want registry_read", retry)
	}
}

// TestMonitorCacheLoadFailureRetriesBounded proves a failed cache load
// retries on the bounded cadence instead of spinning or stalling the
// registry bootstrap.
func TestMonitorCacheLoadFailureRetriesBounded(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad, Err: errMonitorTestFailure}
	registration := testRegistration("user@arch", 1)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{registration}}
	waitWake(t, sub.Changed(), "registry publication")
	receiveObserve(t, runtime.incoming, "catalog observe")

	// The failed load is retried on the poll cadence, not immediately.
	clock.Advance(time.Second)
	requireNoJob(t, runtime.incoming, "immediate cache retry")
	clock.Advance(registryPollInterval)
	// A periodic registry poll may win the admission race; the cache
	// retry must still arrive on its bounded cadence.
	deadline := time.After(5 * time.Second)
	for {
		select {
		case job := <-runtime.incoming:
			if job.Kind == ports.RemoteJobCacheLoad {
				goto retried
			}
		case <-deadline:
			t.Fatal("timed out waiting for cache retry")
		}
	}
retried:
}

// TestMonitorObserveDispatchesOldestDueFirst proves pool fairness: with
// every lane busy except one credit, the endpoint whose check is most
// overdue wins over an alphabetically earlier endpoint due later.
func TestMonitorObserveDispatchesOldestDueFirst(t *testing.T) {
	clock := newFakeClock(time.Unix(1_000, 0))
	runtime := newFakeRuntime()
	monitor, _ := startMonitor(t, clock, runtime)
	sub := monitor.Subscribe()
	defer sub.Close()

	receiveJob(t, runtime.incoming, "registry read")
	receiveJob(t, runtime.incoming, "cache load")
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobCacheLoad}
	waitWake(t, sub.Changed(), "cache publication")
	arch := testRegistration("arch", 1)
	mule := testRegistration("mule", 2)
	runtime.outgoing <- ports.RemoteJobResult{Kind: ports.RemoteJobRegistryRead, Registrations: []domain.RemoteRegistration{arch, mule}}
	waitWake(t, sub.Changed(), "registry publication")
	// Both due now: alphabetical tiebreak admits arch first.
	first := receiveObserve(t, runtime.incoming, "first observe")
	if first.Endpoint != "arch" {
		t.Fatalf("first observe = %q, want arch", first.Endpoint)
	}
	second := receiveObserve(t, runtime.incoming, "second observe")
	if second.Endpoint != "mule" {
		t.Fatalf("second observe = %q, want mule", second.Endpoint)
	}
	// Arch succeeds (healthy cadence ~15s); mule fails (retry ~5s).
	// Six seconds later only mule is due, even though arch sorts first.
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "arch",
		Registration: arch, Generation: first.Generation,
		Catalog: catalogue.RemoteCatalog{},
	}
	runtime.outgoing <- ports.RemoteJobResult{
		Kind: ports.RemoteJobCatalogObserve, Endpoint: "mule",
		Registration: mule, Generation: second.Generation,
		FailureKind: domain.RemoteFailureTransport, Err: errMonitorTestFailure,
	}
	waitSnapshot(t, monitor, "observations", func(snapshot ports.RemoteDirectorySnapshot) bool {
		archHost, ok := snapshot.Find("arch")
		if !ok || archHost.Checking {
			return false
		}
		muleHost, ok := snapshot.Find("mule")
		return ok && !muleHost.Checking
	})
	clock.Advance(6 * time.Second)
	next := receiveObserve(t, runtime.incoming, "oldest-due observe")
	if next.Endpoint != "mule" {
		t.Fatalf("next observe = %q, want mule (overdue retry beats alphabetical order)", next.Endpoint)
	}
}

func TestRetryDelaySequence(t *testing.T) {
	tests := []struct {
		name     string
		failures uint
		want     time.Duration
	}{
		{name: "healthy", failures: 0, want: 15 * time.Second},
		{name: "first retry", failures: 1, want: 5 * time.Second},
		{name: "second retry", failures: 2, want: 10 * time.Second},
		{name: "third retry", failures: 3, want: 20 * time.Second},
		{name: "fourth retry", failures: 4, want: 40 * time.Second},
		{name: "capped", failures: 5, want: 60 * time.Second},
		{name: "stays capped", failures: 100, want: 60 * time.Second},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := retryDelay(test.failures); got != test.want {
				t.Fatalf("retryDelay(%d) = %v, want %v", test.failures, got, test.want)
			}
		})
	}
}

func TestJitteredIsDeterministicAndBounded(t *testing.T) {
	for _, base := range []time.Duration{15 * time.Second, 5 * time.Second, 60 * time.Second} {
		first := jittered(base, "user@arch", 3)
		if second := jittered(base, "user@arch", 3); first != second {
			t.Fatalf("jittered(%v) not deterministic: %v vs %v", base, first, second)
		}
		lower := base * 90 / 100
		upper := base + base*10/100
		if first < lower || first > upper {
			t.Fatalf("jittered(%v) = %v, want within [%v, %v]", base, first, lower, upper)
		}
	}
	if jittered(0, "user@arch", 1) != 0 {
		t.Fatal("zero base must stay zero")
	}
}
