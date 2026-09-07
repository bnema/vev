package remotes

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Monitor is the remote-specific observation owner: it runs the single remote
// policy loop, maintains per-registration check state, and publishes an
// immutable directory snapshot. It is not a reusable lifecycle engine. The
// constructor only sets up memory and injected ports; Run owns execution.
//
// Memory publication uses an immutable snapshot behind an atomic pointer.
// Policy mutation happens on the service loop. Rendering never waits for that
// loop, an I/O lock, or a worker.
type Monitor struct {
	runtime ports.RemoteRuntime
	clock   ports.Clock
	log     *slog.Logger

	current atomic.Pointer[ports.RemoteDirectorySnapshot]

	mu          sync.Mutex
	revision    uint64
	initialized bool
	subscribers map[*directorySubscription]struct{}
	reconcileCh chan struct{}
	// pendingVerify coalesces endpoint-specific verification requests
	// from RequestReconcile; pendingRegistry records a RegistryChanged
	// mutation hint. Both are drained by the loop; the mutex only guards
	// this handoff, never policy state.
	pendingVerify   map[string]struct{}
	pendingRegistry bool
}

// NewMonitor wires a Monitor from its runtime and clock. It performs no I/O
// and starts no goroutines; daemon composition owns starting and stopping it
// through Run.
func NewMonitor(runtime ports.RemoteRuntime, clock ports.Clock, log *slog.Logger) *Monitor {
	if log == nil {
		log = slog.Default()
	}
	m := &Monitor{
		runtime:       runtime,
		clock:         clock,
		log:           log,
		subscribers:   make(map[*directorySubscription]struct{}),
		reconcileCh:   make(chan struct{}, 1),
		pendingVerify: make(map[string]struct{}),
	}
	m.current.Store(&ports.RemoteDirectorySnapshot{Hosts: []ports.RemoteHostSnapshot{}})
	return m
}

var _ ports.RemoteDirectory = (*Monitor)(nil)

// Snapshot returns a defensive copy of the newest publication. It performs no
// I/O and never waits for the service loop.
func (m *Monitor) Snapshot() ports.RemoteDirectorySnapshot {
	return m.current.Load().Clone()
}

// Subscribe registers one capacity-one wake channel owned by the caller.
func (m *Monitor) Subscribe() ports.RemoteDirectorySubscription {
	sub := &directorySubscription{changed: make(chan struct{}, 1), onClose: m.unsubscribe}
	m.mu.Lock()
	m.subscribers[sub] = struct{}{}
	m.mu.Unlock()
	return sub
}

func (m *Monitor) unsubscribe(sub *directorySubscription) {
	m.mu.Lock()
	delete(m.subscribers, sub)
	m.mu.Unlock()
}

// RequestReconcile queues a coalesced verification hint for one endpoint.
// The loop checks that endpoint on its next pass without forcing a
// registry read; a hint for an in-flight check converts to exactly one
// follow-up. It never blocks and never performs a synchronous refresh.
func (m *Monitor) RequestReconcile(endpoint string) {
	m.mu.Lock()
	if endpoint != "" {
		if len(m.pendingVerify) < maxPendingVerifyHints {
			m.pendingVerify[endpoint] = struct{}{}
		}
	} else {
		m.pendingRegistry = true
	}
	m.mu.Unlock()
	m.hint()
}

// RegistryChanged queues a coalesced immediate registry hint after a mutation
// in the owning process. Registry reads stay the only membership authority;
// verification hints are tracked separately.
func (m *Monitor) RegistryChanged() {
	m.mu.Lock()
	m.pendingRegistry = true
	m.mu.Unlock()
	m.hint()
}

func (m *Monitor) hint() {
	select {
	case m.reconcileCh <- struct{}{}:
	default:
	}
}

// drainHints moves coalesced cross-goroutine requests into loop-owned
// policy state. Verification hints merge per endpoint; a registry mutation
// hint expedites the next registry read without touching verification.
func (m *Monitor) drainHints(state *serviceState) {
	m.mu.Lock()
	for endpoint := range m.pendingVerify {
		if len(state.verifyHints) >= maxPendingVerifyHints {
			break
		}
		state.verifyHints[endpoint] = struct{}{}
	}
	clear(m.pendingVerify)
	if m.pendingRegistry {
		state.registryExpedite = true
		m.pendingRegistry = false
	}
	m.mu.Unlock()
}

// Run is the single remote policy loop. It bootstraps registry and cache
// reads independently, admits bounded observation work, applies fenced
// results, and publishes snapshots. It returns when ctx is cancelled after at
// most two seconds of runtime cleanup joining.
func (m *Monitor) Run(ctx context.Context) error {
	if m.runtime == nil {
		return nil
	}
	jobs := make(chan ports.RemoteJob, maxInFlightJobs)
	results := make(chan ports.RemoteJobResult, maxInFlightJobs)
	runtimeDone := make(chan error, 1)
	go func() { runtimeDone <- m.runtime.Run(ctx, jobs, results) }()

	m.loop(ctx, jobs, results)

	shutdown := m.clock.NewTimer(2 * time.Second)
	defer shutdown.Stop()
	// The loop already exited on cancellation; wait for cooperative
	// runtime teardown or the bounded join, whichever comes first.
	// There is no early return on ctx.Done here: the context is already
	// cancelled, so such a branch would skip the join entirely.
	select {
	case err := <-runtimeDone:
		return err
	case <-shutdown.C():
		return nil
	}
}

func (m *Monitor) loop(ctx context.Context, jobs chan<- ports.RemoteJob, results <-chan ports.RemoteJobResult) {
	state := newServiceState()
	timer := m.clock.NewTimer(0)
	defer timer.Stop()
	for {
		now := m.clock.Now()
		if !m.dispatch(ctx, now, state, jobs) {
			return
		}
		markExpired(now, state)
		if state.dirty {
			state.dirty = false
			m.publish(state)
		}
		wakeNow := m.clock.Now()
		wait := state.nextWakeup(wakeNow)
		delay := wait.Sub(wakeNow)
		if delay < 0 {
			delay = 0
		}
		timer.Reset(delay)
		select {
		case <-ctx.Done():
			return
		case <-timer.C():
		case <-m.reconcileCh:
			m.drainHints(state)
		case result, ok := <-results:
			if !ok {
				return
			}
			m.applyResult(m.clock.Now(), state, result)
		}
	}
}

// dispatch admits due work within lane credits. A successful job-channel send
// transfers exactly one credit until exactly one completion retires it. It
// reports false when cancellation wins over admission: the loop must stop.
// Observations dispatch oldest-due-first with an endpoint tiebreak so a busy
// pool cannot starve the tail while the head recycles.
func (m *Monitor) dispatch(ctx context.Context, now time.Time, state *serviceState, jobs chan<- ports.RemoteJob) bool {
	admit := func(job ports.RemoteJob) bool {
		select {
		case jobs <- job:
			return true
		case <-ctx.Done():
			return false
		}
	}
	if state.registryCredit && (!state.registryDone || state.registryExpedite || now.Sub(state.lastRegistryRead) >= registryPollInterval) {
		if state.registryExpedite {
			state.expeditedRead = true
		}
		state.registryCredit = false
		state.registryExpedite = false
		if !admit(ports.RemoteJob{Kind: ports.RemoteJobRegistryRead}) {
			return false
		}
	}
	if state.cacheCredit && !state.cacheDone && !now.Before(state.cacheRetryAt) {
		state.cacheCredit = false
		if !admit(ports.RemoteJob{Kind: ports.RemoteJobCacheLoad}) {
			return false
		}
	}
	// Verification hints convert to exactly one follow-up when the
	// endpoint's own request is still outstanding, and are consumed.
	for endpoint, host := range state.hosts {
		if _, hinted := state.verifyHints[endpoint]; !hinted {
			continue
		}
		if rec, outstanding := state.inFlight[endpoint]; outstanding {
			if rec.registration.Equal(host.registration) {
				host.followUp = true
			}
			delete(state.verifyHints, endpoint)
		} else if host.observing {
			host.followUp = true
			delete(state.verifyHints, endpoint)
		}
	}
	type dueCandidate struct {
		endpoint string
		due      time.Time
	}
	var candidates []dueCandidate
	for endpoint, host := range state.hosts {
		if _, outstanding := state.inFlight[endpoint]; outstanding || host.observing {
			continue
		}
		_, hinted := state.verifyHints[endpoint]
		if !hinted && now.Before(host.nextDue) {
			continue
		}
		due := host.nextDue
		if hinted {
			due = time.Time{}
		}
		candidates = append(candidates, dueCandidate{endpoint: endpoint, due: due})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].due.Equal(candidates[j].due) {
			return candidates[i].endpoint < candidates[j].endpoint
		}
		return candidates[i].due.Before(candidates[j].due)
	})
	for _, candidate := range candidates {
		if state.observeCredits <= 0 {
			break
		}
		host := state.hosts[candidate.endpoint]
		if host == nil {
			continue
		}
		delete(state.verifyHints, candidate.endpoint)
		host.attempts++
		host.observing = true
		host.observeGeneration = domain.RemoteGeneration(host.attempts)
		host.lastAttempt = now
		state.inFlight[candidate.endpoint] = inFlightRequest{
			registration: host.registration,
			generation:   host.observeGeneration,
		}
		state.observeCredits--
		// Admission is itself a visible transition: the host starts
		// checking. Publish it rather than leaving the check
		// invisible until an unrelated revision.
		state.dirty = true
		if !admit(ports.RemoteJob{
			Kind:         ports.RemoteJobCatalogObserve,
			Endpoint:     candidate.endpoint,
			Registration: host.registration,
			Generation:   host.observeGeneration,
			Deadline:     observeTimeout,
		}) {
			return false
		}
	}
	if state.storeCredit && !state.storeActive && state.storePending && !now.Before(state.storeRetryAt) {
		state.storeCredit = false
		state.storeActive = true
		entries := state.pendingStore
		state.pendingStore = nil
		state.storePending = false
		state.stored = entries
		if !admit(ports.RemoteJob{Kind: ports.RemoteJobCacheStore, Entries: entries}) {
			return false
		}
	}
	return true
}

// applyResult retires one admission credit and applies fenced results.
// Observation completions first retire the exact admitted request: unknown
// or superseded requests retire their credit without touching policy
// state, and a result for a removed or re-registered endpoint fences out
// entirely instead of mutating the new registration.
func (m *Monitor) applyResult(now time.Time, state *serviceState, result ports.RemoteJobResult) {
	switch result.Kind {
	case ports.RemoteJobRegistryRead:
		state.registryCredit = true
		state.lastRegistryRead = now
		expedited := state.expeditedRead
		state.expeditedRead = false
		if result.Err != nil {
			// Registry load failure preserves known registrations and
			// retries on the bounded poll cadence instead of spinning:
			// the attempt completed, so only the load error publishes.
			state.registryDone = true
			state.registryError = result.Err
			state.lastRegistryRead = now
			state.dirty = true
			return
		}
		state.registryDone = true
		state.registryInit = true
		state.registryError = nil
		if m.reconcileRegistrations(now, state, result.Registrations) {
			state.dirty = true
		}
		if expedited {
			// A reconcile hint during an in-flight check permits one
			// follow-up, not another concurrent check: only checks
			// still bound to the current registration qualify.
			for endpoint, host := range state.hosts {
				if !host.observing {
					continue
				}
				if rec, ok := state.inFlight[endpoint]; ok && rec.registration.Equal(host.registration) {
					host.followUp = true
				}
			}
		}
	case ports.RemoteJobCacheLoad:
		state.cacheCredit = true
		state.cacheDone = true
		if result.Err != nil {
			m.log.Debug("remote cache load failed, retrying on bounded cadence", "err", result.Err)
			state.cacheDone = false
			state.cacheRetryAt = now.Add(registryPollInterval)
			state.dirty = true
			return
		}
		state.pendingCache = result.Entries
		m.seedPendingCache(state)
		state.dirty = true
	case ports.RemoteJobCatalogObserve:
		if state.observeCredits < maxObserveCredits {
			state.observeCredits++
		}
		request, outstanding := state.inFlight[result.Endpoint]
		if !outstanding {
			// Unknown host or late duplicate completion: retire the
			// credit without touching policy state.
			return
		}
		if request.generation != result.Generation || !request.registration.Equal(result.Registration) {
			// Not the admitted request: a stale duplicate or foreign
			// result. It retires no ownership, so the outstanding
			// request stays fenced until its own completion.
			return
		}
		delete(state.inFlight, result.Endpoint)
		host := state.hosts[result.Endpoint]
		if host == nil || !host.registration.Equal(request.registration) {
			// Removed or re-registered while in flight: the new policy
			// is already due and needs no poke from the old request.
			return
		}
		host.observing = false
		if result.Err != nil {
			m.applyObserveFailure(now, state, host, result)
			return
		}
		m.applyObserveSuccess(now, state, host, result)
	case ports.RemoteJobCacheStore:
		state.storeCredit = true
		state.storeActive = false
		failed := state.stored
		state.stored = nil
		if result.Err != nil {
			m.log.Debug("remote cache store failed, retrying on bounded cadence", "err", result.Err)
			if !state.storePending {
				// Nothing newer superseded the failed payload: retry
				// it once the backoff elapses instead of hot-looping.
				state.pendingStore = failed
				state.storePending = true
			}
			state.storeRetryAt = now.Add(registryPollInterval)
		} else {
			state.lastPersisted = failed
		}
	}
}

// reconcileRegistrations diffs a fresh registry read against policy state.
// Added endpoints start unknown and due immediately; removed endpoints drop
// their policy but never their outstanding request ownership, so in-flight
// results fence out; re-added endpoints with a new incarnation reset as new
// registrations with observing cleared, since any outstanding check belongs
// to the previous registration. It reports whether membership, identity or
// rank changed so identical polls never publish a revision on their own.
func (m *Monitor) reconcileRegistrations(now time.Time, state *serviceState, registrations []domain.RemoteRegistration) bool {
	seen := make(map[string]struct{}, len(registrations))
	changed := false
	for i, record := range registrations {
		if record.IsZero() {
			continue
		}
		seen[record.Endpoint] = struct{}{}
		host, ok := state.hosts[record.Endpoint]
		if !ok {
			state.hosts[record.Endpoint] = &hostPolicy{
				registration: record,
				rank:         i,
				availability: domain.RemoteAvailabilityUnknown,
				nextDue:      now,
			}
			changed = true
			continue
		}
		if host.rank != i {
			host.rank = i
			changed = true
		}
		if host.registration.Equal(record) {
			host.registration = record
			continue
		}
		state.hosts[record.Endpoint] = &hostPolicy{
			registration: record,
			rank:         i,
			availability: domain.RemoteAvailabilityUnknown,
			nextDue:      now,
		}
		changed = true
	}
	for endpoint := range state.hosts {
		if _, ok := seen[endpoint]; !ok {
			delete(state.hosts, endpoint)
			changed = true
		}
	}
	for hinted := range state.verifyHints {
		if _, ok := seen[hinted]; !ok {
			delete(state.verifyHints, hinted)
		}
	}
	if m.seedPendingCache(state) {
		changed = true
	}
	m.queueStore(state)
	return changed
}

// seedPendingCache applies held cache entries to matching registrations that
// have no newer successful inventory, preserving the cached fetch time as
// the inventory age. It reports whether any entry became visible, so the
// registry path publishes seeded sessions even when the registrations
// themselves are unchanged. Unbound entries and entries for unknown endpoints are
// ignored, never assigned to a different registration.
func (m *Monitor) seedPendingCache(state *serviceState) bool {
	if len(state.pendingCache) == 0 || !state.registryDone {
		return false
	}
	remaining := make([]catalogue.RemoteCatalogCacheEntry, 0, len(state.pendingCache))
	seeded := false
	for _, entry := range state.pendingCache {
		if entry.Incarnation == [16]byte{} {
			continue
		}
		host, ok := state.hosts[entry.Host]
		if !ok || host.inventoryConfirmed || host.cacheSeeded || host.registration.Incarnation != entry.Incarnation {
			remaining = append(remaining, entry)
			continue
		}
		host.sessions = append([]catalogue.RemoteCatalogSession(nil), entry.Sessions...)
		host.inventoryKnown = true
		host.lastSuccess = entry.FetchedAt
		host.cacheSeeded = true
		seeded = true
	}
	state.pendingCache = remaining
	return seeded
}

// applyObserveSuccess records a reachable host, replaces inventory on a
// successful empty response, and schedules the next healthy check with
// deterministic jitter.
func (m *Monitor) applyObserveSuccess(now time.Time, state *serviceState, host *hostPolicy, result ports.RemoteJobResult) {
	host.availability = domain.RemoteAvailabilityReachable
	host.consecutiveFailures = 0
	host.lastFailure = domain.RemoteFailure{}
	host.lastSuccess = now
	host.sessions = append([]catalogue.RemoteCatalogSession(nil), result.Catalog.Sessions...)
	host.inventoryKnown = true
	host.inventoryConfirmed = true
	host.expiryNotified = false
	if host.followUp {
		host.followUp = false
		host.nextDue = now
	} else {
		host.nextDue = now.Add(jittered(healthyCheckInterval, host.registration.Endpoint, host.attempts))
	}
	m.queueStore(state)
	state.dirty = true
}

// applyObserveFailure retains prior inventory with its age, records the typed
// failure, and schedules a capped retry with deterministic jitter.
func (m *Monitor) applyObserveFailure(now time.Time, state *serviceState, host *hostPolicy, result ports.RemoteJobResult) {
	kind := result.FailureKind
	if kind == domain.RemoteFailureNone {
		kind = domain.RemoteFailureTransport
	}
	host.availability = availabilityFor(kind)
	host.consecutiveFailures++
	host.lastFailure = domain.RemoteFailure{Kind: kind, Err: result.Err}
	if host.followUp {
		host.followUp = false
		host.nextDue = now
	} else {
		host.nextDue = now.Add(jittered(retryDelay(host.consecutiveFailures), host.registration.Endpoint, host.attempts))
	}
	state.dirty = true
}

// queueStore stages the newest known inventories for serialized
// persistence, superseding any unstarted pending snapshot. Confirmed and
// seeded inventories are both retained so one host's successful check never
// discards another host's still-useful cached rows; unchanged payloads
// never stage duplicate writes. A blocked writer never blocks registry or
// observation lanes.
func (m *Monitor) queueStore(state *serviceState) {
	entries := make([]catalogue.RemoteCatalogCacheEntry, 0, len(state.hosts))
	for _, endpoint := range state.orderedEndpoints() {
		host := state.hosts[endpoint]
		if !host.inventoryKnown {
			continue
		}
		entries = append(entries, catalogue.RemoteCatalogCacheEntry{
			Host:        endpoint,
			FetchedAt:   host.lastSuccess,
			Incarnation: host.registration.Incarnation,
			Sessions:    append([]catalogue.RemoteCatalogSession(nil), host.sessions...),
		})
	}
	if len(entries) == 0 && len(state.lastPersisted) == 0 {
		// Initial emptiness is not a write: only a removal that
		// drops a previously persisted payload stages an empty
		// snapshot (distinguished by lastPersisted above).
		return
	}
	if state.storePending && cacheEntriesEqual(entries, state.pendingStore) {
		return
	}
	if !state.storePending && state.storeActive && cacheEntriesEqual(entries, state.stored) {
		return
	}
	if !state.storePending && !state.storeActive && cacheEntriesEqual(entries, state.lastPersisted) {
		return
	}
	state.pendingStore = entries
	state.storePending = true
}

// markExpired flags newly elapsed inventories so expiry publishes a
// revision on its own transition instead of riding an unrelated one.
func markExpired(now time.Time, state *serviceState) {
	for _, host := range state.hosts {
		if host.inventoryConfirmed && !host.expiryNotified && !now.Before(host.lastSuccess.Add(inventoryExpiryAge)) {
			state.dirty = true
		}
	}
}

// publish renders and installs a new immutable snapshot, then wakes
// subscribers without blocking.
func (m *Monitor) publish(state *serviceState) {
	m.mu.Lock()
	m.revision++
	revision := m.revision
	m.initialized = m.initialized || state.registryInit
	initialized := m.initialized
	subscribers := make([]*directorySubscription, 0, len(m.subscribers))
	for sub := range m.subscribers {
		subscribers = append(subscribers, sub)
	}
	m.mu.Unlock()
	m.current.Store(snapshotPointer(state.buildSnapshot(revision, initialized)))
	for _, sub := range subscribers {
		select {
		case sub.changed <- struct{}{}:
		default:
		}
	}
	// Inventory expiry publishes a revision independently of overlay
	// activity: mark notified episodes after publication.
	now := m.clock.Now()
	for _, host := range state.hosts {
		if host.inventoryConfirmed && !host.expiryNotified && !now.Before(host.lastSuccess.Add(inventoryExpiryAge)) {
			host.expiryNotified = true
		}
	}
}

func snapshotPointer(snapshot ports.RemoteDirectorySnapshot) *ports.RemoteDirectorySnapshot {
	return &snapshot
}

// directorySubscription is one subscriber-owned capacity-one wake channel.
type directorySubscription struct {
	changed chan struct{}
	once    sync.Once
	onClose func(*directorySubscription)
}

// Changed wakes when a newer snapshot is available.
func (s *directorySubscription) Changed() <-chan struct{} { return s.changed }

// Close unregisters the subscription. It is idempotent.
func (s *directorySubscription) Close() {
	s.once.Do(func() {
		if s.onClose != nil {
			s.onClose(s)
		}
	})
}
