package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Observation cadence. The broker deliberately mirrors the legacy remotes
// monitor cadence (internal/usecase/remotes) so a fleet observed through
// either path refreshes on the same schedule. The duplication is temporary:
// the registry is intended to replace that monitor, and these constants move
// to a single owner once the old monitor is removed.
const (
	defaultFreshFor   = 15 * time.Second
	defaultRetryBase  = 5 * time.Second
	defaultRetryLimit = time.Minute
)

// Run lifecycle states. A registry runs exactly once: runIdle is the zero
// value, runActive marks the single admitted run, and runStopped permanently
// refuses further runs.
const (
	runIdle int32 = iota
	runActive
	runStopped
)

// Registry owns configured hosts and their immutable observation projection.
type Registry struct {
	epoch ports.BrokerEpoch
	probe ports.BrokerHostProbe
	clock ports.Clock
	log   *slog.Logger
	store *snapshotWriter

	current  atomic.Pointer[ports.BrokerSnapshot]
	mu       sync.Mutex
	revision ports.BrokerRevision
	// attempts is the registry-wide monotonic attempt token source. Every
	// started observation claims one token, and only the token currently
	// recorded as in flight may complete. Tokens are never reused, so a stale
	// completion is fenced even when the endpoint is removed and re-added with
	// the identical registration.
	attempts uint64
	hosts    map[string]ports.RemoteHostSnapshot
	// inflight records the one admitted attempt per endpoint. The record
	// carries the attempt's derived context cancel so a retired attempt is
	// released the moment its registration is replaced, removed, or the run
	// settles; a ctx-honoring probe therefore stops promptly and churn can
	// never accumulate overlapping probes for one endpoint.
	inflight   map[string]*probeAttempt
	pending    map[string]bool
	tombstones map[string]ports.BrokerHostTombstone
	subs       map[*subscription]struct{}
	wake       chan struct{}
	running    atomic.Int32

	freshFor  time.Duration
	retryBase time.Duration
	retryMax  time.Duration
	jitter    func(base time.Duration, endpoint string, attempt uint64) time.Duration
}

// NewRegistry restores a validated durable snapshot under a fresh broker epoch.
func NewRegistry(epoch ports.BrokerEpoch, store ports.BrokerSnapshotStore, probe ports.BrokerHostProbe, clock ports.Clock, log *slog.Logger) (*Registry, error) {
	if epoch == 0 || probe == nil || clock == nil {
		return nil, errors.New("broker: invalid registry dependencies")
	}
	if log == nil {
		log = slog.Default()
	}
	r := &Registry{
		epoch: epoch, probe: probe, clock: clock, log: log,
		hosts: make(map[string]ports.RemoteHostSnapshot), inflight: make(map[string]*probeAttempt),
		pending: make(map[string]bool), tombstones: make(map[string]ports.BrokerHostTombstone),
		subs: make(map[*subscription]struct{}), wake: make(chan struct{}, 1),
		freshFor: defaultFreshFor, retryBase: defaultRetryBase, retryMax: defaultRetryLimit,
		jitter: jittered,
	}
	if store != nil {
		r.store = newSnapshotWriter(store, log)
		snapshot, err := store.Load()
		if err != nil {
			return nil, err
		}
		if err := r.restore(snapshot); err != nil {
			return nil, err
		}
	}
	r.publishLocked(false)
	return r, nil
}

// restore adopts a validated durable snapshot. A zero-epoch snapshot is only
// the empty placeholder a store returns before its first write: any record
// without an epoch is rejected instead of silently dropped. A persisted
// snapshot from the registry's own epoch is rejected as well: every broker
// process samples a fresh epoch and restarts its revision series at one, so
// adopting same-epoch revisions would make the restored series non-monotonic
// (and a stale same-epoch record indistinguishable from current authority).
// In-flight observation state and retirement tombstones are per-process
// fencing state and never survive a restart.
func (r *Registry) restore(snapshot ports.BrokerSnapshot) error {
	if snapshot.Epoch == 0 {
		if len(snapshot.Hosts) > 0 || len(snapshot.Removed) > 0 || snapshot.Revision != 0 {
			return errors.New("broker: persisted snapshot has no epoch")
		}
		return nil
	}
	if snapshot.Epoch == r.epoch {
		return errors.New("broker: persisted snapshot epoch matches the new registry epoch")
	}
	if err := snapshot.Validate(); err != nil {
		return err
	}
	for _, host := range snapshot.Hosts {
		host = host.Clone()
		host.Checking = false
		r.hosts[host.Endpoint] = host
	}
	return nil
}

// Snapshot returns a defensive immutable copy without I/O.
func (r *Registry) Snapshot() ports.BrokerSnapshot { return r.current.Load().Clone() }

// Subscribe returns a capacity-one notification stream. Slow consumers never
// block publication and always re-read the newest snapshot.
func (r *Registry) Subscribe() ports.BrokerSubscription {
	s := &subscription{changed: make(chan struct{}, 1)}
	s.close = func() {
		r.mu.Lock()
		delete(r.subs, s)
		r.mu.Unlock()
	}
	r.mu.Lock()
	r.subs[s] = struct{}{}
	r.mu.Unlock()
	s.offer()
	return s
}

// SetHosts atomically replaces configured membership. An unchanged membership
// is a no-op: it neither republishes nor restarts an in-flight observation.
// Removed registrations retire bounded tombstones so a stale completion can
// never revive them; replaced or re-added endpoints are fenced by the
// registry attempt token instead.
func (r *Registry) SetHosts(registrations []domain.RemoteRegistration) error {
	if len(registrations) > ports.BrokerMaxHosts {
		return fmt.Errorf("broker: %d host registrations exceed the limit of %d", len(registrations), ports.BrokerMaxHosts)
	}
	next := make(map[string]ports.RemoteHostSnapshot, len(registrations))
	for _, registration := range registrations {
		if err := registration.Validate(); err != nil {
			return err
		}
		if _, duplicate := next[registration.Endpoint]; duplicate {
			return errors.New("broker: duplicate host registration")
		}
		next[registration.Endpoint] = ports.RemoteHostSnapshot{Endpoint: registration.Endpoint, Registration: registration, Availability: domain.RemoteAvailabilityUnknown}
	}

	r.mu.Lock()
	if registrationsUnchanged(r.hosts, next) {
		r.mu.Unlock()
		return nil
	}
	for endpoint, current := range r.hosts {
		candidate, present := next[endpoint]
		if present && current.Registration.Equal(candidate.Registration) {
			// Unchanged registration: keep the live observation as is.
			next[endpoint] = current
			continue
		}
		// A retired registration owns no observation: cancelling its attempt
		// releases a ctx-honoring probe immediately, and dropping its record
		// means the retired completion can never clear or answer the
		// replacement, and the replacement is never blocked behind it. The
		// cancellation happens under the registry lock, so no replacement
		// attempt can start before the retired context is cancelled.
		r.cancelInflightLocked(endpoint)
		delete(r.pending, endpoint)
		if present {
			// The endpoint stays live under a new registration, so its
			// retirement is never published as a tombstone alongside a live
			// host; the attempt token fences the stale completion.
			delete(r.tombstones, endpoint)
			continue
		}
		r.retireLocked(current.Registration)
	}
	for endpoint := range next {
		// Re-adding an endpoint supersedes its tombstone: a live host and a
		// tombstone for the same endpoint are mutually exclusive.
		delete(r.tombstones, endpoint)
	}
	r.hosts = next
	r.publishLocked(true)
	r.mu.Unlock()
	r.hint()
	return nil
}

// cancelInflightLocked cancels the in-flight attempt for one endpoint and
// drops its record. Callers must hold r.mu: the retired attempt's context is
// cancelled before the record disappears, so no replacement attempt can start
// while the retired probe's context is still live.
func (r *Registry) cancelInflightLocked(endpoint string) {
	if attempt := r.inflight[endpoint]; attempt != nil {
		attempt.cancel()
		delete(r.inflight, endpoint)
	}
}

// cancelAllInflightLocked cancels every in-flight attempt and drops the
// records. Callers must hold r.mu.
func (r *Registry) cancelAllInflightLocked() {
	for endpoint, attempt := range r.inflight {
		attempt.cancel()
		delete(r.inflight, endpoint)
	}
}

// RequestProbe is non-blocking and coalesces concurrent demand per endpoint.
func (r *Registry) RequestProbe(endpoint string) {
	r.mu.Lock()
	if _, ok := r.hosts[endpoint]; ok {
		r.pending[endpoint] = true
	}
	r.mu.Unlock()
	r.hint()
}

func (r *Registry) hint() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// Run schedules observations until cancellation. Run owns the registry
// lifecycle and may execute exactly once: a second call is refused so a
// concurrent or restarted caller can never duplicate scheduling. On return it
// clears in-flight observation state, publishes the settled projection, and
// flushes durable persistence before any further write is refused.
func (r *Registry) Run(ctx context.Context) {
	if !r.running.CompareAndSwap(runIdle, runActive) {
		r.log.Warn("broker: registry Run refused; single-run already started or finished")
		return
	}
	defer r.settle()
	timer := r.clock.NewTimer(0)
	defer timer.Stop()
	results := make(chan probeResult, ports.BrokerMaxHosts)
	for {
		now := r.clock.Now()
		r.dispatch(ctx, now, results)
		timer.Reset(r.nextDelay(now))
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		case <-timer.C():
		case result := <-results:
			if ctx.Err() != nil {
				// Shutdown wins: a cancellation is never recorded as an
				// observation failure.
				return
			}
			r.apply(result)
		}
	}
}

// settle is Run's cleanup: nothing stays in flight, the published projection
// carries no transient Checking flag, durable persistence is flushed, and
// every later Run call is refused.
func (r *Registry) settle() {
	r.mu.Lock()
	r.cancelAllInflightLocked()
	cleared := false
	for endpoint, host := range r.hosts {
		if !host.Checking {
			continue
		}
		host.Checking = false
		r.hosts[endpoint] = host
		cleared = true
	}
	if cleared {
		r.publishLocked(true)
	}
	r.mu.Unlock()
	if r.store != nil {
		r.store.close()
	}
	r.running.Store(runStopped)
}

// probeAttempt is one admitted observation: the registry-wide token that
// fences its completion, and the cancel for the context derived from the run
// context. Cancelling the derived context retires the attempt: a probe that
// honors cancellation returns promptly, so a replaced or removed endpoint
// never accumulates overlapping probes, and settle releases every attempt
// before the run loop exits.
type probeAttempt struct {
	token  uint64
	cancel context.CancelFunc
}

// probeResult is one completed observation bound to the exact attempt token
// that started it.
type probeResult struct {
	endpoint     string
	registration domain.RemoteRegistration
	attempt      uint64
	snapshot     ports.RemoteHostSnapshot
	err          error
	at           time.Time
}

func (r *Registry) dispatch(ctx context.Context, now time.Time, results chan<- probeResult) {
	r.mu.Lock()
	started := false
	for endpoint, host := range r.hosts {
		if _, inflight := r.inflight[endpoint]; inflight {
			continue
		}
		if !r.pending[endpoint] && !host.NextDue.IsZero() && now.Before(host.NextDue) {
			continue
		}
		delete(r.pending, endpoint)
		r.attempts++
		attempt := r.attempts
		probeCtx, cancel := context.WithCancel(ctx)
		r.inflight[endpoint] = &probeAttempt{token: attempt, cancel: cancel}
		started = true
		host.Checking = true
		host.LastAttempt = now
		r.hosts[endpoint] = host
		registration := host.Registration
		go func(endpoint string, registration domain.RemoteRegistration, attempt uint64, probeCtx context.Context) {
			snapshot, err := r.probe.Probe(probeCtx, registration)
			result := probeResult{endpoint: endpoint, registration: registration, attempt: attempt, snapshot: snapshot, err: err, at: r.clock.Now()}
			select {
			case results <- result:
			case <-probeCtx.Done():
				// The attempt was retired (replaced, removed, or shutdown), so
				// its completion is already fenced: dropping it keeps churn from
				// queueing results the run loop would only discard.
			}
		}(endpoint, registration, attempt, probeCtx)
	}
	// Only an observation that actually started changes the projection; an
	// idle dispatch must not bump the revision or wake subscribers. The
	// in-flight projection is never persisted: Checking is transient.
	if started {
		r.publishLocked(false)
	}
	r.mu.Unlock()
}

func (r *Registry) apply(result probeResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	current, ok := r.hosts[result.endpoint]
	attempt := r.inflight[result.endpoint]
	if !ok || !current.Registration.Equal(result.registration) || attempt == nil || attempt.token != result.attempt {
		// A stale completion touches neither current authority nor the
		// in-flight slot, which may already belong to a newer registration or
		// attempt.
		return
	}
	delete(r.inflight, result.endpoint)
	// The probe has returned, so releasing its derived context here only
	// frees the attempt's resources; the record is gone before any new
	// attempt can start.
	attempt.cancel()
	current.Checking = false
	current.LastAttempt = result.at

	kind, availability, failed := observationOutcome(result)
	if !failed {
		// Reachable: adopt the observed inventory, but registry-owned
		// accounting (success time, attempt counters, failure episode) stays
		// local.
		observed := result.snapshot.Clone()
		observed.Endpoint = result.endpoint
		observed.Registration = result.registration
		observed.Checking = false
		observed.LastAttempt = result.at
		observed.LastSuccess = result.at
		observed.NextDue = result.at.Add(r.jitter(r.freshFor, result.endpoint, result.attempt))
		observed.ConsecutiveFailures = 0
		observed.LastFailure = domain.RemoteFailure{}
		observed.FailureEpisode = current.FailureEpisode
		r.hosts[result.endpoint] = observed
		r.publishLocked(true)
		return
	}
	// Every non-reachable outcome is a typed failure on the capped retry
	// cadence: the last success, last-known inventory, and failure episode
	// counter stay authoritative.
	current.Availability = availability
	if current.ConsecutiveFailures == 0 {
		current.FailureEpisode++
	}
	current.ConsecutiveFailures++
	current.LastFailure = domain.RemoteFailure{Kind: kind, Err: result.err}
	current.NextDue = result.at.Add(r.jitter(r.retryDelay(current.ConsecutiveFailures), result.endpoint, result.attempt))
	r.hosts[result.endpoint] = current
	r.publishLocked(true)
}

// observationOutcome classifies a completed observation. It reports
// failed=false only for a confirmed reachable result; every other outcome
// carries the typed failure kind and the availability to publish.
func observationOutcome(result probeResult) (domain.RemoteFailureKind, domain.RemoteAvailability, bool) {
	if result.err != nil {
		kind := domain.RemoteFailureTransport
		if errors.Is(result.err, context.DeadlineExceeded) {
			kind = domain.RemoteFailureTimeout
		}
		return kind, availabilityFor(kind), true
	}
	// A projection carrying more sessions than the publication bound is a
	// malformed response: publishing it would produce a snapshot that fails
	// BrokerSnapshot.Validate. Classify the whole projection as an invalid
	// response so the last-known inventory stays authoritative.
	if len(result.snapshot.Sessions) > ports.BrokerMaxSessionsPerHost {
		return domain.RemoteFailureInvalidResponse, domain.RemoteAvailabilityInvalidResponse, true
	}
	switch result.snapshot.Availability {
	case domain.RemoteAvailabilityReachable:
		return domain.RemoteFailureNone, domain.RemoteAvailabilityReachable, false
	case domain.RemoteAvailabilityIncompatible:
		return domain.RemoteFailureIncompatible, domain.RemoteAvailabilityIncompatible, true
	case domain.RemoteAvailabilityAuthFailed:
		return domain.RemoteFailureAuthentication, domain.RemoteAvailabilityAuthFailed, true
	case domain.RemoteAvailabilityInvalidResponse:
		return domain.RemoteFailureInvalidResponse, domain.RemoteAvailabilityInvalidResponse, true
	default:
		// Unknown or Unreachable without an error still means reachability was
		// not confirmed, so it retries on the typed backoff cadence.
		return domain.RemoteFailureTransport, domain.RemoteAvailabilityUnreachable, true
	}
}

// availabilityFor maps a typed failure kind to the published availability.
// The typed kind is preserved separately for notice policy.
func availabilityFor(kind domain.RemoteFailureKind) domain.RemoteAvailability {
	switch kind {
	case domain.RemoteFailureAuthentication:
		return domain.RemoteAvailabilityAuthFailed
	case domain.RemoteFailureIncompatible:
		return domain.RemoteAvailabilityIncompatible
	case domain.RemoteFailureInvalidResponse:
		return domain.RemoteAvailabilityInvalidResponse
	default:
		return domain.RemoteAvailabilityUnreachable
	}
}

// retryDelay is the capped exponential retry cadence for a failure streak.
func (r *Registry) retryDelay(failures uint) time.Duration {
	delay := r.retryBase
	for i := uint(1); i < failures; i++ {
		if delay >= r.retryMax {
			return r.retryMax
		}
		delay *= 2
	}
	if delay > r.retryMax {
		return r.retryMax
	}
	return delay
}

// jittered spreads a base interval deterministically across endpoint and
// attempt: identical inputs always yield the identical duration, and the
// result stays within 90%-110% of the base. No wall clock or global
// randomness participates, so retry and healthy-refresh schedules are
// reproducible instead of racing a shared PRNG.
func jittered(base time.Duration, endpoint string, attempt uint64) time.Duration {
	if base <= 0 {
		return base
	}
	h := fnv.New32a()
	h.Write([]byte(endpoint))
	var counter [8]byte
	binary.BigEndian.PutUint64(counter[:], attempt)
	h.Write(counter[:])
	spread := int64(h.Sum32() % 21)
	return base*90/100 + time.Duration(spread)*base/100
}

func (r *Registry) nextDelay(now time.Time) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	var earliest time.Time
	for endpoint, host := range r.hosts {
		// An observation already in flight is completed by its result, never
		// by the schedule; waiting on it here would spin the run loop.
		if _, inflight := r.inflight[endpoint]; inflight {
			continue
		}
		if r.pending[endpoint] || host.NextDue.IsZero() {
			return 0
		}
		if earliest.IsZero() || host.NextDue.Before(earliest) {
			earliest = host.NextDue
		}
	}
	if earliest.IsZero() {
		return time.Hour
	}
	if delay := earliest.Sub(now); delay > 0 {
		return delay
	}
	return 0
}

// registrationsUnchanged reports exact membership equality: the same
// endpoints bound to the same registration identities. Projection fields such
// as availability, checking, or inventory never participate.
func registrationsUnchanged(current, next map[string]ports.RemoteHostSnapshot) bool {
	if len(current) != len(next) {
		return false
	}
	for endpoint, candidate := range next {
		existing, ok := current[endpoint]
		if !ok || !existing.Registration.Equal(candidate.Registration) {
			return false
		}
	}
	return true
}

// retireLocked records the tombstone for one removed registration at the
// revision of the publication that carries it.
func (r *Registry) retireLocked(registration domain.RemoteRegistration) {
	retired := r.revision + 1
	if retired == 0 {
		retired = 1
	}
	r.tombstones[registration.Endpoint] = ports.BrokerHostTombstone{
		Endpoint:        registration.Endpoint,
		Registration:    registration,
		RetiredRevision: retired,
	}
	r.pruneTombstonesLocked()
}

// pruneTombstonesLocked keeps only the newest BrokerMaxTombstones retirements
// so an endpoint that churns can never grow the published set without bound.
func (r *Registry) pruneTombstonesLocked() {
	for len(r.tombstones) > ports.BrokerMaxTombstones {
		oldest := ""
		var oldestRevision ports.BrokerRevision
		for endpoint, tombstone := range r.tombstones {
			if oldest == "" || tombstone.RetiredRevision < oldestRevision ||
				(tombstone.RetiredRevision == oldestRevision && endpoint < oldest) {
				oldest, oldestRevision = endpoint, tombstone.RetiredRevision
			}
		}
		delete(r.tombstones, oldest)
	}
}

// tombstonesLocked renders the bounded tombstone publication, newest first.
// Live endpoints are never published as tombstones.
func (r *Registry) tombstonesLocked() []ports.BrokerHostTombstone {
	if len(r.tombstones) == 0 {
		return nil
	}
	out := make([]ports.BrokerHostTombstone, 0, len(r.tombstones))
	for endpoint, tombstone := range r.tombstones {
		if _, live := r.hosts[endpoint]; live {
			continue
		}
		out = append(out, tombstone)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].RetiredRevision != out[j].RetiredRevision {
			return out[i].RetiredRevision > out[j].RetiredRevision
		}
		return out[i].Endpoint < out[j].Endpoint
	})
	if len(out) > ports.BrokerMaxTombstones {
		out = out[:ports.BrokerMaxTombstones]
	}
	return out
}

func (r *Registry) publishLocked(persist bool) {
	r.revision++
	hosts := make([]ports.RemoteHostSnapshot, 0, len(r.hosts))
	for _, host := range r.hosts {
		hosts = append(hosts, host.Clone())
	}
	sort.Slice(hosts, func(i, j int) bool { return hosts[i].Endpoint < hosts[j].Endpoint })
	snapshot := ports.BrokerSnapshot{Epoch: r.epoch, Revision: r.revision, Hosts: hosts, Removed: r.tombstonesLocked()}
	r.current.Store(&snapshot)
	if persist && r.store != nil {
		// enqueue only stages an immutable publication: rendering the durable
		// copy and every store call happen on the writer goroutine, never
		// under the registry lock and never on the probe schedule.
		r.store.enqueue(snapshot)
	}
	for sub := range r.subs {
		sub.offer()
	}
}

// durable returns the persistable copy of a publication, rendered by the
// writer goroutine. Checking is transient in-flight state and never reaches
// the durable snapshot.
func durable(snapshot ports.BrokerSnapshot) ports.BrokerSnapshot {
	out := snapshot.Clone()
	for i := range out.Hosts {
		out.Hosts[i].Checking = false
	}
	return out
}

// snapshotWriter serializes durable snapshot writes off the registry lock.
// Enqueue never blocks on store I/O and always coalesces to the newest
// revision; Store calls run on one goroutine, so the durable store never sees
// an out-of-order write. A failed write is logged and retried by the next
// publication, so a store fault never stalls publication. Close flushes the
// newest pending snapshot, stops the writer, and waits, so no write outlives
// shutdown.
type snapshotWriter struct {
	store ports.BrokerSnapshotStore
	log   *slog.Logger

	mu      sync.Mutex
	pending *ports.BrokerSnapshot
	written ports.BrokerRevision
	started bool
	closed  bool
	wake    chan struct{}
	stop    chan struct{}
	done    chan struct{}
}

func newSnapshotWriter(store ports.BrokerSnapshotStore, log *slog.Logger) *snapshotWriter {
	return &snapshotWriter{
		store: store, log: log,
		wake: make(chan struct{}, 1), stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// enqueue stages a snapshot for durable persistence. It coalesces to the
// newest revision, starts the writer on first use, and never blocks on the
// store or copies session payloads. Enqueue after Close is refused: nothing is
// written after shutdown.
func (w *snapshotWriter) enqueue(snapshot ports.BrokerSnapshot) {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		w.log.Debug("broker: durable snapshot write refused after shutdown", "revision", snapshot.Revision)
		return
	}
	if w.pending != nil {
		if snapshot.Revision <= w.pending.Revision {
			w.mu.Unlock()
			return
		}
	} else if snapshot.Revision <= w.written {
		w.mu.Unlock()
		return
	}
	// The publication is immutable after publication, so staging it is a value
	// copy; the defensive durable copy is rendered on the writer goroutine.
	w.pending = &snapshot
	if !w.started {
		w.started = true
		go w.run()
	}
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *snapshotWriter) run() {
	defer close(w.done)
	for {
		select {
		case <-w.stop:
			w.flush()
			return
		case <-w.wake:
			w.flush()
		}
	}
}

// flush writes the newest staged snapshot, skipping revisions the store
// already holds.
func (w *snapshotWriter) flush() {
	for {
		w.mu.Lock()
		snapshot := w.pending
		written := w.written
		w.pending = nil
		w.mu.Unlock()
		if snapshot == nil {
			return
		}
		if snapshot.Revision <= written {
			continue
		}
		if err := w.store.Store(durable(*snapshot)); err != nil {
			w.log.Error("broker: durable snapshot write failed", "revision", snapshot.Revision, "err", err)
			continue
		}
		w.mu.Lock()
		if snapshot.Revision > w.written {
			w.written = snapshot.Revision
		}
		w.mu.Unlock()
	}
}

// close flushes the newest staged snapshot, stops the writer, and waits for
// the drain to finish. It never returns before the in-flight write completes.
func (w *snapshotWriter) close() {
	w.mu.Lock()
	if w.closed {
		w.mu.Unlock()
		return
	}
	w.closed = true
	started := w.started
	w.mu.Unlock()
	if !started {
		return
	}
	close(w.stop)
	<-w.done
}

type subscription struct {
	changed chan struct{}
	once    sync.Once
	close   func()
}

func (s *subscription) Changed() <-chan struct{} { return s.changed }
func (s *subscription) Close()                   { s.once.Do(s.close) }
func (s *subscription) offer() {
	select {
	case s.changed <- struct{}{}:
	default:
	}
}
