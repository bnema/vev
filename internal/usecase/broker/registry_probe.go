package broker

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/fnv"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

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
// A disabled registry never observes, so the demand is dropped without
// scheduling anything. An empty endpoint requests a local re-probe instead of a
// configured remote: the broker's own machine daemon is re-observed ahead of
// its scheduled freshness window, honored by dispatchLocalLocked exactly like
// pending honors remote demand. A registry with no local observation producer
// drops it, just like an unknown remote endpoint is dropped.
func (r *Registry) RequestProbe(endpoint string) {
	if r.observationDisabled {
		return
	}
	r.mu.Lock()
	if endpoint == "" {
		if r.local != nil {
			r.localPending = true
		}
	} else if _, ok := r.hosts[endpoint]; ok {
		r.pending[endpoint] = true
	}
	r.mu.Unlock()
	r.hint()
}

// SetDemand records one live client subscription opening (active=true) or
// closing (active=false). internal/usecase/broker/service.go calls it from
// Service.Subscribe and serviceSubscription.Close, so the demand count tracks
// exactly the connections a client is actively watching. The first
// subscription requests one immediate observation per host; while demand
// stays positive, the demand cadence schedules later observations. With no
// subscription, only explicit RequestProbe calls dispatch. A disabled registry
// still counts subscriptions but never observes.
func (r *Registry) SetDemand(active bool) {
	r.mu.Lock()
	if active {
		if r.demand == 0 {
			r.localPending = r.local != nil
			for endpoint := range r.hosts {
				r.pending[endpoint] = true
			}
		}
		r.demand++
	} else if r.demand > 0 {
		r.demand--
	}
	r.mu.Unlock()
	r.hint()
}

// freshForLocked returns the demand cadence while subscribed. An absent
// daemon uses the longer freshFor interval instead. Callers must hold r.mu.
func (r *Registry) freshForLocked(local bool) time.Duration {
	if r.demand <= 0 {
		return r.freshFor
	}
	if local {
		return r.demandFreshForLocal
	}
	return r.demandFreshForRemote
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
//
// An observation-disabled registry runs the same lifecycle and drain but never
// schedules observation: it arms no timer, starts no probe, and drops every
// reconcile. It still owns and drains its durable writer, so a shutdown never
// returns before the newest staged publication has reached the store.
func (r *Registry) Run(ctx context.Context) {
	if !r.running.CompareAndSwap(runIdle, runActive) {
		r.log.Warn("broker: registry Run refused; single-run already started or finished")
		return
	}
	defer r.settle()
	if r.observationDisabled {
		<-ctx.Done()
		return
	}
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
	if r.localAttempt != nil {
		r.localAttempt.cancel()
		r.localAttempt = nil
	}
	cleared := false
	if r.local != nil && r.localHost.Checking {
		r.localHost.Checking = false
		cleared = true
	}
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
	r.running.Store(runStopped)
	r.mu.Unlock()
	r.store.close()
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
	snapshot     ports.BrokerDaemonObservation
	err          error
	at           time.Time
	// local marks the result of one local probe attempt, which carries no
	// endpoint or registration and is applied to the configured local entry.
	local bool
}

func (r *Registry) dispatch(ctx context.Context, now time.Time, results chan<- probeResult) {
	r.mu.Lock()
	if r.demand == 0 && len(r.pending) == 0 && !r.localPending {
		r.mu.Unlock()
		return
	}
	started := false
	for endpoint, host := range r.hosts {
		if _, inflight := r.inflight[endpoint]; inflight {
			continue
		}
		explicit := r.pending[endpoint]
		if !explicit && (r.demand == 0 || !host.NextDue.IsZero() && now.Before(host.NextDue)) {
			continue
		}
		delete(r.pending, endpoint)
		r.attempts++
		attempt := r.attempts
		probeCtx, cancel := context.WithCancel(ctx)
		r.inflight[endpoint] = &probeAttempt{token: attempt, cancel: cancel}
		if visibleCheck(host, explicit) {
			started = true
			host.Checking = true
		}
		host.LastAttempt = now
		r.hosts[endpoint] = host
		registration := host.Registration
		// The deadline is armed here, under r.mu and at dispatch time, so the
		// attempt's bound starts with the attempt rather than whenever its
		// goroutine is scheduled.
		deadline := r.clock.NewTimer(r.probeTimeout)
		go func(endpoint string, registration domain.RemoteRegistration, attempt uint64, probeCtx context.Context) {
			snapshot, err := observeBounded(probeCtx, deadline, func(ctx context.Context) (ports.BrokerDaemonObservation, error) {
				return r.probe.Probe(ctx, registration)
			})
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
	// Only an observation that visibly started changes the projection; an
	// idle dispatch or a routine refresh must not bump the revision or wake
	// subscribers. The in-flight projection is never persisted: Checking is
	// transient.
	if r.dispatchLocalLocked(ctx, now, results) {
		started = true
	}
	if started {
		r.publishLocked(false)
	}
	r.mu.Unlock()
}

// visibleCheck reports whether starting an observation publishes the transient
// Checking state. A routine scheduled refresh of a reachable daemon stays
// silent until its result lands, so an idle watched broker publishes once per
// observation instead of twice; an explicit request or a daemon that is not
// confirmed reachable still shows that it is being checked.
func visibleCheck(host ports.BrokerDaemonObservation, explicit bool) bool {
	return explicit || host.Availability != domain.RemoteAvailabilityReachable
}

// observeBounded runs one observation under its attempt deadline. retire is the
// attempt's retirement context: a replaced, removed, or settled attempt returns
// at once and its completion is fenced by the caller. The probe itself runs on
// a context derived from retire that the deadline also cancels, so a
// ctx-honoring probe unwinds its dial, handshake, or exchange when either
// fires. The attempt never waits for the probe past its deadline: a probe that
// ignores cancellation is abandoned (its late result is discarded) and the
// attempt still reports errProbeTimeout, so the endpoint's single in-flight
// slot is always released.
func observeBounded(retire context.Context, deadline ports.Timer, observe func(context.Context) (ports.BrokerDaemonObservation, error)) (ports.BrokerDaemonObservation, error) {
	defer deadline.Stop()
	probeCtx, cancel := context.WithCancelCause(retire)
	defer cancel(nil)
	type outcome struct {
		snapshot ports.BrokerDaemonObservation
		err      error
	}
	done := make(chan outcome, 1)
	go func() {
		snapshot, err := observe(probeCtx)
		done <- outcome{snapshot: snapshot, err: err}
	}()
	select {
	case result := <-done:
		return result.snapshot, result.err
	case <-deadline.C():
		cancel(errProbeTimeout)
		return ports.BrokerDaemonObservation{}, errProbeTimeout
	case <-retire.Done():
		return ports.BrokerDaemonObservation{}, retire.Err()
	}
}

func (r *Registry) apply(result probeResult) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if result.local {
		r.applyLocal(result)
		return
	}
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
	if !failed && availability == domain.RemoteAvailabilityNoDaemon {
		current.Availability = availability
		current.LastSuccess = result.at
		// An absent daemon is stable: demand does not require a fresh SSH
		// handshake every two seconds just to confirm it is still absent.
		current.NextDue = result.at.Add(r.jitter(r.freshFor, result.endpoint, result.attempt))
		current.ConsecutiveFailures = 0
		current.LastFailure = domain.RemoteFailure{}
		current.Identity = ""
		current.Incarnation = ports.BrokerDaemonIncarnation{}
		current.ProtocolVersion = 0
		current.Capabilities = 0
		current.InventoryKnown = false
		current.Sessions = nil
		r.hosts[result.endpoint] = current
		r.publishLocked(true)
		return
	}
	if !failed {
		// Configured authority comes only from the registry's own projection:
		// a probe result supplies observed state, never endpoint, registration,
		// policy, display origin, or rank.
		observed := stampObservationAuthority(result.snapshot.Clone(), current)
		observed.Checking = false
		observed.LastAttempt = result.at
		observed.LastSuccess = result.at
		observed.NextDue = result.at.Add(r.jitter(r.freshForLocked(false), result.endpoint, result.attempt))
		observed.ConsecutiveFailures = 0
		observed.LastFailure = domain.RemoteFailure{}
		observed.FailureEpisode = current.FailureEpisode
		// The published observation must be fully valid once authority is
		// stamped: a probe that invents partial identity (identity without a
		// protocol version) or an inventory without a known catalogue is an
		// invalid response, never a published observation. The durable rule is
		// re-checked too: a projection the offline store would reject is never
		// published as if it had persisted. The store applies the same rule.
		if err := observed.Validate(); err != nil {
			r.log.Debug("broker: observation is invalid", "endpoint", result.endpoint, "err", err)
			kind, availability, failed = domain.RemoteFailureInvalidResponse, domain.RemoteAvailabilityInvalidResponse, true
		} else if err := ports.ValidateDurableHostProjection(observed); err != nil {
			r.log.Debug("broker: observation is not persistable", "endpoint", result.endpoint, "err", err)
			kind, availability, failed = domain.RemoteFailureInvalidResponse, domain.RemoteAvailabilityInvalidResponse, true
		} else {
			r.hosts[result.endpoint] = observed
			r.publishLocked(true)
			return
		}
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
	delay := r.retryDelay(current.ConsecutiveFailures)
	if r.demand > 0 {
		delay = demandRetryDelay(r.demandFreshForRemote, current.ConsecutiveFailures)
	}
	current.NextDue = result.at.Add(r.jitter(delay, result.endpoint, result.attempt))
	r.log.Debug("broker_probe_failed", "endpoint", result.endpoint, "kind", kind, "failures", current.ConsecutiveFailures, "err", result.err, "cause", errors.Unwrap(result.err))
	r.hosts[result.endpoint] = current
	r.publishLocked(true)
}

// observationOutcome classifies a completed observation. It reports
// failed=false only for a confirmed reachable result; every other outcome
// carries the typed failure kind and the availability to publish. The caller
// additionally checks the adopted projection against the durable rules
// (ports.ValidateDurableHostProjection), which cover the session bound and
// catalogue validity for the exact incarnation and success time.
func observationOutcome(result probeResult) (domain.RemoteFailureKind, domain.RemoteAvailability, bool) {
	if result.err != nil {
		kind := domain.RemoteFailureTransport
		var typed domain.RemoteFailure
		var brokerErr ports.BrokerError
		switch {
		case errors.As(result.err, &typed) && typed.Kind != domain.RemoteFailureNone:
			// The dialer classified the failure (for example SSH authentication).
			kind = typed.Kind
		case errors.As(result.err, &brokerErr) && (brokerErr.Code == ports.BrokerErrorConflictingPolicy || brokerErr.Code == ports.BrokerErrorIncompatible):
			// SSH worked; the daemon's identity or version was refused.
			kind = domain.RemoteFailureIncompatible
		case errors.Is(result.err, context.DeadlineExceeded):
			kind = domain.RemoteFailureTimeout
		}
		return kind, availabilityFor(kind), true
	}
	switch result.snapshot.Availability {
	case domain.RemoteAvailabilityReachable:
		return domain.RemoteFailureNone, domain.RemoteAvailabilityReachable, false
	case domain.RemoteAvailabilityNoDaemon:
		return domain.RemoteFailureNone, domain.RemoteAvailabilityNoDaemon, false
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

// demandRetryDelay is the watched-host failure backoff: the demand cadence
// doubled per consecutive failure, capped at demandRetryMax.
func demandRetryDelay(base time.Duration, failures uint) time.Duration {
	delay := base
	for i := uint(1); i < failures && delay < demandRetryMax; i++ {
		delay *= 2
	}
	return min(delay, max(base, demandRetryMax))
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
	if r.demand == 0 {
		if r.localPending || len(r.pending) > 0 {
			return 0
		}
		return time.Hour
	}
	var earliest time.Time
	if r.local != nil && r.localAttempt == nil {
		if r.localHost.NextDue.IsZero() {
			return 0
		}
		if earliest.IsZero() || r.localHost.NextDue.Before(earliest) {
			earliest = r.localHost.NextDue
		}
	}
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
