package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Broker-owned local observation (Plan 001 P5.3a).
//
// The broker's own machine daemon is not a configured host: it has no
// endpoint, no registration, and no durable membership. It is observed through
// the same probe lifecycle as a remote host, but its configured authority
// (display origin and policy) is supplied by the composition and its entry is
// always published first, at index zero, and never persisted. Observing the
// local daemon only reads: the probe dials the broker-owned local route and
// completes the daemonmux physical preamble, so it never opens a logical
// session stream (never attaches) and never starts a stopped daemon.

// localProbeKey seeds the deterministic jitter for the local probe schedule.
// It is a fixed token, so local retry and refresh jitter is reproducible and
// independent of the configured remotes. A remote endpoint spelled "local"
// shares the seed, which affects only the jitter spread and never identity.
const localProbeKey = "local"

// LocalProbe observes the broker's own machine daemon without attaching to it
// or starting a stopped daemon. It reports only observed state: the observed
// daemon identity, incarnation, protocol version, capabilities, availability,
// failure, freshness, and session inventory. Configured authority (the local
// flag, display origin, policy, and rank) is stamped by the registry and never
// trusted from the probe; unknown identity, version, or inventory is zero and
// is never invented. The returned observation must satisfy
// ports.BrokerDaemonObservation.Validate once the registry stamps authority.
//
// ProbeLocal must honor cancellation: the registry cancels the attempt context
// when the run settles, and a probe that ignores cancellation delays shutdown.
type LocalProbe interface {
	ProbeLocal(ctx context.Context) (ports.BrokerDaemonObservation, error)
}

// LocalObservation configures observation of the broker's own machine daemon.
// The display origin and policy are configured authority: the registry stamps
// them onto every local publication and never reads them from a probe. The
// probe is the sole observed-state source.
type LocalObservation struct {
	// DisplayOrigin is the presentation hint published for the local entry. It
	// is a display value only and never routing authority.
	DisplayOrigin string
	// Policy is the exact connection policy of the local binding.
	Policy ports.BrokerPolicy
	// Probe observes the local daemon. It is required.
	Probe LocalProbe
}

// validate refuses a local observation the registry could not publish or fence.
func (l *LocalObservation) validate() error {
	if l == nil {
		return nil
	}
	if nilDependency(l.Probe) {
		return errors.New("broker: local observation has no probe")
	}
	if err := l.Policy.Validate(); err != nil {
		return fmt.Errorf("broker: local observation authority: %w", err)
	}
	if l.DisplayOrigin == "" || ports.SanitizeBrokerDisplayText(l.DisplayOrigin) != l.DisplayOrigin {
		return errors.New("broker: local observation has an invalid display origin")
	}
	return nil
}

// observesLocalOnly reports whether this registry observes only the broker's
// own daemon: observation is enabled, a local binding is configured, and no
// remote membership is projected. It is the registry-side answer an authority
// needs, because an authority whose connections expose immutable membership may
// serve an observer only when there is no remote membership to manage.
func (r *Registry) observesLocalOnly() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return !r.observationDisabled && r.local != nil && len(r.hosts) == 0
}

// dispatchLocalLocked admits at most one local probe attempt when its schedule
// says it is due. Callers must hold r.mu. It reports whether an attempt
// started, so dispatch publishes the transient Checking state exactly once.
func (r *Registry) dispatchLocalLocked(ctx context.Context, now time.Time, results chan<- probeResult) bool {
	if r.local == nil || r.localAttempt != nil {
		return false
	}
	if !r.localHost.NextDue.IsZero() && now.Before(r.localHost.NextDue) {
		return false
	}
	r.attempts++
	attempt := r.attempts
	probeCtx, cancel := context.WithCancel(ctx)
	r.localAttempt = &probeAttempt{token: attempt, cancel: cancel}
	r.localHost.Checking = true
	r.localHost.LastAttempt = now
	go func(attempt uint64, probeCtx context.Context) {
		snapshot, err := r.local.Probe.ProbeLocal(probeCtx)
		result := probeResult{local: true, attempt: attempt, snapshot: snapshot, err: err, at: r.clock.Now()}
		select {
		case results <- result:
		case <-probeCtx.Done():
			// The attempt was retired (run settled), so its completion is
			// already fenced: dropping it keeps a settled registry from
			// queueing a result the run loop would only discard.
		}
	}(attempt, probeCtx)
	return true
}

// applyLocal adopts one completed local probe. Callers must hold r.mu, exactly
// as Registry.apply requires. A stale attempt touches nothing. A confirmed
// reachable result publishes the observed state with configured authority
// stamped on top; every other outcome publishes a typed failure and keeps the
// last-known identity and inventory, so an unreachable local daemon never
// reports an invented identity and its availability is never zero.
func (r *Registry) applyLocal(result probeResult) {
	if r.local == nil {
		return
	}
	attempt := r.localAttempt
	if attempt == nil || attempt.token != result.attempt {
		return
	}
	r.localAttempt = nil
	// The probe has returned, so releasing its derived context here only frees
	// the attempt's resources; the record is gone before any new attempt can
	// start.
	attempt.cancel()

	current := r.localHost
	current.Checking = false
	current.LastAttempt = result.at

	kind, availability, failed := observationOutcome(result)
	if !failed {
		observed := stampLocalAuthority(result.snapshot.Clone(), current)
		observed.Checking = false
		observed.LastAttempt = result.at
		observed.LastSuccess = result.at
		observed.NextDue = result.at.Add(r.jitter(r.freshFor, localProbeKey, result.attempt))
		observed.ConsecutiveFailures = 0
		observed.LastFailure = domain.RemoteFailure{}
		observed.FailureEpisode = current.FailureEpisode
		// The published local observation must be fully valid once authority is
		// stamped. A probe that invents partial identity or an invalid session
		// catalogue is an invalid response, never a published observation.
		if err := observed.Validate(); err != nil {
			r.log.Debug("broker: local observation is invalid", "err", err)
			kind, availability, failed = domain.RemoteFailureInvalidResponse, domain.RemoteAvailabilityInvalidResponse, true
		} else if err := validateLocalObservation(observed); err != nil {
			r.log.Debug("broker: local observation is not publishable", "err", err)
			kind, availability, failed = domain.RemoteFailureInvalidResponse, domain.RemoteAvailabilityInvalidResponse, true
		} else {
			r.localHost = observed
			r.publishLocked(true)
			return
		}
	}
	// Every non-reachable outcome is a typed failure on the capped retry
	// cadence: the observed identity, last-known inventory, and failure episode
	// counter stay authoritative.
	current.Availability = availability
	if current.ConsecutiveFailures == 0 {
		current.FailureEpisode++
	}
	current.ConsecutiveFailures++
	current.LastFailure = domain.RemoteFailure{Kind: kind, Err: result.err}
	current.NextDue = result.at.Add(r.jitter(r.retryDelay(current.ConsecutiveFailures), localProbeKey, result.attempt))
	r.localHost = current
	r.publishLocked(true)
}

// stampLocalAuthority copies the configured local authority onto a fresh probe
// result. Probe results supply observed state only, so their local flag,
// endpoint, registration, display origin, policy, and rank fields are discarded
// wholesale: a hostile probe can never promote itself to local authority or
// borrow a remote endpoint.
func stampLocalAuthority(observation, authority ports.BrokerDaemonObservation) ports.BrokerDaemonObservation {
	observation.Local = true
	observation.Endpoint = ""
	observation.Registration = domain.RemoteRegistration{}
	observation.DisplayOrigin = authority.DisplayOrigin
	observation.Policy = authority.Policy
	observation.Rank = authority.Rank
	return observation
}

// validateLocalObservation re-checks the catalogue shape of a local session
// inventory. The durable projection rule cannot apply to a local observation
// (the local daemon is never durable and has no registration incarnation), so
// the catalogue itself is validated directly against the current schema.
func validateLocalObservation(observation ports.BrokerDaemonObservation) error {
	if len(observation.Sessions) == 0 {
		return nil
	}
	return catalogue.ValidateRemoteCatalog(catalogue.RemoteCatalog{
		ProtocolVersion: protocol.Version,
		SchemaVersion:   catalogue.RemoteCatalogSchemaVersion,
		Sessions:        observation.Sessions,
	})
}
