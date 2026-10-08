package broker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
)

// Observation cadence. No client means no scheduled observations; the
// fifteen-second interval only applies to an absent daemon under demand.
const (
	defaultFreshFor   = 15 * time.Second
	defaultRetryBase  = 5 * time.Second
	defaultRetryLimit = time.Minute
	// defaultDemandFreshForLocal and defaultDemandFreshForRemote are the
	// built-in demand cadences RegistryConfig falls back to when
	// DemandFreshForLocal/DemandFreshForRemote is left zero. They apply only
	// while at least one client subscription is live (see Registry.SetDemand).
	defaultDemandFreshForLocal  = 1 * time.Second
	defaultDemandFreshForRemote = 2 * time.Second
	// demandRetryMax caps the failure backoff while a client watches: retries
	// start at the demand cadence and double up to this bound, so an
	// unreachable or refusing host is not redialed every two seconds.
	demandRetryMax = 30 * time.Second
	// defaultProbeTimeout bounds one observation attempt end to end: dial,
	// SSH/QUIC bootstrap, daemonmux preamble, and the catalogue exchange. It
	// sits below the passive retry cap so a peer that never answers costs one
	// bounded attempt instead of pinning the endpoint's single in-flight slot.
	defaultProbeTimeout = 20 * time.Second
)

// errProbeTimeout is the failure an attempt reports when its ports.Clock
// deadline fires before the probe returns. It wraps context.DeadlineExceeded
// so observationOutcome classifies it as a typed timeout.
var errProbeTimeout = fmt.Errorf("broker: observation exceeded its deadline: %w", context.DeadlineExceeded)

// Run lifecycle states. A registry runs exactly once: runIdle is the zero
// value, runActive marks the single admitted run, and runStopped permanently
// refuses further runs.
const (
	runIdle int32 = iota
	runActive
	runStopped
)

// MembershipMode controls whether runtime membership mutation is admitted.
type MembershipMode uint8

const (
	MembershipImmutable MembershipMode = iota
	MembershipMutable
)

// RegistryConfig selects how a Registry runs. The zero value observes hosts
// with immutable membership.
type RegistryConfig struct {
	MembershipMode MembershipMode
	// IncarnationGenerator is an injectable entropy seam for mutable additions.
	// Nil uses crypto/rand.
	IncarnationGenerator func() ([16]byte, error)
	// ObservationDisabled runs the registry as a read-only snapshot owner: it
	// restores and publishes durable state, owns and drains its writer, and
	// honors the Run lifecycle, but issues no probes, arms no timers, and never
	// reconciles. The probe dependency is unused and may be nil. Membership is
	// expected to be immutable for the run; a caller that needs observation
	// keeps the zero-value config.
	ObservationDisabled bool
	// Local, when non-nil, enables observation of the broker's own machine
	// daemon. The registry then publishes the local entry at index zero, ahead
	// of every remote, and never persists it. Local is refused together with
	// ObservationDisabled: a read-only registry owns no local producer.
	Local *LocalObservation
	// DemandFreshForLocal and DemandFreshForRemote are the re-probe cadences
	// apply/applyLocal schedule into NextDue while Registry.SetDemand reports
	// at least one live client subscription. A zero value falls back to
	// defaultDemandFreshForLocal (about 1s) or defaultDemandFreshForRemote
	// (about 2s). With no live subscription, no scheduled probe runs.
	// DemandFreshForRemote is also the first delay of a watched host's
	// failure backoff, which doubles up to max(DemandFreshForRemote, 30s).
	DemandFreshForLocal  time.Duration
	DemandFreshForRemote time.Duration
	// ProbeTimeout bounds every local and remote observation attempt on the
	// registry's ports.Clock. When it fires the attempt is released and
	// recorded as a timeout failure even if the probe never returns. A zero
	// value selects defaultProbeTimeout.
	ProbeTimeout time.Duration
}

// Registry owns configured hosts and their immutable observation projection.
type Registry struct {
	epoch ports.BrokerEpoch
	probe ports.BrokerHostProbe
	clock ports.Clock
	log   *slog.Logger
	store *snapshotWriter
	// observationDisabled is fixed at construction: a disabled registry never
	// observes, so it has no probe, timer, or reconcile work to do.
	observationDisabled bool
	membershipMode      MembershipMode
	newIncarnation      func() ([16]byte, error)

	// hostStore and authority are the synchronous membership owner. Only this
	// registry may mutate the store while it is live; the caller owns Close.
	hostStore ports.BrokerHostStore
	authority ports.BrokerHosts // guarded by mu; independent of publication revision

	current  atomic.Pointer[ports.BrokerSnapshot]
	mu       sync.Mutex
	revision ports.BrokerRevision
	// revisionExhausted records that this epoch consumed its entire revision
	// series. The registry then fails closed: the last valid snapshot stays
	// published, nothing is written to the store, and every mutation path
	// reports ports.ErrBrokerRevisionExhausted instead of wrapping the series to
	// zero or below the newest revision.
	revisionExhausted bool
	// attempts is the registry-wide monotonic attempt token source. Every
	// started observation claims one token, and only the token currently
	// recorded as in flight may complete. Tokens are never reused, so a stale
	// completion is fenced even when the endpoint is removed and re-added with
	// the identical registration.
	attempts uint64
	// hosts is the broker-native projection per endpoint. Configured
	// authority (endpoint, registration, policy, display origin, rank) is
	// stamped here from durable membership; probe results only supply observed
	// state and can never override it.
	hosts map[string]ports.BrokerDaemonObservation
	// order is the publication order of remote hosts: registration order from
	// durable membership. A snapshot carries the local daemon first (when a
	// producer exists) and then these remotes in this order.
	order []string
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

	// local selects observation of the broker's own machine daemon. It is nil
	// for a remote-only registry. localHost is the configured local authority
	// stamped onto every publication (Local, DisplayOrigin, Policy, Rank) plus
	// the observed state a probe supplies; localAttempt is the one admitted
	// local probe. The local entry is never durable and is never invented: until
	// a probe answers, the configured authority is published with unknown
	// availability and zero observed identity.
	local        *LocalObservation
	localHost    ports.BrokerDaemonObservation
	localAttempt *probeAttempt
	// localPending marks explicit demand for a local re-probe ahead of its
	// scheduled freshness window: RequestProbe("") and a completed local control
	// stream or attach both set it. dispatchLocalLocked honors it the same way
	// pending honors remote demand, admitting the attempt before NextDue and
	// clearing the flag the moment the attempt starts.
	localPending bool

	freshFor  time.Duration
	retryBase time.Duration
	retryMax  time.Duration
	jitter    func(base time.Duration, endpoint string, attempt uint64) time.Duration

	// demandFreshForLocal and demandFreshForRemote are the resolved (non-zero)
	// demand cadences; see RegistryConfig.DemandFreshForLocal/Remote.
	demandFreshForLocal  time.Duration
	demandFreshForRemote time.Duration
	// probeTimeout is the resolved (non-zero) per-attempt deadline; see
	// RegistryConfig.ProbeTimeout.
	probeTimeout time.Duration
	// demand counts live client subscriptions: Registry.SetDemand(true) is one
	// subscription opening and SetDemand(false) is one closing. Guarded by mu.
	// While demand > 0, apply and applyLocal schedule subsequent probes.
	demand int
}

// NewRegistryWithConfig restores a validated durable snapshot under a fresh
// broker epoch and selects the run mode from cfg. Observation-disabled mode
// tolerates a nil probe because such a registry never probes, and a registry
// configured with a local observation producer (Local) tolerates a nil remote
// probe as long as it owns no remote membership. Every other dependency is
// required exactly as NewRegistry requires it.
func NewRegistryWithConfig(epoch ports.BrokerEpoch, store ports.BrokerHostStore, probe ports.BrokerHostProbe, clock ports.Clock, log *slog.Logger, cfg RegistryConfig) (*Registry, error) {
	if cfg.MembershipMode != MembershipImmutable && cfg.MembershipMode != MembershipMutable {
		return nil, errors.New("broker: invalid membership mode")
	}
	if epoch == 0 || nilDependency(store) || nilDependency(clock) {
		return nil, errors.New("broker: invalid registry dependencies")
	}
	// A remote probe is required to observe remotes, but a registry that
	// observes only its own machine daemon (Local) may omit it. Such a registry
	// never owns a remote host: projectMembership and the mutation paths refuse
	// remote membership rather than silently never probing it.
	if !cfg.ObservationDisabled && nilDependency(probe) && cfg.Local == nil {
		return nil, errors.New("broker: invalid registry dependencies")
	}
	if log == nil {
		log = slog.Default()
	}
	generator := cfg.IncarnationGenerator
	if generator == nil {
		generator = func() (id [16]byte, err error) { _, err = rand.Read(id[:]); return id, err }
	}
	demandFreshForLocal := cfg.DemandFreshForLocal
	if demandFreshForLocal <= 0 {
		demandFreshForLocal = defaultDemandFreshForLocal
	}
	demandFreshForRemote := cfg.DemandFreshForRemote
	if demandFreshForRemote <= 0 {
		demandFreshForRemote = defaultDemandFreshForRemote
	}
	probeTimeout := cfg.ProbeTimeout
	if probeTimeout <= 0 {
		probeTimeout = defaultProbeTimeout
	}
	r := &Registry{
		epoch: epoch, probe: probe, clock: clock, log: log,
		observationDisabled: cfg.ObservationDisabled,
		membershipMode:      cfg.MembershipMode, newIncarnation: generator,
		hosts: make(map[string]ports.BrokerDaemonObservation), inflight: make(map[string]*probeAttempt),
		pending: make(map[string]bool), tombstones: make(map[string]ports.BrokerHostTombstone),
		subs: make(map[*subscription]struct{}), wake: make(chan struct{}, 1),
		freshFor: defaultFreshFor, retryBase: defaultRetryBase, retryMax: defaultRetryLimit,
		jitter:               jittered,
		demandFreshForLocal:  demandFreshForLocal,
		demandFreshForRemote: demandFreshForRemote,
		probeTimeout:         probeTimeout,
	}
	if cfg.ObservationDisabled && cfg.Local != nil {
		return nil, errors.New("broker: observation-disabled registry cannot observe the local daemon")
	}
	if cfg.Local != nil {
		if err := cfg.Local.validate(); err != nil {
			return nil, err
		}
		// Copy the configured authority: the registry reads it from the probe
		// and publish paths, and a caller that kept the pointer could otherwise
		// mutate authority after construction.
		local := *cfg.Local
		r.local = &local
		r.localHost = ports.BrokerDaemonObservation{Local: true, DisplayOrigin: cfg.Local.DisplayOrigin, Policy: cfg.Local.Policy, Availability: domain.RemoteAvailabilityUnknown}
		if err := r.localHost.Validate(); err != nil {
			return nil, fmt.Errorf("broker: invalid local observation authority: %w", err)
		}
	}
	r.store = newSnapshotWriter(store, log)
	snapshot, err := store.Load()
	if err != nil {
		return nil, err
	}
	if err := r.restore(snapshot); err != nil {
		return nil, err
	}
	hosts, err := store.LoadHosts()
	if err != nil {
		return nil, err
	}
	hosts.Hosts, err = ports.UpgradeBrokerHostRoutes(hosts.Hosts)
	if err != nil {
		return nil, err
	}
	if err := r.projectMembership(hosts); err != nil {
		return nil, err
	}
	r.hostStore = store
	r.authority = ports.BrokerHosts{Revision: hosts.Revision, Hosts: ports.CloneBrokerHostRecords(hosts.Hosts)}
	r.publishLocked(false)
	return r, nil
}

// nilDependency reports whether a required dependency is absent, including a
// typed nil pointer stored behind a non-nil interface. Comparing the interface
// to nil alone would admit a typed nil and defer the failure to the first
// method call, which panics; the reflection check keeps the dependency guard
// total for every nil shape.
func nilDependency(dependency any) bool {
	if dependency == nil {
		return true
	}
	value := reflect.ValueOf(dependency)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Ptr, reflect.Slice:
		return value.IsNil()
	default:
		return false
	}
}

// projectMembership intersects durable membership with the observations a
// previous process left behind. Durable membership is authoritative:
// snapshots cannot resurrect removed hosts or omit never-observed
// registrations. A restored observation is reused only when it belongs to the
// exact same registration identity; any other registration starts unknown and
// carries no inventory.
func (r *Registry) projectMembership(hosts ports.BrokerHosts) error {
	if err := hosts.Validate(); err != nil {
		return err
	}
	if len(hosts.Hosts) > 0 && !r.observationDisabled && nilDependency(r.probe) {
		return errors.New("broker: remote hosts require a host probe")
	}
	next := make(map[string]ports.BrokerDaemonObservation, len(hosts.Hosts))
	order := make([]string, 0, len(hosts.Hosts))
	for rank, record := range hosts.Hosts {
		registration := record.Registration
		restored, present := r.hosts[registration.Endpoint]
		if !present || !restored.Registration.Equal(registration) {
			restored = ports.BrokerDaemonObservation{Availability: domain.RemoteAvailabilityUnknown}
		}
		projection := stampHostAuthority(restored, record, rank)
		// A durable membership whose projection cannot be published is refused
		// here, at the boundary that owns authority, instead of silently
		// leaving the registry unable to publish anything.
		if err := projection.Validate(); err != nil {
			return fmt.Errorf("broker: host record %q is not publishable: %w", registration.Endpoint, err)
		}
		next[registration.Endpoint] = projection
		order = append(order, registration.Endpoint)
	}
	r.hosts = next
	r.order = order
	return nil
}

// stampHostAuthority overwrites the configured authority fields of a daemon
// projection with durable membership authority. Membership is the single
// authority for endpoint, registration, policy, and the presentation hints
// derived from them; a probe, a restored durable observation, or any other
// producer can never supply or override them. The durable observation format
// deliberately carries no policy (see durable), so this stamp is also what
// gives every restored observation a valid policy.
func stampHostAuthority(observation ports.BrokerDaemonObservation, record ports.BrokerHostRecord, rank int) ports.BrokerDaemonObservation {
	observation.Local = false
	observation.Endpoint = record.Registration.Endpoint
	observation.DisplayOrigin = hostDisplayOrigin(record.Registration.Endpoint)
	observation.Rank = rank
	observation.Registration = record.Registration
	observation.Policy = record.Policy
	if observation.Availability == 0 {
		// An unobserved daemon reports the explicit unknown availability; zero
		// is never a valid publication.
		observation.Availability = domain.RemoteAvailabilityUnknown
	}
	return observation
}

// stampObservationAuthority copies the configured authority already stamped on
// a live projection onto a fresh probe result. Probe results supply only
// observed state, so their endpoint, registration, policy, display origin, and
// rank fields are discarded wholesale.
func stampObservationAuthority(observation, authority ports.BrokerDaemonObservation) ports.BrokerDaemonObservation {
	observation.Local = false
	observation.Endpoint = authority.Endpoint
	observation.DisplayOrigin = authority.DisplayOrigin
	observation.Rank = authority.Rank
	observation.Registration = authority.Registration
	observation.Policy = authority.Policy
	return observation
}

// hostDisplayOrigin derives the presentation hint for a configured host. The
// routing endpoint stays authoritative; the origin is only the login prefix
// stripped for display, and an endpoint that carries none is its own origin.
// The hint is sanitized to the display-text rule before it is clamped rune- and
// byte-safely to the ports bound: an endpoint the routing validator accepts may
// still carry bidi or control runes, and rendering those would let a derived
// hint reorder the picker text around it. A derived hint carries no routing
// authority, so the refused runes are dropped rather than the host rejected. A
// target left with no displayable rune at all yields no origin, which its
// caller refuses as unpublishable membership instead of inventing display text.
func hostDisplayOrigin(endpoint string) string {
	origin := ports.SanitizeBrokerDisplayText(domain.RemoteDisplayOrigin(endpoint))
	if len(origin) <= ports.BrokerMaxDisplayOriginBytes {
		return origin
	}
	truncated := origin[:ports.BrokerMaxDisplayOriginBytes]
	for len(truncated) > 0 && !utf8.ValidString(truncated) {
		truncated = truncated[:len(truncated)-1]
	}
	return truncated
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
		if len(snapshot.Daemons) > 0 || len(snapshot.Removed) > 0 || snapshot.Revision != 0 {
			return errors.New("broker: persisted snapshot has no epoch")
		}
		return nil
	}
	if snapshot.Epoch == r.epoch {
		return errors.New("broker: persisted snapshot epoch matches the new registry epoch")
	}
	if err := ports.ValidateDurableSnapshot(snapshot); err != nil {
		return err
	}
	for _, daemon := range snapshot.Daemons {
		daemon = daemon.Clone()
		daemon.Checking = false
		r.hosts[daemon.Endpoint] = daemon
	}
	return nil
}
