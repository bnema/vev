package broker

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"log/slog"
	"reflect"
	"slices"
	"sort"
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

// AddHost adds or pins endpoint under policy. Duplicate same-policy additions
// CAS-verify authority and preserve identity; a conflicting policy is refused.
func (r *Registry) LookupHost(ctx context.Context, endpoint string) (ports.BrokerHostRecord, bool, error) {
	if err := ctx.Err(); err != nil {
		return ports.BrokerHostRecord{}, false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, record := range r.authority.Hosts {
		if record.Registration.Endpoint == endpoint {
			record.Route = record.Route.Clone()
			return record, true, nil
		}
	}
	return ports.BrokerHostRecord{}, false, nil
}

func (r *Registry) AddHost(ctx context.Context, endpoint string, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	if err := domain.ValidateRemoteHostTarget(endpoint); err != nil {
		return domain.RemoteRegistration{}, err
	}
	if err := policy.Validate(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutableLocked(ctx); err != nil {
		return domain.RemoteRegistration{}, err
	}
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for i := range records {
		if records[i].Registration.Endpoint != endpoint {
			continue
		}
		if records[i].Policy != policy {
			return domain.RemoteRegistration{}, ports.ErrBrokerHostConflict
		}
		records[i].Pinned = true
		if err := r.commitMembershipLocked(records); err != nil {
			return domain.RemoteRegistration{}, err
		}
		return records[i].Registration, nil
	}
	id, err := r.newIncarnation()
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	reg, err := domain.NewRemoteRegistration(endpoint, id)
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	if reg.IsZero() {
		return domain.RemoteRegistration{}, errors.New("broker: incarnation generator returned zero")
	}
	route, err := ports.BrokerRouteForTransport(policy.Transport, endpoint)
	if err != nil {
		return domain.RemoteRegistration{}, err
	}
	records = append(records, ports.BrokerHostRecord{Registration: reg, Pinned: true, Policy: policy, Route: route})
	if err := r.commitMembershipLocked(records); err != nil {
		return domain.RemoteRegistration{}, err
	}
	return reg, nil
}

// RemoveHost removes only the exact expected registration.
// BindAuthenticatedIdentity durably binds first-contact identity under the
// exact registration and policy fence. The same binding is idempotent; a
// different identity is refused and never overwrites authority.
func (r *Registry) BindAuthenticatedIdentity(ctx context.Context, request ports.BrokerIdentityBindingRequest) (ports.BrokerDaemonIdentity, error) {
	if err := request.Validate(); err != nil {
		return "", err
	}
	if request.Fence.Local {
		return request.Identity, nil
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for index := range records {
		record := &records[index]
		if record.Identity == request.Identity && record.Policy != request.Policy {
			return "", ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("broker: authenticated daemon identity is already bound under an incompatible policy")}
		}
	}
	for index := range records {
		record := &records[index]
		if !record.Registration.Equal(request.Fence.Registration) || record.Policy != request.Policy {
			continue
		}
		if record.Identity != "" {
			if record.Identity != request.Identity {
				return "", ports.BrokerError{Code: ports.BrokerErrorConflictingPolicy, Cause: errors.New("broker: authenticated daemon identity conflicts with durable binding")}
			}
			return record.Identity, nil
		}
		record.Identity = request.Identity
		if err := r.commitMembershipLocked(records); err != nil {
			return "", err
		}
		return request.Identity, nil
	}
	return "", ports.BrokerError{Code: ports.BrokerErrorUnavailable, Cause: errors.New("broker: stale identity binding fence")}
}

func (r *Registry) RemoveHost(ctx context.Context, expected domain.RemoteRegistration) (bool, error) {
	if err := expected.Validate(); err != nil {
		return false, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutableLocked(ctx); err != nil {
		return false, err
	}
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for i := range records {
		if records[i].Registration.Endpoint != expected.Endpoint {
			continue
		}
		if !records[i].Registration.Equal(expected) {
			return false, ports.ErrBrokerHostConflict
		}
		records = append(records[:i], records[i+1:]...)
		if err := r.commitMembershipLocked(records); err != nil {
			return false, err
		}
		return true, nil
	}
	// A no-op still verifies the loaded revision.
	return false, r.commitMembershipLocked(records)
}

// UpdateHostPolicy updates exact authority and advances its generation.
func (r *Registry) UpdateHostPolicy(ctx context.Context, expected domain.RemoteRegistration, policy ports.BrokerPolicy) (domain.RemoteRegistration, error) {
	if err := expected.Validate(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	if err := policy.Validate(); err != nil {
		return domain.RemoteRegistration{}, err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.mutableLocked(ctx); err != nil {
		return domain.RemoteRegistration{}, err
	}
	records := append([]ports.BrokerHostRecord(nil), r.authority.Hosts...)
	for i := range records {
		if records[i].Registration.Endpoint != expected.Endpoint {
			continue
		}
		if !records[i].Registration.Equal(expected) {
			return domain.RemoteRegistration{}, ports.ErrBrokerHostConflict
		}
		if records[i].Policy == policy {
			if err := r.commitMembershipLocked(records); err != nil {
				return domain.RemoteRegistration{}, err
			}
			return expected, nil
		}
		if expected.Generation == ^domain.RemoteGeneration(0) {
			return domain.RemoteRegistration{}, ports.ErrBrokerRevisionExhausted
		}
		route, err := ports.BrokerRouteForTransport(policy.Transport, expected.Endpoint)
		if err != nil {
			return domain.RemoteRegistration{}, err
		}
		records[i].Registration.Generation++
		records[i].Route = route
		records[i].Policy = policy
		if err := r.commitMembershipLocked(records); err != nil {
			return domain.RemoteRegistration{}, err
		}
		return records[i].Registration, nil
	}
	return domain.RemoteRegistration{}, ports.ErrBrokerHostConflict
}

func (r *Registry) mutableLocked(ctx context.Context) error {
	if r.membershipMode != MembershipMutable {
		return ports.ErrBrokerMembershipImmutable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.running.Load() == runStopped {
		return ports.ErrBrokerRegistryClosed
	}
	if r.revisionExhaustedLocked() {
		return ports.ErrBrokerRevisionExhausted
	}
	return nil
}

// commitMembershipLocked is the sole mutable-membership durable commit path.
// It installs projection only after CAS success.
func (r *Registry) commitMembershipLocked(records []ports.BrokerHostRecord) error {
	if err := ports.ValidateBrokerHostRecords(records); err != nil {
		return err
	}
	if len(records) > 0 && !r.observationDisabled && nilDependency(r.probe) {
		return errors.New("broker: remote hosts require a host probe")
	}
	next := make(map[string]ports.BrokerDaemonObservation, len(records))
	order := make([]string, 0, len(records))
	for rank, record := range records {
		observation := stampHostAuthority(ports.BrokerDaemonObservation{}, record, rank)
		// A membership the registry cannot publish is refused here, before the
		// store CAS, rather than persisted and left to freeze every later
		// publication: an endpoint whose derived display hint is empty after the
		// display rule drops its runes makes every snapshot carrying it invalid.
		if err := observation.Validate(); err != nil {
			return fmt.Errorf("broker: host record %q is not publishable: %w", record.Registration.Endpoint, err)
		}
		next[record.Registration.Endpoint] = observation
		order = append(order, record.Registration.Endpoint)
	}
	if err := r.hostStore.ReplaceHosts(r.authority.Revision, records); err != nil {
		return fmt.Errorf("broker: replace hosts: %w", err)
	}
	r.authority = ports.BrokerHosts{Revision: r.authority.Revision + 1, Hosts: ports.CloneBrokerHostRecords(records)}
	r.setHostsLocked(next, order)
	return nil
}

// ReplaceHosts durably replaces membership and explicit policy using the loaded
// authority revision. It delegates to commitMembershipLocked, the same
// validated commit path the runtime mutations use, so membership whose
// projection could never be published is refused before the store CAS instead
// of being persisted and silently freezing every later publication. Success
// means membership is committed, not that advisory observations have flushed.
// Store errors (including stale authority) are returned without changing the
// projection, probe attempts, or local token. A settled run reports
// ports.ErrBrokerRegistryClosed and an exhausted revision series reports
// ports.ErrBrokerRevisionExhausted; both refuse the mutation before the store
// CAS, so a refusal never changes durable authority either. Conflicts are not
// retried or refreshed implicitly: reconcile by reopening the owner. Callers
// must supply fresh incarnations when removing and re-adding. Even identical
// records perform CAS so stale authority cannot report success. The synchronous
// write holds mu; Snapshot remains lock-free, but scheduling waits for
// membership durability. Observation writes remain asynchronous.
func (r *Registry) ReplaceHosts(records []ports.BrokerHostRecord) error {
	var err error
	records, err = ports.UpgradeBrokerHostRoutes(records)
	if err != nil {
		return err
	}
	if err := ports.ValidateBrokerHostRecords(records); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.running.Load() == runStopped {
		return fmt.Errorf("broker: membership replacement: %w", ports.ErrBrokerRegistryClosed)
	}
	if r.revisionExhaustedLocked() {
		return fmt.Errorf("broker: membership replacement: %w", ports.ErrBrokerRevisionExhausted)
	}
	return r.commitMembershipLocked(records)
}

// setHostsLocked applies only a projection; the public path commits authority
// first. The caller holds mu through both steps, fencing probe completions.
// order is the registration order the projection was built from; it becomes
// the publication order of remote daemons.
func (r *Registry) setHostsLocked(next map[string]ports.BrokerDaemonObservation, order []string) {
	if registrationsUnchanged(r.hosts, next, r.order, order) {
		return
	}
	for endpoint, current := range r.hosts {
		candidate, present := next[endpoint]
		if present && current.Registration.Equal(candidate.Registration) && current.Policy == candidate.Policy {
			// Unchanged authority: keep the live observation as is. Rank is the
			// one authority field that follows membership order rather than the
			// record, so it is re-stamped from the new projection.
			kept := current
			kept.Rank = candidate.Rank
			next[endpoint] = kept
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
	r.order = order
	r.publishLocked(true)
	r.hint()
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
		if !r.pending[endpoint] && (r.demand == 0 || !host.NextDue.IsZero() && now.Before(host.NextDue)) {
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
	// Only an observation that actually started changes the projection; an
	// idle dispatch must not bump the revision or wake subscribers. The
	// in-flight projection is never persisted: Checking is transient.
	if r.dispatchLocalLocked(ctx, now, results) {
		started = true
	}
	if started {
		r.publishLocked(false)
	}
	r.mu.Unlock()
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
		switch {
		case errors.As(result.err, &typed) && typed.Kind != domain.RemoteFailureNone:
			// The dialer classified the failure (for example SSH authentication).
			kind = typed.Kind
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

// registrationsUnchanged reports exact membership equality: the same endpoints
// in the same registration order, bound to the same registration identities and
// policy. Publication order and presentation rank are functions of durable
// membership order, so a reordering of identical records is a membership change
// that must republish; otherwise the same durable state yields different
// publications depending on process history. Projection fields such as
// availability, checking, or inventory never participate.
func registrationsUnchanged(current, next map[string]ports.BrokerDaemonObservation, currentOrder, nextOrder []string) bool {
	if len(current) != len(next) {
		return false
	}
	if !slices.Equal(currentOrder, nextOrder) {
		return false
	}
	for endpoint, candidate := range next {
		existing, ok := current[endpoint]
		if !ok || !existing.Registration.Equal(candidate.Registration) || existing.Policy != candidate.Policy {
			return false
		}
	}
	return true
}

// retireLocked records the tombstone for one removed registration at the
// revision of the publication that carries it. Mutation paths refuse an
// exhausted series before retiring, so the retirement revision can never wrap
// to zero or fall below the last published one.
func (r *Registry) retireLocked(registration domain.RemoteRegistration) {
	retired := r.revision + 1
	r.tombstones[registration.Endpoint] = ports.BrokerHostTombstone{
		Endpoint:        registration.Endpoint,
		Registration:    registration,
		RetiredRevision: retired,
	}
	r.pruneTombstonesLocked()
}

// revisionExhaustedLocked reports whether this epoch has no revision left to
// publish, latching the exhausted state on first detection. Callers must hold
// r.mu.
func (r *Registry) revisionExhaustedLocked() bool {
	if r.revisionExhausted {
		return true
	}
	if r.revision != ^ports.BrokerRevision(0) {
		return false
	}
	r.revisionExhausted = true
	r.log.Error("broker: revision series exhausted; refusing further publication", "epoch", r.epoch)
	return true
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
	// Fail closed on an exhausted series: a wrapped revision would be zero, and
	// a wrapped series would publish revisions below the newest one, either of
	// which silently freezes durable persistence. The last valid snapshot stays
	// published, nothing is stored, and no subscriber is woken for a publication
	// that did not happen.
	if r.revisionExhaustedLocked() {
		return
	}
	// Remote daemons are published in registration order. The broker's own
	// machine daemon, when a producer is configured, is the
	// prepended entry at index zero; without a producer the registry produces no
	// local observation and never invents one.
	daemons := make([]ports.BrokerDaemonObservation, 0, len(r.hosts)+1)
	if r.local != nil {
		daemons = append(daemons, r.localHost.Clone())
	}
	for _, endpoint := range r.order {
		host, ok := r.hosts[endpoint]
		if !ok {
			continue
		}
		daemons = append(daemons, host.Clone())
	}
	snapshot := ports.BrokerSnapshot{Epoch: r.epoch, Revision: r.revision + 1, Daemons: daemons, Removed: r.tombstonesLocked()}
	if err := snapshot.Validate(); err != nil {
		// Fail closed: a publication the wire refuses aborts every client
		// connection on subscribe, so an invalid projection is never published.
		// The last valid snapshot stays current, nothing is persisted, and the
		// revision series does not advance past a publication that never
		// happened.
		r.log.Error("broker: refused invalid snapshot publication", "revision", r.revision+1, "err", err)
		return
	}
	r.revision = snapshot.Revision
	r.current.Store(&snapshot)
	if persist {
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
// the durable snapshot. Retirement tombstones are process-local fencing state:
// they retire registrations only for this process lifetime and a restart never
// adopts them, so persisting them would leak stale authority into durable
// state. Removed is therefore cleared before persistence.
//
// Policy is membership authority, not observation state, so the durable format
// deliberately omits it: membership is the single policy authority, and the
// loader re-stamps every restored observation from the matching
// ports.BrokerHostRecord (see stampHostAuthority). Persisting it would create a
// second, stale authority that a later membership change could contradict.
//
// Only remote observations are durable: a local daemon observation is
// process-local state and is dropped here (the store also rejects one), so a
// future local producer can never break durable persistence.
func durable(snapshot ports.BrokerSnapshot) ports.BrokerSnapshot {
	out := snapshot.Clone()
	out.Removed = nil
	daemons := out.Daemons[:0]
	for _, daemon := range out.Daemons {
		if daemon.Local {
			continue
		}
		daemon.Checking = false
		daemon.LastFailure.Err = nil
		daemon.Policy = ports.BrokerPolicy{}
		daemons = append(daemons, daemon)
	}
	out.Daemons = daemons
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
