package remotes

import (
	"hash/fnv"
	"sort"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	"github.com/bnema/vev/internal/protocol/catalogue"
)

// Monitor cadence and bounds. All policy timing is private and fake-clock
// tested; wall-clock never enters policy decisions directly.
const (
	healthyCheckInterval = 15 * time.Second
	observeTimeout       = 10 * time.Second
	registryPollInterval = 5 * time.Second
	inventoryExpiryAge   = 30 * time.Second
	baseRetryDelay       = 5 * time.Second
	maxRetryDelay        = 60 * time.Second

	// Admission credits bound in-flight work per lane: one registry
	// reader, one startup cache reader, one cache writer, four catalogue
	// observers. The result channel holds all seven possible completions.
	maxObserveCredits = 4
	maxInFlightJobs   = 7
	// maxPendingVerifyHints bounds coalesced endpoint verification
	// requests; beyond it new hints drop and the periodic cadence
	// covers the endpoint.
	maxPendingVerifyHints = 256
)

// hostPolicy is the loop-owned per-registration observation state for one
// endpoint. Only the service loop mutates it; snapshots expose copies.
type hostPolicy struct {
	registration        domain.RemoteRegistration
	rank                int
	availability        domain.RemoteAvailability
	observing           bool
	observeGeneration   domain.RemoteGeneration
	followUp            bool
	attempts            uint64
	consecutiveFailures uint
	lastFailure         domain.RemoteFailure
	lastAttempt         time.Time
	lastSuccess         time.Time
	nextDue             time.Time
	expiryNotified      bool
	sessions            []catalogue.RemoteCatalogSession
	inventoryKnown      bool
	inventoryConfirmed  bool
	// cacheSeeded records that the held cache entry was already applied
	// to this registration: re-seeding identical content must not publish
	// a revision on every poll, while a re-created policy (fresh false)
	// still recovers through the retained entry after a registry flap.
	cacheSeeded bool
}

// inFlightRequest is the exact admitted request for one endpoint. It is
// owned independently of registration membership: removing or replacing a
// registration never deletes it, so a late completion can only retire its
// credit and never mutate a different registration's policy.
type inFlightRequest struct {
	registration domain.RemoteRegistration
	generation   domain.RemoteGeneration
}

// serviceState is the loop-owned monitor state: host policies, admission
// credits, pending cache/store payloads and publication bookkeeping.
type serviceState struct {
	hosts            map[string]*hostPolicy
	inFlight         map[string]inFlightRequest
	verifyHints      map[string]struct{}
	observeCredits   int
	registryCredit   bool
	cacheCredit      bool
	storeCredit      bool
	registryDone     bool
	registryInit     bool
	registryError    error
	lastRegistryRead time.Time
	registryExpedite bool
	expeditedRead    bool
	cacheDone        bool
	cacheRetryAt     time.Time
	pendingCache     []catalogue.RemoteCatalogCacheEntry
	pendingStore     []catalogue.RemoteCatalogCacheEntry
	storePending     bool
	storeActive      bool
	storeRetryAt     time.Time
	stored           []catalogue.RemoteCatalogCacheEntry
	lastPersisted    []catalogue.RemoteCatalogCacheEntry
	dirty            bool
}

func newServiceState() *serviceState {
	return &serviceState{
		hosts:          make(map[string]*hostPolicy),
		inFlight:       make(map[string]inFlightRequest),
		verifyHints:    make(map[string]struct{}),
		observeCredits: maxObserveCredits,
		registryCredit: true,
		cacheCredit:    true,
		storeCredit:    true,
	}
}

// retryDelay returns 5, 10, 20, 40, then 60 seconds capped for consecutive
// failures. Zero failures is the healthy cadence, not a retry.
func retryDelay(failures uint) time.Duration {
	if failures == 0 {
		return healthyCheckInterval
	}
	delay := baseRetryDelay
	for i := uint(1); i < failures; i++ {
		delay *= 2
		if delay >= maxRetryDelay {
			return maxRetryDelay
		}
	}
	if delay > maxRetryDelay {
		return maxRetryDelay
	}
	return delay
}

// jittered spreads a base delay by ±10% deterministically per endpoint and
// attempt so healthy hosts do not synchronize and tests stay stable without
// randomness infrastructure.
func jittered(base time.Duration, endpoint string, attempt uint64) time.Duration {
	if base <= 0 {
		return base
	}
	h := fnv.New32a()
	h.Write([]byte(endpoint))
	var counter [8]byte
	for i := 0; i < 8; i++ {
		counter[i] = byte(attempt >> (8 * i))
	}
	h.Write(counter[:])
	spread := int64(h.Sum32() % 21)
	return base*90/100 + time.Duration(spread)*base/100
}

// orderedEndpoints returns sorted known endpoints for fair deterministic
// dispatch across the observation pool.
func (s *serviceState) orderedEndpoints() []string {
	out := make([]string, 0, len(s.hosts))
	for endpoint := range s.hosts {
		out = append(out, endpoint)
	}
	sort.Strings(out)
	return out
}

// nextWakeup returns the earliest time admittable work may exist. Work
// that cannot be admitted without a completion (no credits, outstanding
// requests, backoff not yet elapsed) never produces an immediate wakeup:
// completions, hints and cancellation wake the loop instead. This keeps a
// blocked lane from busy-spinning the policy loop while fake time is held
// fixed or a lane stays occupied.
func (s *serviceState) nextWakeup(now time.Time) time.Time {
	var next time.Time
	consider := func(t time.Time) {
		if t.IsZero() {
			return
		}
		if next.IsZero() || t.Before(next) {
			next = t
		}
	}
	if s.registryCredit && (s.registryExpedite || !s.registryDone || !now.Before(s.lastRegistryRead.Add(registryPollInterval))) {
		return now
	}
	if s.registryCredit && s.registryDone {
		consider(s.lastRegistryRead.Add(registryPollInterval))
	}
	if s.cacheCredit && !s.cacheDone && !now.Before(s.cacheRetryAt) {
		return now
	}
	if !s.cacheDone {
		consider(s.cacheRetryAt)
	}
	if s.storeCredit && !s.storeActive && s.storePending && !now.Before(s.storeRetryAt) {
		return now
	}
	for endpoint, host := range s.hosts {
		if _, outstanding := s.inFlight[endpoint]; outstanding || host.observing {
			continue
		}
		if _, hinted := s.verifyHints[endpoint]; hinted && s.observeCredits > 0 {
			return now
		}
		if s.observeCredits <= 0 {
			continue
		}
		if now.Before(host.nextDue) {
			consider(host.nextDue)
		} else {
			return now
		}
	}
	for _, host := range s.hosts {
		if host.inventoryConfirmed && !host.expiryNotified {
			consider(host.lastSuccess.Add(inventoryExpiryAge))
		}
	}
	if next.IsZero() || next.Before(now) {
		// Nothing admittable and no future work: idle fallback. Lane
		// completions, hints and cancellation still wake the loop;
		// the timer only bounds how long a missed wake could sleep.
		return now.Add(registryPollInterval)
	}
	return next
}

// cacheEntriesEqual compares cache payloads by host identity, incarnation
// binding, fetch time and exact session content so unchanged inventories
// never stage duplicate writes.
func cacheEntriesEqual(a, b []catalogue.RemoteCatalogCacheEntry) bool {
	if (a == nil) != (b == nil) || len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].Host != b[i].Host || a[i].FetchedAt != b[i].FetchedAt || a[i].Incarnation != b[i].Incarnation {
			return false
		}
		if len(a[i].Sessions) != len(b[i].Sessions) {
			return false
		}
		for j := range a[i].Sessions {
			if !catalogSessionEqual(a[i].Sessions[j], b[i].Sessions[j]) {
				return false
			}
		}
	}
	return true
}

func catalogSessionEqual(a, b catalogue.RemoteCatalogSession) bool {
	if a.LifecycleID != b.LifecycleID || a.Name != b.Name || a.State != b.State ||
		a.Ephemeral != b.Ephemeral || a.Attached != b.Attached ||
		a.LastUsedSeq != b.LastUsedSeq || a.ActiveTabID != b.ActiveTabID || a.Reason != b.Reason {
		return false
	}
	if len(a.Tabs) != len(b.Tabs) {
		return false
	}
	for i := range a.Tabs {
		if a.Tabs[i] != b.Tabs[i] {
			return false
		}
	}
	return true
}

// availabilityFor maps an observation failure kind to the published
// availability. The typed failure kind is preserved separately for notice
// policy; every kind retries at the capped cadence.
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

// buildSnapshot renders the current publication: ordered hosts with cloned
// inventories, preserving last-known availability and sessions while a check
// is in flight.
func (s *serviceState) buildSnapshot(revision uint64, initialized bool) ports.RemoteDirectorySnapshot {
	snapshot := ports.RemoteDirectorySnapshot{
		Revision:      revision,
		Initialized:   initialized,
		RegistryError: s.registryError,
		Hosts:         make([]ports.RemoteHostSnapshot, 0, len(s.hosts)),
	}
	for _, endpoint := range s.orderedEndpoints() {
		host := s.hosts[endpoint]
		snapshot.Hosts = append(snapshot.Hosts, ports.RemoteHostSnapshot{
			Endpoint:            endpoint,
			DisplayOrigin:       domain.RemoteDisplayOrigin(endpoint),
			Rank:                host.rank,
			Availability:        host.availability,
			Checking:            host.observing,
			LastAttempt:         host.lastAttempt,
			LastSuccess:         host.lastSuccess,
			NextDue:             host.nextDue,
			ConsecutiveFailures: host.consecutiveFailures,
			LastFailure:         host.lastFailure,
			InventoryKnown:      host.inventoryKnown,
			Registration:        host.registration,
			Sessions:            append([]catalogue.RemoteCatalogSession(nil), host.sessions...),
		})
	}
	return snapshot
}
