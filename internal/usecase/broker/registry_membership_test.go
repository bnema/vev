package broker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bnema/vev/internal/domain"
	"github.com/bnema/vev/internal/ports"
	portsmocks "github.com/bnema/vev/internal/ports/mocks"
	"github.com/bnema/vev/internal/protocol"
	"github.com/stretchr/testify/require"
)

// Phase A mutable membership: the registry is the only synchronous owner of
// durable authority, so every admitted mutation must CAS the store, install its
// projection only after that CAS succeeds, and leave authority, projection,
// probe attempts, and tombstones untouched on every refusal. The tests below
// drive the public mutation API and the production fencing path directly, so a
// regression in any of those rules is observable instead of being masked by the
// observation scheduler.

// incarnationSequencer is a deterministic entropy seam: every call yields a
// distinct, never-zero incarnation, so a test can assert that a remove/re-add
// really minted a fresh identity and that a refused mutation minted none.
type incarnationSequencer struct {
	mu   sync.Mutex
	next byte
}

func (s *incarnationSequencer) generate() ([16]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.next++
	var id [16]byte
	id[0] = 0xA5
	id[15] = s.next
	return id, nil
}

func (s *incarnationSequencer) generated() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return int(s.next)
}

// membershipStore is a race-safe durable store with an observable CAS counter
// and an injectable CAS failure. The counter proves a mutation reached the
// store (including the no-op paths, which must still CAS), and the injected
// failure proves a store fault is synchronous and never installs a projection.
type membershipStore struct {
	*testStore
	casMu   sync.Mutex
	casErr  error
	casCall int
}

func newMembershipStore(hosts ...ports.BrokerHostRecord) *membershipStore {
	return &membershipStore{testStore: &testStore{hosts: ports.BrokerHosts{Revision: 1, Hosts: hosts}}}
}

func (s *membershipStore) ReplaceHosts(expected uint64, hosts []ports.BrokerHostRecord) error {
	s.casMu.Lock()
	s.casCall++
	err := s.casErr
	s.casMu.Unlock()
	if err != nil {
		return err
	}
	return s.testStore.ReplaceHosts(expected, hosts)
}

func (s *membershipStore) failCAS(err error) {
	s.casMu.Lock()
	s.casErr = err
	s.casMu.Unlock()
}

func (s *membershipStore) casCalls() int {
	s.casMu.Lock()
	defer s.casMu.Unlock()
	return s.casCall
}

// durableAuthority reads the store's committed authority, exactly as a reopen
// would: membership and its revision token come from the store, never from the
// registry's in-memory copy.
func (s *membershipStore) durableAuthority() ports.BrokerHosts {
	s.testStore.mu.Lock()
	defer s.testStore.mu.Unlock()
	return ports.BrokerHosts{Revision: s.testStore.hosts.Revision, Hosts: append([]ports.BrokerHostRecord(nil), s.testStore.hosts.Hosts...)}
}

// newMutableRegistry builds a mutable registry with exact scheduling, a
// deterministic incarnation seam, and its scripted probe.
func newMutableRegistry(t *testing.T, store ports.BrokerHostStore, clock *manualClock, generator func() ([16]byte, error)) (*Registry, *testProbe) {
	t.Helper()
	probe := newTestProbe(2)
	registry, err := NewRegistryWithConfig(1, store, probe, clock, nil, RegistryConfig{
		MembershipMode:       MembershipMutable,
		IncarnationGenerator: generator,
		ObservationDisabled:  false,
	})
	require.NoError(t, err)
	registry.jitter = identityJitter
	return registry, probe
}

func policyWithTrust(trust string) ports.BrokerPolicy {
	policy := poolPolicy()
	policy.Trust = trust
	return policy
}

func pinnedRecord(reg domain.RemoteRegistration, policy ports.BrokerPolicy) ports.BrokerHostRecord {
	return ports.BrokerHostRecord{Registration: reg, Pinned: true, Policy: policy}
}

// admittedAttempt is one in-flight probe attempt a test installs directly, so a
// completion can be delivered to apply without racing the scheduler.
type admittedAttempt struct {
	token  uint64
	ctx    context.Context
	cancel context.CancelFunc
}

func admitMembershipAttempt(t *testing.T, r *Registry, endpoint string) admittedAttempt {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	r.mu.Lock()
	defer r.mu.Unlock()
	r.attempts++
	attempt := admittedAttempt{token: r.attempts, ctx: ctx, cancel: cancel}
	r.inflight[endpoint] = &probeAttempt{token: attempt.token, cancel: cancel}
	return attempt
}

// membershipFences snapshots the tombstone set so a failed mutation can be
// proven to have retired nothing.
func membershipFences(r *Registry) map[string]ports.BrokerHostTombstone {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make(map[string]ports.BrokerHostTombstone, len(r.tombstones))
	for endpoint, tombstone := range r.tombstones {
		out[endpoint] = tombstone
	}
	return out
}

func staleObservation() ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Identity:        "stale-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{7},
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
		InventoryKnown:  true,
	}
}

func currentObservation() ports.BrokerDaemonObservation {
	return ports.BrokerDaemonObservation{
		Identity:        "current-daemon",
		Incarnation:     ports.BrokerDaemonIncarnation{9},
		ProtocolVersion: protocol.Version,
		Availability:    domain.RemoteAvailabilityReachable,
		InventoryKnown:  true,
	}
}

// TestRegistryMutableConcurrentAddsLoseNoUpdate proves concurrent membership
// mutations serialize over durable authority: every added endpoint survives,
// each addition advances the revision by exactly one, and the registry's copy
// still equals what the store committed.
func TestRegistryMutableConcurrentAddsLoseNoUpdate(t *testing.T) {
	const adds = 32
	store := newMembershipStore()
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	start := registry.authority.Revision

	endpoints := make([]string, adds)
	for i := range endpoints {
		endpoints[i] = fmt.Sprintf("host-%d.test", i)
	}

	var wg sync.WaitGroup
	errs := make([]error, adds)
	regs := make([]domain.RemoteRegistration, adds)
	startGate := make(chan struct{})
	for i, endpoint := range endpoints {
		wg.Add(1)
		go func(i int, endpoint string) {
			defer wg.Done()
			<-startGate
			regs[i], errs[i] = registry.AddHost(context.Background(), endpoint, poolPolicy())
		}(i, endpoint)
	}
	close(startGate)
	wg.Wait()

	for i, err := range errs {
		require.NoErrorf(t, err, "concurrent add %q", endpoints[i])
	}
	require.Equal(t, adds, store.casCalls(), "every admitted add must reach the store CAS")
	require.Equal(t, start+adds, registry.authority.Revision, "each add advances the revision exactly once")
	require.Len(t, registry.authority.Hosts, adds)
	require.Equal(t, registry.authority, store.durableAuthority())

	// Admission order is the lock order, not the launch order, so the
	// publication order is compared against the returned identities by endpoint.
	returned := make(map[string]domain.RemoteRegistration, adds)
	for i, endpoint := range endpoints {
		require.False(t, regs[i].IsZero(), "an admitted add returns its minted identity")
		require.Equal(t, endpoint, regs[i].Endpoint)
		require.Equal(t, domain.RemoteGeneration(1), regs[i].Generation)
		returned[endpoint] = regs[i]
	}
	require.Len(t, returned, adds, "every concurrent add minted a distinct identity")

	seen := make(map[string]domain.RemoteRegistration, adds)
	for _, record := range registry.authority.Hosts {
		require.NotContains(t, seen, record.Registration.Endpoint, "no endpoint may be added twice")
		require.Equal(t, returned[record.Registration.Endpoint], record.Registration)
		seen[record.Registration.Endpoint] = record.Registration
		require.Equal(t, poolPolicy(), record.Policy)
		require.True(t, record.Pinned)
	}
	require.Len(t, seen, adds)

	snapshot := registry.Snapshot()
	require.Len(t, snapshot.Daemons, adds)
	require.NoError(t, snapshot.Validate())
	for _, endpoint := range endpoints {
		host, ok := snapshot.Find(endpoint)
		require.True(t, ok, "a concurrently added endpoint must be published")
		require.Equal(t, seen[endpoint], host.Registration)
		require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)
	}
}

// TestRegistryMembershipModeRejectsUnknownMode proves the run mode is a closed
// set: an out-of-range mode is refused at construction, before any store call,
// so a caller can never run a registry whose mutation policy is undefined.
func TestRegistryMembershipModeRejectsUnknownMode(t *testing.T) {
	// The mock has no expectations: any store call during construction fails
	// the test, so a refusal must happen before the durable store is touched.
	store := portsmocks.NewMockBrokerHostStore(t)
	registry, err := NewRegistryWithConfig(1, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil,
		RegistryConfig{MembershipMode: MembershipMode(9)})
	require.ErrorContains(t, err, "invalid membership mode")
	require.Nil(t, registry)
}

// TestRegistryImmutableMembershipRefusesEveryMutation pins the default: a
// registry that was not explicitly configured mutable refuses every membership
// mutation, including the ones that would be no-ops, and refuses them without
// touching the durable store or its own projection.
func TestRegistryImmutableMembershipRefusesEveryMutation(t *testing.T) {
	reg := registration(t, "alpha.test", 1)
	store := portsmocks.NewMockBrokerHostStore(t)
	store.EXPECT().Load().Return(ports.BrokerSnapshot{}, nil).Once()
	store.EXPECT().LoadHosts().Return(ports.BrokerHosts{Revision: 3, Hosts: []ports.BrokerHostRecord{pinnedRecord(reg, poolPolicy())}}, nil).Once()
	registry, err := NewRegistry(1, store, newTestProbe(1), newManualClock(time.Unix(100, 0)), nil)
	require.NoError(t, err)
	authority := registry.authority
	before := registry.Snapshot()

	tests := []struct {
		name string
		run  func(r *Registry) error
	}{
		{"add new endpoint", func(r *Registry) error {
			_, err := r.AddHost(context.Background(), "beta.test", poolPolicy())
			return err
		}},
		{"add duplicate endpoint with equal policy is a no-op", func(r *Registry) error {
			_, err := r.AddHost(context.Background(), reg.Endpoint, poolPolicy())
			return err
		}},
		{"remove the exact registration", func(r *Registry) error {
			_, err := r.RemoveHost(context.Background(), reg)
			return err
		}},
		{"remove an absent endpoint is a no-op", func(r *Registry) error {
			_, err := r.RemoveHost(context.Background(), registration(t, "gamma.test", 3))
			return err
		}},
		{"update to a different policy", func(r *Registry) error {
			_, err := r.UpdateHostPolicy(context.Background(), reg, policyWithTrust("other-trust"))
			return err
		}},
		{"update to the equal policy is a no-op", func(r *Registry) error {
			_, err := r.UpdateHostPolicy(context.Background(), reg, poolPolicy())
			return err
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			require.ErrorIs(t, tc.run(registry), ports.ErrBrokerMembershipImmutable)
			require.Equal(t, authority, registry.authority)
			require.Equal(t, before, registry.Snapshot())
			require.Empty(t, membershipFences(registry))
		})
	}

	// Control: the same duplicate add is admitted when the mode is mutable, so
	// the refusals above are the mode and not the validity of the operation.
	seq := &incarnationSequencer{}
	mutable, _ := newMutableRegistry(t, newMembershipStore(pinnedRecord(reg, poolPolicy())), newManualClock(time.Unix(100, 0)), seq.generate)
	added, err := mutable.AddHost(context.Background(), reg.Endpoint, poolPolicy())
	require.NoError(t, err)
	require.Equal(t, reg, added)
	require.Equal(t, 0, seq.generated(), "a duplicate add must not mint an identity")
}

// TestRegistryMutableAddPinsDuplicatePreservingIdentity proves a duplicate
// same-policy addition pins the existing record without changing its identity,
// and that even that no-op CAS-verifies durable authority.
func TestRegistryMutableAddPinsDuplicatePreservingIdentity(t *testing.T) {
	seq := &incarnationSequencer{}
	store := newMembershipStore(ports.BrokerHostRecord{Registration: registration(t, "alpha.test", 1), Learned: true, Policy: poolPolicy()})
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), seq.generate)
	reg := registration(t, "alpha.test", 1)

	got, err := registry.AddHost(context.Background(), reg.Endpoint, poolPolicy())
	require.NoError(t, err)
	require.Equal(t, reg, got, "a duplicate add preserves the registration identity")
	require.Equal(t, 1, store.casCalls())
	require.Len(t, registry.authority.Hosts, 1)
	record := registry.authority.Hosts[0]
	require.Equal(t, reg, record.Registration)
	require.Equal(t, poolPolicy(), record.Policy)
	require.True(t, record.Pinned, "the duplicate add pins the learned registration")
	require.True(t, record.Learned, "pinning never discards learned provenance")
	require.Equal(t, uint64(2), registry.authority.Revision)
	require.Equal(t, registry.authority, store.durableAuthority())
	require.Equal(t, 0, seq.generated(), "pinning mints no identity")

	host, ok := registry.Snapshot().Find(reg.Endpoint)
	require.True(t, ok)
	require.Equal(t, reg, host.Registration)
	require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)

	// A second duplicate is a no-op that still CAS-verifies: authority moves
	// durably, the projection does not republish, and identity is unchanged.
	before := registry.Snapshot()
	again, err := registry.AddHost(context.Background(), reg.Endpoint, poolPolicy())
	require.NoError(t, err)
	require.Equal(t, reg, again)
	require.Equal(t, 2, store.casCalls())
	require.Equal(t, uint64(3), registry.authority.Revision)
	require.Equal(t, registry.authority, store.durableAuthority())
	require.Equal(t, before, registry.Snapshot())
}

// TestRegistryMutableAddConflictingPolicyConflicts proves a conflicting policy
// is a definite conflict: it is refused before the store CAS, and it mints no
// identity.
func TestRegistryMutableAddConflictingPolicyConflicts(t *testing.T) {
	seq := &incarnationSequencer{}
	reg := registration(t, "alpha.test", 1)
	store := newMembershipStore(pinnedRecord(reg, poolPolicy()))
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), seq.generate)
	authority := registry.authority
	before := registry.Snapshot()

	_, err := registry.AddHost(context.Background(), reg.Endpoint, policyWithTrust("other-trust"))
	require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
	require.Equal(t, 0, store.casCalls(), "a conflict is refused before the CAS")
	require.Equal(t, authority, registry.authority)
	require.Equal(t, before, registry.Snapshot())
	require.Equal(t, 0, seq.generated())
}

// TestRegistryMutableAddEntropyFailureLeavesAuthorityUnchanged pins the entropy
// seam: an unusable incarnation is refused before anything durable is staged.
func TestRegistryMutableAddEntropyFailureLeavesAuthorityUnchanged(t *testing.T) {
	tests := []struct {
		name      string
		generator func() ([16]byte, error)
		want      string
	}{
		{"entropy source fails", func() ([16]byte, error) { return [16]byte{}, errors.New("no entropy") }, "no entropy"},
		{"entropy source returns a zero identity", func() ([16]byte, error) { return [16]byte{}, nil }, "zero"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			store := newMembershipStore()
			registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), tc.generator)
			authority := registry.authority
			before := registry.Snapshot()

			_, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
			require.ErrorContains(t, err, tc.want)
			require.Equal(t, 0, store.casCalls())
			require.Equal(t, authority, registry.authority)
			require.Equal(t, before, registry.Snapshot())
		})
	}
}

// TestRegistryMutableRemoveExactStaleAndAbsent pins removal admission: only the
// exact expected registration is removed, a stale generation or incarnation is
// a definite conflict, and an absent endpoint reports false only after the no-op
// CAS verified the loaded revision.
func TestRegistryMutableRemoveExactStaleAndAbsent(t *testing.T) {
	alpha := registration(t, "alpha.test", 1)
	beta := registration(t, "beta.test", 2)
	store := newMembershipStore(pinnedRecord(alpha, poolPolicy()), pinnedRecord(beta, poolPolicy()))
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	authority := registry.authority
	before := registry.Snapshot()

	staleGeneration := alpha
	staleGeneration.Generation++
	staleIncarnation := alpha
	staleIncarnation.Incarnation = [16]byte{0xEE}
	for name, expected := range map[string]domain.RemoteRegistration{
		"stale generation":  staleGeneration,
		"stale incarnation": staleIncarnation,
	} {
		t.Run(name, func(t *testing.T) {
			removed, err := registry.RemoveHost(context.Background(), expected)
			require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
			require.False(t, removed)
			require.Equal(t, 0, store.casCalls(), "a mismatch is refused before the CAS")
			require.Equal(t, authority, registry.authority)
			require.Equal(t, before, registry.Snapshot())
			require.Empty(t, membershipFences(registry))
		})
	}

	removed, err := registry.RemoveHost(context.Background(), alpha)
	require.NoError(t, err)
	require.True(t, removed)
	require.Equal(t, 1, store.casCalls())
	require.Len(t, registry.authority.Hosts, 1)
	require.Equal(t, beta, registry.authority.Hosts[0].Registration)
	require.Equal(t, uint64(2), registry.authority.Revision)
	require.Equal(t, registry.authority, store.durableAuthority())
	afterRemove := registry.Snapshot()
	require.Len(t, afterRemove.Daemons, 1)
	require.Equal(t, beta, afterRemove.Daemons[0].Registration)
	require.Len(t, afterRemove.Removed, 1)
	require.Equal(t, alpha, afterRemove.Removed[0].Registration)
	require.Equal(t, afterRemove.Revision, afterRemove.Removed[0].RetiredRevision,
		"a tombstone is retired at the revision of the publication that carries it")

	// An absent endpoint is a no-op that still CAS-verifies: durable authority
	// advances, while nothing is republished and nothing is retired.
	absent, err := registry.RemoveHost(context.Background(), alpha)
	require.NoError(t, err)
	require.False(t, absent)
	require.Equal(t, 2, store.casCalls())
	require.Equal(t, uint64(3), registry.authority.Revision)
	require.Equal(t, registry.authority, store.durableAuthority())
	require.Equal(t, afterRemove, registry.Snapshot())
}

// TestRegistryMutableNoOpRemoveVerifiesCAS pins the no-op CAS itself: with
// authority already moved by another mutator, an absent-endpoint removal must
// report a conflict instead of a hollow success, because the loaded revision was
// never verified.
func TestRegistryMutableNoOpRemoveVerifiesCAS(t *testing.T) {
	alpha := registration(t, "alpha.test", 1)
	store := newMembershipStore(pinnedRecord(alpha, poolPolicy()))
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	authority := registry.authority
	before := registry.Snapshot()

	// An illicit second mutator moves durable authority behind the registry.
	require.NoError(t, store.testStore.ReplaceHosts(authority.Revision, []ports.BrokerHostRecord{pinnedRecord(alpha, poolPolicy())}))

	removed, err := registry.RemoveHost(context.Background(), registration(t, "gamma.test", 3))
	require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
	require.False(t, removed)
	require.Equal(t, 1, store.casCalls())
	require.Equal(t, authority, registry.authority)
	require.Equal(t, before, registry.Snapshot())
}

// TestRegistryMutableRemoveReAddGetsFreshIncarnation pins the ABA fence at the
// mutation boundary: a full removal followed by a re-add mints a fresh
// incarnation with a fresh generation series, and the retired registration is
// fenced rather than revived.
func TestRegistryMutableRemoveReAddGetsFreshIncarnation(t *testing.T) {
	seq := &incarnationSequencer{}
	store := newMembershipStore()
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), seq.generate)

	first, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	require.Equal(t, 1, seq.generated())

	removed, err := registry.RemoveHost(context.Background(), first)
	require.NoError(t, err)
	require.True(t, removed)

	second, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	require.Equal(t, 2, seq.generated())
	require.False(t, first.Equal(second), "a re-add must not reuse the retired registration")
	require.NotEqual(t, first.Incarnation, second.Incarnation)
	require.Equal(t, domain.RemoteGeneration(1), second.Generation)
	require.Len(t, registry.authority.Hosts, 1)
	require.Equal(t, second, registry.authority.Hosts[0].Registration)
	require.Equal(t, registry.authority, store.durableAuthority())

	snapshot := registry.Snapshot()
	require.Len(t, snapshot.Daemons, 1)
	require.Equal(t, second, snapshot.Daemons[0].Registration)
	require.Empty(t, snapshot.Removed, "a live endpoint is never published as a tombstone")
	host, ok := snapshot.Find(second.Endpoint)
	require.True(t, ok)
	require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)
}

// TestRegistryMutableUpdatePolicyIncrementsGenerationOnce pins the policy
// update: a changed policy advances the registration generation exactly once and
// is the only thing that does; an unmatchable expectation is a definite conflict
// refused before the CAS.
func TestRegistryMutableUpdatePolicyIncrementsGenerationOnce(t *testing.T) {
	alpha := registration(t, "alpha.test", 1)
	store := newMembershipStore(pinnedRecord(alpha, poolPolicy()))
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)

	// Not found and a stale expectation are definite conflicts, refused before
	// the store is touched.
	authority := registry.authority
	before := registry.Snapshot()
	_, err := registry.UpdateHostPolicy(context.Background(), registration(t, "gamma.test", 3), policyWithTrust("other-trust"))
	require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
	stale := alpha
	stale.Generation++
	_, err = registry.UpdateHostPolicy(context.Background(), stale, policyWithTrust("other-trust"))
	require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
	require.Equal(t, 0, store.casCalls())
	require.Equal(t, authority, registry.authority)
	require.Equal(t, before, registry.Snapshot())

	updated, err := registry.UpdateHostPolicy(context.Background(), alpha, policyWithTrust("other-trust"))
	require.NoError(t, err)
	require.Equal(t, alpha.Generation+1, updated.Generation, "a policy change advances the generation exactly once")
	require.Equal(t, alpha.Incarnation, updated.Incarnation, "a policy change never touches the incarnation")
	require.Equal(t, 1, store.casCalls())
	require.Equal(t, uint64(2), registry.authority.Revision)
	require.Equal(t, registry.authority, store.durableAuthority())
	require.Equal(t, []ports.BrokerHostRecord{pinnedRecord(updated, policyWithTrust("other-trust"))}, store.durableAuthority().Hosts)

	host, ok := registry.Snapshot().Find(alpha.Endpoint)
	require.True(t, ok)
	require.Equal(t, updated, host.Registration)
	require.Equal(t, policyWithTrust("other-trust"), host.Policy)
	require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)
}

// TestRegistryMutableUpdateEqualPolicyPreservesIdentityAndVerifiesCAS proves an
// equal-policy update is an identity-preserving no-op that still CAS-verifies
// durable authority.
func TestRegistryMutableUpdateEqualPolicyPreservesIdentityAndVerifiesCAS(t *testing.T) {
	alpha := registration(t, "alpha.test", 1)
	store := newMembershipStore(pinnedRecord(alpha, poolPolicy()))
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	before := registry.Snapshot()

	got, err := registry.UpdateHostPolicy(context.Background(), alpha, poolPolicy())
	require.NoError(t, err)
	require.Equal(t, alpha, got, "an equal policy preserves the registration identity")
	require.Equal(t, 1, store.casCalls())
	require.Equal(t, uint64(2), registry.authority.Revision)
	require.Equal(t, registry.authority, store.durableAuthority())
	require.Equal(t, before, registry.Snapshot(), "an unchanged policy republishes nothing")

	// The no-op still verified the loaded revision: a revision moved behind the
	// registry turns the same call into a definite conflict.
	require.NoError(t, store.testStore.ReplaceHosts(registry.authority.Revision, []ports.BrokerHostRecord{pinnedRecord(alpha, poolPolicy())}))
	authority := registry.authority
	_, err = registry.UpdateHostPolicy(context.Background(), alpha, poolPolicy())
	require.ErrorIs(t, err, ports.ErrBrokerHostConflict)
	require.Equal(t, 2, store.casCalls())
	require.Equal(t, authority, registry.authority)
	require.Equal(t, before, registry.Snapshot())
}

// TestRegistryMutableUpdatePolicyOverflowRefused pins the generation series: a
// policy change at the last generation fails closed instead of wrapping to zero,
// which would re-trust an already queued observation.
func TestRegistryMutableUpdatePolicyOverflowRefused(t *testing.T) {
	alpha := registration(t, "alpha.test", 1)
	alpha.Generation = ^domain.RemoteGeneration(0)
	store := newMembershipStore(pinnedRecord(alpha, poolPolicy()))
	registry, _ := newMutableRegistry(t, store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	authority := registry.authority
	before := registry.Snapshot()

	_, err := registry.UpdateHostPolicy(context.Background(), alpha, policyWithTrust("other-trust"))
	require.ErrorIs(t, err, ports.ErrBrokerRevisionExhausted)
	require.Equal(t, 0, store.casCalls())
	require.Equal(t, authority, registry.authority)
	require.Equal(t, before, registry.Snapshot())
}

// TestRegistryMutableProbeCompletionFencedByChangedGeneration proves a
// completion whose registration was replaced by a policy update can never
// populate the new generation, whether it carries the retired registration or
// the current one with a retired attempt token.
func TestRegistryMutableProbeCompletionFencedByChangedGeneration(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	store := newMembershipStore()
	registry, _ := newMutableRegistry(t, store, clock, (&incarnationSequencer{}).generate)
	regA, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	retired := admitMembershipAttempt(t, registry, regA.Endpoint)
	regB, err := registry.UpdateHostPolicy(context.Background(), regA, policyWithTrust("other-trust"))
	require.NoError(t, err)
	require.Equal(t, regA.Generation+1, regB.Generation)
	require.ErrorIs(t, retired.ctx.Err(), context.Canceled, "replacing a registration retires its attempt")
	require.Zero(t, attemptFor(registry, regA.Endpoint))

	before := registry.Snapshot()
	for _, tc := range []struct {
		name   string
		result probeResult
	}{
		{"retired registration", probeResult{endpoint: regA.Endpoint, registration: regA, attempt: retired.token, at: clock.Now(), snapshot: staleObservation()}},
		{"current registration with a retired token", probeResult{endpoint: regA.Endpoint, registration: regB, attempt: retired.token, at: clock.Now(), snapshot: staleObservation()}},
		{"retired registration with the current token", probeResult{endpoint: regA.Endpoint, registration: regA, attempt: retired.token + 1, at: clock.Now(), snapshot: staleObservation()}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			registry.apply(tc.result)
			require.Equal(t, before, registry.Snapshot())
			host, ok := registry.Snapshot().Find(regA.Endpoint)
			require.True(t, ok)
			require.Equal(t, regB, host.Registration)
			require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)
			require.Empty(t, host.Identity)
			require.True(t, host.LastSuccess.IsZero())
			require.Zero(t, attemptFor(registry, regA.Endpoint), "a stale completion never admits an attempt")
		})
	}
}

// TestRegistryMutableStaleCompletionDoesNotClearLiveAttempt proves the fence on
// the live side: a completion for the retired generation is dropped while a
// current attempt is in flight, so it neither clears the in-flight slot nor
// publishes under the retired registration.
func TestRegistryMutableStaleCompletionDoesNotClearLiveAttempt(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	registry, _ := newMutableRegistry(t, newMembershipStore(), clock, (&incarnationSequencer{}).generate)
	regA, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	retired := admitMembershipAttempt(t, registry, regA.Endpoint)
	regB, err := registry.UpdateHostPolicy(context.Background(), regA, policyWithTrust("other-trust"))
	require.NoError(t, err)

	live := admitMembershipAttempt(t, registry, regB.Endpoint)
	before := registry.Snapshot()
	for _, result := range []probeResult{
		{endpoint: regB.Endpoint, registration: regA, attempt: retired.token, at: clock.Now(), snapshot: staleObservation()},
		{endpoint: regB.Endpoint, registration: regB, attempt: retired.token, at: clock.Now(), snapshot: staleObservation()},
		{endpoint: regB.Endpoint, registration: regA, attempt: live.token, at: clock.Now(), snapshot: staleObservation()},
	} {
		registry.apply(result)
	}
	require.Equal(t, live.token, attemptFor(registry, regB.Endpoint), "a stale completion must not clear the live attempt")
	require.NoError(t, live.ctx.Err(), "a stale completion must not retire the live attempt")
	require.Equal(t, before, registry.Snapshot())

	// The live attempt still completes normally and publishes its own result.
	registry.apply(probeResult{endpoint: regB.Endpoint, registration: regB, attempt: live.token, at: clock.Now(), snapshot: currentObservation()})
	require.ErrorIs(t, live.ctx.Err(), context.Canceled)
	host, ok := registry.Snapshot().Find(regB.Endpoint)
	require.True(t, ok)
	require.Equal(t, regB, host.Registration)
	require.Equal(t, ports.BrokerDaemonIdentity("current-daemon"), host.Identity)
	require.Equal(t, domain.RemoteAvailabilityReachable, host.Availability)
}

// TestRegistryMutableStaleProbeCannotReviveRemovedRegistration proves the
// remove/re-add fence: a completion for the retired incarnation is dropped after
// the endpoint was re-added under a fresh incarnation, and the re-add supersedes
// rather than revives the retirement.
func TestRegistryMutableStaleProbeCannotReviveRemovedRegistration(t *testing.T) {
	clock := newManualClock(time.Unix(100, 0))
	seq := &incarnationSequencer{}
	registry, _ := newMutableRegistry(t, newMembershipStore(), clock, seq.generate)
	regA, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	retired := admitMembershipAttempt(t, registry, regA.Endpoint)

	removed, err := registry.RemoveHost(context.Background(), regA)
	require.NoError(t, err)
	require.True(t, removed)
	require.ErrorIs(t, retired.ctx.Err(), context.Canceled)
	afterRemove := registry.Snapshot()
	require.Len(t, afterRemove.Removed, 1)
	require.Equal(t, regA, afterRemove.Removed[0].Registration)

	regB, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	require.NotEqual(t, regA.Incarnation, regB.Incarnation)
	before := registry.Snapshot()
	require.Len(t, before.Daemons, 1)
	require.Empty(t, before.Removed)

	registry.apply(probeResult{endpoint: regA.Endpoint, registration: regA, attempt: retired.token, at: clock.Now(), snapshot: staleObservation()})
	require.Equal(t, before, registry.Snapshot())
	host, ok := registry.Snapshot().Find(regA.Endpoint)
	require.True(t, ok)
	require.Equal(t, regB, host.Registration)
	require.Equal(t, domain.RemoteAvailabilityUnknown, host.Availability)
	require.Empty(t, host.Identity)
	require.Zero(t, attemptFor(registry, regA.Endpoint))
}

// TestRegistryMutablePolicyChangeRetiresInflightProbe drives the same fence
// through the production run loop: a policy change retires the in-flight probe
// promptly, a fresh attempt observes the new registration, and the retired
// answer can never surface as the new generation's observation.
func TestRegistryMutablePolicyChangeRetiresInflightProbe(t *testing.T) {
	registry, probe := newMutableRegistry(t, newMembershipStore(), newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	regA, err := registry.AddHost(context.Background(), "alpha.test", poolPolicy())
	require.NoError(t, err)
	startRegistry(t, registry)

	first := receiveCall(t, probe)
	require.Equal(t, regA, first.registration)

	regB, err := registry.UpdateHostPolicy(context.Background(), regA, policyWithTrust("other-trust"))
	require.NoError(t, err)
	require.Equal(t, regA.Generation+1, regB.Generation)
	select {
	case <-probe.canceled:
	case <-time.After(time.Second):
		t.Fatal("a policy change must retire the in-flight probe")
	}

	second := receiveCall(t, probe)
	require.Equal(t, regB, second.registration, "the replacement generation is observed")

	// The retired answer arrives after its attempt was retired; only the new
	// generation's answer may become the published observation.
	first.result <- probeAnswer{snapshot: staleObservation()}
	second.result <- probeAnswer{snapshot: currentObservation()}
	host := waitHost(t, registry, regB.Endpoint, func(host ports.BrokerDaemonObservation) bool {
		return host.Identity == ports.BrokerDaemonIdentity("current-daemon")
	})
	require.Equal(t, regB, host.Registration)
	require.Equal(t, domain.RemoteAvailabilityReachable, host.Availability)
	requireHostStableWithin(t, registry, regB.Endpoint, host, 50*time.Millisecond)
}

// membershipFixture seeds the state a failed mutation must leave untouched: two
// pinned hosts, one live probe attempt, and one retirement tombstone.
type membershipFixture struct {
	registry  *Registry
	store     *membershipStore
	alpha     domain.RemoteRegistration
	attempt   admittedAttempt
	authority ports.BrokerHosts
	before    ports.BrokerSnapshot
	fences    map[string]ports.BrokerHostTombstone
	casBefore int
}

func newMembershipFixture(t *testing.T) *membershipFixture {
	t.Helper()
	alpha := registration(t, "alpha.test", 1)
	fixture := &membershipFixture{alpha: alpha}
	fixture.store = newMembershipStore(pinnedRecord(alpha, poolPolicy()), pinnedRecord(registration(t, "beta.test", 2), poolPolicy()))
	registry, _ := newMutableRegistry(t, fixture.store, newManualClock(time.Unix(100, 0)), (&incarnationSequencer{}).generate)
	fixture.registry = registry
	fixture.attempt = admitMembershipAttempt(t, registry, alpha.Endpoint)

	// Retire beta so the fixture carries a live tombstone alongside the live
	// attempt at the moment the failure is injected.
	removed, err := registry.RemoveHost(context.Background(), registration(t, "beta.test", 2))
	require.NoError(t, err)
	require.True(t, removed)

	fixture.authority = registry.authority
	fixture.before = registry.Snapshot()
	fixture.fences = membershipFences(registry)
	fixture.casBefore = fixture.store.casCalls()
	require.Len(t, fixture.fences, 1)
	return fixture
}

// TestRegistryMutableStoreFailureLeavesStateUnchanged pins the commit ordering:
// a definite or indeterminate store failure returns before any projection,
// authority, attempt, or tombstone changes, and the store was genuinely reached.
func TestRegistryMutableStoreFailureLeavesStateUnchanged(t *testing.T) {
	definite := []struct {
		name string
		err  error
	}{
		{"conflict", ports.ErrBrokerHostConflict},
		{"plain failure", context.DeadlineExceeded},
	}
	indeterminate := errors.New("power loss")

	for _, failure := range definite {
		t.Run(failure.name, func(t *testing.T) {
			runMembershipStoreFailures(t, failure.err)
		})
	}
	t.Run("indeterminate outcome", func(t *testing.T) {
		runMembershipStoreFailures(t, ports.BrokerStoreOutcomeUnknownError{Err: indeterminate})
	})
}

func runMembershipStoreFailures(t *testing.T, storeErr error) {
	t.Helper()
	ops := []struct {
		name string
		run  func(fixture *membershipFixture) (bool, error)
	}{
		{"add new", func(f *membershipFixture) (bool, error) {
			_, err := f.registry.AddHost(context.Background(), "gamma.test", poolPolicy())
			return false, err
		}},
		{"add duplicate", func(f *membershipFixture) (bool, error) {
			_, err := f.registry.AddHost(context.Background(), f.alpha.Endpoint, poolPolicy())
			return false, err
		}},
		{"remove exact", func(f *membershipFixture) (bool, error) {
			return f.registry.RemoveHost(context.Background(), f.alpha)
		}},
		{"remove absent", func(f *membershipFixture) (bool, error) {
			return f.registry.RemoveHost(context.Background(), registration(t, "delta.test", 4))
		}},
		{"update policy", func(f *membershipFixture) (bool, error) {
			_, err := f.registry.UpdateHostPolicy(context.Background(), f.alpha, policyWithTrust("other-trust"))
			return false, err
		}},
		{"update equal policy", func(f *membershipFixture) (bool, error) {
			_, err := f.registry.UpdateHostPolicy(context.Background(), f.alpha, poolPolicy())
			return false, err
		}},
	}
	for _, op := range ops {
		t.Run(op.name, func(t *testing.T) {
			fixture := newMembershipFixture(t)
			fixture.store.failCAS(storeErr)

			removed, err := op.run(fixture)
			require.Error(t, err)
			// A commit that never happened is never reported as a removal, even
			// for the one operation whose success is a boolean: a caller that
			// trusted removed=true from a failed commit would drop live authority.
			require.False(t, removed, "a failed commit must never report a removal")
			var unknown ports.BrokerStoreOutcomeUnknownError
			if errors.As(storeErr, &unknown) {
				require.ErrorAs(t, err, &unknown, "an indeterminate outcome stays classifiable through the registry")
			} else {
				require.ErrorIs(t, err, storeErr)
				require.False(t, errors.As(err, &unknown), "a definite failure is not an unknown outcome")
			}

			require.Equal(t, fixture.authority, fixture.registry.authority, "authority must not move")
			require.Equal(t, fixture.before, fixture.registry.Snapshot(), "projection must not move")
			require.Equal(t, fixture.attempt.token, attemptFor(fixture.registry, fixture.alpha.Endpoint), "the in-flight attempt must survive")
			require.NoError(t, fixture.attempt.ctx.Err(), "a failed store call must not retire the attempt")
			require.Equal(t, fixture.fences, membershipFences(fixture.registry), "tombstones must not move")
			require.Equal(t, fixture.casBefore+1, fixture.store.casCalls(), "the mutation reached the store CAS")
		})
	}
}
